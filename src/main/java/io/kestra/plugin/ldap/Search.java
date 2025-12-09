package io.kestra.plugin.ldap;

import com.unboundid.asn1.ASN1OctetString;
import com.unboundid.ldap.sdk.*;
import com.unboundid.ldap.sdk.controls.SimplePagedResultsControl;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.Builder.Default;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.File;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Retrieve entries from an LDAP server.",
    description = "Search and list entries based on a filter list for each base DN target."
)
@Plugin(
    examples = {
        @Example(
            title = """
                Retrieve LDAP entries.
                In this example, assuming that there is exactly one entry matching each of our filters,
                the output of the task would be four entries in this order (since we search twice in the same base DN):
                (dn, description, mail) of {melusine, metatron, melusine, metatron}.
                """,
            full = true,
            code = """
                id: ldap_search
                namespace: company.team

                tasks:
                  - id: search
                    type: io.kestra.plugin.ldap.Search
                    userDn: cn=admin,dc=orga,dc=en
                    password: admin
                    baseDn: ou=people,dc=orga,dc=en
                    filter: (|(sn=melusine*)(sn=metatron*))
                    attributes:
                      - description
                      - mail
                    hostname: 0.0.0.0
                    port: 15060
                """
        )
    },
    metrics = {
        @Metric(
            name = "entries.found",
            type = Counter.TYPE,
            description = "The total number of LDAP entries found by the search."
        ),
        @Metric(
            name = "search.mean.time",
            type = Timer.TYPE,
            description = "The average time taken to complete the LDAP search."
        )
    }
)
public class Search extends LdapConnection implements RunnableTask<Search.Output> {

    @Schema(
        title = "Filter",
        description = "Filter for the search in the LDAP."
    )
    @Default
    private Property<String> filter = Property.ofValue("(objectclass=*)");

    @Schema(
        title = "Attributes",
        description = """
            Specific attributes to retrieve from the filtered entries. Retrieves all attributes by default.
            Sepcial attributes may be specified :
            "+"   -> OPERATIONAL_ATTRIBUTES
            "1.1" -> NO_ATTRIBUTES
            "0.0" -> ALL_ATTRIBUTES_EXCEPT_OPERATIONAL
                `--> This special attribute canno't be combined with other attributes and the search will ignore everything else.
            """
    )
    @Default
    private Property<List<String>> attributes = Property.ofValue(Collections.singletonList(SearchRequest.ALL_USER_ATTRIBUTES));

    @Schema(
        title = "Base DN",
        description = "Base DN target in the LDAP."
    )
    @Default
    private Property<String> baseDn = Property.ofValue("ou=system");

    @Schema(
        title = "SUB",
        description = """
            Search scope of the filter :
            BASE -- Indicates that only the entry specified by the base DN should be considered.
            ONE -- Indicates that only entries that are immediate subordinates of the entry specified by the base DN (but not the base entry itself) should be considered.
            SUB -- Indicates that the base entry itself and any subordinate entries (to any depth) should be considered.
            SUBORDINATE_SUBTREE -- Indicates that any subordinate entries (to any depth) below the entry specified by the base DN should be considered, but the base entry itself should not be considered, as described in draft-sermersheim-ldap-subordinate-scope."""
    )
    @Default
    @PluginProperty
    private SearchScope sub = SearchScope.SUB;

    @Schema(
        title = "Size limit",
        description = """
            Maximum number of entries to return.

            Use this when you want to LIMIT the total number of results on purpose (sampling / safety).
            If more entries match the filter, only the first N will be returned.

            - Typical use case: you only need a subset (e.g., first 200 users).
            - Consequence: results can be TRUNCATED.
            """
    )
    private Property<Integer> sizeLimit;

    @Schema(
        title = "Page size",
        description = """
            Enable LDAP paging (RFC2696) and fetch results by chunks of this size.

            Use this when you want to RETRIEVE ALL matching entries safely, even if there are more
            than the server limit (often ~1000). The task will perform multiple paged searches until
            all entries are collected.

            - Typical use case: full export / sync of an LDAP tree.
            - Consequence: no truncation, but potentially more requests and longer execution time.
            """
    )
    private Property<Integer> pageSize;

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Result file URI",
            description = "A file that contains zero or more matching queries as LDIF formatted strings."
        )
        private final URI uri;
    }

    @Override
    public Search.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        List<String> results = new ArrayList<>();
        URI storedResults = null;

        String rFilter = runContext.render(this.filter).as(String.class).orElse("(objectclass=*)");
        List<String> rAttributes = runContext.render(this.attributes).asList(String.class);
        String rBaseDn = runContext.render(this.baseDn).as(String.class).orElse("ou=system");
        Integer rSizeLimit = runContext.render(this.sizeLimit).as(Integer.class).orElse(null);
        Integer rPageSize = runContext.render(this.pageSize).as(Integer.class).orElse(null);

        int entriesFound = 0;
        long searchTime;

        try (LDAPConnection connection = this.getLdapConnection(runContext)) {
            rFilter = rFilter.replaceAll("\n\\s*", "");

            SearchRequest request = new SearchRequest(
                rBaseDn,
                sub,
                rFilter,
                rAttributes.contains("0.0")
                    ? SearchRequest.REQUEST_ATTRS_DEFAULT
                    : rAttributes.toArray(new String[0])
            );

            if (rSizeLimit != null) {
                request.setSizeLimit(rSizeLimit);
            }

            boolean acceptSizeLimitExceeded =
                (rSizeLimit != null) || (rPageSize != null && rPageSize > 0);

            long startTime = System.currentTimeMillis();

            List<SearchResultEntry> entries = searchWithOptionalPaging(
                connection,
                request,
                logger,
                rPageSize,
                acceptSizeLimitExceeded
            );

            searchTime = System.currentTimeMillis() - startTime;

            for (SearchResultEntry entry : entries) {
                results.add(entry.toLDIFString());
                entriesFound++;
            }

            File tempfile = runContext.workingDir()
                .createTempFile(String.join("\n", results).getBytes(), ".ldif")
                .toFile();

            storedResults = runContext.storage().putFile(tempfile);

        } catch (LDAPException e) {
            logger.error("LDAP error: {}", e.getResultString());
            throw e;
        }

        runContext.metric(Counter.of("entries.found", entriesFound, "origin", "Search"));
        runContext.metric(Timer.of("search.mean.time", Duration.ofMillis(searchTime), "origin", "Search"));

        return Output.builder()
            .uri(storedResults)
            .build();
    }

    private SearchResult executeSearchAcceptingSizeLimit(
        LDAPConnection connection,
        SearchRequest request,
        Logger logger,
        boolean acceptSizeLimitExceeded
    ) throws LDAPException {
        try {
            return connection.search(request);
        } catch (LDAPSearchException e) {
            // Some servers return SIZE_LIMIT_EXCEEDED even when respecting limits
            if (acceptSizeLimitExceeded && e.getResultCode() == ResultCode.SIZE_LIMIT_EXCEEDED) {
                logger.info("LDAP size limit exceeded; treating as partial success.");
                return e.getSearchResult();
            }
            throw e;
        }
    }

    private List<SearchResultEntry> searchWithOptionalPaging(
        LDAPConnection connection,
        SearchRequest request,
        Logger logger,
        Integer pageSize,
        boolean acceptSizeLimitExceeded
    ) throws LDAPException {

        List<SearchResultEntry> entries = new ArrayList<>();

        if (pageSize == null || pageSize <= 0) {
            SearchResult result = executeSearchAcceptingSizeLimit(
                connection, request, logger, acceptSizeLimitExceeded
            );

            if (result.getResultCode() == ResultCode.SUCCESS ||
                (acceptSizeLimitExceeded && result.getResultCode() == ResultCode.SIZE_LIMIT_EXCEEDED)) {
                entries.addAll(result.getSearchEntries());
            }

            return entries;
        }

        // RFC2696 paging loop
        ASN1OctetString cookie = null;
        do {
            request.setControls(new SimplePagedResultsControl(pageSize, cookie));

            SearchResult pageResult = executeSearchAcceptingSizeLimit(
                connection, request, logger, acceptSizeLimitExceeded
            );

            if (pageResult.getResultCode() != ResultCode.SUCCESS &&
                pageResult.getResultCode() != ResultCode.SIZE_LIMIT_EXCEEDED) {
                logger.warn("Search failed, LDAP response : {}", pageResult.getResultString());
                break;
            }

            entries.addAll(pageResult.getSearchEntries());

            SimplePagedResultsControl responseControl = SimplePagedResultsControl.get(pageResult);
            cookie = responseControl != null ? responseControl.getCookie() : null;

        } while (cookie != null && cookie.getValueLength() > 0);

        return entries;
    }
}
