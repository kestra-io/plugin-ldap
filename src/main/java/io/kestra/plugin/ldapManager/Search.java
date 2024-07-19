package io.kestra.plugin.ldapManager;

import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.ResultCode;
import com.unboundid.ldap.sdk.SearchRequest;
import com.unboundid.ldap.sdk.SearchResult;
import com.unboundid.ldap.sdk.SearchResultEntry;
import com.unboundid.ldap.sdk.SearchScope;

import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;

import java.io.File;

import java.net.URI;

import java.time.Duration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import lombok.Builder.Default;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import org.slf4j.Logger;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Retrieves entries in LDAP.",
    description = "Search and list entries based on a filter list for each base DN target."
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            title = """
                    Retrieve LDAP entries.
                    In this exemple, assuming that their is exactly one entry matching each of our filter,
                    the outputs of the task would be four entries in this order (since we search two times in the same baseDn) :
                    (dn, description, mail) of {melusine, metatron, melusine, metatron}.""",
            code = {
                "description: What your task is supposed to do and why.",
                "userDn: cn=admin,dc=orga,dc=en",
                "password: admin",
                "baseDn: ou=people,dc=orga,dc=en",
                "filter: (|(sn=melusine*)(sn=metatron*))",
                "attributes:",
                "  - description",
                "  - mail",
                "hostname: 0.0.0.0",
                "port: 15060"
            }
        )
    }
)
public class Search extends LdapConnection implements RunnableTask<Search.Output> {
    /**
     * INPUTS ----------------------------------------------------------------------------------------------------------------- //
    **/

    @Schema(
        title = "Filter",
        description = "Filter for the search in the LDAP."
    )
    @PluginProperty(dynamic = true)
    @Default
    private String filter = "(objectclass=*)";

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
    @PluginProperty(dynamic = true)
    @Default
    private List<String> attributes = Arrays.asList(SearchRequest.ALL_USER_ATTRIBUTES);

    @Schema(
        title = "Base DN",
        description = "Base DN target in the LDAP."
    )
    @PluginProperty(dynamic = true)
    @Default
    private String baseDn = "ou=system";

    @Schema(
        title = "SUB",
        description = """
            Search scope of the filter :
            BASE -- Indicates that only the entry specified by the base DN should be considered.
            ONE -- Indicates that only entries that are immediate subordinates of the entry specified by the base DN (but not the base entry itself) should be considered.
            SUB -- Indicates that the base entry itself and any subordinate entries (to any depth) should be considered.
            SUBORDINATE_SUBTREE -- Indicates that any subordinate entries (to any depth) below the entry specified by the base DN should be considered, but the base entry itself should not be considered, as described in draft-sermersheim-ldap-subordinate-scope."""
    )
    @PluginProperty(dynamic = true)
    @Default
    private SearchScope sub = SearchScope.SUB;

    /**
     * OUTPUTS ------------------------------------------------------------------------------------------------------------------- //
    **/

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Result file URI",
            description = "A file that contains zero or more matching queries as LDIF formatted strings."
        )
        private final URI uri;
    }

    /**
     * CODE ------------------------------------------------------------------------------------------------------------------- //
    **/

    @Override
    public Search.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        List<String> results = new ArrayList<>();
        URI storedResults = null;

        Integer entriesFound = 0;
        Long searchTime = 0L;

        try (LDAPConnection connection = this.getLdapConnection(runContext)) {
            filter = filter.replaceAll("\n\\s*", "");
            SearchRequest request = new SearchRequest(
                baseDn, sub, filter,
                attributes.contains("0.0") ? SearchRequest.REQUEST_ATTRS_DEFAULT : attributes.toArray(new String[0])
            );
            Long startTime = System.currentTimeMillis();
            SearchResult result = connection.search(request);
            if (result.getResultCode() == ResultCode.SUCCESS) {
                searchTime = System.currentTimeMillis() - startTime;
                for (SearchResultEntry entry : result.getSearchEntries()) {
                    results.add(entry.toLDIFString());
                    entriesFound++;
                }
                File tempfile = runContext.workingDir().createTempFile(String.join("\n", results).getBytes() ,".ldif").toFile();
                storedResults = runContext.storage().putFile(tempfile);
            } else {
                logger.warn("Search failed with filter {}, LDAP response : {}", filter, result.getResultString());
            }
        } catch (LDAPException e) {
            logger.error("LDAP error: {}", e.getResultString());
            throw e;
        }
        runContext.metric(Counter.of("entries.found", entriesFound, "origin", "retrieve"));
        runContext.metric(Timer.of("searche.meanTime", Duration.ofMillis(searchTime), "origin", "retrieve"));

        return Output.builder()
            .uri(storedResults)
            .build();
    }
}
