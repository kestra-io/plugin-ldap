package io.kestra.plugin.ldap;

import com.unboundid.ldap.sdk.Entry;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPResult;
import com.unboundid.ldap.sdk.ResultCode;
import com.unboundid.ldif.LDIFException;
import com.unboundid.ldif.LDIFReader;

import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

import java.io.IOException;

import java.time.Duration;

import java.util.ArrayList;
import java.util.List;

import lombok.AccessLevel;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import org.slf4j.Logger;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Add LDIF entries to LDAP",
    description = "Reads LDIF records from one or more URIs and issues LDAP add requests for each entry. Operations are independent (non-transactional) and rely on server ACLs; failures on a given entry are logged and processing continues."
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            title = "Insert entries in LDAP server.",
            full = true,
            code = """
                id: ldap_add
                namespace: company.team

                tasks:
                  - id: add
                    type: io.kestra.plugin.ldap.Add
                    description: What your task is supposed to do and why.
                    userDn: cn=admin,dc=orga,dc=en
                    password: admin
                    inputs:
                       - "{{outputs.someTask.uri_of_ldif_formated_file}}"
                    hostname: 0.0.0.0
                    port: 18060
                """
        )
    },
    metrics = {
    @Metric(
        name = "additions.requested",
        type = Counter.TYPE,
        description = "The total number of LDIF addition requests made."
    ),
    @Metric(
        name = "additions.done",
        type = Counter.TYPE,
        description = "The total number of successful additions to the LDAP server."
    ),
    @Metric(
        name = "additions.mean.time",
        type = Timer.TYPE,
        description = "The mean duration of LDAP additions in milliseconds."
    )
}

)
public class Add extends LdapConnection implements RunnableTask<VoidOutput> {
    /**
     * INPUTS ----------------------------------------------------------------------------------------------------------------- //
    **/

    @Schema(
        title = "LDIF input URIs",
        description = "One or more URIs to LDIF files in internal storage; every entry is attempted separately and errors are logged without stopping the task."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private List<String> inputs;

    /**
     * CODE ------------------------------------------------------------------------------------------------------------------- //
    **/

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    @Default
    private Integer additionsDone = 0;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    @Default
    private Integer additionRequests = 0;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    @Default
    private List<Long> additionsTimes = new ArrayList<>();
    /** The kestra logger (slf4j) for the task. */
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    @Default
    private Logger logger = null;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        this.logger = runContext.logger();

        try (LDAPConnection connection = this.getLdapConnection(runContext)) {
            for (String inputUri : inputs) {
                try (LDIFReader reader = Utils.getLDIFReaderFromUri(inputUri, runContext)) {
                    processEntries(reader, connection);
                } catch (Exception e) {
                    this.logger.error("Error reading LDIF file {} : {}", inputUri, e.getMessage());
                }
            }
        } catch (LDAPException e) {
            this.logger.error("LDAP error: {}", e.getResultString());
        }
        runContext.metric(Counter.of("additions.requested", this.additionRequests, "origin", "Add"));
        runContext.metric(Counter.of("additions.done", this.additionsDone, "origin", "Add"));

        if (!this.additionsTimes.isEmpty()) {
            Long meanTime = this.additionsTimes.stream().mapToLong(Long::longValue).sum() / this.additionsDone;
            runContext.metric(Timer.of("additions.mean.time", Duration.ofMillis(meanTime), "origin", "Add"));
        }
        return new VoidOutput();
    }

    /**
     * Processes the entries from the LDIFReader and attempts to add them to the LDAP server.
     * @param reader : The LDIFReader containing the entries to be processed.
     * @param connection : The LDAPConnection to the LDAP server.
     */
    private void processEntries(LDIFReader reader, LDAPConnection connection) throws IOException {
        while (true) {
            Entry entry = null;
            try {
                entry = reader.readEntry();
            } catch (LDIFException e) {
                this.logger.error("Cannot read entry: {}", e.getDataLines());
                continue;
            }
            if (entry == null) break;

            this.additionRequests++;
            Long startTime = System.currentTimeMillis();
            try {
                LDAPResult result = connection.add(entry);
                if (result.getResultCode() == ResultCode.SUCCESS) {
                    this.additionsTimes.add(System.currentTimeMillis() - startTime);
                    this.additionsDone++;
                } else {
                    this.logger.warn("Cannot add entry: {}, LDAP response: {}", entry.toLDIF(), result.getResultString());
                }
            } catch (LDAPException e) {
                this.logger.error("Error adding entry {}: {}", entry.getDN(), e.getResultString());
            }
        }
    }


}
