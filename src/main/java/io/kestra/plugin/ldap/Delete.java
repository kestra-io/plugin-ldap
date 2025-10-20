package io.kestra.plugin.ldap;

import com.unboundid.ldap.sdk.DeleteRequest;
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
    title = "Remove entries from an LDAP server.",
    description = "Remove entries based on a targeted DN list."
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            full = true,
            code = """
                id: ldap_delete
                namespace: company.team
                
                tasks:
                  - id: delete
                    type: io.kestra.plugin.ldap.Delete
                    description: What your task is supposed to do and why.
                    userDn: cn=admin,dc=orga,dc=fr
                    password: admin
                    inputs:
                       - "{{ outputs.some_task.uri_of_ldif_formated_file }}"
                    hostname: 0.0.0.0
                    port: 15060
                """
        )
    },
    metrics = {
    @Metric(
        name = "deletions.requested",
        type = Counter.TYPE,
        description = "The total number of deletion requests made."
    ),
    @Metric(
        name = "deletions.done",
        type = Counter.TYPE,
        description = "The total number of successful deletions from the LDAP server."
    ),
    @Metric(
        name = "deletions.mean.time",
        type = Timer.TYPE,
        description = "The mean duration of LDAP deletions in milliseconds."
    )
}
)
public class Delete extends LdapConnection implements RunnableTask<VoidOutput> {
    /**
     * INPUTS ----------------------------------------------------------------------------------------------------------------- //
    **/

    @Schema(
        title = "File(s) URI(s) containing Distinguished-Name(s)",
        description = "Targeted DN(s) in the LDAP."
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
    private Integer deletionsDone = 0;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    @Default
    private Integer deletionRequests = 0;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    @Default
    private List<Long> deletionsTimes = new ArrayList<>();
    /** The kestra logger (slf4j) for the task. */
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    @Default
    private Logger logger = null;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        this.logger = runContext.logger();

        try (LDAPConnection connection = this.getLdapConnection(runContext)) {
            for (String file : inputs) {
                try (LDIFReader reader = Utils.getLDIFReaderFromUri(file, runContext)) {
                    processEntries(reader, connection);
                } catch (Exception e) {
                    this.logger.warn("Unable to process file {} completly : {}", file, e.getMessage());
                }
            }
        } catch (LDAPException e_l) {
            this.logger.error("LDAP error: {}", e_l.getMessage());
        }

        runContext.metric(Counter.of("deletions.requested", this.deletionRequests, "origin", "delete"));
        runContext.metric(Counter.of("deletions.done", this.deletionsDone, "origin", "delete"));
        if (!this.deletionsTimes.isEmpty()) {
            Long meanTime = this.deletionsTimes.stream().mapToLong(Long::longValue).sum() / this.deletionsDone;
            runContext.metric(Timer.of("deletions.mean.time", Duration.ofMillis(meanTime), "origin", "delete"));
        }
        return new VoidOutput();
    }

    /**
     * Processes the entries from the LDIFReader and attempts to delete them from the LDAP server.
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
            this.deletionRequests++;
            String baseDn = entry.getDN();
            DeleteRequest deleteRequest = new DeleteRequest(baseDn);
            Long startMillis = System.currentTimeMillis();
            try {
                LDAPResult result = connection.delete(deleteRequest);
                if (result.getResultCode() == ResultCode.SUCCESS) {
                    this.deletionsTimes.add(System.currentTimeMillis() - startMillis);
                    this.deletionsDone++;
                } else {
                    this.logger.warn("Cannot remove entry '{}', LDAP response : {}", baseDn, result.getResultString());
                }
            } catch (LDAPException e) {
                this.logger.error("Error deleting DN '{}': {}", baseDn, e.getMessage());
            }
        }
    }
}
