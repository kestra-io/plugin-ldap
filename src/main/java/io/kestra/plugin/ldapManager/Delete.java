package io.kestra.plugin.ldapManager;

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
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

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
    title = "Remove entries in LDAP.",
    description = "Remove entries based on a targeted DN list."
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            code = {
                "description: What your task is supposed to do and why.",
                "userDn: cn=admin,dc=orga,dc=fr",
                "password: admin",
                "inputs:",
                "   - \"{{outputs.someTask.uri_of_ldif_formated_file}}\"",
                "hostname: 0.0.0.0",
                "port: 15060"
            }
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

    private Integer deletionsDone;
    private Integer deletionRequests;
    private List<Long> deletionsTimes;
    /** The kestra logger (slf4j) for the task. */
    private static Logger logger = null;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        logger = runContext.logger();
        this.deletionRequests = 0;
        this.deletionsDone = 0;
        this.deletionsTimes = new ArrayList<>();

        try (LDAPConnection connection = this.getLdapConnection()) {
            for (String file : inputs) {
                URI resolvedUri = resolveKestraUri(file, runContext);
                if (resolvedUri == null) continue;
                try (LDIFReader reader = new LDIFReader(runContext.storage().getFile(resolvedUri))) {
                    processEntries(reader, connection);
                }
            }
        } catch (LDAPException e) {
            logger.error("LDAP error: {}", e.getMessage());
        }

        runContext.metric(Counter.of("deletions.requested", this.deletionRequests, "origin", "delete"));
        runContext.metric(Counter.of("deletions.done", this.deletionsDone, "origin", "delete"));
        if (!this.deletionsTimes.isEmpty()) {
            Long meanTime = this.deletionsTimes.stream().mapToLong(Long::longValue).sum() / this.deletionsDone;
            runContext.metric(Timer.of("deletions.meanTime", Duration.ofMillis(meanTime), "origin", "delete"));
        }
        return new VoidOutput();
    }

    /**
     * Processes the entries from the LDIFReader and attempts to delete them from the LDAP server.
     * @param reader : The LDIFReader containing the entries to be processed.
     * @param connection : The LDAPConnection to the LDAP server.
     */
    private void processEntries(LDIFReader reader, LDAPConnection connection) throws LDAPException, IOException, LDIFException {
        Entry entry;
        while ((entry = reader.readEntry()) != null) {
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
                    logger.warn("Cannot remove entry '{}', LDAP response : {}", baseDn, result.getResultString());
                }
            } catch (LDAPException e) {
                logger.error("Error deleting DN '{}': {}", baseDn, e.getMessage());
            }
        }
    }

    /**
     * Resolves a Kestra pebble or literral URI to a valid Kestra URI.
     * @param file The URI or pebble to be resolved.
     * @param runContext The context of the run.
     * @return The resolved URI, or null if an error occurs.
     */
    private URI resolveKestraUri(String file, RunContext runContext) {
        try {
            return URI.create(runContext.render(file));
        } catch (Exception e) {
            logger.error("Invalid URI syntax: {}", e.getMessage());
            return null;
        }
    }
}
