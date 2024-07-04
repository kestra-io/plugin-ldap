package io.kestra.plugin.ldapManager;

import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPResult;
import com.unboundid.ldap.sdk.ResultCode;
import com.unboundid.ldif.LDIFChangeRecord;
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

import java.util.List;
import java.util.ArrayList;

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
    title = "Modify entries in LDAP.",
    description = "Modify, Delete or Add attributes or DNs following LDIF changeType fields of each entries provided."
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            title = "Modify entries in LDAP server.",
            code = {
                "description: What your task is supposed to do and why.",
                "userDn: cn=admin,dc=orga,dc=en",
                "password: admin",
                "inputs:",
                "   - \"{{outputs.someTask.uri_of_ldif_change_record_formated_file}}\"",
                "hostname: 0.0.0.0",
                "port: 18060"
            }
        )
    }
)
public class Modify extends LdapConnection implements RunnableTask<VoidOutput> {
    /**
     * INPUTS ----------------------------------------------------------------------------------------------------------------- //
    **/

    @Schema(
        title = "URI(s) of input file(s)",
        description = "List of URI(s) of file(s) containing LDIF formatted entries to modify into LDAP. Entries must provide a changeType field."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private List<String> inputs;

    /**
     * CODE ------------------------------------------------------------------------------------------------------------------- //
    **/

    /** The kestra logger (slf4j) for the task. */
    private static Logger logger = null;

    private Integer modificationsDone;
    private Integer modificationRequests;
    private List<Long> modificationsTimes;

    public VoidOutput run(RunContext runContext) throws Exception {
        logger = runContext.logger();
        this.modificationRequests = 0;
        this.modificationsDone = 0;
        this.modificationsTimes = new ArrayList<>();

        try (LDAPConnection connection = this.getLdapConnection()) {
            for (String inputUri : inputs) {
                URI resolvedUri = resolveKestraUri(inputUri, runContext);
                if (resolvedUri == null) continue;

                try (LDIFReader reader = new LDIFReader(runContext.storage().getFile(resolvedUri))) {
                    processEntries(reader, connection);
                } catch (IOException | LDIFException e) {
                    logger.error("Error reading LDIF file: {}", e.getMessage());
                }
            }
        } catch (LDAPException e) {
            logger.error("LDAP error: {}", e.getResultString());
        }
        runContext.metric(Counter.of("modifications.requested", this.modificationRequests, "origin", "input"));
        runContext.metric(Counter.of("modifications.done", this.modificationsDone, "origin", "input"));

        if (!this.modificationsTimes.isEmpty()) {
            Long meanTime = this.modificationsTimes.stream().mapToLong(Long::longValue).sum() / this.modificationsDone;
            runContext.metric(Timer.of("modifications.meanTime", Duration.ofMillis(meanTime), "origin", "input"));
        }
        return new VoidOutput();
    }

    /**
     * Processes the entries from the LDIFReader and attempts to modify them in the LDAP server.
     * @param reader : The LDIFReader containing the entries to be processed.
     * @param connection : The LDAPConnection to the LDAP server.
     */
    private void processEntries(LDIFReader reader, LDAPConnection connection) throws LDAPException, IOException, LDIFException {
        while (true) {
            LDIFChangeRecord entry = null;
            try {
                entry = reader.readChangeRecord();
            } catch (LDIFException e) {
                logger.error("Cannot read entry: {}", e.getDataLines());
                continue;
            }

            if (entry == null) {
                break;
            }

            modificationRequests++;
            long startTime = System.currentTimeMillis();
            try {
                LDAPResult result = entry.processChange(connection);
                if (result.getResultCode() == ResultCode.SUCCESS) {
                    modificationsTimes.add(System.currentTimeMillis() - startTime);
                    modificationsDone++;
                } else {
                    logger.warn("Cannot modify entry: {}, LDAP response: {}", entry.toLDIF(), result.getResultString());
                }
            } catch (LDAPException e) {
                logger.error("Error modifying entry {}: {}", entry.getDN(), e.getResultString());
            }
        }
    }

    /**
     * Resolves a Kestra pebble or literral URI to a valid Kestra URI.
     * @param file : The URI or pebble to be resolved.
     * @param runContext : The context of the run.
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
