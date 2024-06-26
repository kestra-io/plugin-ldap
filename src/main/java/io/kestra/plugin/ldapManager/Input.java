package io.kestra.plugin.ldapManager;

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
    title = "Insert entries in LDAP.",
    description = "Creates a new entry, if allowed, for each line of provided LDIF files."
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            title = "Insert entries in LDAP server.",
            code = {
                "description: What your task is supposed to do and why.",
                "userDn: cn=admin,dc=orga,dc=en",
                "password: admin",
                "inputs:",
                "   - \"{{outputs.someTask.uri_of_ldif_formated_file}}\"",
                "hostname: 0.0.0.0",
                "port: 18060"
            }
        )
    }
)
public class Input extends LdapConnection implements RunnableTask<VoidOutput> {
    /**
     * INPUTS ----------------------------------------------------------------------------------------------------------------- //
    **/

    @Schema(
        title = "URI(s) of input file(s)",
        description = "List of URI(s) of file(s) containing LDIF formatted entries to input into LDAP."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private List<String> inputs;

    /**
     * CODE ------------------------------------------------------------------------------------------------------------------- //
    **/

    private Integer additionsDone;
    private Integer additionRequests;
    private List<Long> additionsTimes;
    /** The kestra logger (slf4j) for the task. */
    private static Logger logger = null;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        logger = runContext.logger();

        this.additionRequests = 0;
        this.additionsDone = 0;
        this.additionsTimes = new ArrayList<>();

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
        runContext.metric(Counter.of("additions.requested", this.additionRequests, "origin", "input"));
        runContext.metric(Counter.of("additions.done", this.additionsDone, "origin", "input"));

        if (!this.additionsTimes.isEmpty()) {
            Long meanTime = this.additionsTimes.stream().mapToLong(Long::longValue).sum() / this.additionsDone;
            runContext.metric(Timer.of("additions.meanTime", Duration.ofMillis(meanTime), "origin", "input"));
        }
        return new VoidOutput();
    }

    /**
     * Processes the entries from the LDIFReader and attempts to add them to the LDAP server.
     * @param reader : The LDIFReader containing the entries to be processed.
     * @param connection : The LDAPConnection to the LDAP server.
     */
    private void processEntries(LDIFReader reader, LDAPConnection connection) throws LDAPException, IOException, LDIFException {
        Entry entry;
        while ((entry = reader.readEntry()) != null) {
            long startTime = System.currentTimeMillis();
            try {
                LDAPResult result = connection.add(entry);
                if (result.getResultCode() == ResultCode.SUCCESS) {
                    this.additionsTimes.add(System.currentTimeMillis() - startTime);
                    this.additionsDone++;
                } else {
                    logger.warn("Cannot add entry {}, LDAP response : {}", entry.getDN(), result.getResultString());
                }
            } catch (LDAPException e) {
                logger.error("Error adding entry {}: {}", entry.getDN(), e.getResultString());
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
