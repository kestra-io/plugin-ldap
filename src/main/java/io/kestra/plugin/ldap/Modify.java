package io.kestra.plugin.ldap;

import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPResult;
import com.unboundid.ldap.sdk.ResultCode;
import com.unboundid.ldif.LDIFChangeRecord;
import com.unboundid.ldif.LDIFException;
import com.unboundid.ldif.LDIFReader;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.Builder.Default;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

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
            full = true,
            code = """
                id: ldap_modify
                namespace: company.team

                tasks:
                  - id: modify
                    type: io.kestra.plugin.ldap.Modify
                    userDn: cn=admin,dc=orga,dc=en
                    password: admin
                    inputs:
                       - "{{ outputs.some_task.uri_of_ldif_change_record_formated_file }}"
                    hostname: 0.0.0.0
                    port: 18060
                """
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

    @NotNull
    private Property<List<String>> inputs;

    /**
     * CODE ------------------------------------------------------------------------------------------------------------------- //
    **/

    /** The kestra logger (slf4j) for the task. */
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    @Default
    private Logger logger = null;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    @Default
    private Integer modificationsDone = 0;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    @Default
    private Integer modificationRequests = 0;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    @Default
    private List<Long> modificationsTimes = new ArrayList<>();

    public VoidOutput run(RunContext runContext) throws Exception {
        this.logger = runContext.logger();

        try (LDAPConnection connection = this.getLdapConnection(runContext)) {
            for (String inputUri : runContext.render(this.inputs).asList(String.class)) {
                try (LDIFReader reader = Utils.getLDIFReaderFromUri(inputUri, runContext)) {
                    processEntries(reader, connection);
                } catch (Exception e) {
                    this.logger.error("Error reading LDIF file {} : {}", inputUri, e.getMessage());
                }
            }
        } catch (LDAPException e) {
            this.logger.error("LDAP error: {}", e.getResultString());
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
                this.logger.error("Cannot read entry: {}", e.getDataLines());
                continue;
            }

            if (entry == null) {
                break;
            }

            this.modificationRequests++;
            long startTime = System.currentTimeMillis();
            try {
                LDAPResult result = entry.processChange(connection);
                if (result.getResultCode() == ResultCode.SUCCESS) {
                    this.modificationsTimes.add(System.currentTimeMillis() - startTime);
                    this.modificationsDone++;
                } else {
                    this.logger.warn("Cannot modify entry: {}, LDAP response: {}", entry.toLDIF(), result.getResultString());
                }
            } catch (LDAPException e) {
                this.logger.error("Error modifying entry {}: {}", entry.getDN(), e.getResultString());
            }
        }
    }
}
