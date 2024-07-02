package io.kestra.plugin.ldapManager;

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonSystemBuilder;

import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.ChangeType;
import com.unboundid.ldap.sdk.Entry;
import com.unboundid.ldap.sdk.Modification;
import com.unboundid.ldif.LDIFChangeRecord;
import com.unboundid.ldif.LDIFException;
import com.unboundid.ldif.LDIFModifyChangeRecord;
import com.unboundid.ldif.LDIFReader;
import com.unboundid.ldif.LDIFRecord;

import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import java.net.URI;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
    title = "Ionise LDIF entries.",
    description = "Transform .ldif files to .ion ones."
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            title = "Make ION entries from LDIF ones.",
            code = { "description: What your task is supposed to do and why.",
            "inputs:",
            " - {{some_uri}}",
            " - {{some_other_uri}}"}
            )
    }
)
public class LdifToIon extends Task implements RunnableTask<LdifToIon.Output> {
    /**
     * INPUTS ------------------------------------------------------------------------------------------------------------------- //
    **/

    @Schema(
        title = "URI(s) of file(s) containing LDIF entries.",
        description = "LDIF file(s) to transform to ION formated ones."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private List<String> inputs;

    /**
     * OUTPUTS ------------------------------------------------------------------------------------------------------------------- //
    **/

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "URI(s) of ION translated file(s)."
        )
        private final List<URI> urisList;
    }

    /**
     * CODE ------------------------------------------------------------------------------------------------------------------- //
    **/

    private Integer count;
    private Integer found;
    /** The kestra logger (slf4j) for the task. */
    private static Logger logger = null;

    @Override
    public LdifToIon.Output run(RunContext runContext) throws Exception {
        logger = runContext.logger();
        List<URI> storedResults = new ArrayList<>();
        this.count = 0;
        this.found = 0;

        for (String path : this.inputs) {
            try {
                storedResults.add(transformLdifToIon(runContext.render(path), runContext));
            } catch (IOException | LDIFException e) {
                logger.error(e.getMessage());
            }
        }
        if (!this.inputs.isEmpty() && storedResults.isEmpty()) {
            throw new Exception("Not a single file has been translated.");
        }
        runContext.metric(Counter.of("entries.found", this.found, "origin", "LdifToIon"));
        runContext.metric(Counter.of("entries.translated", this.count, "origin", "LdifToIon"));

        return Output.builder()
            .urisList(storedResults)
            .build();
    }

    /**
     * Transforms a given LDIF file to ION format.
     * @param ldifFilePath : The path to the LDIF file to be transformed.
     * @param runContext : The context of the run.
     * @return URI of the transformed ION file.
     */
    private URI transformLdifToIon(String ldifFilePath, RunContext runContext) throws IOException, LDIFException {
        IonSystem ionSystem = IonSystemBuilder.standard().build();
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             IonWriter ionWriter = ionSystem.newTextWriter(byteArrayOutputStream);
             InputStream ldifInputStream = runContext.storage().getFile(URI.create(ldifFilePath));
             LDIFReader ldifReader = new LDIFReader(ldifInputStream)) {

            processEntries(ldifReader, ionWriter);
            ionWriter.finish();
            String resultContent = byteArrayOutputStream.toString().replace("} {", "}\n{");
            File tempFile = runContext.workingDir().createTempFile(resultContent.getBytes(), ".ion").toFile();
            return runContext.storage().putFile(tempFile);
        }
    }

    private void processEntries(LDIFReader ldifReader, IonWriter ionWriter) throws IOException {
        while (true) {
            String[] record = null;
            Entry entry = null;
            LDIFChangeRecord changeRecord = null;
            try {
                LDIFRecord ldifRecord = ldifReader.readLDIFRecord();
                if (ldifRecord == null) break;
                record = ldifRecord.toLDIF();
            } catch (LDIFException e) {
                logger.warn("Canno't read LDIF entry {}, {}", e.getDataLines(), e.getMessage());
                continue;
            } catch (IOException e) {
                logger.error(e.getMessage());
                continue;
            }
            try {
                changeRecord = LDIFReader.decodeChangeRecord(record);
            } catch (LDIFException e_change) {
                try {
                    entry = LDIFReader.decodeEntry(record);
                } catch (LDIFException e_entry) {
                    logger.warn("Translation failed, not an Entry nor a ChangeRecord : {}, {}, {}", record.toString(), e_entry.getMessage(), e_change.getMessage());
                }
            }
            if (entry == null && changeRecord == null) break;
            this.found++;
            if (entry != null) {
                writeIonEntry(ionWriter, entry);
            } else {
                writeIonChangeRecord(ionWriter, changeRecord);
            }
            this.count++;
        }
    }

    /**
     * Writes an LDIF entry to the ION writer.
     * @param ionWriter : The ION writer to write the entry to.
     * @param entry : The LDIF entry to be written.
     */
    private void writeIonEntry(IonWriter ionWriter, Entry entry) throws IOException {
        ionWriter.stepIn(IonType.STRUCT);

        ionWriter.setFieldName("dn");
        ionWriter.writeString(entry.getDN());

        ionWriter.setFieldName("attributes");
        writeAttributes(ionWriter, entry.getAttributes());

        ionWriter.stepOut();
    }

    /**
     * Writes an LDIF entry attributes to the ION writer.
     * @param ionWriter : The ION writer to write the entry to.
     * @param attributes : The LDIF entry attributes to be written.
     */
    private void writeAttributes(IonWriter ionWriter, Collection<Attribute> attributes) throws IOException {
        ionWriter.stepIn(IonType.STRUCT);
        for (Attribute attribute : attributes) {
            ionWriter.setFieldName(attribute.getName());
            ionWriter.stepIn(IonType.LIST);
            for (String value : attribute.getValues()) {
                ionWriter.writeString(value);
            }
            ionWriter.stepOut();
        }
        ionWriter.stepOut();
    }

    /**
     * Writes an LDIF changeRecord to the ION writer.
     * @param ionWriter : The ION writer to write the entry to.
     * @param changeRecord : The LDIF changeRecord to be written.
     */
    private void writeIonChangeRecord(IonWriter ionWriter, LDIFChangeRecord changeRecord) throws IOException {
        ionWriter.stepIn(IonType.STRUCT);

        ionWriter.setFieldName("dn");
        ionWriter.writeString(changeRecord.getDN());

        ionWriter.setFieldName("changeType");
        ionWriter.writeString(changeRecord.getChangeType().toString());

        if (changeRecord.getChangeType() == ChangeType.MODIFY) {
            ionWriter.setFieldName("modifications");
            writeModifications(ionWriter, ((LDIFModifyChangeRecord)changeRecord).getModifications());
        }

        ionWriter.stepOut();
    }

    /**
     * Writes an LDIF changeRecord modifications to the ION writer.
     * @param ionWriter : The ION writer to write the entry to.
     * @param modifications : The LDIF changeRecord modifications to be written.
     */
    private void writeModifications(IonWriter ionWriter, Modification[] modifications) throws IOException {
        ionWriter.stepIn(IonType.LIST);
        for (Modification modification : modifications) {
            ionWriter.stepIn(IonType.STRUCT);

            ionWriter.setFieldName("operation");
            ionWriter.writeString(modification.getModificationType().toString());

            ionWriter.setFieldName("attribute");
            ionWriter.writeString(modification.getAttributeName());

            ionWriter.setFieldName("values");
            ionWriter.stepIn(IonType.LIST);
            for (String value : modification.getValues()) {
                ionWriter.writeString(value);
            }
            ionWriter.stepOut();

            ionWriter.stepOut();
        }
        ionWriter.stepOut();
    }
}
