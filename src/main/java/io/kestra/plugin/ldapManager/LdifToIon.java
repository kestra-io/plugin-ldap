package io.kestra.plugin.ldapManager;

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonSystemBuilder;

import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.Entry;
import com.unboundid.ldif.LDIFException;
import com.unboundid.ldif.LDIFReader;

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

            while (true) {
                Entry entry = null;
                try {
                    entry = ldifReader.readEntry();
                } catch (LDIFException e) {
                    logger.error("Cannot read entry: {}", e.getDataLines());
                    continue;
                }
                if (entry == null) break;
                this.found++;
                writeIonEntry(ionWriter, entry);
            }
            ionWriter.finish();
            String resultContent = byteArrayOutputStream.toString().replace("} {", "}\n{");
            File tempFile = runContext.workingDir().createTempFile(resultContent.getBytes(), ".ion").toFile();
            return runContext.storage().putFile(tempFile);
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
        this.count++;
        //TODO: manage changeType
    }

    /**
     * Writes the attributes of an LDIF entry to the ION writer.
     * @param ionWriter : The ION writer to write the attributes to.
     * @param attributes : The collection of attributes to be written.
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
}
