package io.kestra.plugin.ldapManager;

import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.system.IonSystemBuilder;

import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.Entry;
import com.unboundid.ldif.LDIFWriter;

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
    title = "Unionises ION entries.",
    description = "Transform .ion files to .ldif ones."
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            title = "Make LDIF entries from ION ones.",
            code = { "description: What your task is supposed to do and why.",
            "inputs:",
            " - {{some_uri}}",
            " - {{some_other_uri}}"}
            )
    }
)
public class IonToLdif extends Task implements RunnableTask<IonToLdif.Output> {
    /**
     * INPUTS ------------------------------------------------------------------------------------------------------------------- //
    **/

    @Schema(
        title = "URI(s) of file(s) containing ION entries.",
        description = "ION file(s) to transform to LDIF formated ones."
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
            title = "LDIF transformed file(s) URI(s)."
        )
        private final List<URI> urisList;
    }

    /**
     * CODE ------------------------------------------------------------------------------------------------------------------- //
    **/

    private Integer count;

    @Override
    public IonToLdif.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        List<URI> storedResults = new ArrayList<>();
        this.count = 0;

        for (String path : this.inputs) {
            try {
                storedResults.add(transformIonToLdif(runContext.render(path), runContext));
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
        if (!this.inputs.isEmpty() && storedResults.isEmpty()) {
            throw new Exception("Not a single file has been translated.");
        }
        runContext.metric(Counter.of("entries.translated", this.count, "origin", "Ionise"));
        return Output.builder()
            .urisList(storedResults)
            .build();
    }

    /**
     * Transforms a given ION file to LDIF format.
     * @param ionFilePath : The path to the ION file to be transformed.
     * @param runContext : The context of the run.
     * @return URI of the transformed LDIF file.
     */
    private URI transformIonToLdif(String ionFilePath, RunContext runContext) throws IOException, IonException {
        IonSystem ionSystem = IonSystemBuilder.standard().build();
        try (InputStream ionInputStream = runContext.storage().getFile(URI.create(ionFilePath));
             IonReader ionReader = ionSystem.newReader(ionInputStream);
             ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             LDIFWriter ldifWriter = new LDIFWriter(byteArrayOutputStream)) {

            processIonEntries(ionReader, ldifWriter);
            ldifWriter.flush();

            File tempFile = runContext.workingDir().createTempFile(byteArrayOutputStream.toByteArray(), ".ldif").toFile();
            return runContext.storage().putFile(tempFile);
        }
    }

    /**
     * Processes the entries from the ION reader and attempts to write them to the LDIF writer.
     * @param ionReader : The ION reader containing the entries to be processed.
     * @param ldifWriter : The LDIF writer to write the entries to.
     */
    private void processIonEntries(IonReader ionReader, LDIFWriter ldifWriter) throws IOException {
        while (ionReader.next() != null) {
            ionReader.stepIn();
            Entry entry = readIonEntry(ionReader);
            if (entry != null) {
                ldifWriter.writeEntry(entry);
                this.count++;
            }
            ionReader.stepOut();
        }
    }

    /**
     * Reads an entry from the ION reader.
     * @param ionReader : The ION reader to read the entry from.
     * @return The LDIF entry read from the ION reader.
     */
    private Entry readIonEntry(IonReader ionReader) {
        String dn = null;
        List<Attribute> attributes = new ArrayList<>();

        while (ionReader.next() != null) {
            String fieldName = ionReader.getFieldName();
            if ("dn".equals(fieldName)) {
                dn = ionReader.stringValue();
            } else if ("attributes".equals(fieldName)) {
                ionReader.stepIn();
                attributes = readAttributes(ionReader);
                ionReader.stepOut();
            }
        }

        if (dn != null) {
            return new Entry(dn, attributes);
        }
        return null;
    }

    /**
     * Reads the attributes of an entry from the ION reader.
     * @param ionReader : The ION reader to read the attributes from.
     * @return A list of attributes read from the ION reader.
     */
    private List<Attribute> readAttributes(IonReader ionReader) {
        List<Attribute> attributes = new ArrayList<>();

        while (ionReader.next() != null) {
            String attributeName = ionReader.getFieldName();
            ionReader.stepIn();
            List<String> values = new ArrayList<>();
            while (ionReader.next() != null) {
                values.add(ionReader.stringValue());
            }
            ionReader.stepOut();
            attributes.add(new Attribute(attributeName, values));
        }

        return attributes;
    }
}
