package io.kestra.plugin.ldap;

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonSystemBuilder;

import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.ChangeType;
import com.unboundid.ldap.sdk.Entry;
import com.unboundid.ldap.sdk.Modification;
import com.unboundid.ldif.LDIFAddChangeRecord;
import com.unboundid.ldif.LDIFChangeRecord;
import com.unboundid.ldif.LDIFException;
import com.unboundid.ldif.LDIFModifyChangeRecord;
import com.unboundid.ldif.LDIFModifyDNChangeRecord;
import com.unboundid.ldif.LDIFReader;
import com.unboundid.ldif.LDIFRecord;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import java.net.URI;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.Builder.Default;
import lombok.experimental.SuperBuilder;

import org.slf4j.Logger;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Convert LDIF entries from an LDAP server into an ION file.",
    description = "Transform .ldif files to .ion ones."
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            title = "Make ION entries from LDIF ones.",
            full = true,
            code = """
                id: ldap_ldif_to_ion
                namespace: company.team

                inputs:
                  - id: file1
                    type: FILE
                  - id: file2
                    type: FILE

                tasks:
                  - id: ldif_to_ion
                    type: io.kestra.plugin.ldap.LdifToIon
                    inputs:
                      - "{{ inputs.file1 }}"
                      - "{{ inputs.file2 }}"

                """
        ),
        @io.kestra.core.models.annotations.Example(
            title = "INPUT example : here's an LDIF file content that may be inputted :",
            code = {"""
            # simple entry
            dn: cn=bob@orga.com,ou=diffusion_list,dc=orga,dc=com
            description: Some description
            someOtherAttribute: perhaps
            description: Some other description
            someOtherAttribute: perhapsAgain

            # modify changeRecord
            dn: cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com
            changetype: modify
            delete: description
            description: Some description 3
            -
            add: description
            description: Some description 4
            -
            replace: someOtherAttribute
            someOtherAttribute: Loves herself more
            -

            # delete changeRecord
            dn: cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com
            changetype: delete

            # moddn and modrdn are equals, what's mandatory is to specify in the following order : newrdn -> deleteoldrdn -> (optional) newsuperior
            dn: cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com
            changetype: modrdn
            newrdn: cn=triss@orga.com
            deleteoldrdn: 0
            newsuperior: ou=expeople,dc=example,dc=com

            # moddn without new superior
            dn: cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com
            changetype: moddn
            newrdn: cn=triss@orga.com
            deleteoldrdn: 1
            """}
        ),
        @io.kestra.core.models.annotations.Example(
            title = "OUTPUT example : here's an ION file content that may be outputted :",
            code = {"""
            # simple entry
            {dn:"cn=bob@orga.com,ou=diffusion_list,dc=orga,dc=com",attributes:{description:["Some description","Some other description"],someOtherAttribute:["perhaps","perhapsAgain"]}}
            # modify changeRecord
            {dn:"cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com",changeType:"modify",modifications:[{operation:"DELETE",attribute:"description",values:["Some description 3"]},{operation:"ADD",attribute:"description",values:["Some description 4"]},{operation:"REPLACE",attribute:"someOtherAttribute",values:["Loves herself more"]}]}
            # delete changeRecord
            {dn:"cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com",changeType:"delete"}
            # moddn changeRecord (it is mandatory to specify a newrdn and a deleteoldrdn)
            {dn:"cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com",changeType:"moddn",newDn:{newrdn:"cn=triss@orga.com",deleteoldrdn:false,newsuperior:"ou=expeople,dc=example,dc=com"}}
            # moddn changeRecord without new superior (it is optional to specify a new superior field)
            {dn:"cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com",changeType:"moddn",newDn:{newrdn:"cn=triss@orga.com",deleteoldrdn:true}}
            """}
        )
    },
     metrics = {
        @Metric(
            name = "entries.found",
            type = Counter.TYPE,
            description = "The total number of entries found in the LDIF file."
        ),
        @Metric(
            name = "entries.translated",
            type = Counter.TYPE,
            description = "The total number of entries successfully translated to ION format."
        )
    }
)
public class LdifToIon extends Task implements RunnableTask<LdifToIon.Output> {
    /**
     * INPUTS ------------------------------------------------------------------------------------------------------------------- //
    **/

    @Schema(
        title = "URI(s) of file(s) containing LDIF entries."
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

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    @Default
    private Integer translateCount = 0;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    @Default
    private Integer entriesFound = 0;
    /** The kestra logger (slf4j) for the task. */
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    @Default
    private Logger logger = null;

    @Override
    public LdifToIon.Output run(RunContext runContext) throws Exception {
        this.logger = runContext.logger();
        List<URI> storedResults = new ArrayList<>();

        for (String path : this.inputs) {
            try {
                storedResults.add(transformLdifToIon(path, runContext));
            } catch (Exception e) {
                 this.logger.error(e.getMessage());
            }
        }
        if (!this.inputs.isEmpty() && storedResults.isEmpty()) {
            throw new Exception("Not a single file has been translated.");
        }
        runContext.metric(Counter.of("entries.found", this.entriesFound, "origin", "LdifToIon"));
        runContext.metric(Counter.of("entries.translated", this.translateCount, "origin", "LdifToIon"));

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
    private URI transformLdifToIon(String ldifFilePath, RunContext runContext) throws IOException, IllegalVariableEvaluationException, NullPointerException, IllegalArgumentException, IllegalStateException {
        IonSystem ionSystem = IonSystemBuilder.standard().build();
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             IonWriter ionWriter = ionSystem.newTextWriter(byteArrayOutputStream);
             LDIFReader ldifReader = Utils.getLDIFReaderFromUri(ldifFilePath, runContext)) {

            processEntries(ldifReader, ionWriter);
            ionWriter.finish();
            String resultContent = byteArrayOutputStream.toString().replace("} {", "}\n{");
            File tempFile = runContext.workingDir().createTempFile(resultContent.getBytes(), ".ion").toFile();
            return runContext.storage().putFile(tempFile);
        }
    }

    /**
     * Construct entries and changeRecords in an IonWriter by interpreting LDIFReader reads.
     * @param ldifReader : The LDIF reader to get informations from.
     * @param ionWriter : The ION writer to write the entry to.
     */
    @SuppressWarnings("null")
    private void processEntries(LDIFReader ldifReader, IonWriter ionWriter) throws IllegalArgumentException {
        while (true) {
            String[] record = null;
            Entry entry = null;
            LDIFChangeRecord changeRecord = null;
            try {
                LDIFRecord ldifRecord = ldifReader.readLDIFRecord();
                if (ldifRecord == null) break;
                record = ldifRecord.toLDIF();
            } catch (LDIFException e) {
                 this.logger.warn("Canno't read LDIF entry {}, {}", e.getDataLines(), e.getMessage());
                continue;
            } catch (IOException e) {
                 this.logger.error(e.getMessage());
                continue;
            }
            try {
                changeRecord = LDIFReader.decodeChangeRecord(record);
            } catch (LDIFException e_change) {
                try {
                    entry = LDIFReader.decodeEntry(record);
                } catch (LDIFException e_entry) {
                     this.logger.warn("Translation failed, not an Entry nor a ChangeRecord : {}, {}, {}", record.toString(), e_entry.getMessage(), e_change.getMessage());
                }
            }
            if (entry == null && changeRecord == null) break;
            this.entriesFound++;
            try {
                if (entry != null) {
                    writeIonEntry(ionWriter, entry);
                } else {
                    writeIonChangeRecord(ionWriter, changeRecord);
                }
                this.translateCount++;
            } catch (IOException | IllegalArgumentException e) {
                this.logger.warn("Canno't write ION entry {}, {}", entry == null ? changeRecord.toLDIFString() : entry.toLDIFString(), e.getMessage());
            }
        }
    }

    /**
     * Writes an LDIF entry to the ION writer.
     * @param ionWriter : The ION writer to write the entry to.
     * @param entry : The LDIF entry to be written.
     */
    private void writeIonEntry(IonWriter ionWriter, Entry entry) throws IOException, IllegalArgumentException {
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
    private void writeAttributes(IonWriter ionWriter, Collection<Attribute> attributes) throws IOException, IllegalArgumentException {
        ionWriter.stepIn(IonType.STRUCT);
        for (Attribute attribute : attributes) {
            ionWriter.setFieldName(attribute.getName());
            ionWriter.stepIn(IonType.LIST);

            String[] values = attribute.getValues();

            if (values == null || values.length == 0) {
                // No value at all -> single null
                ionWriter.writeNull();
            } else {
                boolean wroteNonEmpty = false;

                // Write only non-empty values
                for (String value : values) {
                    if (value != null && !value.isBlank()) {
                        ionWriter.writeString(value);
                        wroteNonEmpty = true;
                    }
                }

                // If all values were empty/blank -> single null
                if (!wroteNonEmpty) {
                    ionWriter.writeNull();
                }
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
    private void writeIonChangeRecord(IonWriter ionWriter, LDIFChangeRecord changeRecord) throws IOException, IllegalArgumentException {
        ionWriter.stepIn(IonType.STRUCT);

        ionWriter.setFieldName("dn");
        ionWriter.writeString(changeRecord.getDN());

        ionWriter.setFieldName("changeType");
        ionWriter.writeString(changeRecord.getChangeType().toString());

        if (changeRecord.getChangeType() == ChangeType.MODIFY) {
            ionWriter.setFieldName("modifications");
            writeModifications(ionWriter, ((LDIFModifyChangeRecord)changeRecord).getModifications());
        } else if (changeRecord.getChangeType() == ChangeType.MODIFY_DN) {
            ionWriter.setFieldName("newDn");
            writeModifications(ionWriter, (LDIFModifyDNChangeRecord)changeRecord);
        } else if (changeRecord.getChangeType() == ChangeType.ADD) {
            ionWriter.setFieldName("attributes");
            writeAttributes(ionWriter, ((LDIFAddChangeRecord)changeRecord).getEntryToAdd().getAttributes());
        }

        ionWriter.stepOut();
    }

    /**
     * Writes an LDIF changeRecord modifications to the ION writer.
     * @param ionWriter : The ION writer to write the entry to.
     * @param modifications : The LDIF changeRecord modifications to be written.
     */
    private void writeModifications(IonWriter ionWriter, Modification[] modifications) throws IOException, IllegalArgumentException {
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

    /**
     * Writes an LDIF changeDNRecord modifications to the ION writer.
     * @param ionWriter : The ION writer to write the entry to.
     * @param modifications : The LDIF changeDNRecord class from wich to retrieve infos to be written.
     */
    private void writeModifications(IonWriter ionWriter, LDIFModifyDNChangeRecord modifications) throws IOException, IllegalArgumentException {
        ionWriter.stepIn(IonType.STRUCT);
        ionWriter.setFieldName("newrdn");
        ionWriter.writeString(modifications.getNewRDN());
        ionWriter.setFieldName("deleteoldrdn");
        ionWriter.writeBool(modifications.deleteOldRDN());
        if (modifications.getNewSuperiorDN() != null) {
            ionWriter.setFieldName("newsuperior");
            ionWriter.writeString(modifications.getNewSuperiorDN());
        }
        ionWriter.stepOut();
    }
}
