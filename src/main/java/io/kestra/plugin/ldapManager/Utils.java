package io.kestra.plugin.ldapManager;

import io.kestra.core.runners.RunContext;

import java.net.URI;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.system.IonSystemBuilder;
import com.unboundid.ldif.LDIFReader;

/**
 * Provides common tools for the ldapManager plugin.
 */
final public class Utils {
    /**
     * Resolves a Kestra pebble or literral URI to a valid Kestra URI.
     * @param file : The URI or pebble to be resolved. Evaluation (render) will be done inside this function.
     * @param runContext : The context of the run.
     * @return The resolved URI, or null if an error occurs.
     */
    public static URI resolveKestraUri(String file, RunContext runContext) throws Exception {
        try {
            return URI.create(runContext.render(file));
        } catch (Exception e) {
            runContext.logger().error("Invalid URI syntax: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * Resolves a Kestra pebble or literral URI to a valid Kestra URI and returns a new LDIFReader.
     * @param file : The URI or pebble to be resolved. Evaluation (render) will be done inside this function.
     * @param runContext : The context of the run.
     * @return A new LDIFReader to read the provided file.
     */
    public static LDIFReader getLDIFReaderFromUri(String file, RunContext runContext) throws Exception {
        URI resolvedUri = resolveKestraUri(file, runContext);
        return new LDIFReader(runContext.storage().getFile(resolvedUri));
    }

    /**
     * Resolves a Kestra pebble or literral URI to a valid Kestra URI and returns a new IONReader.
     * @param file : The URI or pebble to be resolved. Evaluation (render) will be done inside this function.
     * @param runContext : The context of the run.
     * @return A new IONReader to read the provided file.
     */
    public static IonReader getIONReaderFromUri(String file, RunContext runContext) throws Exception {
        URI resolvedUri = resolveKestraUri(file, runContext);
        IonSystem ionSystem = IonSystemBuilder.standard().build();
        return ionSystem.newReader(runContext.storage().getFile(resolvedUri));
    }
}
