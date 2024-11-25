package io.kestra.plugin.ldap;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Provides common tools for the ldapManager plugin testing.
 * @apiNote Contains immutable common values to test LDAP server responses to the plugin tasks.
 * @apiNote Provides common assertions tools and a SearchTask making function.
 */
final class Commons {

    /** Docker image ref : https://hub.docker.com/r/rroemhild/test-openldap */
    public static final DockerImageName LDAP_IMAGE = DockerImageName.parse("rroemhild/test-openldap:2.1");
    /**
     * Ports that should be exposed to interact with the LDAP test server.
     * @value [0] -> unsecure port
     * @value [1] -> SSL secure port
     */
    public static final Integer[] EXPOSED_PORTS = {10389, 10636};
    public static final Property<String> USER = Property.of("cn=admin,dc=planetexpress,dc=com");
    public static final Property<String> PASS = Property.of("GoodNewsEveryone");

    /**
     * Insert provided contents in separated files in the Kestra storage.
     * @param contents : A list of string to input in Kestra files.
     * @param extension : Extension of the file.
     * @return A new context where each newly created file may be accessed with a pebble expression like {{ file0 }}, {{ file1 }}, {{ fileEtc }}
     */
    public static RunContext getRunContext(List<String> contents, String extension, StorageInterface storageInterface, RunContextFactory runContextFactory) {
        Map<String, String> kestraPaths = new HashMap<>();
        Integer idx = 0;
        for (String content : contents) {
            URI filePath;
            try {
                filePath = storageInterface.put(
                    null,
                    null,
                    URI.create("/" + IdUtils.create() + extension),
                    new ByteArrayInputStream(content.getBytes())
                );
                kestraPaths.put("file" + idx, filePath.toString());
                idx++;
            } catch (Exception e) {
                System.err.println(e.getMessage());
                fail("Unable to load refs files.");
                return null;
            }
        }
        return runContextFactory.of(ImmutableMap.copyOf(kestraPaths));
    }

    /**
     * Assert the equality between the result file content provided by a task and a string.
     * @param expected : The string representing the expected content of the task output. May be null if expected return should be blank.
     * @param file : The outputted URI of the task to make the comparison with.
     * @param storageInterface : The StorageInterface that should contain the file.
     */
    public static void assertResult(String expected, URI file, StorageInterface storageInterface) {
        assertThat("Result file should exist", storageInterface.exists(null, null, file), is(true));
        try (InputStream streamResult = storageInterface.get(null, null, file)) {
            String result = new String(streamResult.readAllBytes(), StandardCharsets.UTF_8).replace("\r\n", "\n");

            System.out.println("Got :\n" + result);
            if (expected == null) {
                System.out.println("Expecting : <Nothing>");
                assertThat("Result should match the reference", result.isBlank());
            } else {
                System.out.println("Expecting :\n" + expected);
                assertThat("Result should match the reference", result.equals(expected));
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            fail("Unable to load results files.");
        }
    }

    /**
     * Assert the equality between result file(s) content provided by a task and string(s).
     * @param expected_results : String(s) representing expected content(s) of the task output. May contain null member if expected return should be blank.
     * @param results : Outputted URI(s) of the task to make the comparison with.
     * @param storageInterface : The StorageInterface that should contain the file(s).
     */
    public static void assertFilesEq(List<URI> results, List<String> expected_results, StorageInterface storageInterface) {
        Integer idx = 0;
        for (String expected_result : expected_results) {
            Commons.assertResult(expected_result, results.get(idx), storageInterface);
            idx++;
        }
    }

    /**
     * Makes an Search task and sets its connecion options to the test LDAP server.
     * @param filter : The filter to use for the search.
     * @param baseDn : The DN to search from.
     * @param attributes : The attributes to retrieve.
     * @param ldap : The ldap container - Should be the conainter of the running test class.
     * @return A ready to run Search task.
     */
    public static Search makeSearchTask(String filter, String baseDn, List<String> attributes, GenericContainer<?> ldap) {
        return Search.builder()
            .hostname(Property.of(ldap.getHost()))
            .port(Property.of(String.valueOf(ldap.getMappedPort(Commons.EXPOSED_PORTS[0]))))
            .userDn(Commons.USER)
            .password(Commons.PASS)
            .baseDn(Property.of(baseDn))
            .filter(Property.of(filter))
            .attributes(Property.of(attributes))

            .build();
    }

    /**
     * Creates a list of pebble String referencing contents URI(s) created by a Commons.getRunContext call.
     * @param x : The number of files provided to the previous call of Commons.getRunContext.
     * @return Pebble expression(s) like {{ file0 }}, {{ file1 }}, {{ fileEtc }}
     */
    public static List<String> makeKestraPebblesForXFiles(Integer x) {
        List<String> kestraFilepaths = new ArrayList<>();
        for (Integer i = 0; i < x; i++) {
            kestraFilepaths.add(String.format("{{file%d}}", i));
        }
        return kestraFilepaths;
    }
}
