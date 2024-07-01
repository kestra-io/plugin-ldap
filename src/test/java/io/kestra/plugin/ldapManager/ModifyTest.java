package io.kestra.plugin.ldapManager;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.net.URI;

import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestInstance;

import org.testcontainers.containers.GenericContainer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import static org.junit.jupiter.api.Assertions.fail;


@KestraTest
@TestInstance(value = Lifecycle.PER_CLASS)
public class ModifyTest {
    public static GenericContainer<?> ldap;

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    /**
     * Start a LDAP server in a container.
     * Configuration may be done through the "Commons.java" class file.
     */
    @SuppressWarnings("resource")
    @BeforeAll
    private void prepare() {
        ldap = new GenericContainer<>(Commons.LDAP_IMAGE).withExposedPorts(Commons.EXPOSED_PORTS);
        ldap.start();
    }

    /** Stop the container and release its ressources. */
    @AfterAll
    private void clear() {
        ldap.close();
    }

    /**
     * Makes an Modifyition task and sets its connecion options to the test LDAP server.
     * @param files : Kestra URI(s) of LDIF formated file(s) containing DN(s) and attributes.
     * @return A ready to run Modification task.
     */
    private Modify makeTask(List<String> files) {
        return Modify.builder()
            .hostname(ldap.getHost())
            .port(ldap.getMappedPort(Commons.EXPOSED_PORTS[0]))
            .userDn(Commons.USER)
            .password(Commons.PASS)

            .inputs(files)

            .build();
    }

    /**
     * Insert provided contents in separated files in the Kestra storage.
     * @param contents : A list of string to input in Kestra files.
     * @return A new context where each newly created file may be accessed with a pebble expression like {{ file0 }}, {{ file1 }}, {{ fileEtc }}
     */
    private RunContext getRunContext(List<String> contents) {
        Map<String, String> kestraPaths = new HashMap<>();
        Integer idx = 0;
        for (String content : contents) {
            URI filePath;
            try {
                filePath = this.storageInterface.put(
                    null, 
                    URI.create("/" + IdUtils.create() + ".ldif"), 
                    new ByteArrayInputStream(content.getBytes())
                );
                kestraPaths.put("file" + idx, filePath.toString());
                idx++;
            } catch (IOException e) {
                System.err.println(e.getMessage());
                fail("Unable to load refs files.");
                return null;
            }
        }
        return this.runContextFactory.of(ImmutableMap.copyOf(kestraPaths));
    }

    /**
     * Assert the equality between the result file content provided by a Search task and a string.
     * @param expected : The string representing the expected content of the search task.
     * @param searchResult : The output of the search task to make the comparison with.
     */
    private void assertResult(String expected, Search.Output searchResult) {
        URI file = searchResult.getUri();
        assertThat("Result file should exist", this.storageInterface.exists(null, file), is(true));
        try (InputStream streamResult = this.storageInterface.get(null, file)) {
            String result = new String(streamResult.readAllBytes(), StandardCharsets.UTF_8).replace("\r\n", "\n");

            System.out.println("CAUTION !! THIS TEST DEPENDS HEAVILY ON THE SEARCH TASK, CHECK THAT ALL --SEARCH TESTS-- PASSED.");
            System.out.println("Got :\n" + result);
            System.out.println("Expecting :\n" + expected);
            assertThat("Result should match the reference", result.equals(expected));
        } catch (IOException e) {
            System.err.println(e.getMessage());
            fail("Unable to load results files.");
        }
    }

    @Test
    void basic_test() throws Exception {
        List<String> inputs = new ArrayList<>();
        List<String> kestraFilepaths = new ArrayList<>();

        // specific test values :
        inputs.add("""
            dn: cn=Bender Bending Rodríguez,ou=people,dc=planetexpress,dc=com
            changeType: modify
            replace: description
            description: Modified entry
            -
            add: employeeType
            employeeType: devTester
            -
            delete: givenName
            -

            dn: cn=Turanga Leela,ou=people,dc=planetexpress,dc=com
            changeType: delete

            dn: cn=Hermes Conrad,ou=people,dc=planetexpress,dc=com
            changeType: modrdn
            newrdn: cn=Conrad Hermes
            deleteoldrdn: 0
            """);// fst file
        String expected = """
            dn:: Y249QmVuZGVyIEJlbmRpbmcgUm9kcsOtZ3VleixvdT1wZW9wbGUsZGM9cGxhbmV0ZXhwcmVzcyxkYz1jb20=
            cn:: QmVuZGVyIEJlbmRpbmcgUm9kcsOtZ3Vleg==
            employeeType: Ship's Robot
            employeeType: devTester
            description: Modified entry

            dn: cn=Conrad Hermes,ou=people,dc=planetexpress,dc=com
            cn: Hermes Conrad
            cn: Conrad Hermes
            description: Human
            employeeType: Bureaucrat
            employeeType: Accountant
            givenName: Hermes
            """;
            /////////////////////////

        RunContext runContext = getRunContext(inputs);
        for (Integer i = 0; i < inputs.size(); i++) {
            kestraFilepaths.add(String.format("{{file%d}}", i));
        }
        Modify task = makeTask(kestraFilepaths);
        task.run(runContext);
        Search check_task = SearchTest.makeTask("(|(description=Modified entry)(sn=Turanga)(cn=Hermes Conrad))", "dc=planetexpress,dc=com", Arrays.asList("description", "givenName", "employeeType", "cn"), ldap);
        Search.Output search_result = check_task.run(runContext);
        assertResult(expected, search_result);
    }
}
