package io.kestra.plugin.ldapManager;

import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.testcontainers.containers.GenericContainer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import jakarta.inject.Inject;
import java.net.URI;
import java.util.List;
import java.io.IOException;
import java.util.Arrays;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


@KestraTest
@TestInstance(value = Lifecycle.PER_CLASS)
public class SearchTest {
    public static GenericContainer<?> ldap;

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @SuppressWarnings("resource")
    @BeforeAll
    private void prepare() {
        ldap = new GenericContainer<>(Commons.LDAP_IMAGE).withExposedPorts(Commons.EXPOSED_PORTS);
        ldap.start();
    }

    @AfterAll
    private void clear() {
        ldap.close();
    }

    /**
     * Makes an Search task and sets its connecion options to the test LDAP server.
     * @param filter : The filter to use for the search.
     * @param baseDn : The DN to search from.
     * @param attributes : The attributes to retrieve.
     * @param ldap : The ldap container - Should be the conainter of the running test class.
     * @return A ready to run Search task.
     */
    public static Search makeTask(String filter, String baseDn, List<String> attributes, GenericContainer<?> ldap) {
        return Search.builder()
            .hostname(ldap.getHost())
            .port(ldap.getMappedPort(Commons.EXPOSED_PORTS[0]))
            .userDn(Commons.USER)
            .password(Commons.PASS)

            .baseDn(baseDn)
            .filter(filter)
            .attributes(attributes)

            .build();
    }

    /**
     * Assert the equality between the result file content provided by a Search task and a string.
     * @param expected_result : The string representing the expected content of the search task.
     * @param runOutput : The output of the search task to make the comparison with.
     */
    private void assertFilesEq(Search.Output runOutput, String expected_result) {
        URI result_uri = runOutput.getUri();
        assertThat("Result file should exist", this.storageInterface.exists(null, result_uri), is(true));
        try (InputStream streamResult = this.storageInterface.get(null, result_uri)) {
            String result = new String(streamResult.readAllBytes(), StandardCharsets.UTF_8).replace("\r\n", "\n");

            System.out.println("Got :\n" + result);
            System.out.println("Expecting :\n" + expected_result);
            assertThat("Result should match the reference", result.equals(expected_result));
        } catch (IOException e) {
            System.err.println(e.getMessage());
            fail("Unable to load results files.");
        }
    }

    @Test
    void basic_test() throws Exception {
        // specific test values :
        String filter = "(ou=people)";
        String baseDn = "dc=planetexpress,dc=com";
        List<String> attributes = Arrays.asList("objectClass", "givenName");

        String expected = """
            dn: ou=people,dc=planetexpress,dc=com
            objectClass: top
            objectClass: organizationalUnit
            """;
        /////////////////////////

        RunContext runContext = this.runContextFactory.of();
        Search task = makeTask(filter, baseDn, attributes, ldap);
        Search.Output runOutput = task.run(runContext);
        assertFilesEq(runOutput, expected);
    }
}
