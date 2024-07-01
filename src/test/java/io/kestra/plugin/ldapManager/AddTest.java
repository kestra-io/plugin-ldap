package io.kestra.plugin.ldapManager;

import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.testcontainers.containers.GenericContainer;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


@KestraTest
@TestInstance(value = Lifecycle.PER_CLASS)
public class AddTest {
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

    private Add makeTask(List<String> files) {
        return Add.builder()
            .hostname(ldap.getHost())
            .port(ldap.getMappedPort(Commons.EXPOSED_PORTS[0]))
            .userDn(Commons.USER)
            .password(Commons.PASS)

            .inputs(files)

            .build();
    }

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

    private void assertResult(List<String> expectedList, Search.Output searchResult) {
        URI file = searchResult.getUri();
        assertThat("Result file should exist", this.storageInterface.exists(null, file), is(true));
        String expectedLdif = String.join("\n", expectedList);
        try (InputStream streamResult = this.storageInterface.get(null, file)) {
            String result = new String(streamResult.readAllBytes(), StandardCharsets.UTF_8).replace("\r\n", "\n");

            System.out.println("CAUTION !! THIS TEST DEPENDS HEAVILY ON THE SEARCH TASK, CHECK THAT ALL --SEARCH TESTS-- PASSED.");
            System.out.println("Got :\n" + result);
            System.out.println("Expecting :\n" + expectedLdif);
            assertThat("Result should match the reference", result.equals(expectedLdif));
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
            dn: cn=Input Man,ou=people,dc=planetexpress,dc=com
            objectClass: inetOrgPerson
            cn: Input Man
            sn: Input
            description: Mutant
            employeeType: Captain
            employeeType: Pilot
            givenName: Input
            mail: Input@planetexpress.com
            ou: Delivering Crew
            uid: input
            userPassword: input
            """);// fst file
        /////////////////////////

        RunContext runContext = getRunContext(inputs);
        for (Integer i = 0; i < inputs.size(); i++) {
            kestraFilepaths.add(String.format("{{file%d}}", i));
        }
        Add task = makeTask(kestraFilepaths);
        task.run(runContext);
        Search check_task = SearchTest.makeTask("(sn=Input)", "dc=planetexpress,dc=com", new ArrayList<String>(), ldap);
        Search.Output search_result = check_task.run(runContext);
        assertResult(inputs, search_result);
    }
}
