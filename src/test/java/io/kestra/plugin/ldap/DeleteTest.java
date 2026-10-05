package io.kestra.plugin.ldap;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.testcontainers.containers.GenericContainer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;

@KestraTest
@TestInstance(value = Lifecycle.PER_CLASS)
public class DeleteTest {
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
     * Makes an Deletion task and sets its connecion options to the test LDAP server.
     * 
     * @param files : Kestra URI(s) of LDIF formated file(s) containing DN(s).
     * @return A ready to run Deletion task.
     */
    private Delete makeTask(List<String> files) {
        return Delete.builder()
            .hostname(Property.ofValue(ldap.getHost()))
            .port(Property.ofValue(ldap.getMappedPort(Commons.EXPOSED_PORTS[0])))
            .userDn(Property.ofValue(Commons.USER))
            .password(Property.ofValue(Commons.PASS))
            .inputs(Property.ofValue(files))
            .build();
    }

    @Test
    void basic_test() throws Exception {
        List<String> inputs = new ArrayList<>();

        // specific test values :
        inputs.add("""
            dn: cn=Philip J. Fry,ou=people,dc=planetexpress,dc=com
            """);// fst file
        /////////////////////////

        RunContext runContext = Commons.getRunContext(inputs, ".ldif", storageInterface, runContextFactory);
        Delete task = makeTask(Commons.makeKestraPebblesForXFiles(inputs.size()));
        // Delete has no output: run() must return null, not VoidOutput, or Jackson serialization fails on the worker and the execution hangs in RUNNING.
        assertThat(task.run(runContext), nullValue());
        Search check_task = Commons.makeSearchTask("(sn=Fry)", "dc=planetexpress,dc=com", Arrays.asList("sn"), ldap);
        Search.Output search_result = check_task.run(runContext);
        System.out.println("CAUTION !! THIS TEST DEPENDS HEAVILY ON THE SEARCH TASK, CHECK THAT ALL --SEARCH TESTS-- PASSED.");
        Commons.assertResult(null, search_result.getUri(), storageInterface);
    }

    /**
     * Tests that a single expression rendering to an entire array of URIs works.
     */
    @Test
    void whole_array_expression_test() throws Exception {
        List<String> inputs = new ArrayList<>();
        inputs.add("""
            dn: cn=Hermes Conrad,ou=people,dc=planetexpress,dc=com
            """);
        inputs.add("""
            dn: cn=John A. Zoidberg,ou=people,dc=planetexpress,dc=com
            """);

        RunContext baseContext = Commons.getRunContext(inputs, ".ldif", storageInterface, runContextFactory);
        List<URI> urisList = new ArrayList<>();
        urisList.add(URI.create(baseContext.render("{{file0}}")));
        urisList.add(URI.create(baseContext.render("{{file1}}")));

        RunContext runContext = runContextFactory.of(Map.of("urisList", urisList));
        Map<String, String> entries = Map.of(
            "Conrad", "cn=Hermes Conrad,ou=people,dc=planetexpress,dc=com",
            "Zoidberg", "cn=John A. Zoidberg,ou=people,dc=planetexpress,dc=com"
        );
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            Search before = Commons.makeSearchTask("(sn=" + entry.getKey() + ")", "dc=planetexpress,dc=com", Arrays.asList("sn"), ldap);
            Commons.assertResult("dn: " + entry.getValue() + "\nsn: " + entry.getKey() + "\n", before.run(runContext).getUri(), storageInterface);
        }

        Delete task = Delete.builder()
            .hostname(Property.ofValue(ldap.getHost()))
            .port(Property.ofValue(ldap.getMappedPort(Commons.EXPOSED_PORTS[0])))
            .userDn(Property.ofValue(Commons.USER))
            .password(Property.ofValue(Commons.PASS))
            .inputs(Property.ofExpression("{{ urisList }}"))
            .build();

        assertThat(task.run(runContext), nullValue());

        Search check = Commons.makeSearchTask("(|(sn=Conrad)(sn=Zoidberg))", "dc=planetexpress,dc=com", Arrays.asList("sn"), ldap);
        Search.Output result = check.run(runContext);
        Commons.assertResult(null, result.getUri(), storageInterface);
    }
}
