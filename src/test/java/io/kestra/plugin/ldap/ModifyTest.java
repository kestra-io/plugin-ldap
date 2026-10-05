package io.kestra.plugin.ldap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
     * 
     * @param files : Kestra URI(s) of LDIF formated file(s) containing DN(s) and attributes.
     * @return A ready to run Modification task.
     */
    private Modify makeTask(List<String> files) {
        return Modify.builder()
            .hostname(Property.ofValue(ldap.getHost()))
            .port(Property.ofValue(ldap.getMappedPort(Commons.EXPOSED_PORTS[0])))
            .userDn(Property.ofValue(Commons.USER))
            .password(Property.ofValue((Commons.PASS)))
            .inputs(Property.ofValue(files))
            .build();
    }

    @Test
    void basic_test() throws Exception {
        List<String> inputs = new ArrayList<>();

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

        RunContext runContext = Commons.getRunContext(inputs, ".ldif", storageInterface, runContextFactory);
        Modify task = makeTask(Commons.makeKestraPebblesForXFiles(inputs.size()));
        // Modify has no output: run() must return null, not VoidOutput, or Jackson serialization fails on the worker and the execution hangs in RUNNING.
        assertThat(task.run(runContext), nullValue());
        Search check_task = Commons
            .makeSearchTask("(|(description=Modified entry)(sn=Turanga)(cn=Hermes Conrad))", "dc=planetexpress,dc=com", Arrays.asList("description", "givenName", "employeeType", "cn"), ldap);
        Search.Output search_result = check_task.run(runContext);
        System.out.println("CAUTION !! THIS TEST DEPENDS HEAVILY ON THE SEARCH TASK, CHECK THAT ALL --SEARCH TESTS-- PASSED.");
        Commons.assertResult(expected, search_result.getUri(), this.storageInterface);
    }

    /**
     * Tests that inputs resolves a list of per-element expressions (two Pebble URIs) from runContext variables,
     * and both LDIF change records are applied to LDAP.
     */
    @Test
    void multi_uri_test() throws Exception {
        List<String> ldifContents = new ArrayList<>();

        // File 0: modify description on Philip J. Fry
        ldifContents.add("""
            dn: cn=Philip J. Fry,ou=people,dc=planetexpress,dc=com
            changeType: modify
            replace: description
            description: Multi URI test file 0
            -
            """);

        // File 1: modify description on Amy Wong
        ldifContents.add("""
            dn: cn=Amy Wong+sn=Kroker,ou=people,dc=planetexpress,dc=com
            changeType: modify
            replace: description
            description: Multi URI test file 1
            -
            """);

        RunContext runContext = Commons.getRunContext(ldifContents, ".ldif", storageInterface, runContextFactory);
        // makeKestraPebblesForXFiles(2) returns ["{{file0}}", "{{file1}}"]
        Modify task = makeTask(Commons.makeKestraPebblesForXFiles(ldifContents.size()));
        assertThat(task.run(runContext), nullValue());

        // Verify both modifications were applied
        Search check = Commons.makeSearchTask(
            "(description=Multi URI test file*)",
            "dc=planetexpress,dc=com",
            Arrays.asList("description"),
            ldap
        );
        Search.Output result = check.run(runContext);
        // Both entries should match: we expect exactly two results
        String expected = """
            dn: cn=Amy Wong+sn=Kroker,ou=people,dc=planetexpress,dc=com
            description: Multi URI test file 1

            dn: cn=Philip J. Fry,ou=people,dc=planetexpress,dc=com
            description: Multi URI test file 0
            """;
        Commons.assertResult(expected, result.getUri(), this.storageInterface);
    }

    /**
     * Tests that a single expression rendering to an entire array of URIs works.
     */
    @Test
    void whole_array_expression_test() throws Exception {
        List<String> ldifContents = new ArrayList<>();
        ldifContents.add("""
            dn: cn=Philip J. Fry,ou=people,dc=planetexpress,dc=com
            changeType: modify
            replace: description
            description: Array expression file 0
            -
            """);
        ldifContents.add("""
            dn: cn=Amy Wong+sn=Kroker,ou=people,dc=planetexpress,dc=com
            changeType: modify
            replace: description
            description: Array expression file 1
            -
            """);

        RunContext baseContext = Commons.getRunContext(ldifContents, ".ldif", storageInterface, runContextFactory);
        List<java.net.URI> urisList = new ArrayList<>();
        urisList.add(java.net.URI.create(baseContext.render("{{file0}}")));
        urisList.add(java.net.URI.create(baseContext.render("{{file1}}")));
        
        RunContext runContext = runContextFactory.of(java.util.Map.of("urisList", urisList));

        Modify task = Modify.builder()
            .hostname(Property.ofValue(ldap.getHost()))
            .port(Property.ofValue(ldap.getMappedPort(Commons.EXPOSED_PORTS[0])))
            .userDn(Property.ofValue(Commons.USER))
            .password(Property.ofValue(Commons.PASS))
            .inputs(Property.ofExpression("{{ urisList }}"))
            .build();
            
        assertThat(task.run(runContext), nullValue());

        Search check = Commons.makeSearchTask(
            "(description=Array expression file*)",
            "dc=planetexpress,dc=com",
            Arrays.asList("description"),
            ldap
        );
        Search.Output result = check.run(runContext);
        String expected = """
            dn: cn=Amy Wong+sn=Kroker,ou=people,dc=planetexpress,dc=com
            description: Array expression file 1

            dn: cn=Philip J. Fry,ou=people,dc=planetexpress,dc=com
            description: Array expression file 0
            """;
        Commons.assertResult(expected, result.getUri(), this.storageInterface);
    }
}
