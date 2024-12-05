package io.kestra.plugin.ldap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.testcontainers.containers.GenericContainer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
    public void prepare() {
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
            .hostname(Property.of(ldap.getHost()))
            .port(Property.of(String.valueOf(ldap.getMappedPort(Commons.EXPOSED_PORTS[0]))))
            .userDn(Commons.USER)
            .password(Commons.PASS)

            .inputs(Property.of(files))

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
        task.run(runContext);
        Search check_task = Commons.makeSearchTask("(|(description=Modified entry)(sn=Turanga)(cn=Hermes Conrad))", "dc=planetexpress,dc=com", Arrays.asList("description", "givenName", "employeeType", "cn"), ldap);
        Search.Output search_result = check_task.run(runContext);
        System.out.println("CAUTION !! THIS TEST DEPENDS HEAVILY ON THE SEARCH TASK, CHECK THAT ALL --SEARCH TESTS-- PASSED.");
        Commons.assertResult(expected, search_result.getUri(), this.storageInterface);
    }
}
