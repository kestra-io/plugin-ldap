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
import java.util.List;


@KestraTest
@TestInstance(value = Lifecycle.PER_CLASS)
public class AddTest {
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
    public void clear() {
        ldap.close();
    }

    /**
     * Makes an Addition task and sets its connecion options to the test LDAP server.
     * @param files : Kestra URI(s) of LDIF formated file(s) containing DN(s) and attributes.
     * @return A ready to run Addition task.
     */
    private Add makeTask(List<String> files) {
        return Add.builder()
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

        RunContext runContext = Commons.getRunContext(inputs, ".ldif", storageInterface, runContextFactory);
        Add task = makeTask(Commons.makeKestraPebblesForXFiles(inputs.size()));
        task.run(runContext);
        Search check_task = Commons.makeSearchTask("(sn=Input)", "dc=planetexpress,dc=com", new ArrayList<String>(), ldap);
        Search.Output search_result = check_task.run(runContext);
        System.out.println("CAUTION !! THIS TEST DEPENDS HEAVILY ON THE SEARCH TASK, CHECK THAT ALL --SEARCH TESTS-- PASSED.");
        Commons.assertResult(String.join("\n", inputs), search_result.getUri(), storageInterface);
    }
}
