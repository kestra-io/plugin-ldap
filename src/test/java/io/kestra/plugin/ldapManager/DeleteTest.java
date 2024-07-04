package io.kestra.plugin.ldapManager;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;

import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestInstance;

import org.testcontainers.containers.GenericContainer;

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
     * @param files : Kestra URI(s) of LDIF formated file(s) containing DN(s).
     * @return A ready to run Deletion task.
     */
    private Delete makeTask(List<String> files) {
        return Delete.builder()
            .hostname(ldap.getHost())
            .port(ldap.getMappedPort(Commons.EXPOSED_PORTS[0]))
            .userDn(Commons.USER)
            .password(Commons.PASS)

            .inputs(files)

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
        task.run(runContext);
        Search check_task = Commons.makeSearchTask("(sn=Fry)", "dc=planetexpress,dc=com", Arrays.asList("sn"), ldap);
        Search.Output search_result = check_task.run(runContext);
        System.out.println("CAUTION !! THIS TEST DEPENDS HEAVILY ON THE SEARCH TASK, CHECK THAT ALL --SEARCH TESTS-- PASSED.");
        Commons.assertResult(null, search_result.getUri(), storageInterface);
    }
}
