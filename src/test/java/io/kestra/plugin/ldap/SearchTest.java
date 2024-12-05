package io.kestra.plugin.ldap;

import io.kestra.core.junit.annotations.KestraTest;
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

import java.util.Arrays;
import java.util.List;


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
    public void prepare() {
        ldap = new GenericContainer<>(Commons.LDAP_IMAGE).withExposedPorts(Commons.EXPOSED_PORTS);
        ldap.start();
    }

    @AfterAll
    private void clear() {
        ldap.close();
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
        Search task = Commons.makeSearchTask(filter, baseDn, attributes, ldap);
        Search.Output runOutput = task.run(runContext);
        Commons.assertResult(expected, runOutput.getUri(), this.storageInterface);
    }
}
