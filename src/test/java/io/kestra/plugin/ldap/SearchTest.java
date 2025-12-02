package io.kestra.plugin.ldap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
@Testcontainers
@TestInstance(value = Lifecycle.PER_CLASS)
public class SearchTest {

    @Container
    public static GenericContainer<?> ldap = new GenericContainer<>(Commons.LDAP_IMAGE)
        .withExposedPorts(Commons.EXPOSED_PORTS)
        .waitingFor(Wait.forListeningPort());

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @BeforeAll
    void initDirectory() throws Exception {
        loadBulkPeopleEntries();
    }

    @AfterAll
    void clear() {
        ldap.close();
    }

    @Test
    void basic_test() throws Exception {
        String filter = "(ou=people)";
        String baseDn = "dc=planetexpress,dc=com";
        List<String> attributes = Arrays.asList("objectClass", "givenName");

        String expected = """
            dn: ou=people,dc=planetexpress,dc=com
            objectClass: top
            objectClass: organizationalUnit
            """;

        RunContext runContext = this.runContextFactory.of();
        Search task = Commons.makeSearchTask(filter, baseDn, attributes, ldap);
        Search.Output runOutput = task.run(runContext);

        Commons.assertResult(expected, runOutput.getUri(), this.storageInterface);
    }

    @Test
    void size_limit_should_limit_number_of_entries() throws Exception {
        // Broad filter that matches multiple entries in the test LDAP dataset
        String filter = "(objectClass=person)";
        String baseDn = "dc=planetexpress,dc=com";
        List<String> attributes = Arrays.asList("cn", "sn");

        RunContext runContext = this.runContextFactory.of();

        Search task = Search.builder()
            .hostname(Property.ofValue(ldap.getHost()))
            .port(Property.ofValue(String.valueOf(ldap.getMappedPort(Commons.EXPOSED_PORTS[0]))))
            .userDn(Property.ofValue(Commons.USER))
            .password(Property.ofValue(Commons.PASS))
            .baseDn(Property.ofValue(baseDn))
            .filter(Property.ofValue(filter))
            .attributes(Property.ofValue(attributes))
            .sizeLimit(Property.ofValue(1))
            .build();

        Search.Output runOutput = task.run(runContext);

        try (InputStream is = storageInterface.get(TenantService.MAIN_TENANT, null, runOutput.getUri())) {
            String ldif = new String(is.readAllBytes(), StandardCharsets.UTF_8);

            // Count entries by counting "dn:" lines
            Pattern dnPattern = Pattern.compile("(?m)^dn:");
            Matcher matcher = dnPattern.matcher(ldif);

            int dnCount = 0;
            while (matcher.find()) {
                dnCount++;
            }

            assertEquals(
                1,
                dnCount,
                "Expected exactly 1 LDAP entry when sizeLimit=1"
            );
        }
    }

    @Test
    void page_size_should_return_all_entries_without_error() throws Exception {
        String filter = "(objectClass=person)";
        String baseDn = "dc=planetexpress,dc=com";
        List<String> attributes = Arrays.asList("cn", "sn");

        RunContext runContext = this.runContextFactory.of();

        Search pagingTask = Search.builder()
            .hostname(Property.ofValue(ldap.getHost()))
            .port(Property.ofValue(String.valueOf(ldap.getMappedPort(Commons.EXPOSED_PORTS[0]))))
            .userDn(Property.ofValue(Commons.USER))
            .password(Property.ofValue(Commons.PASS))
            .baseDn(Property.ofValue(baseDn))
            .filter(Property.ofValue(filter))
            .attributes(Property.ofValue(attributes))
            .pageSize(Property.ofValue(200)) // out of 1000 (see @BeforeAll)
            .build();

        Search.Output pagingOutput = pagingTask.run(runContext);
        int pagingCount = countDnEntries(pagingOutput.getUri());

        // We expect at least the 1000 injected + whatever already existed in fixture
        // Assert we got our bulk data
        assertTrue(pagingCount >= 1000, "Paging should retrieve at least the 1000 injected entries");
    }

    private int countDnEntries(URI uri) throws Exception {
        try (InputStream is = storageInterface.get(TenantService.MAIN_TENANT, null, uri)) {
            String ldif = new String(is.readAllBytes(), StandardCharsets.UTF_8);

            Pattern dnPattern = Pattern.compile("(?m)^dn:");
            Matcher matcher = dnPattern.matcher(ldif);

            int dnCount = 0;
            while (matcher.find()) {
                dnCount++;
            }
            return dnCount;
        }
    }

    private void loadBulkPeopleEntries() throws Exception {
        // Generate LDIF inside container to avoid ARG_MAX and permission issues
        String script = """
                    rm -f /tmp/bulk.ldif
                    i=1
                    while [ $i -le %d ]; do
                      cat >> /tmp/bulk.ldif <<EOF
            dn: uid=user$i,ou=people,dc=planetexpress,dc=com
            objectClass: top
            objectClass: person
            objectClass: organizationalPerson
            objectClass: inetOrgPerson
            uid: user$i
            cn: User $i
            sn: $i
            givenName: User
            mail: user$i@planetexpress.com

            EOF
                      i=$((i+1))
                    done
            """.formatted(1000);

        var write = ldap.execInContainer("sh", "-c", script);
        if (write.getExitCode() != 0) {
            throw new IllegalStateException(
                "Failed to generate LDIF in container: " +
                    write.getStdout() + "\n" + write.getStderr()
            );
        }

        var add = ldap.execInContainer(
            "ldapadd",
            "-x",
            "-D", Commons.USER,
            "-w", Commons.PASS,
            "-H", "ldap://localhost:" + Commons.EXPOSED_PORTS[0],
            "-f", "/tmp/bulk.ldif"
        );

        if (add.getExitCode() != 0) {
            throw new IllegalStateException(
                "ldapadd failed: " + add.getStdout() + "\n" + add.getStderr()
            );
        }

        ldap.start();
    }
}
