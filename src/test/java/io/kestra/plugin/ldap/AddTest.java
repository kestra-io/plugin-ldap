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

import io.kestra.core.http.client.configurations.SslOptions;
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
     * Makes an Addition task and sets its connecion options to the test LDAP server.
     * 
     * @param files : Kestra URI(s) of LDIF formated file(s) containing DN(s) and attributes.
     * @return A ready to run Addition task.
     */
    private Add makeTask(List<String> files) {
        return Add.builder()
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
        // Add has no output: run() must return null, not VoidOutput, or Jackson serialization fails on the worker and the execution hangs in RUNNING.
        assertThat(task.run(runContext), nullValue());
        Search check_task = Commons.makeSearchTask("(sn=Input)", "dc=planetexpress,dc=com", new ArrayList<String>(), ldap);
        Search.Output search_result = check_task.run(runContext);
        System.out.println("CAUTION !! THIS TEST DEPENDS HEAVILY ON THE SEARCH TASK, CHECK THAT ALL --SEARCH TESTS-- PASSED.");
        Commons.assertResult(String.join("\n", inputs), search_result.getUri(), storageInterface);
    }

    @Test
    void addAndSearch_usingSsl() throws Exception {
        List<String> inputs = new ArrayList<>();
        String input = """
            dn: cn=Complete SSL User,ou=people,dc=planetexpress,dc=com
            objectClass: inetOrgPerson
            cn: Complete SSL User
            sn: CompleteSslUser
            description: User added and verified entirely over SSL
            employeeType: CEO
            givenName: SecureCore
            mail: complete.ssl.ceo@planetexpress.com
            ou: Executive Wing
            uid: ceoSecureUser
            userPassword: ceoSecurePassword
            """;
        inputs.add(input);

        RunContext runContext = Commons.getRunContext(inputs, ".ldif", storageInterface, runContextFactory);

        Add addTask = makeSslTask(Commons.makeKestraPebblesForXFiles(inputs.size()));
        assertThat(addTask.run(runContext), nullValue());
        Search check_task = Commons.makeSslSearchTask("(sn=CompleteSslUser)", "dc=planetexpress,dc=com", new ArrayList<String>(), ldap);
        Search.Output searchResult = check_task.run(runContext);
        Commons.assertResult(input, searchResult.getUri(), storageInterface);
    }

    private Add makeSslTask(List<String> files) {
        return Add.builder()
            .hostname(Property.ofValue(ldap.getHost()))
            .port(Property.ofValue(ldap.getMappedPort(Commons.EXPOSED_PORTS[1])))
            .userDn(Property.ofValue(Commons.USER))
            .password(Property.ofValue(Commons.PASS))
            .sslOptions(SslOptions.builder().insecureTrustAllCertificates(Property.ofValue(true)).build())
            .inputs(Property.ofValue(files))
            .build();
    }

    /**
     * Tests that a single expression rendering to an entire array of URIs works.
     */
    @Test
    void whole_array_expression_test() throws Exception {
        List<String> inputs = new ArrayList<>();
        inputs.add("""
            dn: cn=Array Zero,ou=people,dc=planetexpress,dc=com
            objectClass: inetOrgPerson
            cn: Array Zero
            sn: ArrayExpression
            """);
        inputs.add("""
            dn: cn=Array One,ou=people,dc=planetexpress,dc=com
            objectClass: inetOrgPerson
            cn: Array One
            sn: ArrayExpression
            """);

        RunContext baseContext = Commons.getRunContext(inputs, ".ldif", storageInterface, runContextFactory);
        List<URI> urisList = new ArrayList<>();
        urisList.add(URI.create(baseContext.render("{{file0}}")));
        urisList.add(URI.create(baseContext.render("{{file1}}")));

        RunContext runContext = runContextFactory.of(Map.of("urisList", urisList));
        Add task = Add.builder()
            .hostname(Property.ofValue(ldap.getHost()))
            .port(Property.ofValue(ldap.getMappedPort(Commons.EXPOSED_PORTS[0])))
            .userDn(Property.ofValue(Commons.USER))
            .password(Property.ofValue(Commons.PASS))
            .inputs(Property.ofExpression("{{ urisList }}"))
            .build();

        assertThat(task.run(runContext), nullValue());

        for (String cn : List.of("Array Zero", "Array One")) {
            Search check = Commons.makeSearchTask("(cn=" + cn + ")", "dc=planetexpress,dc=com", Arrays.asList("cn"), ldap);
            Search.Output result = check.run(runContext);
            Commons.assertResult("dn: cn=" + cn + ",ou=people,dc=planetexpress,dc=com\ncn: " + cn + "\n", result.getUri(), storageInterface);
        }
    }
}
