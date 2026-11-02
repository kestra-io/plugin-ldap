package io.kestra.plugin.ldap;

import java.security.GeneralSecurityException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.SASLQualityOfProtection;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@TestInstance(value = Lifecycle.PER_CLASS)
public class LdapConnectionTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void sanitize_null_returns_null() {
        assertThat(LdapConnection.sanitize(null), is(nullValue()));
    }

    @Test
    void sanitize_removes_null_bytes() {
        var result = LdapConnection.sanitize("hello\u0000world");
        assertThat(result, is("helloworld"));
    }

    @Test
    void sanitize_replaces_control_chars_with_question_mark() {
        var input = "a\u0001b\u0008c\u000Bd\u000Ce\u000Ef\u001Fg";
        var result = LdapConnection.sanitize(input);
        assertThat(result, is("a?b?c?d?e?f?g"));
    }

    @Test
    void sanitize_preserves_normal_text() {
        var input = "Hello, World! This is a normal string with numbers 12345.";
        var result = LdapConnection.sanitize(input);
        assertThat(result, is(input));
    }

    @Test
    void sanitize_preserves_newlines_and_tabs() {
        var input = "line1\nline2\rline3\tindented";
        var result = LdapConnection.sanitize(input);
        assertThat(result, is(input));
    }

    @Test
    void sanitize_handles_mixed_content() {
        var input = "Error\u0000: invalid \u0001entry\nDN: cn=test\u001F";
        var result = LdapConnection.sanitize(input);
        assertThat(result, is("Error: invalid ?entry\nDN: cn=test?"));
    }

    @Test
    void saslAllowedQoP_default_contains_all_three_qop_levels() throws Exception {
        var task = Search.builder()
            .id("test")
            .type(Search.class.getName())
            .build();

        RunContext runContext = runContextFactory.of();
        var rendered = runContext.render(task.getSaslAllowedQoP()).asList(SASLQualityOfProtection.class);

        var expected = List.of(
            SASLQualityOfProtection.AUTH_CONF,
            SASLQualityOfProtection.AUTH_INT,
            SASLQualityOfProtection.AUTH
        );

        assertThat(rendered, is(expected));
    }

    @Test
    void failed_bind_closes_the_connection() throws Exception {
        var config = new InMemoryDirectoryServerConfig("dc=example,dc=com");
        config.addAdditionalBindCredentials("cn=admin", "secret");
        var server = new InMemoryDirectoryServer(config);
        server.startListening();

        try {
            var opened = new AtomicReference<LDAPConnection>();
            var task = new Search() {
                {
                    hostname = Property.ofValue("localhost");
                    port = Property.ofValue(server.getListenPort());
                    userDn = Property.ofValue("cn=admin");
                    password = Property.ofValue("wrong");
                }

                @Override
                public LDAPConnection createLdapConnection(String hostname, int port, boolean trustAllCertificates) throws LDAPException, GeneralSecurityException {
                    var connection = super.createLdapConnection(hostname, port, trustAllCertificates);
                    opened.set(connection);
                    return connection;
                }
            };

            RunContext runContext = runContextFactory.of();
            assertThrows(LDAPException.class, () -> task.getLdapConnection(runContext));

            // The caller never receives the connection, so nobody else can close it
            assertThat(opened.get().isConnected(), is(false));
        } finally {
            server.shutDown(true);
        }
    }
}
