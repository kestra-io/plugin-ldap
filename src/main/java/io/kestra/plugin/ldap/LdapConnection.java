package io.kestra.plugin.ldap;

import java.security.GeneralSecurityException;
import java.util.List;

import javax.net.ssl.SSLSocketFactory;

import org.slf4j.Logger;

import com.unboundid.ldap.sdk.BindRequest;
import com.unboundid.ldap.sdk.GSSAPIBindRequest;
import com.unboundid.ldap.sdk.GSSAPIBindRequestProperties;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.SASLQualityOfProtection;
import com.unboundid.ldap.sdk.SimpleBindRequest;
import com.unboundid.util.ssl.SSLUtil;
import com.unboundid.util.ssl.TrustAllTrustManager;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.client.configurations.SslOptions;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@Getter
@NoArgsConstructor
public abstract class LdapConnection extends Task {
    @Schema(
        title = "Hostname",
        description = "Hostname for connection."
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> hostname;

    @Schema(
        title = "Port",
        description = "A whole number describing the port for connection."
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<Integer> port;

    @Schema(
        title = "User",
        description = "Username for connection."
    )
    @NotNull
    @PluginProperty(group = "main", secret = true)
    protected Property<String> userDn;

    @Schema(
        title = "Password",
        description = "User password for connection."
    )
    @NotNull
    @PluginProperty(group = "main", secret = true)
    protected Property<String> password;

    @Schema(
        title = "Authentication method",
        description = "Authentication method to use with the LDAP server.",
        allowableValues = { "simple", "gssapi" }
    )
    @Builder.Default
    @PluginProperty(group = "connection")
    protected Property<String> authMethod = Property.ofValue("simple");

    @Schema(
        title = "Kerberos key distribution center",
        description = """
            Needed for GSSAPI authentication method.
            If this is not provided, an attempt will be made to determine the appropriate value from the system configuration."""
    )
    @PluginProperty(group = "advanced")
    protected Property<String> kdc;

    @Schema(
        title = "Realm",
        description = """
            Needed for GSSAPI authentication method.
            If this is not provided, an attempt will be made to determine the appropriate value from the system configuration."""
    )
    @PluginProperty(group = "advanced")
    protected Property<String> realm;

    @Schema(
        title = "SSL Configuration",
        description = "Configure SSL/LDAPS connection parameters."
    )
    @PluginProperty(group = "connection")
    protected SslOptions sslOptions;

    @Schema(
        title = "saslAllowedQoP",
        description = """
            Used for GSSAPI authentication method only.
            The list of allowed qualities of protection that may be used for communication
            after authentication has completed, ordered from most preferred to least preferred.

            By default, the full list is allowed, sorted from the most secure to the least secure:
                - AUTH_CONF # This ensures that third-party observers will not be able to decipher communication between the client and server (i.e., that the communication will be encrypted).
                - AUTH_INT     # This ensure that the communication cannot be altered in an undetectable manner.
                - AUTH            # Only authentication is to be performed, with no integrity or confidentiality protection for subsequent communication.
            """
    )
    @Builder.Default
    @PluginProperty(group = "connection")
    private Property<List<SASLQualityOfProtection>> saslAllowedQoP =
        Property.ofValue(List.of(
            SASLQualityOfProtection.AUTH_CONF,
            SASLQualityOfProtection.AUTH_INT,
            SASLQualityOfProtection.AUTH
        ));

    /**
     * Opens a connection with user provided informations.
     *
     * @param runContext : A context that may evaluate pebble expressions regarding the connection informations.
     * @return A new LDAPConnection to perform action with the LDAP server.
     */
    protected LDAPConnection getLdapConnection(RunContext runContext) throws Exception, LDAPException, IllegalVariableEvaluationException {
        Logger logger = runContext.logger();

        String authMethodProperty = runContext.render(authMethod).as(String.class).orElse("simple");
        final boolean trustAllCertificates = sslOptions != null && runContext.render(sslOptions.getInsecureTrustAllCertificates()).as(Boolean.class).orElse(false);

        try {
            LDAPConnection connection = createLdapConnection(
                runContext.render(hostname).as(String.class).orElseThrow(),
                (runContext.render(port).as(Integer.class).orElseThrow()),
                trustAllCertificates
            );
            BindRequest bindRequest;

            switch (authMethodProperty) {
                case "simple":
                    bindRequest = new SimpleBindRequest(
                        runContext.render(userDn).as(String.class).orElseThrow(),
                        runContext.render(password).as(String.class).orElseThrow()
                    );
                    break;
                case "gssapi":
                    String kdcProperty = runContext.render(kdc).as(String.class).orElse(null);
                    String realmProperty = runContext.render(realm).as(String.class).orElse(null);

                    GSSAPIBindRequestProperties gssapiProperties = new GSSAPIBindRequestProperties(
                        runContext.render(userDn).as(String.class).orElse(null),
                        runContext.render(password).as(String.class).orElse(null)
                    );

                    if (kdcProperty != null) {
                        gssapiProperties.setKDCAddress(kdcProperty);
                    }
                    if (realmProperty != null) {
                        gssapiProperties.setRealm(realmProperty);
                    }

                    gssapiProperties.setAllowedQoP(
                        runContext.render(saslAllowedQoP).asList(SASLQualityOfProtection.class)
                    );
                    gssapiProperties.setRefreshKrb5Config(true);
                    bindRequest = new GSSAPIBindRequest(gssapiProperties);
                    break;
                default:
                    throw new IllegalArgumentException(String.format("Invalid authentication method \"%s\".", authMethodProperty));
            }
            connection.bind(bindRequest);
            return connection;
        } catch (LDAPException e) {
            logger.error(
                """
                LDAP connection/bind failed
                - resultCode        : {}
                - diagnosticMessage : {}
                - resultString      : {}
                - message           : {}
                """,
                String.valueOf(e.getResultCode()),
                sanitize(e.getDiagnosticMessage()),
                sanitize(e.getResultString()),
                sanitize(e.getMessage())
            );
            throw new LDAPException( e.getResultCode(), sanitize(e.getMessage()), e );
        }
    }

    // Remove control characters from the string, as they can cause issues when logging or displaying error messages with POSTGRES BACKEND exceptions.
    static String sanitize(String s) {
        if (s == null) {
            return null;
        }
        return s
            .replace("\u0000", "")
            .replaceAll("[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]", "?");
    }

    public LDAPConnection createLdapConnection(String hostname, int port, boolean trustAllCertificates) throws LDAPException, GeneralSecurityException {
        LDAPConnection connection;

        if (trustAllCertificates) {
            SSLUtil sslUtil = new SSLUtil(new TrustAllTrustManager());
            SSLSocketFactory sslSocketFactory = sslUtil.createSSLSocketFactory();
            connection = new LDAPConnection(sslSocketFactory, hostname, port);
        } else {
            connection = new LDAPConnection(hostname, port);
        }
        return connection;
    }
}
