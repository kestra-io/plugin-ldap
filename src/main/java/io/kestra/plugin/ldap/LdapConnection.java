package io.kestra.plugin.ldap;

import com.unboundid.ldap.sdk.BindRequest;
import com.unboundid.ldap.sdk.GSSAPIBindRequest;
import com.unboundid.ldap.sdk.GSSAPIBindRequestProperties;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.SimpleBindRequest;

import com.unboundid.util.ssl.SSLUtil;
import com.unboundid.util.ssl.TrustAllTrustManager;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.client.configurations.SslOptions;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import org.slf4j.Logger;

import javax.net.ssl.SSLSocketFactory;
import java.security.GeneralSecurityException;

@SuperBuilder
@Getter
@NoArgsConstructor
public abstract class LdapConnection extends Task {
    @Schema(
        title = "Hostname",
        description = "Hostname for connection."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected String hostname;

    @Schema(
        title = "Port",
        description = "A whole number describing the port for connection."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected String port;

    @Schema(
        title = "User",
        description = "Username for connection."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected String userDn;

    @Schema(
        title = "Password",
        description = "User password for connection."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected String password;

    @Schema(
        title = "Authentication method",
        description = "Authentication method to use with the LDAP server.",
        allowableValues = {"simple", "gssapi"}
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @Default
    protected String authMethod = "simple";

    @Schema(
        title = "Kerberos key distribution center",
        description = """
            Needed for GSSAPI authentication method.
            If set, property realm must be set too.
            If this is not provided, an attempt will be made to determine the appropriate value from the system configuration."""
    )
    @PluginProperty(dynamic = true)
    protected String kdc;

    @Schema(
        title = "Realm",
        description = """
            Needed for GSSAPI authentication method.
            If set, property kdc must be set too.
            If this is not provided, an attempt will be made to determine the appropriate value from the system configuration."""
    )
    @PluginProperty(dynamic = true)
    protected String realm;

    @Schema(
        title = "SSL Configuration",
        description = "Configure SSL/LDAPS connection parameters."
    )
    protected SslOptions sslOptions;

    /**
     * Opens a connection with user provided informations.
     *
     * @param runContext : A context that may evaluate pebble expressions regarding the connection informations.
     * @return A new LDAPConnection to perform action with the LDAP server.
     */
    protected LDAPConnection getLdapConnection(RunContext runContext) throws Exception, LDAPException, IllegalVariableEvaluationException {
        Logger logger = runContext.logger();

        String authMethodProperty = runContext.render(authMethod);
        final boolean trustAllCertificates = sslOptions != null && runContext.render(sslOptions.getInsecureTrustAllCertificates()).as(Boolean.class).orElse(false);

        try {
            LDAPConnection connection = createLdapConnection(
                runContext.render(hostname),
                Integer.parseInt(runContext.render(port)),
                trustAllCertificates
            );
            BindRequest bindRequest;

            switch (authMethodProperty) {
                case "simple":
                    bindRequest = new SimpleBindRequest(
                        runContext.render(userDn),
                        runContext.render(password)
                    );
                    break;
                case "gssapi":
                    String kdcProperty = runContext.render(kdc);
                    String realmProperty = runContext.render(realm);

                    GSSAPIBindRequestProperties gssapiProperties = new GSSAPIBindRequestProperties(
                        runContext.render(userDn),
                        runContext.render(password)
                    );

                    if (
                        (kdcProperty == null && realmProperty != null) ||
                            (kdcProperty != null && realmProperty == null)
                    ) {
                        throw new IllegalArgumentException("Property kdc and realm both must be set or neither must be set.");
                    }

                    if (kdcProperty != null) {
                        gssapiProperties.setKDCAddress(kdcProperty);
                    }
                    if (realmProperty != null) {
                        gssapiProperties.setRealm(realmProperty);
                    }

                    bindRequest = new GSSAPIBindRequest(gssapiProperties);
                    break;
                default:
                    throw new IllegalArgumentException(String.format("Invalid authentication method \"%s\".", authMethodProperty));
            }
            connection.bind(bindRequest);
            return connection;
        } catch (LDAPException e) {
            logger.error("LDAP connextion error: {}", e.getResultString());
            throw e;
        }
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
