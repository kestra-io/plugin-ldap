package io.kestra.plugin.ldapManager;

import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Task;

import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

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
        description = "Port for connection."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected Integer port;

    @Schema(
        title = "User DN",
        description = "User DN for connection."
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

    /**
     * Opens a connection with user provided informations.
     * @return A new LDAPConnection to perform action with the LDAP server.
     * @throws LDAPException
     */
    protected LDAPConnection getLdapConnection() throws LDAPException {
        return new LDAPConnection(hostname, port, userDn, password);
    }
}