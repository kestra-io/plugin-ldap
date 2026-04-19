# Kestra LDAP Plugin

## What

- Provides plugin components under `io.kestra.plugin.ldap`.
- Includes classes such as `Delete`, `Search`, `Add`, `Utils`.

## Why

- What user problem does this solve? Teams need to query and modify LDAP directories from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps LDAP steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on LDAP.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `ldap`

### Key Plugin Classes

- `io.kestra.plugin.ldap.Add`
- `io.kestra.plugin.ldap.Delete`
- `io.kestra.plugin.ldap.IonToLdif`
- `io.kestra.plugin.ldap.LdifToIon`
- `io.kestra.plugin.ldap.Modify`
- `io.kestra.plugin.ldap.Search`

### Project Structure

```
plugin-ldap/
├── src/main/java/io/kestra/plugin/ldap/
├── src/test/java/io/kestra/plugin/ldap/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
