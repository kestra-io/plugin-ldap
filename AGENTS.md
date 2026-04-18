# Kestra LDAP Plugin

## What

- Provides plugin components under `io.kestra.plugin.ldap`.
- Includes classes such as `Delete`, `Search`, `Add`, `Utils`.

## Why

- This plugin integrates Kestra with LDAP.
- It provides tasks that query and modify LDAP directories.

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
