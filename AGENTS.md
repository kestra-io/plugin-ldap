# Kestra LDAP Plugin

## What

Perform LDAP related tasks in Kestra. Exposes 6 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with LDAP, allowing orchestration of LDAP-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
