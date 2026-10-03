package io.kestra.plugin.ldap;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.serializers.JacksonMapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Pure deserialization tests for Modify.inputs.
 * No LDAP container is needed — these verify that the YAML parser
 * can handle both a bare Pebble expression and a list literal.
 */
class ModifyInputsParseTest {

    private static final ObjectMapper YAML = JacksonMapper.ofYaml();

    @Test
    void bare_expression_deserializes() throws Exception {
        String yaml = """
            hostname: localhost
            port: 389
            userDn: cn=admin,dc=example,dc=com
            password: secret
            inputs: "{{ outputs.convert_to_ldif.urisList }}"
            """;

        Modify task = YAML.readValue(yaml, Modify.class);
        assertThat("Modify task should deserialize from a bare expression", task, is(notNullValue()));
        assertThat("inputs property should not be null", task.getInputs(), is(notNullValue()));
    }

    @Test
    void list_literal_deserializes() throws Exception {
        String yaml = """
            hostname: localhost
            port: 389
            userDn: cn=admin,dc=example,dc=com
            password: secret
            inputs:
              - "{{ outputs.task_a.uri }}"
              - "{{ outputs.task_b.uri }}"
            """;

        Modify task = YAML.readValue(yaml, Modify.class);
        assertThat("Modify task should deserialize from a list literal", task, is(notNullValue()));
        assertThat("inputs property should not be null", task.getInputs(), is(notNullValue()));
    }
}
