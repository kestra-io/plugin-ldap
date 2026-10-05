package io.kestra.plugin.ldap;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.serializers.JacksonMapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Pure deserialization tests for the inputs of Add, Delete, IonToLdif and LdifToIon.
 * No LDAP container is needed: these verify that the YAML parser
 * can handle both a bare Pebble expression and a list literal, as ModifyInputsParseTest does for Modify.
 */
class InputsParseTest {

    private static final ObjectMapper YAML = JacksonMapper.ofYaml();

    private static final String CONNECTION = """
        hostname: localhost
        port: 389
        userDn: cn=admin,dc=example,dc=com
        password: secret
        """;

    private static final String BARE_EXPRESSION = """
        inputs: "{{ outputs.convert_to_ldif.urisList }}"
        """;

    private static final String LIST_LITERAL = """
        inputs:
          - "{{ outputs.task_a.uri }}"
          - "{{ outputs.task_b.uri }}"
        """;

    @Test
    void add_bare_expression_deserializes() throws Exception {
        Add task = YAML.readValue(CONNECTION + BARE_EXPRESSION, Add.class);
        assertThat("inputs property should not be null", task.getInputs(), notNullValue());
    }

    @Test
    void add_list_literal_deserializes() throws Exception {
        Add task = YAML.readValue(CONNECTION + LIST_LITERAL, Add.class);
        assertThat("inputs property should not be null", task.getInputs(), notNullValue());
    }

    @Test
    void delete_bare_expression_deserializes() throws Exception {
        Delete task = YAML.readValue(CONNECTION + BARE_EXPRESSION, Delete.class);
        assertThat("inputs property should not be null", task.getInputs(), notNullValue());
    }

    @Test
    void delete_list_literal_deserializes() throws Exception {
        Delete task = YAML.readValue(CONNECTION + LIST_LITERAL, Delete.class);
        assertThat("inputs property should not be null", task.getInputs(), notNullValue());
    }

    @Test
    void ionToLdif_bare_expression_deserializes() throws Exception {
        IonToLdif task = YAML.readValue(BARE_EXPRESSION, IonToLdif.class);
        assertThat("inputs property should not be null", task.getInputs(), notNullValue());
    }

    @Test
    void ionToLdif_list_literal_deserializes() throws Exception {
        IonToLdif task = YAML.readValue(LIST_LITERAL, IonToLdif.class);
        assertThat("inputs property should not be null", task.getInputs(), notNullValue());
    }

    @Test
    void ldifToIon_bare_expression_deserializes() throws Exception {
        LdifToIon task = YAML.readValue(BARE_EXPRESSION, LdifToIon.class);
        assertThat("inputs property should not be null", task.getInputs(), notNullValue());
    }

    @Test
    void ldifToIon_list_literal_deserializes() throws Exception {
        LdifToIon task = YAML.readValue(LIST_LITERAL, LdifToIon.class);
        assertThat("inputs property should not be null", task.getInputs(), notNullValue());
    }
}
