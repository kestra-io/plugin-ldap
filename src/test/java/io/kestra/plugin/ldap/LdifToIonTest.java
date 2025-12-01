package io.kestra.plugin.ldap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
public class LdifToIonTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void basic_test() throws Exception {
        List<String> inputs = new ArrayList<>();
        List<String> expectations = new ArrayList<>();

        // specific test values :
        inputs.add("""
            dn: cn=bob@orga.com,ou=diffusion_list,dc=orga,dc=com
            description: Some description 1
            someOtherAttribute: perhaps
            description: Melusine lover
            someOtherAttribute: perhapsAgain

            dn: cn=tony@orga.com,ou=diffusion_list,dc=orga,dc=com
            description: Some description 2
            description: Melusine lover as well
            someOtherAttribute: perhaps 2
            someOtherAttribute: perhapsAgain 2""");// fst file
        inputs.add("""
            dn: cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com
            description: Some description 3
            someOtherAttribute: Melusine lover, obviously

            dn: cn=yennefer@orga.com,ou=diffusion_list,dc=orga,dc=com
            description: Some description 2
            someOtherAttribute: Loves herself""");// scnd file
        inputs.add("""
            dn: cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com
            changetype: modify
            delete: description
            description: Some description 3
            -
            add: description
            description: Some description 4
            -
            replace: someOtherAttribute
            someOtherAttribute: Loves herself more
            -
            increment: uidNumber
            uidNumber: -4
            -

            dn: cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com
            changetype: delete

            dn: cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com
            changetype: modrdn
            newrdn: cn=triss@orga.com
            deleteoldrdn: 0
            newsuperior: ou=expeople,dc=example,dc=com

            dn: cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com
            changetype: moddn
            newrdn: cn=triss@orga.com
            deleteoldrdn: 1
            newsuperior: ou=expeople,dc=example,dc=com

            dn: cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com
            changetype: moddn
            newrdn: cn=triss@orga.com
            deleteoldrdn: 1

            dn: cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com
            changetype: add
            description: Some description 3
            someOtherAttribute: Melusine lover, obviously
            """);// third file, includes changeType
        expectations.add("""
            {dn:"cn=bob@orga.com,ou=diffusion_list,dc=orga,dc=com",attributes:{description:["Some description 1","Melusine lover"],someOtherAttribute:["perhaps","perhapsAgain"]}}
            {dn:"cn=tony@orga.com,ou=diffusion_list,dc=orga,dc=com",attributes:{description:["Some description 2","Melusine lover as well"],someOtherAttribute:["perhaps 2","perhapsAgain 2"]}}""");// fst file
        expectations.add("""
            {dn:"cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com",attributes:{description:["Some description 3"],someOtherAttribute:["Melusine lover, obviously"]}}
            {dn:"cn=yennefer@orga.com,ou=diffusion_list,dc=orga,dc=com",attributes:{description:["Some description 2"],someOtherAttribute:["Loves herself"]}}""");// scnd file
        expectations.add("""
            {dn:"cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com",changeType:"modify",modifications:[{operation:"DELETE",attribute:"description",values:["Some description 3"]},{operation:"ADD",attribute:"description",values:["Some description 4"]},{operation:"REPLACE",attribute:"someOtherAttribute",values:["Loves herself more"]},{operation:"INCREMENT",attribute:"uidNumber",values:["-4"]}]}
            {dn:"cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com",changeType:"delete"}
            {dn:"cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com",changeType:"moddn",newDn:{newrdn:"cn=triss@orga.com",deleteoldrdn:false,newsuperior:"ou=expeople,dc=example,dc=com"}}
            {dn:"cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com",changeType:"moddn",newDn:{newrdn:"cn=triss@orga.com",deleteoldrdn:true,newsuperior:"ou=expeople,dc=example,dc=com"}}
            {dn:"cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com",changeType:"moddn",newDn:{newrdn:"cn=triss@orga.com",deleteoldrdn:true}}
            {dn:"cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com",changeType:"add",attributes:{description:["Some description 3"],someOtherAttribute:["Melusine lover, obviously"]}}""");// third file
        /////////////////////////

        RunContext runContext = Commons.getRunContext(inputs, ".ldif", storageInterface, runContextFactory);
        LdifToIon task = LdifToIon.builder().inputs(Commons.makeKestraPebblesForXFiles(inputs.size())).build();
        LdifToIon.Output runOutput = task.run(runContext);
        Commons.assertFilesEq(runOutput.getUrisList(), expectations, storageInterface);
    }

    @Test
    void should_write_null_for_empty_attribute_values() throws Exception {
        // LDIF with empty attribute value
        String ldifInput = """
        dn: cn=john.doe,ou=users,dc=example,dc=com
        mail:
        givenName: John
        sn: Doe
        """;

        List<String> inputs = List.of(ldifInput);
        RunContext runContext = Commons.getRunContext(inputs, ".ldif", storageInterface, runContextFactory);
        LdifToIon task = LdifToIon.builder().inputs(Commons.makeKestraPebblesForXFiles(inputs.size())).build();
        LdifToIon.Output runOutput = task.run(runContext);

        String ionResult;
        try (var is = storageInterface.get(TenantService.MAIN_TENANT, null, runOutput.getUrisList().getFirst())) {
            ionResult = new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }

        // ✅ Expected behavior after fix: mail should be written as null
        assertTrue(
            ionResult.contains("mail:[null]"),
            "Empty LDAP attributes should be written as null instead of empty string."
        );

        // Ensure other attributes are still serialized correctly
        assertTrue(
            ionResult.contains("givenName:[\"John\"]") && ionResult.contains("sn:[\"Doe\"]"),
            "Non-empty attributes should remain unchanged."
        );
    }
}
