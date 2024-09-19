package io.kestra.plugin.ldap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;

import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

@KestraTest
public class IonToLdifTest {
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
            {dn:"cn=bob@orga.com,ou=diffusion_list,dc=orga,dc=com",attributes:{description:["Some description 1","Melusine lover"],someOtherAttribute:["perhaps","perhapsAgain"]}}
            {dn:"cn=tony@orga.com,ou=diffusion_list,dc=orga,dc=com",attributes:{description:["Some description 2","Melusine lover as well"],someOtherAttribute:["perhaps 2",base64::"TGlzdGUgZCfDVVQ=","perhapsAgain 2"]}}""");// fst file
        inputs.add("""
            {dn:"cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com",attributes:{description:["Some description 3"],someOtherAttribute:["Melusine lover, obviously"]}}
            {dn:"cn=yennefer@orga.com,ou=diffusion_list,dc=orga,dc=com",attributes:{description:["Some description 2"],someOtherAttribute:["Loves herself"]}}""");// scnd file
        inputs.add("""
            {dn:"cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com",changeType:"modify",modifications:[{operation:"DELETE",attribute:"description",values:["Some description 3"]},{operation:"ADD",attribute:"description",values:["Some description 4"]},{operation:"REPLACE",attribute:"someOtherAttribute",values:["Loves herself more"]},{operation:"INCREMENT",attribute:"uidNumber",values:["-4"]}]}            {dn:"cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com",changeType:"delete"}
            {dn:"cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com",changeType:"moddn",newDn:{newrdn:"cn=triss@orga.com",deleteoldrdn:false,newsuperior:"ou=expeople,dc=example,dc=com"}}
            {dn:"cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com",changeType:"moddn",newDn:{newrdn:"cn=triss@orga.com",deleteoldrdn:true,newsuperior:"ou=expeople,dc=example,dc=com"}}
            {dn:"cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com",changeType:"moddn",newDn:{newrdn:"cn=triss@orga.com",deleteoldrdn:true}}
            {dn:"cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com",changeType:"add",attributes:{description:[base64::"TGlzdGUgZCfDVVQ="],someOtherAttribute:["Melusine lover, obviously"]}}""");// third file, includes changeType
        expectations.add("""
            dn: cn=bob@orga.com,ou=diffusion_list,dc=orga,dc=com
            description: Some description 1
            description: Melusine lover
            someOtherAttribute: perhaps
            someOtherAttribute: perhapsAgain

            dn: cn=tony@orga.com,ou=diffusion_list,dc=orga,dc=com
            description: Some description 2
            description: Melusine lover as well
            someOtherAttribute: perhaps 2
            someOtherAttribute:: TGlzdGUgZCfDVVQ=
            someOtherAttribute: perhapsAgain 2

            """);// fst file
        expectations.add("""
            dn: cn=triss@orga.com,ou=diffusion_list,dc=orga,dc=com
            description: Some description 3
            someOtherAttribute: Melusine lover, obviously

            dn: cn=yennefer@orga.com,ou=diffusion_list,dc=orga,dc=com
            description: Some description 2
            someOtherAttribute: Loves herself

            """);// scnd file
        expectations.add("""
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
            changetype: moddn
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
            description:: TGlzdGUgZCfDVVQ=
            someOtherAttribute: Melusine lover, obviously

            """);// third file
        /////////////////////////

        RunContext runContext = Commons.getRunContext(inputs, ".ion", storageInterface, runContextFactory);
        IonToLdif task = IonToLdif.builder().inputs(Commons.makeKestraPebblesForXFiles(inputs.size())).build();
        IonToLdif.Output runOutput = task.run(runContext);
        Commons.assertFilesEq(runOutput.getUrisList(), expectations, storageInterface);
    }
}
