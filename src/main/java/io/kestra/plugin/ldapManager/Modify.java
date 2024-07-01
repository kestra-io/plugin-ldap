package io.kestra.plugin.ldapManager;

import com.unboundid.ldap.sdk.*;
import com.unboundid.ldap.sdk.controls.PostReadRequestControl;
import com.unboundid.ldap.sdk.controls.PostReadResponseControl;
import com.unboundid.util.ssl.SSLUtil;
import com.unboundid.util.ssl.TrustAllTrustManager;

import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;

import javax.net.ssl.SSLSocketFactory;
import java.util.List;

public class Modify extends LdapConnection implements RunnableTask<VoidOutput> {

    public VoidOutput run(RunContext runContext) throws Exception {

        // Establish a connection to the LDAP server
        LDAPConnection connection = this.getLdapConnection();
        try {
            // Specify the DN of the entry to modify
            String entryDN = "uid=user1,ou=users";

            // Create the modifications
            Modification modification1 = new Modification(ModificationType.REPLACE, "givenName", "NewGivenName");
            Modification modification2 = new Modification(ModificationType.ADD, "description", "This is a test description");

            // Create the modify request
            ModifyRequest modifyRequest = new ModifyRequest(entryDN, modification1, modification2);

            // Optionally, add a control to get the modified entry after the modification
            modifyRequest.addControl(new PostReadRequestControl("givenName", "description"));

            // Perform the modify operation
            LDAPResult modifyResult = connection.modify(modifyRequest);

            // Check the result of the modify operation
            if (modifyResult.getResultCode() == ResultCode.SUCCESS) {
                System.out.println("Entry modified successfully.");
                // Process the response control if present
                modifyResult.
                List<Control> responseControls = modifyResult.getResponseControls();
                for (Control control : responseControls) {
                    if (control instanceof PostReadResponseControl) {
                        SearchResultEntry modifiedEntry = ((PostReadResponseControl) control).getSearchResultEntry();
                        System.out.println("Modified entry: " + modifiedEntry.toLDIFString());
                    }
                }
            } else {
                System.err.println("Failed to modify entry: " + modifyResult.getDiagnosticMessage());
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}
