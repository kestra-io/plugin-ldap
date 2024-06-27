package io.kestra.plugin.ldapManager;

import org.testcontainers.utility.DockerImageName;

final class Commons {

    /** Docker image ref : https://hub.docker.com/r/rroemhild/test-openldap */
    public static final DockerImageName LDAP_IMAGE = DockerImageName.parse("rroemhild/test-openldap:2.1");
    /**
     * @value [0] -> unsecure port
     * @value [1] -> SSL secure port
     */
    public static final Integer[] EXPOSED_PORTS = {10389, 10636};
    public static final String USER = "cn=admin,dc=planetexpress,dc=com";
    public static final String PASS = "GoodNewsEveryone";
}
