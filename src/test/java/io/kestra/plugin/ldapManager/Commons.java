package io.kestra.plugin.ldapManager;

import org.testcontainers.utility.DockerImageName;

final class Commons {
    public static final DockerImageName LDAP_IMAGE = DockerImageName.parse("rroemhild/test-openldap:2.1");
    public static final Integer[] EXPOSED_PORTS = {10389, 10636};
    public static final String USER = "cn=admin,dc=planetexpress,dc=com";
    public static final String PASS = "GoodNewsEveryone";
}
