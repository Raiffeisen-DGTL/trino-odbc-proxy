package ru.raiffeisen.trino.arrow.flight.sql.config;

import org.apache.commons.configuration2.Configuration;

public final class LdapConfiguration {

  public final String LDAP_PROVIDER;
  public final String LDAP_DOMAIN;
  public final String LDAP_REFERRAL;
  public final int LDAP_TIMEOUT;
  public final int LDAP_RETRY_CNT;
  public final int LDAP_RETRY_INTERVAL;

  public final String LDAP_SEARCH_DN1;
  public final String LDAP_SEARCH_DN2;
  public final String LDAP_SEARCH_DN3;
  public final String LDAP_SEARCH_FILTER;
  public final String LDAP_SEARCH_ATTR;
  public final String LDAP_AUTH_ROLE;

  public LdapConfiguration(Configuration config) {
    LDAP_PROVIDER = config.getString("ldap.provider", null);
    LDAP_DOMAIN = config.getString("ldap.domain", null);
    LDAP_REFERRAL = config.getString("ldap.referral", "follow");
    LDAP_TIMEOUT = config.getInt("ldap.timeout", 5000);
    LDAP_RETRY_CNT = config.getInt("ldap.retries.count", 3);
    LDAP_RETRY_INTERVAL = config.getInt("ldap.retries.interval", 5000);
    LDAP_SEARCH_DN1 = config.getString("ldap.search.dn1", null);
    LDAP_SEARCH_DN2 = config.getString("ldap.search.dn2", null);
    LDAP_SEARCH_DN3 = config.getString("ldap.search.dn3", null);
    LDAP_SEARCH_FILTER = config.getString("ldap.search.filter", null);
    LDAP_SEARCH_ATTR = config.getString("ldap.search.attribute", null);
    LDAP_AUTH_ROLE = config.getString("ldap.auth.role", null);
  }
}
