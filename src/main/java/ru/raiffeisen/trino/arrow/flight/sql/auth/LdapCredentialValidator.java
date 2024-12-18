package ru.raiffeisen.trino.arrow.flight.sql.auth;

import static org.slf4j.LoggerFactory.getLogger;
import static ru.raiffeisen.trino.arrow.flight.sql.auth.SslUtils.createSSLContext;
import static ru.raiffeisen.trino.arrow.flight.sql.logging.LoggingUtils.*;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.*;
import javax.naming.AuthenticationException;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;
import javax.net.ssl.SSLContext;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import ru.raiffeisen.trino.arrow.flight.sql.config.LdapConfiguration;
import ru.raiffeisen.trino.arrow.flight.sql.config.SslConfiguration;

public class LdapCredentialValidator implements BasicCallHeaderAuthenticator.CredentialValidator {
  private static final Logger log = getLogger(LdapCredentialValidator.class);
  private static final String USER_ENV = "${USER}";

  private final String ldapUrl;
  private final String ldapDomain;
  private final String ldapReferral;
  private final int ldapTimout;
  private final int ldapRetryCnt;
  private final int ldapRetryInterval;

  private final String ldapSearchDn1;
  private final String ldapSearchDn2;
  private final String ldapSearchDn3;
  private final String ldapSearchFilter;
  private final String ldapSearchAttr;
  private final String ldapAuthRole;

  private final SSLContext sslContext;

  public LdapCredentialValidator(LdapConfiguration ldapConfig, SslConfiguration sslConfig)
      throws GeneralSecurityException, IOException, ConfigurationException {

    // check for missing LDAP configurations:
    final HashMap<String, String> requiredConfigs = new HashMap<>();
    requiredConfigs.put("ldap.provider", ldapConfig.LDAP_PROVIDER);
    requiredConfigs.put("ldap.domain", ldapConfig.LDAP_DOMAIN);
    requiredConfigs.put("ldap.search.dn1", ldapConfig.LDAP_SEARCH_DN1);
    requiredConfigs.put("ldap.search.filter", ldapConfig.LDAP_SEARCH_FILTER);
    requiredConfigs.put("ldap.search.attribute", ldapConfig.LDAP_SEARCH_ATTR);
    requiredConfigs.put("ldap.auth.role", ldapConfig.LDAP_AUTH_ROLE);
    final List<String> missedConfigs =
        requiredConfigs.entrySet().stream()
            .filter(kv -> kv.getValue() == null)
            .map(Map.Entry::getKey)
            .toList();

    if (!missedConfigs.isEmpty()) {
      throw new ConfigurationException(
          "In order to use LDAP authorization following missing configurations must set: "
              + missedConfigs);
    }

    this.ldapUrl = ldapConfig.LDAP_PROVIDER;
    this.ldapDomain = ldapConfig.LDAP_DOMAIN;
    this.ldapReferral = ldapConfig.LDAP_REFERRAL;
    this.ldapTimout = ldapConfig.LDAP_TIMEOUT;
    this.ldapRetryCnt = ldapConfig.LDAP_RETRY_CNT;
    this.ldapRetryInterval = ldapConfig.LDAP_RETRY_INTERVAL;

    this.ldapSearchDn1 = ldapConfig.LDAP_SEARCH_DN1;
    this.ldapSearchDn2 = ldapConfig.LDAP_SEARCH_DN2;
    this.ldapSearchDn3 = ldapConfig.LDAP_SEARCH_DN3;
    this.ldapSearchFilter = ldapConfig.LDAP_SEARCH_FILTER;
    this.ldapSearchAttr = ldapConfig.LDAP_SEARCH_ATTR;
    this.ldapAuthRole = ldapConfig.LDAP_AUTH_ROLE;

    this.sslContext =
        createSSLContext(
            sslConfig.KEYSTORE_PATH,
            sslConfig.KEYSTORE_PASSWORD,
            sslConfig.KEYSTORE_TYPE,
            sslConfig.TRUSTSTORE_PATH,
            sslConfig.TRUSTSTORE_PASSWORD,
            sslConfig.TRUSTSTORE_TYPE);
  }

  private Properties setLdapProperties(String distinguishedUsername, String password) {
    Properties props = new Properties();

    LdapSslSocketFactory.setSslContextForCurrentThread(sslContext);

    props.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
    props.put(Context.PROVIDER_URL, ldapUrl);
    props.put(Context.SECURITY_AUTHENTICATION, "simple");
    props.put(Context.REFERRAL, ldapReferral);
    props.put(Context.SECURITY_PRINCIPAL, distinguishedUsername);
    props.put(Context.SECURITY_CREDENTIALS, password);
    props.put("com.sun.jndi.ldap.read.timeout", String.valueOf(ldapTimout));
    props.put("com.sun.jndi.ldap.connect.timeout", String.valueOf(ldapTimout));
    props.put("java.naming.ldap.factory.socket", LdapSslSocketFactory.class.getName());

    return props;
  }

  private DirContext getLdapContext(String user, String password, int attempt)
      throws NamingException {
    String distinguishedUsername = ldapDomain + "\\" + user.strip().toLowerCase();
    Properties ldapProps = setLdapProperties(distinguishedUsername, password);
    try {
      return new InitialDirContext(ldapProps);
    } catch (AuthenticationException e) {
      log.error("Password validation failed for user DN [{}]", distinguishedUsername);
      throw new AuthenticationException("Invalid credentials: " + e.getMessage());
    } catch (NamingException ne) {
      log.error("Naming exception for attempt [{}].", attempt + 1);
      if (attempt < ldapRetryCnt) {
        try {
          Thread.sleep(ldapRetryInterval);
        } catch (InterruptedException ex) {
          log.error(ex.getMessage(), ex);
        }
        return getLdapContext(distinguishedUsername, password, attempt + 1);
      } else {
        throw ne;
      }
    }
  }

  private String substituteUser(String filter, String user) {
    return filter.replace(USER_ENV, user.strip().toLowerCase());
  }

  private NamingEnumeration<SearchResult> searchUserGroups(String user, DirContext ctx)
      throws NamingException {
    SearchControls searchControls = new SearchControls();
    searchControls.setReturningAttributes(new String[] {ldapSearchAttr});
    searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

    // first searching in primary DN:
    log.debug("Searching for user roles in primary DN.");
    NamingEnumeration<SearchResult> search1 =
        ctx.search(ldapSearchDn1, substituteUser(ldapSearchFilter, user), searchControls);
    if (search1.hasMore()) {
      return search1;
    }

    // if primary DN return empty result, try secondary one:
    log.debug("No roles found in primary DN");
    if (ldapSearchDn2 == null) {
      log.debug("Secondary DN is not configured. Returning empty search result.");
      return search1;
    }

    log.debug("Searching for user roles in secondary DN.");
    NamingEnumeration<SearchResult> search2 =
        ctx.search(ldapSearchDn2, substituteUser(ldapSearchFilter, user), searchControls);
    if (search2.hasMore()) {
      return search2;
    }

    // finally search in third DN and return its result:
    log.debug("No roles found in secondary DN");
    if (ldapSearchDn3 == null) {
      log.debug("Tertiary DN is not configured. Returning empty search result.");
      return search2;
    }

    log.debug("Searching for user roles in tertiary DN.");
    return ctx.search(ldapSearchDn3, substituteUser(ldapSearchFilter, user), searchControls);
  }

  private HashSet<String> getUserGroups(String user, DirContext ctx) throws NamingException {
    HashSet<String> groups = new HashSet<>();
    try {
      NamingEnumeration<SearchResult> searchResult = searchUserGroups(user, ctx);
      try {
        if (!searchResult.hasMore()) {
          log.debug("User roles are not found within configured DNs.");
          return groups;
        }

        Attribute groupAttr = searchResult.next().getAttributes().get(ldapSearchAttr);
        if (groupAttr == null) {
          log.debug("User is not a member of any group");
          return groups;
        }

        log.debug("Group details: [{}]", groupAttr);
        for (int i = 0; i < groupAttr.size(); i++) {
          groups.add(groupAttr.get(i).toString());
        }
      } finally {
        searchResult.close();
      }
    } finally {
      ctx.close();
    }
    return groups;
  }

  private boolean isUserAuthorised(HashSet<String> userGroups) {
    return userGroups.contains(ldapAuthRole);
  }

  @Override
  public CallHeaderAuthenticator.AuthResult validate(String user, String password) {
    setLogCtx("ldap_auth", user);
    try {
      DirContext ctx = getLdapContext(user, password, 0);
      log.debug("User is authenticated. Searching for user role membership...");
      HashSet<String> userGroups = getUserGroups(user, ctx);
      log.debug("User roles collected. Checking if user is authorized.");
      if (isUserAuthorised(userGroups)) {
        log.info("User is authorized.");
        return new ProxyAuthResult(user, password);
      } else {
        log.error("User role membership does not contain required role to be authorized.");
        throw CallStatus.UNAUTHENTICATED.toRuntimeException();
      }
    } catch (NamingException e) {
      log.error(e.getMessage(), e);
      throw CallStatus.UNAUTHENTICATED.toRuntimeException();
    } finally {
      clearLogCtx();
    }
  }
}
