package ru.raiffeisen.trino.arrow.flight.sql.config;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;

import java.util.Properties;

import static org.slf4j.LoggerFactory.getLogger;
import static ru.raiffeisen.trino.arrow.flight.sql.logging.LoggingUtils.*;

public final class ServerConfiguration {
  private static final Logger log = getLogger(ServerConfiguration.class);
  private static final String ENV_PREFIX = "TF_";
  private static final String APP_PROPERTIES = "application.properties";
  private final TrinoConfiguration trinoConfiguration;
  private final FlightConfiguration flightConfiguration;
  private final LdapConfiguration ldapConfiguration;
  private final SslConfiguration sslConfiguration;

  public ServerConfiguration() throws ConfigurationException {
    Configuration config = readConfiguration();
    this.trinoConfiguration = new TrinoConfiguration(config);
    this.flightConfiguration = new FlightConfiguration(config);
    this.ldapConfiguration = new LdapConfiguration(config);
    this.sslConfiguration = new SslConfiguration(config);
  }

  public TrinoConfiguration getTrinoConfiguration() {
    return this.trinoConfiguration;
  }

  public FlightConfiguration getFlightConfiguration() {
    return this.flightConfiguration;
  }

  public LdapConfiguration getLdapConfiguration() {
    return this.ldapConfiguration;
  }

  public SslConfiguration getSslConfiguration() {
    return this.sslConfiguration;
  }

  private static String toPropertiesName(String env) {
    return env.substring(ENV_PREFIX.length()).toLowerCase().replace("_", ".");
  }

  private static void setFromEnv(Configuration config) {
    Properties envProps = System.getProperties();
    envProps.putAll(System.getenv());

    for (String key : envProps.stringPropertyNames()) {
      if (key.startsWith(ENV_PREFIX)) {
        config.setProperty(toPropertiesName(key), envProps.getProperty(key));
      }
    }
  }

  private Configuration readConfiguration() throws ConfigurationException {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    Configuration config = new Configurations().properties(classloader.getResource(APP_PROPERTIES));
    setFromEnv(config);
    return config;
  }
  
  public void logConfig() {
    setLogCtxEvent("configuration");
    
    // FLIGHT CONFIGURATION:
    log.info("flight.host = {}", this.flightConfiguration.FLIGHT_HOST);
    log.info("flight.port = {}", this.flightConfiguration.FLIGHT_PORT);
    log.info("flight.ssl = {}", this.flightConfiguration.FLIGHT_SSL);
    log.info("flight.batch.size = {}", this.flightConfiguration.FLIGHT_BATCH_SIZE);
    log.info("flight.backpressure.threshold = {}", this.flightConfiguration.FLIGHT_BACKPRESSURE_THRESHOLD);
    log.info("flight.backpressure.timeout = {}", this.flightConfiguration.FLIGHT_BACKPRESSURE_TIMEOUT);
    log.info("flight.server.readonly = {}", this.flightConfiguration.FLIGHT_SQL_DDL_CATALOGS_SUPPORT);
    log.info("flight.sql.ddl.catalogs.support = {}", this.flightConfiguration.FLIGHT_SQL_DDL_CATALOGS_SUPPORT);
    log.info("flight.sql.ddl.schemas.support = {}", this.flightConfiguration.FLIGHT_SQL_DDL_SCHEMAS_SUPPORT);
    log.info("flight.sql.ddl.all.tables.selectable = {}", this.flightConfiguration.FLIGHT_SQL_DDL_ALL_TABLES_SELECTABLE);
    log.info("flight.sql.identifier.quote.char = {}", this.flightConfiguration.FLIGHT_SQL_IDENTIFIER_QUOTE_CHAR);
    log.info("flight.sql.identifier.case.sensitivity = {}", this.flightConfiguration.FLIGHT_SQL_IDENTIFIER_CASE_SENSITIVITY);
    log.info("flight.sql.identifier.quoted.case.sensitivity = {}", this.flightConfiguration.FLIGHT_SQL_IDENTIFIER_QUOTED_CASE_SENSITIVITY);
    log.info("flight.sql.max.table.columns = {}", this.flightConfiguration.FLIGHT_SQL_MAX_TABLE_COLUMNS);
    
    // TRINO CONFIGURATION:
    log.info("trino.host = {}", this.trinoConfiguration.TRINO_HOST);
    log.info("trino.port = {}", this.trinoConfiguration.TRINO_PORT);
    log.info("trino.ssl = {}", this.trinoConfiguration.TRINO_SSL);
    log.info("trino.version = {}", this.trinoConfiguration.TRINO_VERSION);
    log.info("trino.driver.version = {}", this.trinoConfiguration.TRINO_DRIVER_VERSION);
    
    // LDAP CONFIGURATION:
    log.info("ldap.provider = {}", this.ldapConfiguration.LDAP_PROVIDER);
    log.info("ldap.domain = {}", this.ldapConfiguration.LDAP_DOMAIN);
    log.info("ldap.referral = {}", this.ldapConfiguration.LDAP_REFERRAL);
    log.info("ldap.timeout = {}", this.ldapConfiguration.LDAP_TIMEOUT);
    log.info("ldap.retries.count = {}", this.ldapConfiguration.LDAP_RETRY_CNT);
    log.info("ldap.retries.interval = {}", this.ldapConfiguration.LDAP_RETRY_INTERVAL);
    log.info("ldap.search.dn1 = {}", this.ldapConfiguration.LDAP_SEARCH_DN1);
    log.info("ldap.search.dn2 = {}", this.ldapConfiguration.LDAP_SEARCH_DN2);
    log.info("ldap.search.dn3 = {}", this.ldapConfiguration.LDAP_SEARCH_DN3);
    log.info("ldap.search.filter = {}", this.ldapConfiguration.LDAP_SEARCH_FILTER);
    log.info("ldap.search.attribute = {}", this.ldapConfiguration.LDAP_SEARCH_ATTR);
    log.info("ldap.auth.role = {}", this.ldapConfiguration.LDAP_AUTH_ROLE);
    
    // SSL CONFIGURATION:
    log.info("ssl.keystore.path = {}", this.sslConfiguration.KEYSTORE_PATH);
    log.info("ssl.keystore.password = {}", this.sslConfiguration.KEYSTORE_PASSWORD);
    log.info("ssl.keystore.type = {}", this.sslConfiguration.KEYSTORE_TYPE);
    log.info("ssl.truststore.path = {}", this.sslConfiguration.TRUSTSTORE_PATH);
    log.info("ssl.truststore.password = {}", this.sslConfiguration.TRUSTSTORE_PASSWORD);
    log.info("ssl.truststore.type = {}", this.sslConfiguration.TRUSTSTORE_TYPE);
    
    clearLogCtx();
  }
}
