package ru.raiffeisen.trino.arrow.flight.sql.config;

import org.apache.commons.configuration2.Configuration;

public class SslConfiguration {

  public final String KEYSTORE_PATH;
  public final String KEYSTORE_PASSWORD;
  public final String KEYSTORE_TYPE;
  public final String TRUSTSTORE_PATH;
  public final String TRUSTSTORE_PASSWORD;
  public final String TRUSTSTORE_TYPE;

  public SslConfiguration(Configuration config) {
    KEYSTORE_PATH =
        config.getString(
            "ssl.keystore.path", System.getProperty("java.home") + "/lib/security/cacerts");
    KEYSTORE_PASSWORD = config.getString("ssl.keystore.password", "changeit");
    KEYSTORE_TYPE = config.getString("ssl.keystore.type", "jks");
    TRUSTSTORE_PATH = config.getString("ssl.truststore.path", KEYSTORE_PATH);
    TRUSTSTORE_PASSWORD = config.getString("ssl.truststore.password", KEYSTORE_PASSWORD);
    TRUSTSTORE_TYPE = config.getString("ssl.truststore.type", KEYSTORE_TYPE);
  }
}
