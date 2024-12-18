package ru.raiffeisen.trino.arrow.flight.sql.config;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;

public final class TrinoConfiguration {
  public final String TRINO_HOST;
  public final int TRINO_PORT;
  public final boolean TRINO_SSL;
  
  public final String TRINO_VERSION;
  public final String TRINO_DRIVER_VERSION;

  public TrinoConfiguration(Configuration config) throws ConfigurationException {
    TRINO_HOST = config.getString("trino.host", "localhost");
    TRINO_PORT = config.getInt("trino.port", 443);
    TRINO_SSL = config.getBoolean("trino.ssl", false);

    TRINO_VERSION = config.getString("trino.version", "452");
    TRINO_DRIVER_VERSION = config.getString("trino.driver.version", "452");
  }
}
