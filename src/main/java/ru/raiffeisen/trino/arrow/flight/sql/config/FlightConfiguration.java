package ru.raiffeisen.trino.arrow.flight.sql.config;

import org.apache.commons.configuration2.Configuration;

import org.apache.commons.configuration2.ex.ConfigurationException;

public final class FlightConfiguration {
  public final String FLIGHT_HOST;
  public final int FLIGHT_PORT;
  public final boolean FLIGHT_SSL;

  public enum AUTH_TYPE {
    LDAP,
    TRINO
  }

  public final AUTH_TYPE FLIGHT_AUTH_TYPE;

  public final int FLIGHT_BATCH_SIZE;
  public final int FLIGHT_BACKPRESSURE_THRESHOLD;
  public final int FLIGHT_BACKPRESSURE_TIMEOUT;
  public final boolean FLIGHT_SERVER_READONLY;
  public final boolean FLIGHT_SQL_DDL_CATALOGS_SUPPORT;
  public final boolean FLIGHT_SQL_DDL_SCHEMAS_SUPPORT;
  public final boolean FLIGHT_SQL_DDL_ALL_TABLES_SELECTABLE;
  public final String FLIGHT_SQL_IDENTIFIER_QUOTE_CHAR;
  public final int FLIGHT_SQL_IDENTIFIER_CASE_SENSITIVITY;
  public final int FLIGHT_SQL_IDENTIFIER_QUOTED_CASE_SENSITIVITY;
  public final int FLIGHT_SQL_MAX_TABLE_COLUMNS;

  public FlightConfiguration(Configuration config) throws ConfigurationException {
    FLIGHT_HOST = config.getString("flight.host", "localhost");
    FLIGHT_PORT = config.getInt("flight.port", 32010);
    FLIGHT_SSL = config.getBoolean("flight.ssl", false);

    final String authType =
        config.getString("flight.auth.type", AUTH_TYPE.LDAP.name()).toUpperCase();
    try {
      FLIGHT_AUTH_TYPE = AUTH_TYPE.valueOf(authType);
    } catch (IllegalArgumentException e) {
      throw new ConfigurationException(
          String.format(
              "Unsupported authorization option: %s. Allowed authorization options are: %s, %s",
              authType, AUTH_TYPE.TRINO, AUTH_TYPE.LDAP));
    }

    FLIGHT_BATCH_SIZE = config.getInt("flight.batch.size", 1024);
    FLIGHT_BACKPRESSURE_THRESHOLD =
        config.getInt("flight.backpressure.threshold", 10 * 1024 * 1024);
    FLIGHT_BACKPRESSURE_TIMEOUT = config.getInt("flight.backpressure.timeout", 30 * 1000);
    FLIGHT_SERVER_READONLY = config.getBoolean("flight.server.readonly", false);
    FLIGHT_SQL_DDL_CATALOGS_SUPPORT = config.getBoolean("flight.sql.ddl.catalogs.support", false);
    FLIGHT_SQL_DDL_SCHEMAS_SUPPORT = config.getBoolean("flight.sql.ddl.schemas.support", false);
    FLIGHT_SQL_DDL_ALL_TABLES_SELECTABLE =
        config.getBoolean("flight.sql.ddl.all.tables.selectable", false);
    FLIGHT_SQL_IDENTIFIER_QUOTE_CHAR = config.getString("flight.sql.identifier.quote.char", "\"");
    FLIGHT_SQL_IDENTIFIER_CASE_SENSITIVITY =
        config.getInt("flight.sql.identifier.case.sensitivity", 0);
    FLIGHT_SQL_IDENTIFIER_QUOTED_CASE_SENSITIVITY =
        config.getInt("flight.sql.identifier.quoted.case.sensitivity", 0);
    FLIGHT_SQL_MAX_TABLE_COLUMNS = config.getInt("flight.sql.max.table.columns", 1024);
  }
}
