package ru.raiffeisen.trino.arrow.flight.sql.connection;

import io.trino.jdbc.NonRegisteringTrinoDriver;
import org.slf4j.Logger;
import ru.raiffeisen.trino.arrow.flight.sql.config.TrinoConfiguration;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static org.slf4j.LoggerFactory.getLogger;

public final class TrinoConnectionFactory implements AutoCloseable {
  private static final Logger log = getLogger(TrinoConnectionFactory.class);

  private static final String URL_PREFIX = "jdbc:trino";
  private static final String SERVER_NAME = "Trino";
  private final NonRegisteringTrinoDriver trinoDriver = new NonRegisteringTrinoDriver();
  
  private final String serverVersion;
  private final String driverVersion;
  private final String trinoUri;
  private final boolean sslEnabled;

  private String buildUri(String host, int port) {
    try {
      URI uri = new URI(URL_PREFIX, null, host, port, null, null, null);
      return uri.toString();
    } catch (URISyntaxException e) {
      log.error(
          "Unable to build TRINO URI for [host = {}] and [port = {}] with following error:",
          host,
          port);
      throw new RuntimeException(e);
    }
  }

  private Properties initConnnectionProperties() {
    Properties props = new Properties();
    props.setProperty("SSL", String.valueOf(sslEnabled));
    return props;
  }

  public TrinoConnectionFactory(TrinoConfiguration trinoConfig) {
    this.trinoUri = buildUri(trinoConfig.TRINO_HOST, trinoConfig.TRINO_PORT);
    this.sslEnabled = trinoConfig.TRINO_SSL;
    
    this.serverVersion = trinoConfig.TRINO_VERSION;
    this.driverVersion = trinoConfig.TRINO_DRIVER_VERSION;
  }
  
  public String getServerName() { return SERVER_NAME; }
  public String getServerVersion() { return this.serverVersion; }
  public String getDriverVersion() { return this.driverVersion; }

  public Connection getConnection(String user, String password) throws SQLException {
    Properties props = initConnnectionProperties();
    props.setProperty("user", user);
    props.setProperty("password", password);

    log.info("Creating new connection to Trino.");
    return trinoDriver.connect(trinoUri, props);
  }

  @Override
  public void close() {
    trinoDriver.close();
  }
}
