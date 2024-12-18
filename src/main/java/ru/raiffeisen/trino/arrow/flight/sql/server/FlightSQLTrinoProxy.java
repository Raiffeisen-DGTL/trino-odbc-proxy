package ru.raiffeisen.trino.arrow.flight.sql.server;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.GeneratedBearerTokenAuthenticator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import ru.raiffeisen.trino.arrow.flight.sql.auth.LdapCredentialValidator;
import ru.raiffeisen.trino.arrow.flight.sql.auth.TrinoCredentialsValidator;
import ru.raiffeisen.trino.arrow.flight.sql.config.FlightConfiguration;
import ru.raiffeisen.trino.arrow.flight.sql.config.ServerConfiguration;
import ru.raiffeisen.trino.arrow.flight.sql.config.TrinoConfiguration;
import ru.raiffeisen.trino.arrow.flight.sql.connection.TrinoConnectionFactory;
import ru.raiffeisen.trino.arrow.flight.sql.logging.LoggingMiddleware;
import ru.raiffeisen.trino.arrow.flight.sql.metrics.FlightSqlMetrics;
import ru.raiffeisen.trino.arrow.flight.sql.producer.TrinoFlightSQLProducer;

public class FlightSQLTrinoProxy {

  public static void main(String[] args) throws Exception {
    // register custom JMX metrics:
    FlightSqlMetrics.registerMetrics();

    // Read server configuration:
    ServerConfiguration serverConfig = new ServerConfiguration();
    serverConfig.logConfig();

    FlightConfiguration flightConfig = serverConfig.getFlightConfiguration();
    TrinoConfiguration trinoConfig = serverConfig.getTrinoConfiguration();

    // setup Trino connection factory:
    TrinoConnectionFactory trinoConnFactory = new TrinoConnectionFactory(trinoConfig);

    Location flightLocation =
        Location.forGrpcInsecure(flightConfig.FLIGHT_HOST, flightConfig.FLIGHT_PORT);

    // Instantiate authenticator for authorization middleware:
    final BasicCallHeaderAuthenticator.CredentialValidator validator =
        switch (flightConfig.FLIGHT_AUTH_TYPE) {
          case LDAP ->
              new LdapCredentialValidator(
                  serverConfig.getLdapConfiguration(), serverConfig.getSslConfiguration());
          case TRINO -> new TrinoCredentialsValidator(trinoConnFactory);
        };

    CallHeaderAuthenticator authenticator = new BasicCallHeaderAuthenticator(validator);

    // build and start server:
    try (final BufferAllocator allocator = new RootAllocator();
        final TrinoFlightSQLProducer proxy =
            new TrinoFlightSQLProducer(trinoConnFactory, flightConfig, allocator);
        final FlightServer server =
            FlightServer.builder(allocator, flightLocation, proxy)
                .backpressureThreshold(flightConfig.FLIGHT_BACKPRESSURE_THRESHOLD)
                .headerAuthenticator(new GeneratedBearerTokenAuthenticator(authenticator))
                .middleware(
                    FlightServerMiddleware.Key.of("logging"), new LoggingMiddleware.Factory())
                .build()) {
      server.start();
      server.awaitTermination();
    }
  }
}
