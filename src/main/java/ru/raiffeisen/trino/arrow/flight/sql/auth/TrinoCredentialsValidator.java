package ru.raiffeisen.trino.arrow.flight.sql.auth;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.slf4j.Logger;
import ru.raiffeisen.trino.arrow.flight.sql.connection.TrinoConnectionFactory;

import java.sql.Connection;
import java.sql.SQLException;

import static org.slf4j.LoggerFactory.getLogger;
import static ru.raiffeisen.trino.arrow.flight.sql.logging.LoggingUtils.clearLogCtx;
import static ru.raiffeisen.trino.arrow.flight.sql.logging.LoggingUtils.setLogCtx;

public class TrinoCredentialsValidator implements BasicCallHeaderAuthenticator.CredentialValidator {
  private static final Logger log = getLogger(TrinoCredentialsValidator.class);
  private final TrinoConnectionFactory trinoConnFactory;

  public TrinoCredentialsValidator(TrinoConnectionFactory trinoConnFactory) {
    this.trinoConnFactory = trinoConnFactory;
  }

  @Override
  public CallHeaderAuthenticator.AuthResult validate(String user, String password) {
    setLogCtx("trino_auth", user);
    try (Connection conn = trinoConnFactory.getConnection(user, password)) {
      if (conn.isValid(60)) {
        log.info("User is authorized.");
        return new ProxyAuthResult(user, password);
      } else {
        log.error("Connection to trino is invalid. User is not authorized.");
        throw CallStatus.UNAUTHENTICATED.toRuntimeException();
      }
    } catch (SQLException e) {
      log.error("Unable to connect to trino due to following error: {}", e.getMessage());
      throw CallStatus.UNAUTHENTICATED.toRuntimeException();
    } finally {
      clearLogCtx();
    }
  }
}
