package ru.raiffeisen.trino.arrow.flight.sql.auth;

import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;

import java.util.Base64;

public class ProxyAuthResult implements CallHeaderAuthenticator.AuthResult {
  private final String token;

  ProxyAuthResult(String user, String password) {
    String userInfo = user + ":" + password;
    this.token = Base64.getEncoder().encodeToString(userInfo.getBytes());
  }

  @Override
  public String getPeerIdentity() {
    return token;
  }
}
