package ru.raiffeisen.trino.arrow.flight.sql.auth;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class LdapSslSocketFactory extends SocketFactory {

  private static final ThreadLocal<SSLContext> SSL_CONTEXT = new ThreadLocal<>();

  private final SocketFactory socketFactory;

  public LdapSslSocketFactory(SocketFactory socketFactory) {
    this.socketFactory = requireNonNull(socketFactory, "socketFactory is null");
  }

  @SuppressWarnings({"unused", "MethodOverridesStaticMethodOfSuperclass"})
  public static SocketFactory getDefault() {
    SSLContext sslContext = SSL_CONTEXT.get();
    checkState(sslContext != null, "SSLContext was not set");
    return new LdapSslSocketFactory(sslContext.getSocketFactory());
  }

  public static void setSslContextForCurrentThread(SSLContext sslContext) {
    SSL_CONTEXT.set(sslContext);
  }

  @Override
  public Socket createSocket(String host, int port) throws IOException {
    return socketFactory.createSocket(host, port);
  }

  @Override
  public Socket createSocket(String host, int port, InetAddress localHost, int localPort)
      throws IOException {
    return socketFactory.createSocket(host, port, localHost, localPort);
  }

  @Override
  public Socket createSocket(InetAddress host, int port) throws IOException {
    return socketFactory.createSocket(host, port);
  }

  @Override
  public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort)
      throws IOException {
    return socketFactory.createSocket(address, port, localAddress, localPort);
  }
}
