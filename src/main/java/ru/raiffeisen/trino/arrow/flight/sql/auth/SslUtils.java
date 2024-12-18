package ru.raiffeisen.trino.arrow.flight.sql.auth;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.Arrays;

import static java.util.Collections.list;

final class SslUtils {

  private SslUtils() {}

  static SSLContext createSSLContext(
      String keyStorePath,
      String keyStorePassword,
      String keyStoreType,
      String trustStorePath,
      String trustStorePassword,
      String trustStoreType)
      throws GeneralSecurityException, IOException {
    // load keyStore and trustStore:
    KeyStore keyStore = loadStore(keyStorePath, keyStorePassword, keyStoreType, true);
    KeyStore trustStore = loadStore(trustStorePath, trustStorePassword, trustStoreType, false);
    
    // create KeyManagerFactory
    KeyManagerFactory keyManagerFactory =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());
    KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();
    
    // create TrustManagerFactory
    TrustManagerFactory trustManagerFactory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(trustStore);

    // get X509TrustManager
    TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
    if (trustManagers.length != 1 || !(trustManagers[0] instanceof X509TrustManager)) {
      throw new RuntimeException(
          "Unexpected default trust managers:" + Arrays.toString(trustManagers));
    }
    
    // create SSLContext
    SSLContext result = SSLContext.getInstance("SSL");
    result.init(keyManagers, trustManagers, null);
    return result;
  }

  private static KeyStore loadStore(String path, String password, String type, Boolean validate) throws GeneralSecurityException, IOException {
    KeyStore store = KeyStore.getInstance(type);
    try (InputStream in = new FileInputStream(path)) {
      store.load(in, password.toCharArray());
    }
    if (validate) validateCertificates(store);
    return store;
  }
  
  private static void validateCertificates(KeyStore keyStore) throws GeneralSecurityException {
    for (String alias : list(keyStore.aliases())) {
      if (!keyStore.isKeyEntry(alias)) {
        continue;
      }
      Certificate certificate = keyStore.getCertificate(alias);
      if (!(certificate instanceof X509Certificate)) {
        continue;
      }

      try {
        ((X509Certificate) certificate).checkValidity();
      } catch (CertificateExpiredException e) {
        throw new CertificateExpiredException("KeyStore certificate is expired: " + e.getMessage());
      } catch (CertificateNotYetValidException e) {
        throw new CertificateNotYetValidException(
            "KeyStore certificate is not yet valid: " + e.getMessage());
      }
    }
  }
}
