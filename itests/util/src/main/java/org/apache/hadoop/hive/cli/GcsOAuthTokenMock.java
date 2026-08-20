/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.cli;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import org.apache.hadoop.hive.ql.QTestSystemProperties;

/**
 * Minimal Google OAuth2/STS token endpoint mock for Gravitino {@code gcs-token} credential vending.
 *
 * <p>Runs two endpoints on separate ports:
 * <ul>
 *   <li>Plain HTTP {@code POST /token} for service-account {@code token_uri}</li>
 *   <li>HTTPS {@code POST /v1/token} with {@code CN=sts.googleapis.com} for Google STS</li>
 * </ul>
 * The test wires {@link GcsStsProxyContainer} to forward {@code sts.googleapis.com:443} to the
 * HTTPS port, and imports {@link #getCertificatePath()} into the Gravitino container truststore.
 */
public final class GcsOAuthTokenMock implements AutoCloseable {

  /** Token value returned to Gravitino; fake-gcs-server does not validate it. */
  public static final String ACCESS_TOKEN = "gcs-test-access-token";
  private static final String STS_CN = "sts.googleapis.com";
  static final String HOST_GATEWAY = GcsFakeServerContainers.HOST_GATEWAY;

  private static final byte[] OAUTH_TOKEN_RESPONSE = String.format(
      "{\"access_token\":\"%s\",\"expires_in\":3600,\"token_type\":\"Bearer\"}",
      ACCESS_TOKEN).getBytes(StandardCharsets.UTF_8);

  private static final byte[] STS_TOKEN_RESPONSE = String.format(
      "{\"access_token\":\"%s\",\"issued_token_type\":\"urn:ietf:params:oauth:token-type:access_token\","
          + "\"token_type\":\"Bearer\",\"expires_in\":3600}",
      ACCESS_TOKEN).getBytes(StandardCharsets.UTF_8);

  private HttpServer httpServer;
  private HttpsServer httpsServer;
  private Path certificatePath;
  private Path workDir;

  private static volatile String opensslCommand;
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  /** Starts HTTP and HTTPS servers on ephemeral ports. */
  public void start() throws IOException, InterruptedException, KeyStoreException, NoSuchAlgorithmException,
      CertificateException, UnrecoverableKeyException, KeyManagementException {
    workDir = createWorkDir();
    Path keyPath = workDir.resolve("key.pem");
    certificatePath = workDir.resolve("cert.pem");
    generateSelfSignedCertificate(keyPath, certificatePath);

    TokenHandler oauthTokenHandler = new TokenHandler(OAUTH_TOKEN_RESPONSE);
    httpServer = HttpServer.create(new InetSocketAddress(0), 0);
    httpServer.createContext("/token", oauthTokenHandler);
    httpServer.start();

    SSLContext sslContext = buildSslContext(keyPath, certificatePath);
    httpsServer = HttpsServer.create(new InetSocketAddress(0), 0);
    httpsServer.setHttpsConfigurator(new com.sun.net.httpserver.HttpsConfigurator(sslContext));
    httpsServer.createContext("/v1/token", new TokenHandler(STS_TOKEN_RESPONSE));
    httpsServer.start();
  }

  /** Port forwarded by {@link GcsStsProxyContainer} ({@code sts.googleapis.com:443}). */
  public int getStsPort() {
    return httpsServer.getAddress().getPort();
  }

  public String getTokenUri() {
    return String.format("http://%s:%d/token", HOST_GATEWAY, httpServer.getAddress().getPort());
  }

  /** PEM certificate to import into the Gravitino JVM truststore. */
  public Path getCertificatePath() {
    return certificatePath;
  }

  @Override
  public void close() {
    if (httpServer != null) {
      httpServer.stop(0);
      httpServer = null;
    }
    if (httpsServer != null) {
      httpsServer.stop(0);
      httpsServer = null;
    }
    if (workDir != null) {
      deleteQuietly(workDir);
      workDir = null;
      certificatePath = null;
    }
  }

  private static SSLContext buildSslContext(Path keyPath, Path certPath)
      throws IOException, InterruptedException, KeyStoreException, NoSuchAlgorithmException,
          CertificateException, UnrecoverableKeyException, KeyManagementException {
    Path pkcs12 = keyPath.getParent().resolve("keystore.p12");
    char[] keystorePassword = ephemeralKeystorePassword();
    try {
      ProcessBuilder pb = new ProcessBuilder(
          resolveOpenSslCommand(), "pkcs12", "-export",
          "-inkey", keyPath.toString(),
          "-in", certPath.toString(),
          "-out", pkcs12.toString(),
          "-passout", "pass:" + new String(keystorePassword),
          "-name", STS_CN);
      runOrThrow(pb);
      KeyStore keyStore = KeyStore.getInstance("PKCS12");
      try (InputStream in = Files.newInputStream(pkcs12)) {
        keyStore.load(in, keystorePassword);
      }
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(keyStore, keystorePassword);
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(kmf.getKeyManagers(), null, null);
      return sslContext;
    } finally {
      Arrays.fill(keystorePassword, '\0');
    }
  }

  private static char[] ephemeralKeystorePassword() {
    byte[] random = new byte[16];
    SECURE_RANDOM.nextBytes(random);
    return Base64.getEncoder().encodeToString(random).toCharArray();
  }

  private static void generateSelfSignedCertificate(Path keyPath, Path certPath)
      throws IOException, InterruptedException {
    ProcessBuilder pb = new ProcessBuilder(
        resolveOpenSslCommand(), "req", "-x509", "-newkey", "rsa:2048",
        "-keyout", keyPath.toString(),
        "-out", certPath.toString(),
        "-days", "1", "-nodes",
        "-subj", "/CN=" + STS_CN);
    runOrThrow(pb);
  }

  private static String resolveOpenSslCommand() throws IOException {
    if (opensslCommand == null) {
      synchronized (GcsOAuthTokenMock.class) {
        if (opensslCommand == null) {
          opensslCommand = findOpenSslExecutable();
        }
      }
    }
    return opensslCommand;
  }

  private static String findOpenSslExecutable() throws IOException {
    for (String candidate : new String[] {"/usr/bin/openssl", "/bin/openssl"}) {
      Path path = Paths.get(candidate);
      if (Files.isExecutable(path)) {
        return candidate;
      }
    }
    throw new IOException("openssl executable not found (/usr/bin/openssl, /bin/openssl)");
  }

  private static Path createWorkDir() throws IOException {
    Path tmpParent = Paths.get(
        QTestSystemProperties.getTempDir() != null
            ? QTestSystemProperties.getTempDir()
            : System.getProperty("test.tmp.dir", "target/tmp"))
        .toAbsolutePath()
        .normalize();
    Files.createDirectories(tmpParent);
    FileAttribute<Set<PosixFilePermission>> ownerOnlyDir =
        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"));
    return Files.createTempDirectory(tmpParent, "gcs-oauth-mock-", ownerOnlyDir);
  }

  private static void runOrThrow(ProcessBuilder pb) throws IOException, InterruptedException {
    pb.environment().put("PATH", "/usr/bin:/bin");
    Process process = pb.start();
    if (!process.waitFor(30, TimeUnit.SECONDS)) {
      process.destroyForcibly();
      throw new IOException("Timed out running: " + String.join(" ", pb.command()));
    }
    if (process.exitValue() != 0) {
      throw new IOException("Command failed (" + process.exitValue() + "): "
          + String.join(" ", pb.command()));
    }
  }

  private static void deleteQuietly(Path dir) {
    try (var paths = Files.walk(dir)) {
      paths.sorted(Comparator.reverseOrder()).forEach(path -> {
        try {
          Files.deleteIfExists(path);
        } catch (IOException ignored) {
          // best effort cleanup
        }
      });
    } catch (IOException ignored) {
      // best effort cleanup
    }
  }

  private static final class TokenHandler implements HttpHandler {
    private final byte[] responseBody;

    private TokenHandler(byte[] responseBody) {
      this.responseBody = responseBody;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
      if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
        exchange.sendResponseHeaders(405, -1);
        exchange.close();
        return;
      }
      exchange.getRequestBody().readAllBytes();
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, responseBody.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(responseBody);
      }
    }
  }
}
