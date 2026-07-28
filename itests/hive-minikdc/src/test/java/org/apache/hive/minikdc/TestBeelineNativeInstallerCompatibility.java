/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.minikdc;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.hadoop.hive.jdbc.SSLTestUtils;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBeelineNativeInstallerCompatibility {

  private static final long PROCESS_TIMEOUT_SECONDS = 60;

  private static String beelineNativeBinary;
  private static MiniHiveKdc miniHiveKdc;
  private static MiniHS2 hs2Kerb;
  private static MiniHS2 hs2Ssl;
  private static String krb5ConfPath;
  private static Path krb5CacheFilePath;

  @BeforeClass
  public static void beforeTest() throws Exception {
    beelineNativeBinary = System.getProperty("beeline.native.binary");
    Assume.assumeNotNull(beelineNativeBinary);
    Path beelineBinaryPath = Paths.get(beelineNativeBinary);
    Assume.assumeTrue(Files.isRegularFile(beelineBinaryPath));
    Assume.assumeTrue(Files.isExecutable(beelineBinaryPath));

    miniHiveKdc = new MiniHiveKdc();
    krb5ConfPath = miniHiveKdc.miniKdc.getKrb5conf().getAbsolutePath();

    Map<String, String> kerbConfOverlay = new HashMap<>();
    kerbConfOverlay.put(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname, MiniHS2.HS2_ALL_MODE);
    kerbConfOverlay.put(HiveConf.ConfVars.HIVE_SCHEDULED_QUERIES_EXECUTOR_ENABLED.varname, "false");
    kerbConfOverlay.put(HiveConf.ConfVars.HIVE_QUERY_HISTORY_ENABLED.varname, "false");
    // Avoid the ZooKeeper-backed lock manager (no ZK running in this test);
    // otherwise every query hangs 30s trying to reach connectString ":2181".
    kerbConfOverlay.put(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    hs2Kerb = MiniHiveKdc.getMiniHS2WithKerb(miniHiveKdc, new HiveConf());
    hs2Kerb.start(kerbConfOverlay);

    // Start a second HS2 instance with SSL enabled, but no Kerberos authentication.
    // This is to test that beeline can connect to HS2 with SSL and TLS enabled
    HiveConf sslHiveConf = new HiveConf();
    sslHiveConf.set("hadoop.security.authentication", "simple");
    sslHiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL,
        miniHiveKdc.getFullyQualifiedServicePrincipal(MiniHiveKdc.HIVE_SERVICE_PRINCIPAL));
    sslHiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB,
        miniHiveKdc.getKeyTabFile(miniHiveKdc.getServicePrincipalForUser(MiniHiveKdc.HIVE_SERVICE_PRINCIPAL)));
    Map<String, String> sslConfOverlay = new HashMap<>();
    SSLTestUtils.setBinaryConfOverlay(sslConfOverlay);
    SSLTestUtils.setSslConfOverlay(sslConfOverlay);
    sslConfOverlay.put(HiveConf.ConfVars.HIVE_SCHEDULED_QUERIES_EXECUTOR_ENABLED.varname, "false");
    sslConfOverlay.put(HiveConf.ConfVars.HIVE_QUERY_HISTORY_ENABLED.varname, "false");
    sslConfOverlay.put(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    hs2Ssl = new MiniHS2.Builder().withConf(sslHiveConf).withAuthenticationType("NONE").build();
    hs2Ssl.start(sslConfOverlay);

    Assume.assumeTrue(isCommandAvailable("kinit"));
    Path cacheDir = Paths.get("target", "beeline-native-installer-compat");
    Files.createDirectories(cacheDir);
    krb5CacheFilePath = cacheDir.resolve("krb5cc_beeline_test");

    String userPrincipal = miniHiveKdc.getFullyQualifiedUserPrincipal(MiniHiveKdc.HIVE_TEST_USER_1);
    BeelineResult kinitResult = runCommand(List.of("kinit", "-kt",
        miniHiveKdc.getKeyTabFile(MiniHiveKdc.HIVE_TEST_USER_1), userPrincipal), kerberosEnv());
    Assume.assumeTrue("Skipping test because kinit failed: " + kinitResult.output, kinitResult.exitCode == 0);
  }

  @AfterClass
  public static void afterTest() throws Exception {
    Exception firstException = null;
    if (hs2Kerb != null) {
      try {
        hs2Kerb.stop();
      } catch (Exception e) {
        firstException = e;
      }
    }
    if (hs2Ssl != null) {
      try {
        hs2Ssl.stop();
      } catch (Exception e) {
        if (firstException == null) {
          firstException = e;
        } else {
          firstException.addSuppressed(e);
        }
      }
    }
    if (miniHiveKdc != null) {
      try {
        miniHiveKdc.shutDown();
      } catch (Exception e) {
        if (firstException == null) {
          firstException = e;
        } else {
          firstException.addSuppressed(e);
        }
      }
    }
    if (krb5CacheFilePath != null) {
      Files.deleteIfExists(krb5CacheFilePath);
    }
    if (firstException != null) {
      throw firstException;
    }
  }

  @Test
  public void testKerberosAuthBinary() throws Exception {
    BeelineResult result = runBeeline(List.of("-u", hs2Kerb.getJdbcURL(), "-n", MiniHiveKdc.HIVE_TEST_USER_1,
        "-e", "select 'KERBEROS_BINARY_OK'"));
    assertBeelineSuccess(result, "KERBEROS_BINARY_OK");
  }

  @Test
  public void testSslTlsBinary() throws Exception {
    String sslUrl = hs2Ssl.getJdbcURL("default", SSLTestUtils.SSL_CONN_PARAMS);
    BeelineResult result = runBeelineSsl(List.of("-u", sslUrl, "-n", "ssl_user", "-p", "ssl_password",
        "-e", "select 'SSL_BINARY_OK'"));
    assertBeelineSuccess(result, "SSL_BINARY_OK");
  }

  @Test
  public void testHttpTransport() throws Exception {
    BeelineResult result = runBeeline(List.of("-u", hs2Kerb.getHttpJdbcURL(), "-n", MiniHiveKdc.HIVE_TEST_USER_1,
        "-e", "select 'HTTP_TRANSPORT_OK'"));
    assertBeelineSuccess(result, "HTTP_TRANSPORT_OK");
  }

  @Test
  public void testScriptExecutionWithFile() throws Exception {
    Path scriptPath = Paths.get("target", "beeline-native-script.sql");
    Files.createDirectories(scriptPath.getParent());
    Files.writeString(scriptPath, "select 'SCRIPT_EXECUTION_OK';\n", StandardCharsets.UTF_8);
    BeelineResult result = runBeeline(List.of("-u", hs2Kerb.getJdbcURL(), "-n", MiniHiveKdc.HIVE_TEST_USER_1,
        "-f", scriptPath.toAbsolutePath().toString()));
    assertBeelineSuccess(result, "SCRIPT_EXECUTION_OK");
  }

  @Test
  public void testHiveConfAndHiveVarExpansion() throws Exception {
    BeelineResult result = runBeeline(List.of("-u", hs2Kerb.getJdbcURL(), "-n", MiniHiveKdc.HIVE_TEST_USER_1,
        "--hiveconf", "beeline.native.conf.value=configured",
        "--hivevar", "beelineNativeVar=configuredVar",
        "-e", "set beeline.native.conf.value;select '${hivevar:beelineNativeVar}'"));
    assertBeelineSuccess(result, "beeline.native.conf.value=configured", "configuredVar");
  }

  private static void assertBeelineSuccess(BeelineResult result, String... expectedSnippets) {
    assertEquals("Unexpected beeline exit code. Output:\n" + result.output, 0, result.exitCode);
    for (String expectedSnippet : expectedSnippets) {
      assertTrue("Did not find expected output snippet '" + expectedSnippet + "'. Output:\n" + result.output,
          result.output.contains(expectedSnippet));
    }
  }

  private static BeelineResult runBeeline(List<String> args) throws Exception {
    List<String> command = new ArrayList<>();
    command.add(beelineNativeBinary);
    command.addAll(args);
    return runCommand(command, kerberosEnv());
  }

  // SSL test uses NONE authentication, so no Kerberos credentials should be
  // in the environment — only TERM=dumb to prevent jline from failing without a tty.
  private static BeelineResult runBeelineSsl(List<String> args) throws Exception {
    List<String> command = new ArrayList<>();
    command.add(beelineNativeBinary);
    command.addAll(args);
    return runCommand(command, plainEnv());
  }

  private static BeelineResult runCommand(List<String> command, Map<String, String> env) throws Exception {
    ProcessBuilder processBuilder = new ProcessBuilder(command);
    processBuilder.redirectErrorStream(true);
    processBuilder.environment().putAll(env);
    processBuilder.directory(new File("."));
    Process process = processBuilder.start();
    StringBuilder output = new StringBuilder();

    Thread outputReader = new Thread(() -> {
      try (BufferedReader bufferedReader =
               new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
        String line;
        while ((line = bufferedReader.readLine()) != null) {
          output.append(line).append(System.lineSeparator());
        }
      } catch (Exception e) {
        output.append(System.lineSeparator()).append("Error while reading process output: ").append(e.getMessage());
      }
    });
    outputReader.start();

    boolean finished = process.waitFor(PROCESS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    if (!finished) {
      process.destroyForcibly();
      finished = process.waitFor(10, TimeUnit.SECONDS);
    }
    outputReader.join(TimeUnit.SECONDS.toMillis(5));
    int exitCode = finished ? process.exitValue() : -1;
    if (!finished) {
      output.append(System.lineSeparator()).append("Process timed out after ")
          .append(PROCESS_TIMEOUT_SECONDS).append(" seconds");
    }
    return new BeelineResult(exitCode, output.toString());
  }

  private static boolean isCommandAvailable(String command) throws Exception {
    BeelineResult result = runCommand(List.of("sh", "-c", "command -v \"$1\"", "--", command), new HashMap<>());
    return result.exitCode == 0;
  }

  private static Map<String, String> kerberosEnv() {
    Map<String, String> env = new HashMap<>();
    env.put("KRB5_CONFIG", krb5ConfPath);
    env.put("KRB5CCNAME", "FILE:" + krb5CacheFilePath.toAbsolutePath());
    env.put("_JAVA_OPTIONS", "-Djava.security.krb5.conf=" + krb5ConfPath);
    // beeline runs under a ProcessBuilder with no controlling tty; force jline
    // to pick a dumb terminal. Without this, jline scans provider SPIs and
    // throws IllegalStateException before beeline can execute the -e query.
    env.put("TERM", "dumb");
    return env;
  }

  private static Map<String, String> plainEnv() {
    Map<String, String> env = new HashMap<>();
    // Force jline to use a dumb terminal (no controlling tty under ProcessBuilder).
    env.put("TERM", "dumb");
    return env;
  }

  private static final class BeelineResult {
    private final int exitCode;
    private final String output;

    private BeelineResult(int exitCode, String output) {
      this.exitCode = exitCode;
      this.output = output;
    }
  }
}
