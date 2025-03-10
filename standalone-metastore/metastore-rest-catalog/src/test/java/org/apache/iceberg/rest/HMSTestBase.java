/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.rest;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlFeatures;
import org.apache.commons.jexl3.MapContext;
import org.apache.commons.jexl3.introspection.JexlPermissions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreSchemaInfo;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.properties.HMSPropertyManager;
import org.apache.hadoop.hive.metastore.properties.PropertyManager;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hive.iceberg.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.hive.iceberg.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.hive.iceberg.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.hive.IcebergTestHelper;
import org.eclipse.jetty.server.Server;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class HMSTestBase {
  protected static final Logger LOG = LoggerFactory.getLogger(HMSTestBase.class.getName());
  protected static final String BASE_DIR = System.getProperty("basedir");
  protected static Random RND = new Random(20230922);
  protected static final String USER_1 = "USER_1";
  protected static final String DB_NAME = "hivedb";
  /** A Jexl engine for convenience. */
  static final JexlEngine JEXL;
  static {
    JexlFeatures features = new JexlFeatures()
        .sideEffect(false)
        .sideEffectGlobal(false);
    JexlPermissions p = JexlPermissions.RESTRICTED
        .compose("org.apache.hadoop.hive.metastore.*", "org.apache.iceberg.*");
    JEXL = new JexlBuilder()
        .features(features)
        .permissions(p)
        .create();
  }

  protected static final long EVICTION_INTERVAL = TimeUnit.SECONDS.toMillis(10);
  private static final File JWT_AUTHKEY_FILE =
      new File(BASE_DIR,"src/test/resources/auth/jwt/jwt-authorized-key.json");
  protected static final File JWT_NOAUTHKEY_FILE =
      new File(BASE_DIR,"src/test/resources/auth/jwt/jwt-unauthorized-key.json");
  protected static final File JWT_JWKS_FILE =
      new File(BASE_DIR,"src/test/resources/auth/jwt/jwt-verification-jwks.json");
  protected static final int MOCK_JWKS_SERVER_PORT = 8089;
  @ClassRule
  public static final WireMockRule MOCK_JWKS_SERVER = new WireMockRule(MOCK_JWKS_SERVER_PORT);


  public static class TestSchemaInfo extends MetaStoreSchemaInfo {
    public TestSchemaInfo(String metastoreHome, String dbType) throws HiveMetaException {
      super(metastoreHome, dbType);
    }
    @Override
    public String getMetaStoreScriptDir() {
      return new File(BASE_DIR, "../metastore-server/src/main/sql/derby").getAbsolutePath();      
    }
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  protected Configuration conf = null;
  protected String NS = "hms" + RND.nextInt(100);

  protected int port = -1;
  protected int catalogPort = -1;
  protected final String catalogPath = "hmscatalog";
  protected static final int WAIT_FOR_SERVER = 5000;
  // for direct calls
  protected Catalog catalog;
  protected SupportsNamespaces nsCatalog;

  protected int createMetastoreServer(Configuration conf) throws Exception {
    return MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf);
  }

  protected void stopMetastoreServer(int port) {
    MetaStoreTestUtils.close(port);
  }

  @Before
  public void setUp() throws Exception {
    NS = "hms" + RND.nextInt(100);
    conf = MetastoreConf.newMetastoreConf();
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.CAPABILITY_CHECK, false);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    // new 2024-10-02
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.SCHEMA_VERIFICATION, false);

    conf.setBoolean(MetastoreConf.ConfVars.METRICS_ENABLED.getVarname(), true);
    // "hive.metastore.warehouse.dir"
    String whpath = new File(BASE_DIR,"target/tmp/warehouse/managed").toURI().toString();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.WAREHOUSE, whpath);
    HiveConf.setVar(conf, HiveConf.ConfVars.METASTORE_WAREHOUSE, whpath);
    // "hive.metastore.warehouse.external.dir"
    String extwhpath = new File(BASE_DIR,"target/tmp/warehouse/external").toURI().toString();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL, extwhpath);
    conf.set(HiveConf.ConfVars.HIVE_METASTORE_WAREHOUSE_EXTERNAL.varname, extwhpath);

    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.SCHEMA_INFO_CLASS, "org.apache.iceberg.rest.HMSTestBase$TestSchemaInfo");
    // Events that get cleaned happen in batches of 1 to exercise batching code
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.EVENT_CLEAN_MAX_EVENTS, 1L);
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.ICEBERG_CATALOG_SERVLET_PORT, 0);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.ICEBERG_CATALOG_SERVLET_AUTH, "jwt");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.ICEBERG_CATALOG_SERVLET_PATH, catalogPath);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_METASTORE_AUTHENTICATION_JWT_JWKS_URL,
        "http://localhost:" + MOCK_JWKS_SERVER_PORT + "/jwks");
    MOCK_JWKS_SERVER.stubFor(get("/jwks")
        .willReturn(ok()
            .withBody(Files.readAllBytes(JWT_JWKS_FILE.toPath()))));
    Metrics.initialize(conf);
    // The server
    port = createMetastoreServer(conf);
    System.out.println("Starting MetaStore Server on port " + port);
    // The manager decl
    PropertyManager.declare(NS, HMSPropertyManager.class);
    // The client
    HiveMetaStoreClient client = createClient(conf);
    Assert.assertNotNull("Unable to connect to the MetaStore server", client);

    // create a managed root
    String location = temp.newFolder("hivedb2023").getAbsolutePath();
    Database db = new Database(DB_NAME, "catalog test", location, Collections.emptyMap());
    client.createDatabase(db);

    Catalog ice = acquireServer();
      catalog =  ice;
      nsCatalog = catalog instanceof SupportsNamespaces? (SupportsNamespaces) catalog : null;
      catalogPort = HiveMetaStore.getCatalogServletPort();
  }
  
  private static String format(String format, Object... params) {
    return org.slf4j.helpers.MessageFormatter.arrayFormat(format, params).getMessage();
  }

  private static Catalog acquireServer() throws InterruptedException {
    final int wait = 200;
    Server iceServer = HiveMetaStore.getServletServer();
    int tries = WAIT_FOR_SERVER / wait;
    while(iceServer == null && tries-- > 0) {
      Thread.sleep(wait);
      iceServer = HiveMetaStore.getServletServer();
    }
    if (iceServer != null) {
      boolean starting;
      tries = WAIT_FOR_SERVER / wait;
      while((starting = iceServer.isStarting()) && tries-- > 0) {
        Thread.sleep(wait);
      }
      if (starting) {
        LOG.warn("server still starting after {}ms", WAIT_FOR_SERVER);
      }
      Catalog ice = HMSCatalogFactory.getLastCatalog();
      if (ice == null) {
        throw new NullPointerException(format("unable to acquire catalog after {}ms", WAIT_FOR_SERVER));
      }
      return ice;
    } else {
      throw new NullPointerException(format("unable to acquire server after {}ms", WAIT_FOR_SERVER));
    }
  }

  protected HiveMetaStoreClient createClient(Configuration conf) throws Exception {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_URIS, "");
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.EXECUTE_SET_UGI, false);
    return new HiveMetaStoreClient(conf);
  }

  /**
   * @param apis a list of api calls
   * @return the map of HMSCatalog route counter metrics keyed by their names
   */
  static Map<String, Long> reportMetricCounters(String... apis) {
    Map<String, Long> map = new LinkedHashMap<>();
    MetricRegistry registry = Metrics.getRegistry();
    List<String> names = HMSCatalogAdapter.getMetricNames(apis);
    for(String name : names) {
      Counter counter = registry.counter(name);
      if (counter != null) {
        long count = counter.getCount();
        map.put(name, count);
      }
    }
    return map;
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (port >= 0) {
    System.out.println("Stopping MetaStore Server on port " + port);
        stopMetastoreServer(port);
        port = -1;
      }
      // Clear the SSL system properties before each test.
      System.clearProperty(ObjectStore.TRUSTSTORE_PATH_KEY);
      System.clearProperty(ObjectStore.TRUSTSTORE_PASSWORD_KEY);
      System.clearProperty(ObjectStore.TRUSTSTORE_TYPE_KEY);
      //
      IcebergTestHelper.invalidatePoolCache();
    } finally {
      catalog = null;
      nsCatalog = null;
      catalogPort = -1;
      conf = null;
    }
  }

  protected String generateJWT()  throws Exception {
    return generateJWT(JWT_AUTHKEY_FILE.toPath());
  }
  protected String generateJWT(Path path)  throws Exception {
    return generateJWT(USER_1, path, TimeUnit.MINUTES.toMillis(5));
  }

  private static String generateJWT(String user, Path keyFile, long lifeTimeMillis) throws Exception {
    RSAKey rsaKeyPair = RSAKey.parse(new String(java.nio.file.Files.readAllBytes(keyFile), StandardCharsets.UTF_8));
    // Create RSA-signer with the private key
    JWSSigner signer = new RSASSASigner(rsaKeyPair);
    JWSHeader header = new JWSHeader
        .Builder(JWSAlgorithm.RS256)
        .keyID(rsaKeyPair.getKeyID())
        .build();
    Date now = new Date();
    Date expirationTime = new Date(now.getTime() + lifeTimeMillis);
    JWTClaimsSet claimsSet = new JWTClaimsSet.Builder()
        .jwtID(UUID.randomUUID().toString())
        .issueTime(now)
        .issuer("auth-server")
        .subject(user)
        .expirationTime(expirationTime)
        .claim("custom-claim-or-payload", "custom-claim-or-payload")
        .build();
    SignedJWT signedJWT = new SignedJWT(header, claimsSet);
    // Compute the RSA signature
    signedJWT.sign(signer);
    return signedJWT.serialize();
  }

  /**
   * Performs a Json client call.
   * @param jwt the jwt token
   * @param url the url
   * @param method the http method
   * @param arg the argument that will be transported as JSon
   * @return the result the was returned through Json
   * @throws IOException if marshalling the request/response fail
   */
  public static Object clientCall(String jwt, URL url, String method, Object arg) throws IOException {
    return clientCall(jwt, url, method, true, arg);
  }

  public static class ServerResponse {
    private final int code;
    private final String content;
    public ServerResponse(int code, String content) {
      this.code = code;
      this.content = content;
    }
  }

  /**
   * Performs an http client call.
   * @param jwt a JWT bearer token (can be null)
   * @param url the url to call
   * @param method the http method to use
   * @param json whether the call is application/json (true) or application/x-www-form-urlencoded (false)
   * @param arg the query argument
   * @return the (JSON) response
   * @throws IOException
   */
  public static Object clientCall(String jwt, URL url, String method, boolean json, Object arg) throws IOException {
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    try {
      if ("PATCH".equals(method)) {
        con.setRequestMethod("POST");
        con.setRequestProperty("X-HTTP-Method-Override", "PATCH");
      } else {
        con.setRequestMethod(method);
      }
      con.setRequestProperty(MetaStoreUtils.USER_NAME_HTTP_HEADER, url.getUserInfo());
      if (json) {
        con.setRequestProperty("Content-Type", "application/json");
      } else {
        con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
      }
      con.setRequestProperty("Accept", "application/json");
      if (jwt != null) {
        con.setRequestProperty("Authorization", "Bearer " + jwt);
      }
      con.setDoInput(true);
      if (arg != null) {
        con.setDoOutput(true);
        try (DataOutputStream wr = new DataOutputStream(con.getOutputStream())) {
          if (json) {
            wr.writeBytes(serialize(arg));
          } else {
            wr.writeBytes(arg.toString());
          }
          wr.flush();
        }
      }
      // perform http method
      return httpResponse(con);
    } finally {
      con.disconnect();
    }
  }

  private static Object httpResponse(HttpURLConnection con) throws IOException {
    int responseCode = con.getResponseCode();
    InputStream responseStream = con.getErrorStream();
    if (responseStream == null) {
      try {
        responseStream = con.getInputStream();
      } catch (IOException e) {
        return new ServerResponse(responseCode, e.getMessage());
      }
    }
    if (responseStream != null) {
      try (BufferedReader reader = new BufferedReader(
              new InputStreamReader(responseStream, StandardCharsets.UTF_8))) {
        // if not strictly ok, check we are still receiving a JSON
        if (responseCode != HttpServletResponse.SC_OK) {
          String contentType = con.getContentType();
          if (contentType == null || !contentType.contains("application/json")) {
            String line;
            StringBuilder response = new StringBuilder("error " + responseCode + ":");
            while ((line = reader.readLine()) != null) response.append(line);
            return new ServerResponse(responseCode, response.toString());
          }
        }
        // there might be no answer which is still ok
        Object r = reader.ready() ? deserialize(reader) : new HashMap<>(1);
        if (r instanceof Map) {
          ((Map<String, Object>) r).put("status", responseCode);
        }
        return r;
      }
    }
    return responseCode;
}

    private static final ObjectMapper MAPPER = RESTObjectMapper.mapper();

    static <T> String serialize(T object) {
        try {
        return MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException xany) {
            throw new RuntimeException(xany);
        }
    }

    static <T> T deserialize(Reader s) {
        try {
            return MAPPER.readValue(s, new TypeReference<T>() {});
        } catch (IOException xany) {
            throw new RuntimeException(xany);
        }
    }
    
  static Object eval(Object properties, String expr) {
    try {
      JexlContext context = properties instanceof Map
              ? new MapContext((Map) properties)
              : JexlEngine.EMPTY_CONTEXT;
      Object result = JEXL.createScript(expr).execute(context, properties);
      return result;
    } catch (JexlException xany) {
      throw xany;
    }
  }
}