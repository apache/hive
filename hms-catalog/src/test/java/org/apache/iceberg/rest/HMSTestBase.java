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

import com.google.gson.Gson;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreSchemaInfo;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.properties.HMSPropertyManager;
import org.apache.hadoop.hive.metastore.properties.PropertyManager;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.eclipse.jetty.server.Server;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.github.tomakehurst.wiremock.junit.WireMockRule;

import javax.servlet.http.HttpServletResponse;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;

public abstract class HMSTestBase {
  protected static final String baseDir = System.getProperty("basedir");

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  public static class TestSchemaInfo extends MetaStoreSchemaInfo {

    public TestSchemaInfo(String metastoreHome, String dbType) throws HiveMetaException {
      super(metastoreHome, dbType);
    }
    @Override
    public String getMetaStoreScriptDir() {
      return  new File(baseDir,"src/test/resources").getAbsolutePath() + File.separatorChar +
          "scripts" + File.separatorChar + "metastore" +
          File.separatorChar + "upgrade" + File.separatorChar + dbType;
    }
  }

  protected static final String DB_NAME = "hivedb";
  protected static final long EVICTION_INTERVAL = TimeUnit.SECONDS.toMillis(10);
  private static final File jwtAuthorizedKeyFile =
      new File(baseDir,"src/test/resources/auth/jwt/jwt-authorized-key.json");
  protected static final File jwtUnauthorizedKeyFile =
      new File(baseDir,"src/test/resources/auth/jwt/jwt-unauthorized-key.json");
  protected static final File jwtVerificationJWKSFile =
      new File(baseDir,"src/test/resources/auth/jwt/jwt-verification-jwks.json");

  public static final String USER_1 = "USER_1";

  protected static final int MOCK_JWKS_SERVER_PORT = 8089;
  @ClassRule
  public static final WireMockRule MOCK_JWKS_SERVER = new WireMockRule(MOCK_JWKS_SERVER_PORT);
  // the url part
  /**
   * Abstract the property client access on a given namespace.
   */


  protected Configuration conf = null;

  protected static final Logger LOG = LoggerFactory.getLogger(HMSTestBase.class.getName());
  static Random RND = new Random(20230922);
  protected String NS = "hms" + RND.nextInt(100);
  //protected PropertyClient client;
  protected int port = -1;
  protected int catalogPort = -1;
  protected final String catalogPath = "hmscatalog";

  protected HMSCatalog catalog;
  protected HiveMetaStoreClient metastoreClient;

  protected int createMetastoreServer(Configuration conf) throws Exception {
    return HMSTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf);
  }

  protected void stopMetastoreServer(int port) {
    HMSTestUtils.close(port);
  }

  @Before
  public void setUp() throws Exception {
    NS = "hms" + RND.nextInt(100);
    conf = MetastoreConf.newMetastoreConf();
    HMSTestUtils.setConfForStandloneMode(conf);
    String whpath = new File(baseDir,"target/tmp/warehouse").getAbsolutePath().toString();
    // "hive.metastore.warehouse.external.dir"
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.WAREHOUSE, whpath);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL, whpath);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.SCHEMA_INFO_CLASS, "org.apache.iceberg.rest.HMSTestBase$TestSchemaInfo");
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    // Events that get cleaned happen in batches of 1 to exercise batching code
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.EVENT_CLEAN_MAX_EVENTS, 1L);
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.CATALOG_SERVLET_PORT, 0);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CATALOG_SERVLET_AUTH, "JWT");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CATALOG_SERVLET_PATH, catalogPath);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_METASTORE_AUTHENTICATION_JWT_JWKS_URL,
        "http://localhost:" + MOCK_JWKS_SERVER_PORT + "/jwks");
    MOCK_JWKS_SERVER.stubFor(get("/jwks")
        .willReturn(ok()
            .withBody(Files.readAllBytes(jwtVerificationJWKSFile.toPath()))));
    // The server
    port = createMetastoreServer(conf);
    System.out.println("Starting MetaStore Server on port " + port);
    // The manager decl
    PropertyManager.declare(NS, HMSPropertyManager.class);
    // The client
    metastoreClient = createClient(conf, port);
    Assert.assertNotNull("Unable to connect to the MetaStore server", metastoreClient);

    catalog = new HMSCatalog(conf);
    catalog.initialize("hive", Collections.emptyMap());
    catalogPort = createCatalogServer(conf, catalog);

    Warehouse wh = new Warehouse(conf);
    String location0 = wh.getDefaultDatabasePath("hivedb2023", false).toString();
    //wh.getDefaultDatabasePath()
    String location = temp.newFolder("hivedb2023").getAbsolutePath().toString();
    Database db = new Database("hivedb", "catalog test", location, Collections.emptyMap());
    metastoreClient.createDatabase(db);
  }

  protected HiveMetaStoreClient createClient(Configuration conf, int port) throws Exception {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_URIS, "thrift://localhost:" + port);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.EXECUTE_SET_UGI, false);
    return new HiveMetaStoreClient(conf);
  }

  @After
  public synchronized void tearDown() throws Exception {
    try {
      if (port >= 0) {
        stopMetastoreServer(port);
        port = -1;
      }
      if (catalogPort >= 0) {
        stopCatalogServer(catalogPort);
      }
      // Clear the SSL system properties before each test.
      System.clearProperty(ObjectStore.TRUSTSTORE_PATH_KEY);
      System.clearProperty(ObjectStore.TRUSTSTORE_PASSWORD_KEY);
      System.clearProperty(ObjectStore.TRUSTSTORE_TYPE_KEY);
      //
    } finally {
      //client = null;
      conf = null;
    }
  }

  protected String generateJWT()  throws Exception {
    return generateJWT(jwtAuthorizedKeyFile.toPath());
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

  protected static final String CLI = "hmscli";
  Server servletServer = null;
  int sport = -1;

  /**
   * Creates and starts the catalog server.
   * @param conf
   * @return the server port
   * @throws Exception
   */
  protected int createCatalogServer(Configuration conf, HMSCatalog catalog) throws Exception {
    if (servletServer == null) {
      servletServer = HMSCatalogServer.startCatalogServer(conf, catalog);
      if (servletServer == null || !servletServer.isStarted()) {
        Assert.fail("http server did not start");
      }
      sport = servletServer.getURI().getPort();
    }
    return sport;
  }

  /**
   * Stops the catalog server.
   * @param port the server port
   * @throws Exception
   */
  protected void stopCatalogServer(int port) throws Exception {
    if (servletServer != null) {
      servletServer.stop();
      servletServer = null;
      sport = -1;
    }
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
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod(method);
    con.setRequestProperty(MetaStoreUtils.USER_NAME_HTTP_HEADER, url.getUserInfo());
    con.setRequestProperty("Content-Type", "application/json");
    con.setRequestProperty("Accept", "application/json");
    if (jwt != null) {
      con.setRequestProperty("Authorization","Bearer " + jwt);
    }
    con.setDoInput(true);
    if (arg != null) {
      con.setDoOutput(true);
      DataOutputStream wr = new DataOutputStream(con.getOutputStream());
      wr.writeBytes(new Gson().toJson(arg));
      wr.flush();
      wr.close();
    }
    int responseCode = con.getResponseCode();
    if (responseCode == HttpServletResponse.SC_OK) {
      try (Reader reader = new BufferedReader(
          new InputStreamReader(con.getInputStream(), StandardCharsets.UTF_8))) {
        return new Gson().fromJson(reader, Object.class);
      }
    }
    return null;
  }

}