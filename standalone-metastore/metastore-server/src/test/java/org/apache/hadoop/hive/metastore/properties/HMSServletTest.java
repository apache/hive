/* * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.properties;

import com.google.gson.Gson;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.PropertyServlet;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.eclipse.jetty.server.Server;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import javax.servlet.http.HttpServletResponse;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;

public class HMSServletTest extends HMSTestBase {

  protected static final String baseDir = System.getProperty("basedir");
  private static final File jwtAuthorizedKeyFile =
      new File(baseDir,"src/test/resources/auth/jwt/jwt-authorized-key.json");
  private static final File jwtUnauthorizedKeyFile =
      new File(baseDir,"src/test/resources/auth/jwt/jwt-unauthorized-key.json");
  protected static final File jwtVerificationJWKSFile =
      new File(baseDir,"src/test/resources/auth/jwt/jwt-verification-jwks.json");

  public static final String USER_1 = "USER_1";

  protected static final int MOCK_JWKS_SERVER_PORT = 8089;
  @ClassRule
  public static final WireMockRule MOCK_JWKS_SERVER = new WireMockRule(MOCK_JWKS_SERVER_PORT);
  // the url part
  protected static final String CLI = "hmscli";
  Server servletServer = null;
  int sport = -1;


  @Override protected int createServer(Configuration conf) throws Exception {
    // need store before server for servlet
    if (objectStore == null) {
      MetastoreConf.setVar(conf, MetastoreConf.ConfVars.PROPERTIES_SERVLET_AUTH, "JWT");
      MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_METASTORE_AUTHENTICATION_JWT_JWKS_URL,
          "http://localhost:" + MOCK_JWKS_SERVER_PORT + "/jwks");
      MOCK_JWKS_SERVER.stubFor(get("/jwks")
          .willReturn(ok()
              .withBody(Files.readAllBytes(jwtVerificationJWKSFile.toPath()))));
      boolean inited = createStore(conf);
      LOG.info("MetaStore store initialization " + (inited ? "successful" : "failed"));
    }
    if (servletServer == null) {
      servletServer = PropertyServlet.startServer(conf);
      if (servletServer == null || !servletServer.isStarted()) {
        Assert.fail("http server did not start");
      }
      sport = servletServer.getURI().getPort();
    }
    return sport;
  }

  /**
   * Stops the server.
   * @param port the server port
   */
  @Override protected void stopServer(int port) throws Exception {
    if (servletServer != null) {
      servletServer.stop();
      servletServer = null;
      sport = -1;
    }
  }

  @Override
  protected PropertyClient createClient(Configuration conf, int sport) throws Exception {
    URL url = new URL("http://hive@localhost:" + sport + "/" + CLI + "/" + NS);
    String jwt = generateJWT();
    return new JSonClient(jwt, url);
  }

  protected String generateJWT()  throws Exception {
    return generateJWT(USER_1, jwtAuthorizedKeyFile.toPath(), TimeUnit.MINUTES.toMillis(5));
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
   * A property client that uses http as transport.
   */
  public static class JSonClient implements HttpPropertyClient {
    private final URL url;
    private final String jwt;
    JSonClient(String token, URL url) {
      this.jwt = token;
      this.url = url;
    }

    public boolean setProperties(Map<String, String> properties) {
      try {
        clientCall(jwt, url, "PUT", properties);
        return true;
      } catch(IOException xio) {
        return false;
      }
    }

    @Override
    public Map<String, Map<String, String>> getProperties(String mapPrefix, String mapPredicate, String... selection)  {
      Map<String, Object> args = new TreeMap<>();
      args.put("prefix", mapPrefix);
      if (mapPredicate != null) {
        args.put("predicate", mapPredicate);
      }
      if (selection != null && selection.length > 0) {
        args.put("selection", selection);
      }
      try {
        Object result = clientCall(jwt, url, "POST", args);
        return result instanceof Map? (Map<String, Map<String, String>>) result : null ;
      } catch(IOException xio) {
        return null;
      }
    }

    @Override
    public Map<String, String> getProperties(List<String> selection) {
      try {
        Map<String, Object> args = new TreeMap<>();
        args.put("method", "fetchProperties");
        args.put("keys", selection);
        Object result = clientCall(jwt, url, "POST", args);
        return result instanceof Map? (Map<String, String>) result : null ;
      } catch(IOException xio) {
        return null;
      }
    }
  }


  @Test
  public void testServletEchoA() throws Exception {
    URL url = new URL("http://hive@localhost:" + sport + "/" + CLI + "/" + NS);
    Map<String, String> json = Collections.singletonMap("method", "echo");
    String jwt = generateJWT();
    // succeed
    Object response = clientCall(jwt, url, "POST", json);
    Assert.assertNotNull(response);
    Assert.assertEquals(json, response);
    // fail (null jwt)
    response = clientCall(null, url, "POST", json);
    Assert.assertNull(response);
  }

  @Test
  public void testProperties1() throws Exception {
      runOtherProperties1(client);
  }

  @Test
  public void testProperties0() throws Exception {
      runOtherProperties0(client);

    HttpClient client = new HttpClient();
    String jwt = generateJWT();
    GetMethod method = new GetMethod("http://hive@localhost:" + sport + "/" + CLI + "/" + NS);
    method.addRequestHeader("Authorization", "Bearer " + jwt);
    method.addRequestHeader("Content-Type", "application/json");
    method.addRequestHeader("Accept", "application/json");
    method.addRequestHeader(MetaStoreUtils.USER_NAME_HTTP_HEADER, "hive");

    NameValuePair[] nvps = new NameValuePair[]{
        new NameValuePair("key", "db0.table01.fillFactor"),
        new NameValuePair("key", "db0.table04.fillFactor")
    };
    method.setQueryString(nvps);

    int httpStatus = client.executeMethod(method);
    Assert.assertEquals(HttpServletResponse.SC_OK, httpStatus);
    String resp = method.getResponseBodyAsString();
    Map<String,String> result = ( Map<String,String>) new Gson().fromJson(resp, Map.class);
    Assert.assertEquals(2, result.size());
  }

  @Test
  public void testServletEchoB() throws Exception {
    HttpClient client = new HttpClient();
    String jwt = generateJWT();
    String msgBody = "{\"method\":\"echo\"}";
    PostMethod method = createPost(jwt, msgBody);

    int httpStatus = client.executeMethod(method);
    Assert.assertEquals(HttpServletResponse.SC_OK, httpStatus);
    String resp = method.getResponseBodyAsString();
    Assert.assertNotNull(resp);
    Assert.assertEquals(msgBody, resp);
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
    con.setDoOutput(true);
    con.setDoInput(true);
    DataOutputStream wr = new DataOutputStream(con.getOutputStream());
    wr.writeBytes(new Gson().toJson(arg));
    wr.flush();
    wr.close();
    int responseCode = con.getResponseCode();
    if (responseCode == HttpServletResponse.SC_OK) {
      try (Reader reader = new BufferedReader(
          new InputStreamReader(con.getInputStream(), StandardCharsets.UTF_8))) {
        return new Gson().fromJson(reader, Object.class);
      }
    }
    return null;
  }

  /**
   * Create a PostMethod populated with the expected attributes.
   * @param jwt the security token
   * @param msgBody the actual (json) payload
   * @return the method to be executed by an Http client
   * @throws Exception
   */
  private PostMethod createPost(String jwt, String msgBody) throws Exception {
    PostMethod method = new PostMethod("http://hive@localhost:" + sport + "/" + CLI + "/" + NS);

    method.addRequestHeader("Authorization","Bearer " + jwt);
    method.addRequestHeader("Content-Type", "application/json");
    method.addRequestHeader("Accept", "application/json");

    StringRequestEntity sre = new StringRequestEntity(msgBody, "application/json", "utf-8");
    method.setRequestEntity(sre);
    return method;
  }

}
