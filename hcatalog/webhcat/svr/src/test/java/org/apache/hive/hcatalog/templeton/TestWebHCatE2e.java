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
package org.apache.hive.hcatalog.templeton;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.eclipse.jetty.http.HttpStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.Assert;

/**
 * A set of tests exercising e2e WebHCat DDL APIs.  These tests are somewhat
 * between WebHCat e2e (hcatalog/src/tests/e2e/templeton) tests and simple58
 *
 * unit tests.  This will start a WebHCat server and make REST calls to it.
 * It doesn't need Hadoop or (standalone) metastore to be running.
 * Running this is much simpler than e2e tests.
 *
 * Most of these tests check that HTTP Status code is what is expected and
 * Hive Error code {@link org.apache.hadoop.hive.ql.ErrorMsg} is what is
 * expected.
 *
 * It may be possible to extend this to more than just DDL later.
 */
@Ignore("HIVE-26343")
public class TestWebHCatE2e {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestWebHCatE2e.class);
  private static String templetonBaseUrl =
      "http://localhost:50111/templeton/v1";
  private static final String username= "johndoe";
  private static final String ERROR_CODE = "errorCode";
  private static Main templetonServer;
  private static final String charSet = "UTF-8";

  @BeforeClass
  public static void startHebHcatInMem() throws Exception {
    Exception webhcatException = null;
    int webhcatPort = 0;
    boolean webhcatStarted = false;

    for (int tryCount = 0; tryCount < MetaStoreTestUtils.RETRY_COUNT; tryCount++) {
      try {
        if (tryCount == MetaStoreTestUtils.RETRY_COUNT - 1) {
          /* Last try to get a port.  Just use default 50111.  */
          webhcatPort = 50111;
          LOG.warn("Unable to find free port; using default: " + webhcatPort);
        }
        else {
          webhcatPort = MetaStoreTestUtils.findFreePort();
        }
        templetonBaseUrl = templetonBaseUrl.replace("50111", Integer.toString(webhcatPort));
        templetonServer = new Main(new String[] { "-D" + AppConfig.UNIT_TEST_MODE + "=true",
            "-D" + AppConfig.PORT + "=" + webhcatPort });
        LOG.info("Starting Main; WebHCat using port: " + webhcatPort);
        templetonServer.run();
        LOG.info("Main started");
        webhcatStarted = true;
        break;
      } catch (Exception ce) {
        LOG.info("Attempt to Start WebHCat using port: " + webhcatPort + " failed");
        webhcatException = ce;
      }
    }

    if (!webhcatStarted) {
      throw webhcatException;
    }
  }

  @AfterClass
  public static void stopWebHcatInMem() {
    if(templetonServer != null) {
      LOG.info("Stopping Main");
      templetonServer.stop();
      LOG.info("Main stopped");
    }
  }

  private static Map<String, String> jsonStringToSortedMap(String jsonStr) {
    Map<String, String> sortedMap;
    try {
      sortedMap = (new ObjectMapper()).readValue(jsonStr,
          new TypeReference<TreeMap<String, String>>() {});
    } catch (Exception ex) {
      throw new RuntimeException(
          "Exception converting json string to sorted map " + ex, ex);
    }

    return sortedMap;
  }

  @Test
  public void getStatus() throws IOException {
    LOG.debug("+getStatus()");
    MethodCallRetVal p = doHttpCall(templetonBaseUrl + "/status", HTTP_METHOD_TYPE.GET);
    Assert.assertEquals(p.getAssertMsg(), HttpStatus.OK_200, p.httpStatusCode);
    // Must be deterministic order map for comparison across Java versions
    Assert.assertTrue(p.getAssertMsg(),
        jsonStringToSortedMap("{\"status\":\"ok\",\"version\":\"v1\"}").equals(
            jsonStringToSortedMap(p.responseBody)));

    LOG.debug("-getStatus()");
  }

  @Ignore("not ready due to HIVE-4824")
  @Test
  public void listDataBases() throws IOException {
    LOG.debug("+listDataBases()");
    MethodCallRetVal p = doHttpCall(templetonBaseUrl + "/ddl/database", HTTP_METHOD_TYPE.GET);
    Assert.assertEquals(p.getAssertMsg(), HttpStatus.OK_200, p.httpStatusCode);
    Assert.assertEquals(p.getAssertMsg(), "{\"databases\":[\"default\"]}", p.responseBody);
    LOG.debug("-listDataBases()");
  }

  /**
   * Check that we return correct status code when the URL doesn't map to any method
   * in {@link Server}
   */
  @Test
  public void invalidPath() throws IOException {
    MethodCallRetVal p = doHttpCall(templetonBaseUrl + "/no_such_mapping/database", HTTP_METHOD_TYPE.GET);
    Assert.assertEquals(p.getAssertMsg(), HttpStatus.NOT_FOUND_404, p.httpStatusCode);
  }
  /**
   * tries to drop table in a DB that doesn't exist
   */

  @Ignore("not ready due to HIVE-4824")
  @Test
  public void dropTableNoSuchDB() throws IOException {
    MethodCallRetVal p = doHttpCall(templetonBaseUrl +
      "/ddl/database/no_such_db/table/t1", HTTP_METHOD_TYPE.DELETE);
    Assert.assertEquals(p.getAssertMsg(), HttpStatus.NOT_FOUND_404, p.httpStatusCode);
    Assert.assertEquals(p.getAssertMsg(),
      ErrorMsg.DATABASE_NOT_EXISTS.getErrorCode(),
      getErrorCode(p.responseBody));
  }

  /**
   * tries to drop table in a DB that doesn't exist
   */
  @Ignore("not ready due to HIVE-4824")
  @Test
  public void dropTableNoSuchDbIfExists() throws IOException {
    MethodCallRetVal p = doHttpCall(templetonBaseUrl + "/ddl/database/no_such_db/table/t1",
      HTTP_METHOD_TYPE.DELETE, null, new NameValuePair[]
      {new NameValuePair("ifExists", "true")});
    Assert.assertEquals(p.getAssertMsg(), HttpStatus.NOT_FOUND_404, p.httpStatusCode);
    Assert.assertEquals(p.getAssertMsg(), ErrorMsg.DATABASE_NOT_EXISTS.getErrorCode(), getErrorCode(p.responseBody));
  }

  /**
   * tries to drop table that doesn't exist (with ifExists=true)
  */
  @Ignore("not ready due to HIVE-4824")
  @Test
  public void dropTableIfExists() throws IOException {
    MethodCallRetVal p = doHttpCall(templetonBaseUrl + "/ddl/database/default/table/no_such_table",
      HTTP_METHOD_TYPE.DELETE, null, new NameValuePair[]
      {new NameValuePair("ifExists", "true")});
    Assert.assertEquals(p.getAssertMsg(), HttpStatus.OK_200, p.httpStatusCode);
  }

  @Ignore("not ready due to HIVE-4824")
  @Test
  public void createDataBase() throws IOException {
    Map<String, Object> props = new HashMap<String, Object>();
    props.put("comment", "Hello, there");
    props.put("location", System.getProperty("test.warehouse.dir"));
    Map<String, String> props2 = new HashMap<String, String>();
    props2.put("prop", "val");
    props.put("properties", props2);
    //{ "comment":"Hello there", "location":"file:///tmp/warehouse", "properties":{"a":"b"}}
    MethodCallRetVal p = doHttpCall(templetonBaseUrl + "/ddl/database/newdb", HTTP_METHOD_TYPE.PUT, props, null);
    Assert.assertEquals(p.getAssertMsg(), HttpStatus.OK_200, p.httpStatusCode);
  }

  @Ignore("not ready due to HIVE-4824")
  @Test
  public void createTable() throws IOException {
    //{ "comment":"test", "columns": [ { "name": "col1", "type": "string" } ], "format": { "storedAs": "rcfile" } }
    Map<String, Object> props = new HashMap<String, Object>();
    props.put("comment", "Table in default db");
    Map<String, Object> col = new HashMap<String, Object>();
    col.put("name", "col1");
    col.put("type", "string");
    List<Map<String, Object>> colList = new ArrayList<Map<String, Object>>(1);
    colList.add(col);
    props.put("columns", colList);
    Map<String, Object> format = new HashMap<String, Object>();
    format.put("storedAs", "rcfile");
    props.put("format", format);
    MethodCallRetVal createTbl = doHttpCall(templetonBaseUrl + "/ddl/database/default/table/test_table", HTTP_METHOD_TYPE.PUT, props, null);
    Assert.assertEquals(createTbl.getAssertMsg(), HttpStatus.OK_200, createTbl.httpStatusCode);
    LOG.info("createTable() resp: " + createTbl.responseBody);

    MethodCallRetVal descTbl = doHttpCall(templetonBaseUrl + "/ddl/database/default/table/test_table", HTTP_METHOD_TYPE.GET);
    Assert.assertEquals(descTbl.getAssertMsg(), HttpStatus.OK_200, descTbl.httpStatusCode);
  }

  @Ignore("not ready due to HIVE-4824")
  @Test
  public void describeNoSuchTable() throws IOException {
    MethodCallRetVal p = doHttpCall(templetonBaseUrl +
      "/ddl/database/default/table/no_such_table", HTTP_METHOD_TYPE.GET);
    Assert.assertEquals(p.getAssertMsg(), HttpStatus.NOT_FOUND_404,
      p.httpStatusCode);
    Assert.assertEquals(p.getAssertMsg(),
      ErrorMsg.INVALID_TABLE.getErrorCode(),
      getErrorCode(p.responseBody));
  }

  @Test
  public void getHadoopVersion() throws Exception {
    MethodCallRetVal p = doHttpCall(templetonBaseUrl + "/version/hadoop",
        HTTP_METHOD_TYPE.GET);
    Assert.assertEquals(HttpStatus.OK_200, p.httpStatusCode);
    Map<String, Object> props = JsonBuilder.jsonToMap(p.responseBody);
    Assert.assertEquals("hadoop", props.get("module"));
    Assert.assertTrue(p.getAssertMsg(),
        ((String)props.get("version")).matches("[1-3].[0-9]+.[0-9]+.*"));
  }

  @Test
  public void getHiveVersion() throws Exception {
    MethodCallRetVal p = doHttpCall(templetonBaseUrl + "/version/hive",
        HTTP_METHOD_TYPE.GET);
    Assert.assertEquals(HttpStatus.OK_200, p.httpStatusCode);
    Map<String, Object> props = JsonBuilder.jsonToMap(p.responseBody);
    Assert.assertEquals("hive", props.get("module"));
    Assert.assertTrue(p.getAssertMsg(),
        ((String) props.get("version")).matches("[0-9]+.[0-9]+.[0-9]+.*"));
  }

  @Test
  public void getPigVersion() throws Exception {
    MethodCallRetVal p = doHttpCall(templetonBaseUrl + "/version/pig",
        HTTP_METHOD_TYPE.GET);
    Assert.assertEquals(HttpStatus.NOT_IMPLEMENTED_501, p.httpStatusCode);
    Map<String, Object> props = JsonBuilder.jsonToMap(p.responseBody);
    Assert.assertEquals(p.getAssertMsg(), "Pig version request not yet " +
        "implemented", props.get("error"));
  }

  /**
   * It's expected that Templeton returns a properly formatted JSON object when it
   * encounters an error.  It should have {@code ERROR_CODE} element in it which
   * should be the Hive canonical error msg code.
   * @return the code or -1 if it cannot be found
   */
  private static int getErrorCode(String jsonErrorObject) throws IOException {
    @SuppressWarnings("unchecked")//JSON key is always a String
    Map<String, Object> retProps = JsonBuilder.jsonToMap(jsonErrorObject + "blah blah");
    int hiveRetCode = -1;
    if(retProps.get(ERROR_CODE) !=null) {
      hiveRetCode = Integer.parseInt(retProps.get(ERROR_CODE).toString());
    }
    return hiveRetCode;
  }

  /**
   * Encapsulates information from HTTP method call
   */
  private static class MethodCallRetVal {
    private final int httpStatusCode;
    private final String responseBody;
    private final String submittedURL;
    private final String methodName;
    private MethodCallRetVal(int httpStatusCode, String responseBody, String submittedURL, String methodName) {
      this.httpStatusCode = httpStatusCode;
      this.responseBody = responseBody;
      this.submittedURL = submittedURL;
      this.methodName = methodName;
    }
    String getAssertMsg() {
      return methodName + " " + submittedURL + " " + responseBody;
    }
  }

  private static enum HTTP_METHOD_TYPE {GET, POST, DELETE, PUT}
  private static MethodCallRetVal doHttpCall(String uri, HTTP_METHOD_TYPE type) throws IOException {
    return doHttpCall(uri, type, null, null);
  }

  /**
   * Does a basic HTTP GET and returns Http Status code + response body
   * Will add the dummy user query string
   */
  private static MethodCallRetVal doHttpCall(String uri, HTTP_METHOD_TYPE type, Map<String, Object> data, NameValuePair[] params) throws IOException {
    HttpClient client = new HttpClient();
    HttpMethod method;
    switch (type) {
    case GET:
      method = new GetMethod(uri);
      break;
    case DELETE:
      method = new DeleteMethod(uri);
      break;
    case PUT:
      method = new PutMethod(uri);
      if(data == null) {
        break;
      }
      String msgBody = JsonBuilder.mapToJson(data);
      LOG.info("Msg Body: " + msgBody);
      StringRequestEntity sre = new StringRequestEntity(msgBody, "application/json", charSet);
      ((PutMethod)method).setRequestEntity(sre);
      break;
    default:
      throw new IllegalArgumentException("Unsupported method type: " + type);
    }
    if(params == null) {
      method.setQueryString(new NameValuePair[] {new NameValuePair("user.name", username)});
    }
    else {
      NameValuePair[] newParams = new NameValuePair[params.length + 1];
      System.arraycopy(params, 0, newParams, 1, params.length);
      newParams[0] = new NameValuePair("user.name", username);
      method.setQueryString(newParams);
    }
    String actualUri = "no URI";
    try {
      actualUri = method.getURI().toString();//should this be escaped string?
      LOG.debug(type + ": " + method.getURI().getEscapedURI());
      int httpStatus = client.executeMethod(method);
      LOG.debug("Http Status Code=" + httpStatus);
      String resp = method.getResponseBodyAsString();
      LOG.debug("response: " + resp);
      return new MethodCallRetVal(httpStatus, resp, actualUri, method.getName());
    }
    catch (IOException ex) {
      LOG.error("doHttpCall() failed", ex);
    }
    finally {
      method.releaseConnection();
    }
    return new MethodCallRetVal(-1, "Http " + type + " failed; see log file for details", actualUri, method.getName());
  }
}
