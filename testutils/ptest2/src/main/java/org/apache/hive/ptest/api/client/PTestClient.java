/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.ptest.api.client;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.hive.ptest.api.Status;
import org.apache.hive.ptest.api.request.TestListRequest;
import org.apache.hive.ptest.api.request.TestLogRequest;
import org.apache.hive.ptest.api.request.TestStartRequest;
import org.apache.hive.ptest.api.request.TestStatusRequest;
import org.apache.hive.ptest.api.response.GenericResponse;
import org.apache.hive.ptest.api.response.TestListResponse;
import org.apache.hive.ptest.api.response.TestLogResponse;
import org.apache.hive.ptest.api.response.TestStartResponse;
import org.apache.hive.ptest.api.response.TestStatus;
import org.apache.hive.ptest.api.response.TestStatusResponse;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;

/**
 * Quick and dirty REST client for the PTest server. It's not expected the scope
 * of this project will expand significantly therefore a simple REST client should suffice.
 */
public class PTestClient {
  private static final ImmutableMap<Class<?>, EndPointResponsePair> REQUEST_TO_ENDPOINT =
      ImmutableMap.<Class<?>, EndPointResponsePair>builder()
      .put(TestStartRequest.class, new EndPointResponsePair("/testStart", TestStartResponse.class))
      .put(TestStatusRequest.class, new EndPointResponsePair("/testStatus", TestStatusResponse.class))
      .put(TestLogRequest.class, new EndPointResponsePair("/testLog", TestLogResponse.class))
      .put(TestListRequest.class, new EndPointResponsePair("/testList", TestListResponse.class))
      .build();

  private static final String HELP_LONG = "help";
  private static final String HELP_SHORT = "h";
  private static final String ENDPOINT = "endpoint";
  private static final String LOGS_ENDPOINT = "logsEndpoint";
  private static final String COMMAND = "command";
  private static final String PASSWORD = "password";
  private static final String PROFILE = "profile";
  private static final String PATCH = "patch";
  private static final String JIRA = "jira";
  private static final String OUTPUT_DIR = "outputDir";
  private static final String TEST_HANDLE = "testHandle";
  private static final String CLEAR_LIBRARY_CACHE = "clearLibraryCache";
  private static final int MAX_RETRIES = 10;
  private final String mApiEndPoint;
  private final String mLogsEndpoint;
  private final ObjectMapper mMapper;
  private final DefaultHttpClient mHttpClient;
  private final String testOutputDir;

  public PTestClient(String logsEndpoint, String apiEndPoint, String password, String testOutputDir)
      throws MalformedURLException {
    this.testOutputDir = testOutputDir;
    if (!Strings.isNullOrEmpty(testOutputDir)) {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(logsEndpoint),
          "logsEndPoint must be specified if " + OUTPUT_DIR + " is specified");
      if (logsEndpoint.endsWith("/")) {
        this.mLogsEndpoint = logsEndpoint;
      } else {
        this.mLogsEndpoint = logsEndpoint + "/";
      }
    } else {
      this.mLogsEndpoint = null;
    }
    if(apiEndPoint.endsWith("/")) {
      this.mApiEndPoint = apiEndPoint + "api/v1";
    } else {
      this.mApiEndPoint = apiEndPoint + "/api/v1";
    }
    URL apiURL = new URL(mApiEndPoint);
    mMapper = new ObjectMapper();
    mHttpClient = new DefaultHttpClient();
    mHttpClient.getCredentialsProvider().setCredentials(
        new AuthScope(apiURL.getHost(), apiURL.getPort(), AuthScope.ANY_REALM),
        new UsernamePasswordCredentials("hive", password));
  }
  public boolean testStart(String profile, String testHandle,
      String jira, String patch, boolean clearLibraryCache)
          throws Exception {
    patch = Strings.nullToEmpty(patch).trim();
    if(!patch.isEmpty()) {
      byte[] bytes = Resources.toByteArray(new URL(patch));
      if(bytes.length == 0) {
        throw new IllegalArgumentException("Patch " + patch + " was zero bytes");
      }
    }
    TestStartRequest startRequest = new TestStartRequest(profile, testHandle, jira, patch, clearLibraryCache);
    post(startRequest, false);
    boolean result = false;
    try {
      result = testTailLog(testHandle);
      if(testOutputDir != null) {
        downloadTestResults(testHandle, testOutputDir);
      }
    } finally {
      System.out.println("\n\nLogs are located: " + mLogsEndpoint + testHandle + "\n\n");
    }
    return result;
  }
  public boolean testList()
      throws Exception {
    TestListRequest testListRequest = new TestListRequest();
    TestListResponse testListResponse = post(testListRequest, true);
    for(TestStatus testStatus : testListResponse.getEntries()) {
      System.out.println(testStatus);
    }
    return true;
  }
  public boolean testTailLog(String testHandle)
      throws Exception {
    testHandle = Strings.nullToEmpty(testHandle).trim();
    if(testHandle.isEmpty()) {
      throw new IllegalArgumentException("TestHandle is required");
    }
    TestStatusRequest statusRequest = new TestStatusRequest(testHandle);
    TestStatusResponse statusResponse;
    do {
      TimeUnit.SECONDS.sleep(5);
      statusResponse = post(statusRequest, true);
    } while(Status.isPending(statusResponse.getTestStatus().getStatus()));
    long offset = 0;
    do {
      long length = statusResponse.getTestStatus().getLogFileLength();
      if(length > offset) {
        offset = printLogs(testHandle, offset);
      } else {
        TimeUnit.SECONDS.sleep(5);
      }
      statusResponse = post(statusRequest, true);
    } while(Status.isInProgress(statusResponse.getTestStatus().getStatus()));
    while(offset < statusResponse.getTestStatus().getLogFileLength()) {
      offset = printLogs(testHandle, offset);
    }
    Status.assertOKOrFailed(statusResponse.getTestStatus().getStatus());
    return Status.isOK(statusResponse.getTestStatus().getStatus());
  }
  private void downloadTestResults(String testHandle, String testOutputDir)
      throws Exception {
    HttpGet request = new HttpGet(mLogsEndpoint + testHandle + "/test-results.tar.gz");
    FileOutputStream output = null;
    try {
      HttpResponse httpResponse = mHttpClient.execute(request);
      StatusLine statusLine = httpResponse.getStatusLine();
      if(statusLine.getStatusCode() != 200) {
        throw new RuntimeException(statusLine.getStatusCode() + " " + statusLine.getReasonPhrase());
      }
      output = new FileOutputStream(new File(testOutputDir, "test-results.tar.gz"));
      IOUtils.copyLarge(httpResponse.getEntity().getContent(), output);
      output.flush();
    } finally {
      request.abort();
      if(output != null) {
        output.close();
      }
    }
  }
  private long printLogs(String testHandle, long offset)
      throws Exception {
    TestLogRequest logsRequest = new TestLogRequest(testHandle, offset, 64 * 1024);
    TestLogResponse logsResponse = post(logsRequest, true);
    System.out.print(logsResponse.getBody());
    return logsResponse.getOffset();
  }
  private <S extends GenericResponse> S post(Object payload, boolean agressiveRetry)
      throws Exception {
    EndPointResponsePair endPointResponse = Preconditions.
        checkNotNull(REQUEST_TO_ENDPOINT.get(payload.getClass()), payload.getClass().getName());
    HttpPost request = new HttpPost(mApiEndPoint + endPointResponse.getEndpoint());
    try {
      String payloadString = mMapper.writeValueAsString(payload);
      StringEntity params = new StringEntity(payloadString);
      request.addHeader("content-type", "application/json");
      request.setEntity(params);
      if(agressiveRetry) {
        mHttpClient.setHttpRequestRetryHandler(new PTestHttpRequestRetryHandler());          
      }
      HttpResponse httpResponse = mHttpClient.execute(request);
      StatusLine statusLine = httpResponse.getStatusLine();
      if(statusLine.getStatusCode() != 200) {
        throw new IllegalStateException(statusLine.getStatusCode() + " " + statusLine.getReasonPhrase());
      }
      String response = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
      @SuppressWarnings("unchecked")
      S result =  (S)endPointResponse.
      getResponseClass().cast(mMapper.readValue(response, endPointResponse.getResponseClass()));
      Status.assertOK(result.getStatus());
      if(System.getProperty("DEBUG_PTEST_CLIENT") != null) {
        System.err.println("payload " + payloadString);
        if(result instanceof TestLogResponse) {
          System.err.println("response " + ((TestLogResponse)result).getOffset() + " " + ((TestLogResponse)result).getStatus());
        } else {
          System.err.println("response " + response);
        }
      }
      Thread.sleep(1000);
      return result;
    } finally {
      request.abort();
    }
  }
  private static class PTestHttpRequestRetryHandler implements HttpRequestRetryHandler {
    @Override
    public boolean retryRequest(IOException exception, int executionCount,
        HttpContext context) {
      System.err.println("LOCAL ERROR: " + exception.getMessage());
      exception.printStackTrace();
      if(executionCount > MAX_RETRIES) {
        return false;
      }
      try {
        Thread.sleep(30L * 1000L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return true;
    }
    
  }
  private static class EndPointResponsePair {
    final String endpoint;
    final Class<? extends GenericResponse> responseClass;
    public EndPointResponsePair(String endpoint,
        Class<? extends GenericResponse> responseClass) {
      this.endpoint = endpoint;
      this.responseClass = responseClass;
    }
    public String getEndpoint() {
      return endpoint;
    }
    public Class<? extends GenericResponse> getResponseClass() {
      return responseClass;
    }
  }
  private static void assertRequired(CommandLine commandLine, String[] requiredOptions) {
    for(String requiredOption : requiredOptions) {
      if(!commandLine.hasOption(requiredOption)) {
        throw new IllegalArgumentException(requiredOption + " is required");
      }
    }
  }
  public static void main(String[] args) throws Exception {
    //TODO This line can be removed once precommit jenkins jobs move to Java 8
    System.setProperty("https.protocols", "TLSv1,TLSv1.1,TLSv1.2");
    CommandLineParser parser = new GnuParser();
    Options options = new Options();
    options.addOption(HELP_SHORT, HELP_LONG, false, "Display help text and exit");
    options.addOption(null, ENDPOINT, true, "Service to use. E.g. http://localhost/ (Required)");
    options.addOption(null, COMMAND, true, "Command: [testStart, testStop, testTailLog, testList] (Required)");
    options.addOption(null, PASSWORD, true, "Password for service. Any committer should know this otherwise as private@. (Required)");
    options.addOption(null, PROFILE, true, "Test profile such as trunk-mr1 or trunk-mr2 (Required for testStart)");
    options.addOption(null, PATCH, true, "URI to patch, must start with http(s):// (Optional for testStart)");
    options.addOption(null, JIRA, true, "JIRA to post the results to e.g.: HIVE-XXXX");
    options.addOption(null, TEST_HANDLE, true, "Server supplied test handle. (Required for testStop and testTailLog)");
    options.addOption(null, OUTPUT_DIR, true, "Directory to download and save test-results.tar.gz to. (Optional for testStart)");
    options.addOption(null, CLEAR_LIBRARY_CACHE, false, "Before starting the test, delete the ivy and maven directories (Optional for testStart)");
    options.addOption(null, LOGS_ENDPOINT, true, "URL to get the logs");

    CommandLine commandLine = parser.parse(options, args);

    if(commandLine.hasOption(HELP_SHORT)) {
      new HelpFormatter().printHelp(PTestClient.class.getName(), options, true);
      System.exit(0);
    }
    assertRequired(commandLine, new String[] {
        COMMAND,
        PASSWORD,
        ENDPOINT
    });
    PTestClient client = new PTestClient(commandLine.getOptionValue(LOGS_ENDPOINT), commandLine.getOptionValue(ENDPOINT),
        commandLine.getOptionValue(PASSWORD), commandLine.getOptionValue(OUTPUT_DIR));
    String command = commandLine.getOptionValue(COMMAND);
    boolean result;
    if("testStart".equalsIgnoreCase(command)) {
      assertRequired(commandLine, new String[] {
          PROFILE,
          TEST_HANDLE
      });
      result = client.testStart(commandLine.getOptionValue(PROFILE), commandLine.getOptionValue(TEST_HANDLE),
          commandLine.getOptionValue(JIRA), commandLine.getOptionValue(PATCH),
          commandLine.hasOption(CLEAR_LIBRARY_CACHE));
    } else if("testTailLog".equalsIgnoreCase(command)) {
      result = client.testTailLog(commandLine.getOptionValue(TEST_HANDLE));
    } else if("testList".equalsIgnoreCase(command)) {
      result = client.testList();
    } else {
      throw new IllegalArgumentException("Unknown " + COMMAND + ": " + command);
    }
    if(result) {
      System.exit(0);
    } else {
      System.exit(1);
    }
  }
}
