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
package org.apache.hive.ptest.execution;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;

import com.google.common.collect.Sets;
import org.apache.commons.cli.*;
import org.apache.commons.io.FilenameUtils;
import org.apache.hive.ptest.api.server.TestLogger;
import org.apache.hive.ptest.execution.conf.Context;
import org.apache.hive.ptest.execution.conf.TestConfiguration;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScheme;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.AuthState;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.ExecutionContext;
import org.apache.http.protocol.HttpContext;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

class JIRAService {
  static final int MAX_MESSAGES = 200;
  static final String TRIMMED_MESSAGE = "**** This message was trimmed, see log for full details ****";
  private final Logger mLogger;
  private final String mName;
  private final String mBuildTag;
  private final String mPatch;
  private final String mUrl;
  private final String mUser;
  private final String mPassword;
  private final String mJenkinsURL;
  private final String mLogsURL;

  private static final String OPT_HELP_SHORT = "h";
  private static final String OPT_HELP_LONG = "help";
  private static final String OPT_USER_SHORT = "u";
  private static final String OPT_USER_LONG = "user";
  private static final String OPT_PASS_SHORT = "p";
  private static final String OPT_PASS_LONG = "password";
  private static final String OPT_FILE_SHORT = "f";
  private static final String OPT_FILE_LONG = "file";

  public JIRAService(Logger logger, TestConfiguration configuration, String buildTag) {
    mLogger = logger;
    mName = configuration.getJiraName();
    mBuildTag = buildTag;
    mPatch = configuration.getPatch();
    mUrl = configuration.getJiraUrl();
    mUser = configuration.getJiraUser();
    mPassword = configuration.getJiraPassword();
    mJenkinsURL = configuration.getJenkinsURL();
    mLogsURL = configuration.getLogsURL();
  }

  void postComment(boolean error, int numTestsExecuted, SortedSet<String> failedTests,
    List<String> messages) {
    postComment(error, numTestsExecuted, failedTests, messages, new HashSet<String>());
  }

  void postComment(boolean error, int numTestsExecuted, SortedSet<String> failedTests,
    List<String> messages, Set<String> addedTests) {
    String comments = generateComments(error, numTestsExecuted, failedTests, messages, addedTests);
    publishComments(comments);
  }

  @VisibleForTesting
  String generateComments(boolean error, int numTestsExecuted, SortedSet<String> failedTests,
    List<String> messages, Set<String> addedTests) {
    BuildInfo buildInfo = formatBuildTag(mBuildTag);
    String buildTagForLogs = formatBuildTagForLogs(mBuildTag);
    List<String> comments = Lists.newArrayList();
    comments.add("");
    comments.add("");
    if (!mPatch.isEmpty()) {
      comments.add("Here are the results of testing the latest attachment:");
      comments.add(mPatch);
    }
    comments.add("");
    if (error && numTestsExecuted == 0) {
      comments.add(formatError("-1 due to build exiting with an error"));
    } else {
      if (addedTests.size() > 0) {
        comments.add(formatSuccess("+1 due to " + addedTests.size() + " test(s) being added or modified."));
      } else {
        comments.add(formatError("-1 due to no test(s) being added or modified."));
      }
      comments.add("");
      if (numTestsExecuted == 0) {
        comments.add(formatError("-1 due to no tests executed"));
      } else {
        if (failedTests.isEmpty()) {
          comments.add(formatSuccess("+1 due to " + numTestsExecuted + " tests passed"));
        } else {
          comments.add(formatError("-1 due to " + failedTests.size()
            + " failed/errored test(s), " + numTestsExecuted + " tests executed"));
          comments.add("*Failed tests:*");
          comments.add("{noformat}");
          comments.addAll(failedTests);
          comments.add("{noformat}");
        }
      }
    }
    comments.add("");
    comments.add("Test results: " + mJenkinsURL + "/" +
      buildInfo.getFormattedBuildTag() + "/testReport");
    comments.add("Console output: " + mJenkinsURL + "/" +
      buildInfo.getFormattedBuildTag() + "/console");
    comments.add("Test logs: " + mLogsURL + buildTagForLogs);
    comments.add("");
    if (!messages.isEmpty()) {
      comments.add("Messages:");
      comments.add("{noformat}");
      comments.addAll(trimMessages(messages));
      comments.add("{noformat}");
      comments.add("");
    }
    comments.add("This message is automatically generated.");
    String attachmentId = parseAttachementId(mPatch);
    comments.add("");
    comments.add("ATTACHMENT ID: " + attachmentId +
      " - " + buildInfo.getBuildName());
    mLogger.info("Comment: " + Joiner.on("\n").join(comments));
    return Joiner.on("\n").join(comments);
  }

  void publishComments(String comments) {
    DefaultHttpClient httpClient = new DefaultHttpClient();
    try {
      String url = String.format("%s/rest/api/2/issue/%s/comment", mUrl, mName);
      URL apiURL = new URL(mUrl);
      httpClient.getCredentialsProvider()
        .setCredentials(
          new AuthScope(apiURL.getHost(), apiURL.getPort(),
            AuthScope.ANY_REALM),
          new UsernamePasswordCredentials(mUser, mPassword));
      BasicHttpContext localcontext = new BasicHttpContext();
      localcontext.setAttribute("preemptive-auth", new BasicScheme());
      httpClient.addRequestInterceptor(new PreemptiveAuth(), 0);
      HttpPost request = new HttpPost(url);
      ObjectMapper mapper = new ObjectMapper();
      StringEntity params = new StringEntity(mapper.writeValueAsString(new Body(comments)));
      request.addHeader("Content-Type", "application/json");
      request.setEntity(params);
      HttpResponse httpResponse = httpClient.execute(request, localcontext);
      StatusLine statusLine = httpResponse.getStatusLine();
      if (statusLine.getStatusCode() != 201) {
        throw new RuntimeException(statusLine.getStatusCode() + " "
          + statusLine.getReasonPhrase());
      }
      mLogger.info("JIRA Response Metadata: " + httpResponse);
    } catch (Exception e) {
      mLogger.error("Encountered error attempting to post comment to " + mName,
        e);
    } finally {
      httpClient.getConnectionManager().shutdown();
    }
  }

  static List<String> trimMessages(List<String> messages) {
    int size = messages.size();
    if (size > MAX_MESSAGES) {
      messages = messages.subList(size - MAX_MESSAGES, size);
      messages.add(0, TRIMMED_MESSAGE);
    }
    return messages;
  }

  @SuppressWarnings("unused")
  private static class Body {
    private String body;

    public Body() {

    }

    public Body(String body) {
      this.body = body;
    }

    public String getBody() {
      return body;
    }

    public void setBody(String body) {
      this.body = body;
    }
  }


  public static class BuildInfo {
    private String buildName;
    private String formattedBuildTag;

    public BuildInfo(String buildName, String formattedBuildTag) {
      this.buildName = buildName;
      this.formattedBuildTag = formattedBuildTag;
    }

    public String getBuildName() {
      return buildName;
    }

    public String getFormattedBuildTag() {
      return formattedBuildTag;
    }
  }

  /**
   * Hive-Build-123 to Hive-Build/123
   */
  @VisibleForTesting
  static BuildInfo formatBuildTag(String buildTag) {
    if (buildTag.contains("-")) {
      int lastDashIndex = buildTag.lastIndexOf("-");
      String buildName = buildTag.substring(0, lastDashIndex);
      String buildId = buildTag.substring(lastDashIndex + 1);
      String formattedBuildTag = buildName + "/" + buildId;
      return new BuildInfo(buildName, formattedBuildTag);
    }
    throw new IllegalArgumentException("Build tag '" + buildTag + "' must contain a -");
  }

  static String formatBuildTagForLogs(String buildTag) {
    if (buildTag.endsWith("/")) {
      return buildTag;
    } else {
      return buildTag + "/";
    }
  }

  private static String formatError(String msg) {
    return String.format("{color:red}ERROR:{color} %s", msg);
  }

  private static String formatSuccess(String msg) {
    return String.format("{color:green}SUCCESS:{color} %s", msg);
  }

  static class PreemptiveAuth implements HttpRequestInterceptor {

    public void process(final HttpRequest request, final HttpContext context)
      throws HttpException, IOException {
      AuthState authState = (AuthState) context.getAttribute(ClientContext.TARGET_AUTH_STATE);
      if (authState.getAuthScheme() == null) {
        AuthScheme authScheme = (AuthScheme) context.getAttribute("preemptive-auth");
        CredentialsProvider credsProvider = (CredentialsProvider) context.getAttribute(ClientContext.CREDS_PROVIDER);
        HttpHost targetHost = (HttpHost) context.getAttribute(ExecutionContext.HTTP_TARGET_HOST);
        if (authScheme != null) {
          Credentials creds = credsProvider.getCredentials(new AuthScope(
            targetHost.getHostName(), targetHost.getPort()));
          if (creds == null) {
            throw new HttpException(
              "No credentials for preemptive authentication");
          }
          authState.update(authScheme, creds);
        }
      }
    }
  }

  private static String parseAttachementId(String patch) {
    if (patch == null) {
      return "";
    }
    String result = FilenameUtils.getPathNoEndSeparator(patch.trim());
    if (result == null) {
      return "";
    }
    result = FilenameUtils.getName(result.trim());
    if (result == null) {
      return "";
    }
    return result.trim();
  }

  private static void assertRequired(CommandLine commandLine, String[] requiredOptions) throws IllegalArgumentException {
    for (String requiredOption : requiredOptions) {
      if (!commandLine.hasOption(requiredOption)) {
        throw new IllegalArgumentException("--" + requiredOption + " is required");
      }
    }
  }

  private static final String FIELD_BUILD_STATUS = "buildStatus";
  private static final String FIELD_BUILD_TAG = "buildTag";
  private static final String FIELD_LOGS_URL = "logsURL";
  private static final String FIELD_JENKINS_URL = "jenkinsURL";
  private static final String FIELD_PATCH_URL = "patchUrl";
  private static final String FIELD_JIRA_NAME = "jiraName";
  private static final String FIELD_JIRA_URL = "jiraUrl";
  private static final String FIELD_REPO = "repository";
  private static final String FIELD_REPO_NAME = "repositoryName";
  private static final String FIELD_REPO_TYPE = "repositoryType";
  private static final String FIELD_REPO_BRANCH = "branch";
  private static final String FIELD_NUM_TESTS_EXECUTED = "numTestsExecuted";
  private static final String FIELD_FAILED_TESTS = "failedTests";
  private static final String FIELD_MESSAGES = "messages";
  private static final String FIELD_JIRA_USER = "jiraUser";
  private static final String FIELD_JIRA_PASS = "jiraPassword";

  private static Map<String, Class> supportedJsonFields = new HashMap<String, Class>() {
    {
      put(FIELD_BUILD_STATUS, Integer.class);
      put(FIELD_BUILD_TAG, String.class);
      put(FIELD_LOGS_URL, String.class);
      put(FIELD_JENKINS_URL, String.class);
      put(FIELD_PATCH_URL, String.class);
      put(FIELD_JIRA_NAME, String.class);
      put(FIELD_JIRA_URL, String.class);
      put(FIELD_REPO, String.class);
      put(FIELD_REPO_NAME, String.class);
      put(FIELD_REPO_TYPE, String.class);
      put(FIELD_REPO_BRANCH, String.class);
      put(FIELD_NUM_TESTS_EXECUTED, Integer.class);
      put(FIELD_FAILED_TESTS, SortedSet.class);
      put(FIELD_MESSAGES, List.class);
    }
  };

  private static Map<String, Object> parseJsonFile(String jsonFile) throws IOException {
    JsonFactory jsonFactory = new JsonFactory();
    JsonParser jsonParser = jsonFactory.createJsonParser(new File(jsonFile));
    Map<String, Object> values = new HashMap<String, Object>();

    while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
      String fieldName = jsonParser.getCurrentName();
      if (supportedJsonFields.containsKey(fieldName)) {
        jsonParser.nextToken();

        Class clazz = supportedJsonFields.get(fieldName);
        if (clazz == String.class) {
          values.put(fieldName, jsonParser.getText());
        } else if (clazz == Integer.class) {
          values.put(fieldName, Integer.valueOf(jsonParser.getText()));
        } else if (clazz == SortedSet.class) {
          SortedSet<String> failedTests = new TreeSet<String>();
          while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
            failedTests.add(jsonParser.getText());
          }

          values.put(fieldName, failedTests);
        } else if (clazz == List.class) {
          List<String> messages = new ArrayList<String>();
          while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
            messages.add(jsonParser.getText());
          }

          values.put(fieldName, messages);
        }
      }
    }

    jsonParser.close();
    return values;
  }

  private static CommandLine parseCommandLine(String[] args) throws ParseException {
    CommandLineParser parser = new GnuParser();

    Options options = new Options();
    options.addOption(OPT_HELP_SHORT, OPT_HELP_LONG, false, "Display help text and exit");
    options.addOption(OPT_USER_SHORT, OPT_USER_LONG, true, "Jira username.");
    options.addOption(OPT_PASS_SHORT, OPT_PASS_LONG, true, "Jira password.");
    options.addOption(OPT_FILE_SHORT, OPT_FILE_LONG, true, "Pathname to file (JSON format) that will be post as Jira comment.");

    CommandLine cmd = parser.parse(options, args);

    // If help option is requested, then display help and exit
    if (cmd.hasOption(OPT_HELP_LONG)) {
      new HelpFormatter().printHelp(JIRAService.class.getName(), options, true);
      return null;
    }

    assertRequired(cmd, new String[]{
      OPT_USER_LONG,
      OPT_PASS_LONG,
      OPT_FILE_LONG
    });

    return cmd;
  }

  public static void main(String[] args) throws Exception {
    CommandLine cmd = null;

    try {
      cmd = parseCommandLine(args);
    } catch (ParseException e) {
      System.out.println("Error parsing command arguments: " + e.getMessage());
      System.exit(1);
    }

    // If null is returned, then help message was displayed in parseCommandLine method
    if (cmd == null) {
      System.exit(0);
    }

    Map<String, Object> jsonValues = parseJsonFile(cmd.getOptionValue(OPT_FILE_LONG));

    Map<String, String> context = Maps.newHashMap();
    context.put(FIELD_JIRA_URL, (String) jsonValues.get(FIELD_JIRA_URL));
    context.put(FIELD_JIRA_USER, cmd.getOptionValue(OPT_USER_LONG));
    context.put(FIELD_JIRA_PASS, cmd.getOptionValue(OPT_PASS_LONG));
    context.put(FIELD_LOGS_URL, (String) jsonValues.get(FIELD_LOGS_URL));
    context.put(FIELD_REPO, (String) jsonValues.get(FIELD_REPO));
    context.put(FIELD_REPO_NAME, (String) jsonValues.get(FIELD_REPO_NAME));
    context.put(FIELD_REPO_TYPE, (String) jsonValues.get(FIELD_REPO_TYPE));
    context.put(FIELD_REPO_BRANCH, (String) jsonValues.get(FIELD_REPO_BRANCH));
    context.put(FIELD_JENKINS_URL, (String) jsonValues.get(FIELD_JENKINS_URL));

    TestLogger logger = new TestLogger(System.err, TestLogger.LEVEL.TRACE);
    TestConfiguration configuration = new TestConfiguration(new Context(context), logger);
    configuration.setJiraName((String) jsonValues.get(FIELD_JIRA_NAME));
    configuration.setPatch((String) jsonValues.get(FIELD_PATCH_URL));

    JIRAService service = new JIRAService(logger, configuration, (String) jsonValues.get(FIELD_BUILD_TAG));
    List<String> messages = (List) jsonValues.get(FIELD_MESSAGES);
    SortedSet<String> failedTests = (SortedSet) jsonValues.get(FIELD_FAILED_TESTS);
    boolean error = (Integer) jsonValues.get(FIELD_BUILD_STATUS) == 0 ? false : true;
    service.postComment(error, (Integer) jsonValues.get(FIELD_NUM_TESTS_EXECUTED), failedTests, messages);
  }
}