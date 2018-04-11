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

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

/**
 * Utility class for the Precommit test job queue on Jenkins
 */
public class JenkinsQueueUtil {

  private static final String JSON_ITEMS_FIELD = "items";
  private static final String JSON_TASK_FIELD = "task";
  private static final String JSON_TASK_NAME_FIELD = "name";
  private static final String JSON_PARAMETERS_FIELD = "parameters";
  private static final String JSON_PARAMETER_NAME_FIELD = "name";
  private static final String JSON_PARAMETER_VALUE_FIELD = "value";

  private static final String JOB_NAME = "PreCommit-HIVE-Build";
  private static final String ISSUE_FIELD_KEY = "ISSUE_NUM";
  private static final String JIRA_KEY_PREFIX = "HIVE-";

  /**
   * Looks up the current queue of the precommit job on a jenkins instance (specified by
   * PTestClient.JENKINS_QUEUE_URL), and checks if current Jira is standing in queue already (i.e.
   * will be executed in the future too)
   *
   * @param commandLine PTestClient's command line option values' list
   * @return whether or not the Jira specified in the command line can be found in the job queue
   */
  public static boolean isJiraAlreadyInQueue(CommandLine commandLine) {
    if (!(commandLine.hasOption(PTestClient.JENKINS_QUEUE_URL) &&
            commandLine.hasOption(PTestClient.JIRA))) {
      return false;
    }
    try {
      System.out.println("Checking " + JOB_NAME + " queue...");
      String queueJson = httpGet(commandLine.getOptionValue(PTestClient.JENKINS_QUEUE_URL));
      List<String> jirasInQueue = parseJiras(queueJson);
      if (jirasInQueue.size() > 0) {
        System.out.println(JOB_NAME + " has the following jira(s) in queue: " + jirasInQueue);
      } else {
        return false;
      }

      String jira = commandLine.getOptionValue(PTestClient.JIRA).replaceAll(JIRA_KEY_PREFIX,"");
      if (jirasInQueue.contains(jira)) {
        return true;
      }

    } catch (IOException e) {
      System.err.println("Error checking " + JOB_NAME + " build queue: " + e);
    }
    return false;
  }

  /**
   * Parses raw json to produce a list of Jira number strings.
   * @param queueJson
   * @return
   * @throws IOException
   */
  private static List<String> parseJiras(String queueJson) throws IOException {
    List<String> jirasInQueue = new ArrayList<>();
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode rootNode = objectMapper.readTree(queueJson);
    List<JsonNode> items = Lists.newArrayList(rootNode.findValue(JSON_ITEMS_FIELD).iterator());
    for (JsonNode item : items) {
      String taskName = item.path(JSON_TASK_FIELD).path(JSON_TASK_NAME_FIELD).asText();
      if (JOB_NAME.equals(taskName)) {
        List<JsonNode> parameters = Lists.newArrayList(item.findValue(JSON_PARAMETERS_FIELD));
        for (JsonNode parameter : parameters) {
          if (ISSUE_FIELD_KEY.equals(parameter.path(JSON_PARAMETER_NAME_FIELD).asText())) {
            jirasInQueue.add(parameter.path(JSON_PARAMETER_VALUE_FIELD).asText());
          }
        }
      }
    }
    return jirasInQueue;
  }

  private static String httpGet(String url)
          throws IOException {

    HttpGet request = new HttpGet(url);
    try {
      CloseableHttpClient httpClient = HttpClientBuilder
              .create()
              .setSslcontext(SSLContexts.custom().useProtocol("TLSv1.2").build())
              .setRetryHandler(new PTestClient.PTestHttpRequestRetryHandler())
              .build();
      request.addHeader("content-type", "application/json");
      HttpResponse httpResponse = httpClient.execute(request);
      StatusLine statusLine = httpResponse.getStatusLine();
      if (statusLine.getStatusCode() != 200) {
        throw new IllegalStateException(statusLine.getStatusCode() + " " + statusLine.getReasonPhrase());
      }
      String response = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
      return response;
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      e.printStackTrace();
      throw new IOException(e.getMessage());
    } finally {
      request.abort();
    }
  }


}
