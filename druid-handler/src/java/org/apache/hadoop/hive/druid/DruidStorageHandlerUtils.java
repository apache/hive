/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.druid;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.core.MediaType;

import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.InputStreamResponseHandler;

import io.druid.indexing.common.task.HadoopIndexTask;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.BaseQuery;

/**
 * Utils class for Druid storage handler.
 */
public final class DruidStorageHandlerUtils {

  private static final String SMILE_CONTENT_TYPE = "application/x-jackson-smile";

  /**
   * Mapper to use to serialize/deserialize Druid objects (JSON)
   */
  public static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  /**
   * Mapper to use to serialize/deserialize Druid objects (SMILE)
   */
  public static final ObjectMapper SMILE_MAPPER = new DefaultObjectMapper(new SmileFactory());

  /**
   * Method that creates a request for Druid JSON task
   * @param mapper
   * @param address
   * @param task
   * @return
   * @throws IOException
   */
  public static Request createTaskRequest(String address, HadoopIndexTask task)
          throws IOException {
    return new Request(HttpMethod.POST,
                    new URL(String.format("%s/druid/indexer/v1/task/", "http://" + address)))
            .setContent(JSON_MAPPER.writeValueAsBytes(task))
            .setHeader(HttpHeaders.Names.CONTENT_TYPE, MediaType.APPLICATION_JSON);
  }

  /**
   * Method that creates a request for Druid JSON task status
   * @param mapper
   * @param address
   * @param taskID
   * @return
   * @throws IOException
   */
  public static Request createTaskStatusRequest(String address, String taskId)
          throws IOException {
    return new Request(HttpMethod.GET,
                    new URL(String.format("%s/druid/indexer/v1/task/%s/status",
                            "http://" + address, taskId)));
  }

  /**
   * Method that creates a request for Druid JSON task shutdown
   * @param mapper
   * @param address
   * @param taskID
   * @return
   * @throws IOException
   */
  public static Request createTaskShutdownRequest(String address, String taskId)
          throws IOException {
    return new Request(HttpMethod.POST,
                    new URL(String.format("%s/druid/indexer/v1/task/%s/shutdown",
                            "http://" + address, taskId)));
  }

  /**
   * Method that creates a request for Druid JSON query (using SMILE).
   * @param mapper
   * @param address
   * @param query
   * @return
   * @throws IOException
   */
  public static Request createQueryRequest(String address, BaseQuery<?> query)
          throws IOException {
    return new Request(HttpMethod.POST, new URL(String.format("%s/druid/v2/", "http://" + address)))
            .setContent(SMILE_MAPPER.writeValueAsBytes(query))
            .setHeader(HttpHeaders.Names.CONTENT_TYPE, SMILE_CONTENT_TYPE);
  }

  /**
   * Method that submits a request to an Http address and retrieves the result.
   * The caller is responsible for closing the stream once it finishes consuming it.
   * @param client
   * @param request
   * @return
   * @throws IOException
   */
  public static InputStream submitRequest(HttpClient client, Request request)
          throws IOException {
    InputStream response;
    try {
      response = client.go(request, new InputStreamResponseHandler()).get();
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    } catch (InterruptedException e) {
      throw new IOException(e.getCause());
    }
    return response;
  }

  /**
   * Method that monitors a task. It will send a status request every sleepTime milliseconds, till
   * either the task is completed, or the total time is greater than timeout milliseconds. Returns
   * the final status of the task.
   * @param client
   * @param address
   * @param initialStatus
   * @param timeout
   * @param sleepTime
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  public static JsonNode monitorTask(HttpClient client, String address, String taskId,
          long initialSleep, long timeout, long sleepTime) throws IOException, InterruptedException {
    // Create request to monitor the status
    final Request taskStatusRequest = createTaskStatusRequest(address, taskId);
    long startTime = System.currentTimeMillis();
    // Sleep for the initial time to let Druid start the indexing task
    // Note that the initial sleep counts towards the total timeout
    Thread.sleep(initialSleep);
    long endTime = startTime;
    JsonNode statusResponse;
    do {
      InputStream response = submitRequest(client, taskStatusRequest);
      statusResponse = JSON_MAPPER.readValue(response, JsonNode.class);
      Thread.sleep(sleepTime);
      endTime = System.currentTimeMillis();
    } while (isRunnable(statusResponse) && (timeout < 0 || endTime - startTime < timeout));
    return statusResponse;
  }

  /**
   * Returns whether the JSON response from the Druid coordinator specifies that
   * the task is still running.
   * @param status JSON response from the Druid coordinator
   * @return true if we can infer that the task is still running, false otherwise
   */
  protected static boolean isRunnable(JsonNode status) {
    String s = extractStatusFromResponse(status);
    if (s == null) {
      // Null response, consider it is finished
      return false;
    }
    return s.equalsIgnoreCase("RUNNING");
  }

  /**
   * Returns whether the JSON response from the Druid coordinator specifies that
   * the task has finished.
   * @param status JSON response from the Druid coordinator
   * @return true if we can infer that the task has been completed, false otherwise
   */
  protected static boolean isComplete(JsonNode status) {
    String s = extractStatusFromResponse(status);
    if (s == null) {
      // Null response, consider it is finished
      return true;
    }
    return !s.equalsIgnoreCase("RUNNING");
  }

  /**
   * Returns whether the JSON response from the Druid coordinator specifies that
   * the task has finished and its execution has been successful.
   * @param status JSON response from the Druid coordinator
   * @return true if we can infer that the task has succeeded, false otherwise
   */
  protected static boolean isSuccess(JsonNode status) {
    String s = extractStatusFromResponse(status);
    if (s == null) {
      // Null response, consider it failed
      return false;
    }
    return s.equalsIgnoreCase("SUCCESS");
  }

  /**
   * Returns whether the JSON response from the Druid coordinator specifies that
   * the task has finished and its execution has failed.
   * @param status JSON response from the Druid coordinator
   * @return true if we cannot infer that the task failed or we cannot infer that
   * the task has succeeded, false otherwise
   */
  protected static boolean isFailure(JsonNode status) {
    String s = extractStatusFromResponse(status);
    if (s == null) {
      // Null response, consider it failed
      return true;
    }
    return s.equalsIgnoreCase("FAILED");
  }

  private static String extractStatusFromResponse(JsonNode status) {
    JsonNode taskStatus = status.get("status");
    if (taskStatus != null) {
      return taskStatus.get("status").textValue();
    }
    return null;
  }

  /**
   * Extract the duration of the task from the JSON response of the Druid
   * coordinator.
   * @param status JSON response from the Druid coordinator
   * @return the task duration in ms, if available; null otherwise
   */
  protected static Long extractDurationFromResponse(JsonNode status) {
    JsonNode taskStatus = status.get("status");
    if (taskStatus != null) {
      return taskStatus.get("duration").longValue();
    }
    return null;
  }

}
