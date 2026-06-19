/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.llap.daemon;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HistoryLogger {


  private static final String HISTORY_EVENT_TYPE = "Event";
  private static final String HISTORY_APPLICATION_ID = "ApplicationId";
  private static final String HISTORY_CONTAINER_ID = "ContainerId";
  private static final String HISTORY_SUBMIT_TIME = "SubmitTime";
  private static final String HISTORY_START_TIME = "StartTime";
  private static final String HISTORY_END_TIME = "EndTime";
  private static final String HISTORY_QUERY_ID = "QueryId";
  private static final String HISTORY_DAG_ID = "DagId";
  private static final String HISTORY_VERTEX_NAME = "VertexName";
  private static final String HISTORY_TASK_ID = "TaskId";
  private static final String HISTORY_ATTEMPT_ID = "TaskAttemptId";
  private static final String HISTORY_THREAD_NAME = "ThreadName";
  private static final String HISTORY_HOSTNAME = "HostName";
  private static final String HISTORY_SUCCEEDED = "Succeeded";

  private static final String EVENT_TYPE_FRAGMENT_START = "FRAGMENT_START";
  private static final String EVENT_TYPE_FRAGMENT_END = "FRAGMENT_END";

  private static final Logger HISTORY_LOGGER = LoggerFactory.getLogger(HistoryLogger.class);

  public static void logFragmentStart(String applicationIdStr, String containerIdStr,
                                      String hostname,
                                      String queryId, int dagIdentifier, String vertexName, int taskId,
                                      int attemptId) {
    HISTORY_LOGGER.info(
        constructFragmentStartString(applicationIdStr, containerIdStr, hostname, queryId, dagIdentifier,
            vertexName, taskId, attemptId));
  }

  public static void logFragmentEnd(String applicationIdStr, String containerIdStr, String hostname,
                                    String queryId, int dagIdentifier, String vertexName, int taskId, int attemptId,
                                    String threadName, long startTime, boolean failed) {
    HISTORY_LOGGER.info(constructFragmentEndString(applicationIdStr, containerIdStr, hostname,
        queryId, dagIdentifier, vertexName, taskId, attemptId, threadName, startTime, failed));
  }


  private static String constructFragmentStartString(String applicationIdStr, String containerIdStr,
                                                     String hostname, String queryId, int dagIdentifier,
                                                     String vertexName, int taskId, int attemptId) {
    HistoryLineBuilder lb = new HistoryLineBuilder(EVENT_TYPE_FRAGMENT_START);
    lb.addHostName(hostname);
    lb.addAppid(applicationIdStr);
    lb.addContainerId(containerIdStr);
    lb.addQueryId(queryId);
    lb.addDagId(dagIdentifier);
    lb.addVertexName(vertexName);
    lb.addTaskId(taskId);
    lb.addTaskAttemptId(attemptId);
    lb.addTime(HISTORY_SUBMIT_TIME);
    return lb.toString();
  }

  private static String constructFragmentEndString(String applicationIdStr, String containerIdStr,
                                                   String hostname, String queryId, int dagIdentifier,
                                                   String vertexName, int taskId, int attemptId,
                                                   String threadName, long startTime, boolean succeeded) {
    HistoryLineBuilder lb = new HistoryLineBuilder(EVENT_TYPE_FRAGMENT_END);
    lb.addHostName(hostname);
    lb.addAppid(applicationIdStr);
    lb.addContainerId(containerIdStr);
    lb.addQueryId(queryId);
    lb.addDagId(dagIdentifier);
    lb.addVertexName(vertexName);
    lb.addTaskId(taskId);
    lb.addTaskAttemptId(attemptId);
    lb.addThreadName(threadName);
    lb.addSuccessStatus(succeeded);
    lb.addTime(HISTORY_START_TIME, startTime);
    lb.addTime(HISTORY_END_TIME);
    return lb.toString();
  }

  private static class HistoryLineBuilder {
    private final StringBuilder sb = new StringBuilder();

    HistoryLineBuilder(String eventType) {
      sb.append(HISTORY_EVENT_TYPE).append("=").append(eventType);
    }

    HistoryLineBuilder addHostName(String hostname) {
      return setKeyValue(HISTORY_HOSTNAME, hostname);
    }

    HistoryLineBuilder addAppid(String appId) {
      return setKeyValue(HISTORY_APPLICATION_ID, appId);
    }

    HistoryLineBuilder addContainerId(String containerId) {
      return setKeyValue(HISTORY_CONTAINER_ID, containerId);
    }

    HistoryLineBuilder addQueryId(String queryId) {
      return setKeyValue(HISTORY_QUERY_ID, queryId);
    }

    HistoryLineBuilder addDagId(int dagId) {
      return setKeyValue(HISTORY_DAG_ID, String.valueOf(dagId));
    }

    HistoryLineBuilder addVertexName(String vertexName) {
      return setKeyValue(HISTORY_VERTEX_NAME, vertexName);
    }

    HistoryLineBuilder addTaskId(int taskId) {
      return setKeyValue(HISTORY_TASK_ID, String.valueOf(taskId));
    }

    HistoryLineBuilder addTaskAttemptId(int attemptId) {
      return setKeyValue(HISTORY_ATTEMPT_ID, String.valueOf(attemptId));
    }

    HistoryLineBuilder addThreadName(String threadName) {
      return setKeyValue(HISTORY_THREAD_NAME, threadName);
    }

    HistoryLineBuilder addTime(String timeParam, long millis) {
      return setKeyValue(timeParam, String.valueOf(millis));
    }

    HistoryLineBuilder addTime(String timeParam) {
      return setKeyValue(timeParam, String.valueOf(System.currentTimeMillis()));
    }

    HistoryLineBuilder addSuccessStatus(boolean status) {
      return setKeyValue(HISTORY_SUCCEEDED, String.valueOf(status));
    }


    private HistoryLineBuilder setKeyValue(String key, String value) {
      sb.append(", ").append(key).append("=").append(value);
      return this;
    }

    @Override
    public String toString() {
      return sb.toString();
    }
  }
}
