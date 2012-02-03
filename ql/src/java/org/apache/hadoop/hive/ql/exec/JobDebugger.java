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

package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.errors.ErrorAndSolution;
import org.apache.hadoop.hive.ql.exec.errors.TaskLogProcessor;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;

/**
 * JobDebugger takes a RunningJob that has failed and grabs the top 4 failing
 * tasks and outputs this information to the Hive CLI.
 */
public class JobDebugger implements Runnable {
  private final JobConf conf;
  private final RunningJob rj;
  private final LogHelper console;
  // Mapping from task ID to the number of failures
  private final Map<String, Integer> failures = new HashMap<String, Integer>();
  private final Set<String> successes = new HashSet<String>(); // Successful task ID's
  private final Map<String, TaskInfo> taskIdToInfo = new HashMap<String, TaskInfo>();

  // Used for showJobFailDebugInfo
  private static class TaskInfo {
    String jobId;
    Set<String> logUrls;

    public TaskInfo(String jobId) {
      this.jobId = jobId;
      logUrls = new HashSet<String>();
    }

    public void addLogUrl(String logUrl) {
      logUrls.add(logUrl);
    }

    public Set<String> getLogUrls() {
      return logUrls;
    }

    public String getJobId() {
      return jobId;
    }
  }

  public JobDebugger(JobConf conf, RunningJob rj, LogHelper console) {
    this.conf = conf;
    this.rj = rj;
    this.console = console;
  }

  public void run() {
    try {
      showJobFailDebugInfo();
    } catch (IOException e) {
      console.printError(e.getMessage());
    }
  }
  private String getTaskAttemptLogUrl(String taskTrackerHttpAddress, String taskAttemptId) {
    return taskTrackerHttpAddress + "/tasklog?taskid=" + taskAttemptId + "&start=-8193";
  }

  class TaskLogGrabber implements Runnable {

    public void run() {
      try {
        getTaskLogs();
      } catch (IOException e) {
        console.printError(e.getMessage());
      }
    }

    private void getTaskLogs() throws IOException {
      int startIndex = 0;
      while (true) {
        TaskCompletionEvent[] taskCompletions = rj.getTaskCompletionEvents(startIndex);

        if (taskCompletions == null || taskCompletions.length == 0) {
          break;
        }

        boolean more = true;
        boolean firstError = true;
        for (TaskCompletionEvent t : taskCompletions) {
          // getTaskJobIDs returns Strings for compatibility with Hadoop versions
          // without TaskID or TaskAttemptID
          String[] taskJobIds = ShimLoader.getHadoopShims().getTaskJobIDs(t);

          if (taskJobIds == null) {
            console.printError("Task attempt info is unavailable in this Hadoop version");
            more = false;
            break;
          }

          // For each task completion event, get the associated task id, job id
          // and the logs
          String taskId = taskJobIds[0];
          String jobId = taskJobIds[1];
          if (firstError) {
            console.printError("Examining task ID: " + taskId + " (and more) from job " + jobId);
            firstError = false;
          }

          TaskInfo ti = taskIdToInfo.get(taskId);
          if (ti == null) {
            ti = new TaskInfo(jobId);
            taskIdToInfo.put(taskId, ti);
          }
          // These tasks should have come from the same job.
          assert (ti.getJobId() != null &&  ti.getJobId().equals(jobId));
          ti.getLogUrls().add(getTaskAttemptLogUrl(t.getTaskTrackerHttp(), t.getTaskId()));

          // If a task failed, then keep track of the total number of failures
          // for that task (typically, a task gets re-run up to 4 times if it
          // fails

          if (t.getTaskStatus() != TaskCompletionEvent.Status.SUCCEEDED) {
            Integer failAttempts = failures.get(taskId);
            if (failAttempts == null) {
              failAttempts = Integer.valueOf(0);
            }
            failAttempts = Integer.valueOf(failAttempts.intValue() + 1);
            failures.put(taskId, failAttempts);
          } else {
            successes.add(taskId);
          }
        }
        if (!more) {
          break;
        }
        startIndex += taskCompletions.length;
      }
    }
  }

  @SuppressWarnings("deprecation")
  private void showJobFailDebugInfo() throws IOException {


    console.printError("Error during job, obtaining debugging information...");
    // Loop to get all task completion events because getTaskCompletionEvents
    // only returns a subset per call
    TaskLogGrabber tlg = new TaskLogGrabber();
    Thread t = new Thread(tlg);
    try {
      t.start();
      t.join(HiveConf.getIntVar(conf, HiveConf.ConfVars.TASKLOG_DEBUG_TIMEOUT));
    } catch (InterruptedException e) {
      console.printError("Timed out trying to finish grabbing task log URLs, "
          + "some task info may be missing");
    }

    // Remove failures for tasks that succeeded
    for (String task : successes) {
      failures.remove(task);
    }

    if (failures.keySet().size() == 0) {
      return;
    }

    // Find the highest failure count
    int maxFailures = 0;
    for (Integer failCount : failures.values()) {
      if (maxFailures < failCount.intValue()) {
        maxFailures = failCount.intValue();
      }
    }

    // Display Error Message for tasks with the highest failure count
    String jtUrl = JobTrackerURLResolver.getURL(conf);

    for (String task : failures.keySet()) {
      if (failures.get(task).intValue() == maxFailures) {
        TaskInfo ti = taskIdToInfo.get(task);
        String jobId = ti.getJobId();
        String taskUrl = jtUrl + "/taskdetails.jsp?jobid=" + jobId + "&tipid=" + task.toString();

        TaskLogProcessor tlp = new TaskLogProcessor(conf);
        for (String logUrl : ti.getLogUrls()) {
          tlp.addTaskAttemptLogUrl(logUrl);
        }

        List<ErrorAndSolution> errors = tlp.getErrors();

        StringBuilder sb = new StringBuilder();
        // We use a StringBuilder and then call printError only once as
        // printError will write to both stderr and the error log file. In
        // situations where both the stderr and the log file output is
        // simultaneously output to a single stream, this will look cleaner.
        sb.append("\n");
        sb.append("Task with the most failures(" + maxFailures + "): \n");
        sb.append("-----\n");
        sb.append("Task ID:\n  " + task + "\n\n");
        sb.append("URL:\n  " + taskUrl + "\n");

        for (ErrorAndSolution e : errors) {
          sb.append("\n");
          sb.append("Possible error:\n  " + e.getError() + "\n\n");
          sb.append("Solution:\n  " + e.getSolution() + "\n");
        }
        sb.append("-----\n");

        console.printError(sb.toString());

        // Only print out one task because that's good enough for debugging.
        break;
      }
    }
    return;

  }
}
