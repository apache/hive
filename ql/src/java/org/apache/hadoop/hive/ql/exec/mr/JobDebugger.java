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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.mr;

import java.io.IOException;
import java.lang.Exception;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
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
  private final Map<String, List<List<String>>> stackTraces;
  // Mapping from task ID to the number of failures
  private final Map<String, Integer> failures = new HashMap<String, Integer>();
  private final Set<String> successes = new HashSet<String>(); // Successful task ID's
  private final Map<String, TaskInfo> taskIdToInfo = new HashMap<String, TaskInfo>();
  private String diagnosticMesg;
  private int maxFailures = 0;

  // Used for showJobFailDebugInfo
  private static class TaskInfo {
    String jobId;
    Set<String> logUrls;
    int errorCode;  // Obtained from the HiveException thrown
    String[] diagnosticMesgs;

    public TaskInfo(String jobId) {
      this.jobId = jobId;
      logUrls = new HashSet<String>();
      errorCode = 0;
      diagnosticMesgs = null;
    }

    public void addLogUrl(String logUrl) {
      logUrls.add(logUrl);
    }

    public void setErrorCode(int errorCode) {
      this.errorCode = errorCode;
    }

    public void setDiagnosticMesgs(String[] diagnosticMesgs) {
      this.diagnosticMesgs = diagnosticMesgs;
    }

    public Set<String> getLogUrls() {
      return logUrls;
    }

    public String getJobId() {
      return jobId;
    }

    public int getErrorCode() {
      return errorCode;
    }

    public String[] getDiagnosticMesgs() {
      return diagnosticMesgs;
    }
  }

  public JobDebugger(JobConf conf, RunningJob rj, LogHelper console) {
    this.conf = conf;
    this.rj = rj;
    this.console = console;
    this.stackTraces = null;
  }

  public JobDebugger(JobConf conf, RunningJob rj, LogHelper console,
      Map<String, List<List<String>>> stackTraces) {
    this.conf = conf;
    this.rj = rj;
    this.console = console;
    this.stackTraces = stackTraces;
  }

  public void run() {
    try {
      diagnosticMesg = showJobFailDebugInfo();
    } catch (IOException e) {
      console.printError(e.getMessage());
    }
  }

  public static int extractErrorCode(String[] diagnostics) {
    int result = 0;
    Pattern errorCodeRegex = ErrorMsg.getErrorCodePattern();
    for (String mesg : diagnostics) {
      Matcher matcher = errorCodeRegex.matcher(mesg);
      if (matcher.find()) {
        result = Integer.parseInt(matcher.group(1));
        // We don't exit the loop early because we want to extract the error code
        // corresponding to the bottommost error coded exception.
      }
    }
    return result;
  }

  class TaskInfoGrabber implements Runnable {

    public void run() {
      try {
        getTaskInfos();
      } catch (Exception e) {
        console.printError(e.getMessage());
      }
    }

    private void getTaskInfos() throws IOException, MalformedURLException {
      int startIndex = 0;
      while (true) {
        TaskCompletionEvent[] taskCompletions = rj.getTaskCompletionEvents(startIndex);

        if (taskCompletions == null || taskCompletions.length == 0) {
          break;
        }

        boolean more = true;
        boolean firstError = true;
        for (TaskCompletionEvent t : taskCompletions) {
          // For each task completion event, get the associated task id, job id
          // and the logs
          String taskId = t.getTaskAttemptId().getTaskID().toString();
          String jobId = t.getTaskAttemptId().getJobID().toString();
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
          String taskAttemptLogUrl = ShimLoader.getHadoopShims().getTaskAttemptLogUrl(
            conf, t.getTaskTrackerHttp(), t.getTaskId());
          if (taskAttemptLogUrl != null) {
            ti.getLogUrls().add(taskAttemptLogUrl);
          }

          // If a task failed, fetch its error code (if available).
          // Also keep track of the total number of failures for that
          // task (typically, a task gets re-run up to 4 times if it fails.
          if (t.getTaskStatus() != TaskCompletionEvent.Status.SUCCEEDED) {
            String[] diags = rj.getTaskDiagnostics(t.getTaskAttemptId());
            ti.setDiagnosticMesgs(diags);
            if (ti.getErrorCode() == 0) {
              ti.setErrorCode(extractErrorCode(diags));
            }

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

  private void computeMaxFailures() {
    maxFailures = 0;
    for (Integer failCount : failures.values()) {
      if (maxFailures < failCount.intValue()) {
        maxFailures = failCount.intValue();
      }
    }
  }

  private String showJobFailDebugInfo() throws IOException {
    console.printError("Error during job, obtaining debugging information...");
    if (!conf.get("mapred.job.tracker", "local").equals("local")) {
      // Show Tracking URL for remotely running jobs.
      console.printError("Job Tracking URL: " + rj.getTrackingURL());
    }
    // Loop to get all task completion events because getTaskCompletionEvents
    // only returns a subset per call
    TaskInfoGrabber tlg = new TaskInfoGrabber();
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
      return null;
    }
    // Find the highest failure count
    computeMaxFailures() ;

    // Display Error Message for tasks with the highest failure count
    String jtUrl = null;
    try {
      jtUrl = JobTrackerURLResolver.getURL(conf);
    } catch (Exception e) {
      console.printError("Unable to retrieve URL for Hadoop Task logs. "
          + e.getMessage());
    }

    String msg = null;
    for (String task : failures.keySet()) {
      if (failures.get(task).intValue() == maxFailures) {
        TaskInfo ti = taskIdToInfo.get(task);
        String jobId = ti.getJobId();
        String taskUrl = (jtUrl == null) ? null :
          jtUrl + "/taskdetails.jsp?jobid=" + jobId + "&tipid=" + task.toString();

        TaskLogProcessor tlp = new TaskLogProcessor(conf);
        for (String logUrl : ti.getLogUrls()) {
          tlp.addTaskAttemptLogUrl(logUrl);
        }

        if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.JOB_DEBUG_CAPTURE_STACKTRACES) &&
            stackTraces != null) {
          if (!stackTraces.containsKey(jobId)) {
            stackTraces.put(jobId, new ArrayList<List<String>>());
          }
          stackTraces.get(jobId).addAll(tlp.getStackTraces());
        }

        if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.SHOW_JOB_FAIL_DEBUG_INFO)) {
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
          if (taskUrl != null) {
            sb.append("URL:\n  " + taskUrl + "\n");
          }

          for (ErrorAndSolution e : errors) {
            sb.append("\n");
            sb.append("Possible error:\n  " + e.getError() + "\n\n");
            sb.append("Solution:\n  " + e.getSolution() + "\n");
          }
          sb.append("-----\n");

          sb.append("Diagnostic Messages for this Task:\n");
          String[] diagMesgs = ti.getDiagnosticMesgs();
          for (String mesg : diagMesgs) {
            sb.append(mesg + "\n");
          }
          msg = sb.toString();
          console.printError(msg);
        }

        // Only print out one task because that's good enough for debugging.
        break;
      }
    }
    return msg;
  }

  public String getDiagnosticMesg() {
    return diagnosticMesg;
  }

  public int getErrorCode() {
    for (String task : failures.keySet()) {
      if (failures.get(task).intValue() == maxFailures) {
        TaskInfo ti = taskIdToInfo.get(task);
        return ti.getErrorCode();
      }
    }
    // Should never reach here unless there were no failed tasks.
    return 0;
  }
}
