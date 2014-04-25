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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton.tool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hive.hcatalog.templeton.LauncherDelegator.JobType;

/*
 * This class provides support to collect mapreduce stderr/stdout/syslogs
 * from jobtracker, and stored into a hdfs location. The log directory layout is:
 * <ul compact>
 * <li>logs/$job_id (directory for $job_id)
 * <li>logs/$job_id/job.xml.html
 * <li>logs/$job_id/$attempt_id (directory for $attempt_id)
 * <li>logs/$job_id/$attempt_id/stderr
 * <li>logs/$job_id/$attempt_id/stdout
 * <li>logs/$job_id/$attempt_id/syslog 
 * Since there is no API to retrieve mapreduce log from jobtracker, the code retrieve
 * it from jobtracker ui and parse the html file. The current parser only works with
 * Hadoop 1, for Hadoop 2, we would need a different parser
 */
public class LogRetriever {
  String statusDir;
  JobType jobType;
  private static final String attemptDetailPatternInString = "<a href=\"(taskdetails.jsp\\?.*?)\">";
  private static Pattern attemptDetailPattern = null;
  private static final String attemptLogPatternInString = "Last 8KB</a><br/><a href=\"(.*?tasklog\\?attemptid=.*?)\">All</a>";
  private static Pattern attemptLogPattern = null;
  private static final String attemptIDPatternInString = "attemptid=(.*)?&";
  private static Pattern attemptIDPattern = null;
  private static final String attemptStartTimePatternInString = "<td>(\\d{1,2}-[A-Za-z]{3}-\\d{4} \\d{2}:\\d{2}:\\d{2})(<br/>)?</td>";
  private static Pattern attemptStartTimePattern = null;
  private static final String attemptEndTimePatternInString = "<td>(\\d{1,2}-[A-Za-z]{3}-\\d{4} \\d{2}:\\d{2}:\\d{2}) \\(.*\\)(<br/>)?</td>";
  private static Pattern attemptEndTimePattern = null;
  private FileSystem fs;
  private JobClient jobClient = null;
  private Configuration conf = null;

  // Class to store necessary information for an attempt to log
  static class AttemptInfo {
    public String id;
    public URL baseUrl;
    public enum AttemptStatus {COMPLETED, FAILED};
    AttemptStatus status;
    public String startTime;
    public String endTime;
    public String type = "unknown";

    @Override
    public String toString() {
      return id + "\t" + baseUrl.toString() + "\t" + status.toString() + "\t" + type
          + "\t" + startTime + "\t" + endTime + "\n";
    }
  }

  /*
   * @param statusDir directory of statusDir defined for the webhcat job. It is supposed
   *                  to contain stdout/stderr/syslog for the webhcat controller job
   * @param jobType   Currently we support pig/hive/stream/generic mapreduce. The specific
   *                  parser will parse the log of the controller job and retrieve job_id
   *                  of all mapreduce jobs it launches. The generic mapreduce parser works
   *                  when the program use JobClient.runJob to submit the job, but if the program
   *                  use other API, generic mapreduce parser is not guaranteed to find the job_id
   * @param conf      Configuration for webhcat
   */
  public LogRetriever(String statusDir, JobType jobType, Configuration conf)
    throws IOException {
    this.statusDir = statusDir;
    this.jobType = jobType;
    attemptDetailPattern = Pattern.compile(attemptDetailPatternInString);
    attemptLogPattern = Pattern.compile(attemptLogPatternInString);
    attemptIDPattern = Pattern.compile(attemptIDPatternInString);
    attemptStartTimePattern = Pattern.compile(attemptStartTimePatternInString);
    attemptEndTimePattern = Pattern.compile(attemptEndTimePatternInString);
    Path statusPath = new Path(statusDir);
    fs = statusPath.getFileSystem(conf);
    jobClient = new JobClient(new JobConf(conf));
    this.conf = conf;
  }

  public void run() throws IOException {
    String logDir = statusDir + "/logs";

    fs.mkdirs(new Path(logDir));

    // Get jobids from job status dir
    JobIDParser jobIDParser = null;
    switch (jobType) {
    case PIG:
      jobIDParser = new PigJobIDParser(statusDir, conf);
      break;
    case HIVE:
      jobIDParser = new HiveJobIDParser(statusDir, conf);
      break;
    case SQOOP:
    case JAR:
    case STREAMING:
      jobIDParser = new JarJobIDParser(statusDir, conf);
      break;
    default:
      System.err
        .println("Unknown job type:" + jobType!=null? jobType.toString():"null"
        + ", only pig/hive/jar/streaming/sqoop are supported, skip logs");
      return;
    }
    List<String> jobs = new ArrayList<String>();
    try {
      jobs = jobIDParser.parseJobID();
    } catch (IOException e) {
      System.err.println("Cannot retrieve jobid from log file");
      e.printStackTrace();
    }

    // Log jobs
    PrintWriter listWriter = null;
    try {
      listWriter = new PrintWriter(new OutputStreamWriter(
          fs.create(new Path(logDir, "list.txt"))));
      for (String job : jobs) {
        try {
          logJob(logDir, job, listWriter);
        } catch (IOException e) {
          System.err.println("Cannot retrieve log for " + job);
          e.printStackTrace();
        }
      }
    } finally {
      if (listWriter!=null) {
        listWriter.close();
      }
    }
  }

  private void logJob(String logDir, String jobID, PrintWriter listWriter)
    throws IOException {
    RunningJob rj = jobClient.getJob(JobID.forName(jobID));
    String jobURLString = rj.getTrackingURL();

    Path jobDir = new Path(logDir, jobID);
    fs.mkdirs(jobDir);

    // Log jobconf
    try {
      logJobConf(jobID, jobURLString, jobDir.toString());
    } catch (IOException e) {
      System.err.println("Cannot retrieve job.xml.html for " + jobID);
      e.printStackTrace();
    }

    listWriter.println("job: " + jobID + "(" + "name=" + rj.getJobName() + ","
        + "status=" + JobStatus.getJobRunState(rj.getJobState()) + ")");

    // Get completed attempts
    List<AttemptInfo> attempts = new ArrayList<AttemptInfo>();
    for (String type : new String[] { "map", "reduce", "setup", "cleanup" }) {
      try {
        List<AttemptInfo> successAttempts = getCompletedAttempts(jobID,
            jobURLString, type);
        attempts.addAll(successAttempts);
      } catch (IOException e) {
        System.err.println("Cannot retrieve " + type + " tasks for " + jobID);
        e.printStackTrace();
      }
    }

    // Get failed attempts
    try {
      List<AttemptInfo> failedAttempts = getFailedAttempts(jobID, jobURLString);
      attempts.addAll(failedAttempts);
    } catch (IOException e) {
      System.err.println("Cannot retrieve failed attempts for " + jobID);
      e.printStackTrace();
    }

    // Log attempts
    for (AttemptInfo attempt : attempts) {
      try {
        logAttempt(jobID, attempt, jobDir.toString());
        listWriter.println("  attempt:" + attempt.id + "(" + "type="
            + attempt.type + "," + "status=" + attempt.status + ","
            + "starttime=" + attempt.startTime + "," + "endtime="
            + attempt.endTime + ")");
      } catch (IOException e) {
        System.err.println("Cannot log attempt " + attempt.id);
        e.printStackTrace();
      }
    }

    listWriter.println();
  }

  // Utility to get patterns from a url, every array element is match for one
  // pattern
  private List<String>[] getMatches(URL url, Pattern[] pattern)
    throws IOException {
    List<String>[] results = new ArrayList[pattern.length];
    for (int i = 0; i < pattern.length; i++) {
      results[i] = new ArrayList<String>();
    }

    URLConnection urlConnection = url.openConnection();
    BufferedReader reader = new BufferedReader(new InputStreamReader(
        urlConnection.getInputStream()));
    String line;
    while ((line = reader.readLine()) != null) {
      for (int i = 0; i < pattern.length; i++) {
        Matcher matcher = pattern[i].matcher(line);
        if (matcher.find()) {
          results[i].add(matcher.group(1));
        }
      }
    }
    reader.close();
    return results;
  }

  // Retrieve job conf into logDir
  private void logJobConf(String job, String jobURLInString, String jobDir)
    throws IOException {
    URL jobURL = new URL(jobURLInString);
    String fileInURL = "/jobconf.jsp?jobid=" + job;
    URL jobTasksURL = new URL(jobURL.getProtocol(), jobURL.getHost(),
        jobURL.getPort(), fileInURL);
    URLConnection urlConnection = jobTasksURL.openConnection();
    BufferedReader reader = null;
    PrintWriter writer = null;
    try {
      reader = new BufferedReader(new InputStreamReader(
        urlConnection.getInputStream()));
  
      writer = new PrintWriter(new OutputStreamWriter(
        fs.create(new Path(jobDir, "job.xml.html"))));
  
      // Copy conf file
      String line;
      while ((line = reader.readLine()) != null) {
        writer.println(line);
      }
    } finally {
      if (reader!=null) {
        reader.close();
      }
      if (writer!=null) {
        writer.close();
      }
    }
  }

  // Get completed attempts from jobtasks.jsp
  private List<AttemptInfo> getCompletedAttempts(String job,
      String jobURLInString, String type) throws IOException {
    // Get task detail link from the jobtask page
    String fileInURL = "/jobtasks.jsp?jobid=" + job + "&type=" + type
        + "&pagenum=1&state=completed";
    URL jobURL = new URL(jobURLInString);
    URL jobTasksURL = new URL(jobURL.getProtocol(), jobURL.getHost(),
        jobURL.getPort(), fileInURL);
    List<String>[] taskAttemptURLAndTimestamp = getMatches(jobTasksURL,
      new Pattern[] { attemptDetailPattern, attemptStartTimePattern,
        attemptEndTimePattern });
    List<AttemptInfo> results = new ArrayList<AttemptInfo>();

    // Go to task details, fetch task tracker url
    for (int i = 0; i < taskAttemptURLAndTimestamp[0].size(); i++) {
      String taskString = taskAttemptURLAndTimestamp[0].get(i);
      URL taskDetailsURL = new URL(jobURL.getProtocol(), jobURL.getHost(),
          jobURL.getPort(), "/" + taskString);
      List<String>[] attemptLogStrings = getMatches(taskDetailsURL,
          new Pattern[] { attemptLogPattern });
      for (String attemptLogString : attemptLogStrings[0]) {
        AttemptInfo attempt = new AttemptInfo();
        attempt.baseUrl = new URL(attemptLogString);
        attempt.startTime = taskAttemptURLAndTimestamp[1].get(i);
        attempt.endTime = taskAttemptURLAndTimestamp[2].get(i);
        attempt.type = type;
        Matcher matcher = attemptIDPattern.matcher(attemptLogString);
        if (matcher.find()) {
          attempt.id = matcher.group(1);
        }
        attempt.status = AttemptInfo.AttemptStatus.COMPLETED;
        results.add(attempt);
      }
    }

    return results;
  }

  // Get failed attempts from jobfailures.jsp
  private List<AttemptInfo> getFailedAttempts(String job, String jobURLInString)
    throws IOException {
    String fileInURL = "/jobfailures.jsp?jobid=" + job
        + "&kind=all&cause=failed";
    URL jobURL = new URL(jobURLInString);
    URL url = new URL(jobURL.getProtocol(), jobURL.getHost(), jobURL.getPort(),
        fileInURL);
    List<String>[] attemptLogStrings = getMatches(url,
        new Pattern[] { attemptDetailPattern });
    List<String> failedTaskStrings = new ArrayList<String>();
    for (String attempt : attemptLogStrings[0]) {
      if (!failedTaskStrings.contains(attempt)) {
        failedTaskStrings.add(attempt);
      }
    }
    List<AttemptInfo> results = new ArrayList<AttemptInfo>();
    for (String taskString : failedTaskStrings) {
      URL taskDetailsURL = new URL(jobURL.getProtocol(), jobURL.getHost(),
        jobURL.getPort(), "/" + taskString);
      List<String>[] taskAttemptURLAndTimestamp = getMatches(taskDetailsURL,
        new Pattern[] { attemptLogPattern, attemptStartTimePattern,
          attemptEndTimePattern });
      for (int i = 0; i < taskAttemptURLAndTimestamp[0].size(); i++) {
        String attemptLogString = taskAttemptURLAndTimestamp[0].get(i);
        AttemptInfo attempt = new AttemptInfo();
        attempt.baseUrl = new URL(attemptLogString);
        attempt.startTime = taskAttemptURLAndTimestamp[1].get(i);
        attempt.endTime = taskAttemptURLAndTimestamp[2].get(i);
        Matcher matcher = attemptIDPattern.matcher(attemptLogString);
        if (matcher.find()) {
          attempt.id = matcher.group(1);
        }
        if (attempt.id.contains("_r_")) {
          attempt.type = "reduce";
        }
        attempt.status = AttemptInfo.AttemptStatus.COMPLETED.FAILED;
        results.add(attempt);
      }
    }

    return results;
  }

  // Retrieve attempt log into logDir
  private void logAttempt(String job, AttemptInfo attemptInfo, String logDir)
    throws IOException {
    Path attemptDir = new Path(logDir, attemptInfo.id);
    fs.mkdirs(attemptDir);
    for (String type : new String[] { "stderr", "stdout", "syslog" }) {
      // Retrieve log from task tracker
      String fileInURL = "tasklog?attemptid=" + attemptInfo.id
          + "&plaintext=true&filter=" + type;
      URL url = new URL(attemptInfo.baseUrl.getProtocol(),
          attemptInfo.baseUrl.getHost(), attemptInfo.baseUrl.getPort(), "/"
              + fileInURL);

      URLConnection urlConnection = url.openConnection();
      BufferedReader reader = null;
      PrintWriter writer = null;
      
      try {
        reader = new BufferedReader(new InputStreamReader(
          urlConnection.getInputStream()));
  
        writer = new PrintWriter(new OutputStreamWriter(
          fs.create(new Path(attemptDir, type))));
  
        // Copy log file
        String line;
        while ((line = reader.readLine()) != null) {
          writer.println(line);
        }
      } finally {
        if (reader!=null) {
          reader.close();
        }
        if (writer!=null) {
          writer.close();
        }
      }
    }
  }
}
