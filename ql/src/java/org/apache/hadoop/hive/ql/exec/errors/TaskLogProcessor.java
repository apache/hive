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

package org.apache.hadoop.hive.ql.exec.errors;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;

/**
 * TaskLogProcessor reads the logs from failed task attempts and tries to figure
 * out what the cause of the error was using various heuristics.
 */
public class TaskLogProcessor {

  private final Map<ErrorHeuristic, HeuristicStats> heuristics =
    new HashMap<ErrorHeuristic, HeuristicStats>();
  private final List<String> taskLogUrls = new ArrayList<String>();

  private JobConf conf = null;
  // Query is the hive query string i.e. "SELECT * FROM src;" associated with
  // this set of tasks logs
  private String query = null;

  public TaskLogProcessor(JobConf conf) {
    this.conf = conf;
    query = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYSTRING);

    heuristics.put(new ScriptErrorHeuristic(), new HeuristicStats());
    heuristics.put(new MapAggrMemErrorHeuristic(), new HeuristicStats());
    heuristics.put(new DataCorruptErrorHeuristic(), new HeuristicStats());
    for(ErrorHeuristic e : heuristics.keySet()) {
      e.init(query, conf);
    }
  }

  /**
   * Adds a task log URL for the heuristics to read through.
   * @param url
   */
  public void addTaskAttemptLogUrl(String url) {
    taskLogUrls.add(url);
  }

  private static class HeuristicStats {

    // The number of times eh has returned non-null errors
    private int triggerCount = 0;
    // All ErrorAndSolutions that ErrorHeuristic has generated. For the same error, they
    // should be the same though it's possible that different file paths etc
    // could generate different error messages
    private final List<ErrorAndSolution> ens = new ArrayList<ErrorAndSolution>();

    HeuristicStats() {
    }

    int getTriggerCount() {
      return triggerCount;
    }

    void incTriggerCount() {
      triggerCount++;
    }

    List<ErrorAndSolution> getErrorAndSolutions() {
      return ens;
    }

    void addErrorAndSolution(ErrorAndSolution e) {
      ens.add(e);
    }
  }

  /**
   * Processes the provided task logs using the known error heuristics to get
   * the matching errors.
   * @return A ErrorAndSolution from the ErrorHeuristic that most frequently
   * generated matches. In case of ties, multiple ErrorAndSolutions will be
   * returned.
   */
  public List<ErrorAndSolution> getErrors() {

    for(String urlString : taskLogUrls) {

      // Open the log file, and read in a line. Then feed the line into
      // each of the ErrorHeuristics. Repeat for all the lines in the log.
      URL taskAttemptLogUrl;
      try {
        taskAttemptLogUrl = new URL(urlString);
      } catch(MalformedURLException e) {
        throw new RuntimeException("Bad task log url", e);
      }
      BufferedReader in;
      try {
        in = new BufferedReader(
            new InputStreamReader(taskAttemptLogUrl.openStream()));
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
          for(ErrorHeuristic e : heuristics.keySet()) {
            e.processLogLine(inputLine);
          }
        }
        in.close();
      } catch (IOException e) {
        throw new RuntimeException("Error while reading from task log url", e);
      }

      // Once the lines of the log file have been fed into the ErrorHeuristics,
      // see if they have detected anything. If any has, record
      // what ErrorAndSolution it gave so we can later return the most
      // frequently occurring error
      for(Entry<ErrorHeuristic, HeuristicStats> ent : heuristics.entrySet()) {
        ErrorHeuristic eh = ent.getKey();
        HeuristicStats hs = ent.getValue();

        ErrorAndSolution es = eh.getErrorAndSolution();
        if(es != null) {
          hs.incTriggerCount();
          hs.addErrorAndSolution(es);
        }
      }

    }

    // Return the errors that occur the most frequently
    int max = 0;
    for(HeuristicStats hs : heuristics.values()) {
      if(hs.getTriggerCount() > max) {
        max = hs.getTriggerCount();
      }
    }

    List<ErrorAndSolution> errors = new ArrayList<ErrorAndSolution>();
    for(HeuristicStats hs : heuristics.values()) {
      if(hs.getTriggerCount() == max) {
        if(hs.getErrorAndSolutions().size() > 0) {
          // An error heuristic could have generated different ErrorAndSolution
          // for each task attempt, but most likely they are the same. Plus,
          // one of those is probably good enough for debugging
          errors.add(hs.getErrorAndSolutions().get(0));
        }
      }
    }

    return errors;
  }

}
