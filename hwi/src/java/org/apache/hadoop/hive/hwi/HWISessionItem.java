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

package org.apache.hadoop.hive.hwi;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.cli.OptionsProcessor;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.history.HiveHistoryViewer;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * HWISessionItem can be viewed as a wrapper for a Hive shell. With it the user
 * has a session on the web server rather then in a console window.
 *
 */
public class HWISessionItem implements Runnable, Comparable<HWISessionItem> {

  protected static final Log l4j = LogFactory.getLog(HWISessionItem.class
      .getName());

  /** Represents the state a session item can be in. */
  public enum WebSessionItemStatus {
    NEW, READY, QUERY_SET, QUERY_RUNNING, DESTROY, KILL_QUERY
  };

  /** The Web Interface sessionName this is used to identify the session. */
  private final String sessionName;

  /**
   * Respresents the current status of the session. Used by components to
   * determine state. Operations will throw exceptions if the item is not in the
   * correct state.
   */
  private HWISessionItem.WebSessionItemStatus status;

  private CliSessionState ss;

  /** Standard out from the session will be written to this local file. */
  private String resultFile;

  /** Standard error from the session will be written to this local file. */
  private String errorFile;

  /**
   * The results from the Driver. This is used for storing the most result
   * results from the driver in memory.
   */
  private ArrayList<ArrayList<String>> resultBucket;

  /** Limits the resultBucket to be no greater then this size. */
  private int resultBucketMaxSize;

  /** List of queries that this item should/has operated on. */
  private List<String> queries;

  /** status code results of queries. */
  private List<Integer> queryRet;

  /** Reference to the configuration. */
  private HiveConf conf;

  /** User privileges. */
  private HWIAuth auth;

  public Thread runnable;

  /**
   * Threading SessionState issues require us to capture a reference to the hive
   * history file and store it.
   */
  private String historyFile;

  /**
   * Creates an instance of WebSessionItem, sets status to NEW.
   */
  public HWISessionItem(HWIAuth auth, String sessionName) {
    this.auth = auth;
    this.sessionName = sessionName;
    l4j.debug("HWISessionItem created");
    status = WebSessionItemStatus.NEW;
    queries = new ArrayList<String>();
    queryRet = new ArrayList<Integer>();
    resultBucket = new ArrayList<ArrayList<String>>();
    resultBucketMaxSize = 1000;
    runnable = new Thread(this);
    runnable.start();

    l4j.debug("Wait for NEW->READY transition");
    synchronized (runnable) {
      if (status != WebSessionItemStatus.READY) {
        try {
          runnable.wait();
        } catch (Exception ex) {
        }
      }
    }
    l4j.debug("NEW->READY transition complete");
  }

  /**
   * This is the initialization process that is carried out for each
   * SessionItem. The goal is to emulate the startup of CLIDriver.
   */
  private void itemInit() {
    l4j.debug("HWISessionItem itemInit start " + getSessionName());
    OptionsProcessor oproc = new OptionsProcessor();

    if (System.getProperty("hwi-args") != null) {
      String[] parts = System.getProperty("hwi-args").split("\\s+");
      if (!oproc.process_stage1(parts)) {
      }
    }

    try {
      LogUtils.initHiveLog4j();
    } catch (LogInitializationException e) {
      l4j.warn(e);
    }
    conf = new HiveConf(SessionState.class);
    ss = new CliSessionState(conf);
    SessionState.start(ss);
    queries.add("set hadoop.job.ugi=" + auth.getUser() + ","
        + auth.getGroups()[0]);
    queries.add("set user.name=" + auth.getUser());
    /*
     * HiveHistoryFileName will not be accessible outside this thread. We must
     * capture this now.
     */
    historyFile = SessionState.get().getHiveHistory().getHistFileName();
    l4j.debug("HWISessionItem itemInit Complete " + getSessionName());
    status = WebSessionItemStatus.READY;

    synchronized (runnable) {
      runnable.notifyAll();
    }
  }

  /**
   * HWISessionItem is a Runnable instance. Calling this method will change the
   * status to QUERY_SET and notify(). The run method detects this and then
   * continues processing.
   */
  public void clientStart() throws HWIException {
    if (status == WebSessionItemStatus.QUERY_RUNNING) {
      throw new HWIException("Query already running");
    }
    status = WebSessionItemStatus.QUERY_SET;
    synchronized (runnable) {
      runnable.notifyAll();
    }
    l4j.debug(getSessionName() + " Query is set to start");
  }

  public void clientKill() throws HWIException {
    if (status != WebSessionItemStatus.QUERY_RUNNING) {
      throw new HWIException("Can not kill that which is not running.");
    }
    status = WebSessionItemStatus.KILL_QUERY;
    l4j.debug(getSessionName() + " Query is set to KILL_QUERY");
  }

  /** This method clears the private member variables. */
  public void clientRenew() throws HWIException {
    throwIfRunning();
    queries = new ArrayList<String>();
    queryRet = new ArrayList<Integer>();
    resultBucket = new ArrayList<ArrayList<String>>();
    resultFile = null;
    errorFile = null;
    // this.conf = null;
    // this.ss = null;
    status = WebSessionItemStatus.NEW;
    l4j.debug(getSessionName() + " Query is renewed to start");
  }

  /**
   * This is a callback style function used by the HiveSessionManager. The
   * HiveSessionManager notices this and attempts to stop the query.
   */
  protected void killIt() {
    l4j.debug(getSessionName() + " Attempting kill.");
    if (runnable != null) {
      try {
        runnable.join(1000);
        l4j.debug(getSessionName() + " Thread join complete");
      } catch (InterruptedException e) {
        l4j.error(getSessionName() + " killing session caused exception ", e);
      }
    }
  }

  /**
   * Helper function to get configuration variables.
   *
   * @param wanted
   *          a ConfVar
   * @return Value of the configuration variable.
   */
  public String getHiveConfVar(HiveConf.ConfVars wanted) throws HWIException {
    String result = null;
    try {
      result = ss.getConf().getVar(wanted);
    } catch (Exception ex) {
      throw new HWIException(ex);
    }
    return result;
  }

  public String getHiveConfVar(String s) throws HWIException {
    String result = null;
    try {
      result = conf.get(s);
    } catch (Exception ex) {
      throw new HWIException(ex);
    }
    return result;
  }

  /*
   * mapred.job.tracker could be host:port or just local
   * mapred.job.tracker.http.address could be host:port or just host In some
   * configurations http.address is set to 0.0.0.0 we are combining the two
   * variables to provide a url to the job tracker WUI if it exists. If hadoop
   * chose the first available port for the JobTracker HTTP port will can not
   * determine it.
   */
  public String getJobTrackerURL(String jobid) throws HWIException {
    String jt = this.getHiveConfVar("mapred.job.tracker");
    String jth = this.getHiveConfVar("mapred.job.tracker.http.address");
    String[] jtparts = null;
    String[] jthttpParts = null;
    if (jt.equalsIgnoreCase("local")) {
      jtparts = new String[2];
      jtparts[0] = "local";
      jtparts[1] = "";
    } else {
      jtparts = jt.split(":");
    }
    if (jth.contains(":")) {
      jthttpParts = jth.split(":");
    } else {
      jthttpParts = new String[2];
      jthttpParts[0] = jth;
      jthttpParts[1] = "";
    }
    return jtparts[0] + ":" + jthttpParts[1] + "/jobdetails.jsp?jobid=" + jobid
        + "&refresh=30";
  }

  @Override
  /*
   * HWISessionItem uses a wait() notify() system. If the thread detects conf to
   * be null, control is transfered to initItem(). A status of QUERY_SET causes
   * control to transfer to the runQuery() method. DESTROY will cause the run
   * loop to end permanently.
   */
  public void run() {
    synchronized (runnable) {
      while (status != HWISessionItem.WebSessionItemStatus.DESTROY) {
        if (status == WebSessionItemStatus.NEW) {
          itemInit();
        }

        if (status == WebSessionItemStatus.QUERY_SET) {
          runQuery();
        }

        try {
          runnable.wait();
        } catch (InterruptedException e) {
          l4j.error("in wait() state ", e);
        }
      } // end while
    } // end sync
  } // end run

  /**
   * runQuery iterates the list of queries executing each query.
   */
  public void runQuery() {
    FileOutputStream fos = null;
    if (getResultFile() != null) {
      try {
        fos = new FileOutputStream(new File(resultFile));
        ss.out = new PrintStream(fos, true, "UTF-8");
      } catch (java.io.FileNotFoundException fex) {
        l4j.error(getSessionName() + " opening resultfile " + resultFile, fex);
      } catch (java.io.UnsupportedEncodingException uex) {
        l4j.error(getSessionName() + " opening resultfile " + resultFile, uex);
      }
    } else {
      l4j.debug(getSessionName() + " Output file was not specified");
    }
    l4j.debug(getSessionName() + " state is now QUERY_RUNNING.");
    status = WebSessionItemStatus.QUERY_RUNNING;

    // expect one return per query
    queryRet = new ArrayList<Integer>(queries.size());
    for (int i = 0; i < queries.size(); i++) {
      String cmd = queries.get(i);
      String cmd_trimmed = cmd.trim();
      String[] tokens = cmd_trimmed.split("\\s+");
      String cmd_1 = cmd_trimmed.substring(tokens[0].length()).trim();
      CommandProcessor proc = null;
      try {
        proc = CommandProcessorFactory.get(tokens[0]);
      } catch (SQLException e) {
        l4j.error(getSessionName() + " error processing " + cmd, e);
      }
      if (proc != null) {
        if (proc instanceof Driver) {
          Driver qp = (Driver) proc;
          qp.setTryCount(Integer.MAX_VALUE);
          try {
          queryRet.add(Integer.valueOf(qp.run(cmd).getResponseCode()));
          ArrayList<String> res = new ArrayList<String>();
          try {
            while (qp.getResults(res)) {
              ArrayList<String> resCopy = new ArrayList<String>();
              resCopy.addAll(res);
              resultBucket.add(resCopy);
              if (resultBucket.size() > resultBucketMaxSize) {
                resultBucket.remove(0);
              }
              for (String row : res) {
                if (ss != null) {
                  if (ss.out != null) {
                    ss.out.println(row);
                  }
                } else {
                  throw new RuntimeException("ss was null");
                }
              }
              res.clear();
            }

          } catch (IOException ex) {
            l4j.error(getSessionName() + " getting results " + getResultFile()
                + " caused exception.", ex);
          }
          } catch (CommandNeedRetryException e) {
            // this should never happen since we Driver.setTryCount(Integer.MAX_VALUE)
            l4j.error(getSessionName() + " Exception when executing", e);
          } finally {
            qp.close();
          }
        } else {
          try {
            queryRet.add(Integer.valueOf(proc.run(cmd_1).getResponseCode()));
          } catch (CommandNeedRetryException e) {
            // this should never happen if there is no bug
            l4j.error(getSessionName() + " Exception when executing", e);
          }
        }
      } else {
        // processor was null
        l4j.error(getSessionName()
            + " query processor was not found for query " + cmd);
      }
    } // end for

    // cleanup
    try {
      if (fos != null) {
        fos.close();
      }
    } catch (IOException ex) {
      l4j.error(getSessionName() + " closing result file " + getResultFile()
          + " caused exception.", ex);
    }
    status = WebSessionItemStatus.READY;
    l4j.debug(getSessionName() + " state is now READY");
    synchronized (runnable) {
      runnable.notifyAll();
    }
  }

  /**
   * This is a chained call to SessionState.setIsSilent(). Use this if you do
   * not want the result file to have information status
   */
  public void setSSIsSilent(boolean silent) throws HWIException {
    if (ss == null) {
      throw new HWIException("Session State is null");
    }
    ss.setIsSilent(silent);
  }

  /**
   * This is a chained call to SessionState.getIsSilent().
   */
  public boolean getSSIsSilent() throws HWIException {
    if (ss == null) {
      throw new HWIException("Session State is null");
    }
    return ss.getIsSilent();
  }

  /** to support sorting/Set. */
  public int compareTo(HWISessionItem other) {
    if (other == null) {
      return -1;
    }
    return getSessionName().compareTo(other.getSessionName());
  }

  /**
   *
   * @return the HiveHistoryViewer for the session
   * @throws HWIException
   */
  public HiveHistoryViewer getHistoryViewer() throws HWIException {
    if (ss == null) {
      throw new HWIException("Session state was null");
    }
    /*
     * we can not call this.ss.get().getHiveHistory().getHistFileName() directly
     * as this call is made from a a Jetty thread and will return null
     */
    HiveHistoryViewer hv = new HiveHistoryViewer(historyFile);
    return hv;
  }

  /**
   * Uses the sessionName property to compare to sessions.
   *
   * @return true if sessionNames are equal false otherwise
   */
  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (!(other instanceof HWISessionItem)) {
      return false;
    }
    HWISessionItem o = (HWISessionItem) other;
    if (getSessionName().equals(o.getSessionName())) {
      return true;
    } else {
      return false;
    }
  }

  public String getResultFile() {
    return resultFile;
  }

  public void setResultFile(String resultFile) {
    this.resultFile = resultFile;
  }

  /**
   * The session name is an identifier to recognize the session.
   *
   * @return the session's name
   */
  public String getSessionName() {
    return sessionName;
  }

  /**
   * Used to represent to the user and other components what state the
   * HWISessionItem is in. Certain commands can only be run when the application
   * is in certain states.
   *
   * @return the current status of the session
   */
  public WebSessionItemStatus getStatus() {
    return status;
  }

  /**
   * Currently unused.
   *
   * @return a String with the full path to the error file.
   */
  public String getErrorFile() {
    return errorFile;
  }

  /**
   * Currently unused.
   *
   * @param errorFile
   *          the full path to the file for results.
   */
  public void setErrorFile(String errorFile) {
    this.errorFile = errorFile;
  }

  /**
   * @return the auth
   */
  public HWIAuth getAuth() {
    return auth;
  }

  /**
   * @param auth
   *          the auth to set
   */
  protected void setAuth(HWIAuth auth) {
    this.auth = auth;
  }

  /** Returns an unmodifiable list of queries. */
  public List<String> getQueries() {
    return java.util.Collections.unmodifiableList(queries);
  }

  /**
   * Adds a new query to the execution list.
   *
   * @param query
   *          query to be added to the list
   */
  public void addQuery(String query) throws HWIException {
    throwIfRunning();
    queries.add(query);
  }

  /**
   * Removes a query from the execution list.
   *
   * @param item
   *          the 0 based index of the item to be removed
   */
  public void removeQuery(int item) throws HWIException {
    throwIfRunning();
    queries.remove(item);
  }

  public void clearQueries() throws HWIException {
    throwIfRunning();
    queries.clear();
  }

  /** returns the value for resultBucketMaxSize. */
  public int getResultBucketMaxSize() {
    return resultBucketMaxSize;
  }

  /**
   * sets the value for resultBucketMaxSize.
   *
   * @param size
   *          the new size
   */
  public void setResultBucketMaxSize(int size) {
    resultBucketMaxSize = size;
  }

  /** gets the value for resultBucket. */
  public ArrayList<ArrayList<String>> getResultBucket() {
    return resultBucket;
  }

  /**
   * The HWISessionItem stores the result of each query in an array.
   *
   * @return unmodifiable list of return codes
   */
  public List<Integer> getQueryRet() {
    return java.util.Collections.unmodifiableList(queryRet);
  }

  /**
   * If the ItemStatus is QueryRunning most of the configuration is in a read
   * only state.
   */
  private void throwIfRunning() throws HWIException {
    if (status == WebSessionItemStatus.QUERY_RUNNING) {
      throw new HWIException("Query already running");
    }
  }

}
