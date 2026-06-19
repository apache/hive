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

package org.apache.hadoop.hive.ql.exec;

import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.mapred.Reporter;

/**
 * AutoProgressor periodically sends updates to the job tracker so that it
 * doesn't consider this task attempt dead if there is a long period of
 * inactivity. This can be configured with a timeout so that it doesn't run
 * indefinitely.
 */
public class AutoProgressor {
  private final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

  // Timer that reports every 5 minutes to the jobtracker. This ensures that
  // even if the operator returning rows for greater than that
  // duration, a progress report is sent to the tracker so that the tracker
  // does not think that the job is dead.
  Timer rpTimer = null;
  // Timer that tops rpTimer after a long timeout, e.g. 1 hr
  Timer srpTimer = null;
  // Name of the class to report for
  String logClassName = null;
  int notificationInterval;
  long timeout;
  Reporter reporter;

  class ReporterTask extends TimerTask {

    /**
     * Reporter to report progress to the jobtracker.
     */
    private Reporter rp;

    /**
     * Constructor.
     */
    public ReporterTask(Reporter rp) {
      if (rp != null) {
        this.rp = rp;
      }
    }

    @Override
    public void run() {
      if (rp != null) {
        LOG
            .info("ReporterTask calling reporter.progress() for "
            + logClassName);
        rp.progress();
      }
    }
  }

  class StopReporterTimerTask extends TimerTask {

    /**
     * Task to stop the reporter timer once we hit the timeout
     */
    private final ReporterTask rt;

    public StopReporterTimerTask(ReporterTask rp) {
      this.rt = rp;
    }

    @Override
    public void run() {
      if (rt != null) {
        LOG.info("Stopping reporter timer for " + logClassName);
        rt.cancel();
      }
    }
  }

  /**
   *
   * @param logClassName
   * @param reporter
   * @param notificationInterval - interval for reporter updates (in ms)
   */
  AutoProgressor(String logClassName, Reporter reporter,
      int notificationInterval) {
    this.logClassName = logClassName;
    this.reporter = reporter;
    this.notificationInterval = notificationInterval;
    this.timeout = 0;
  }

  /**
   *
   * @param logClassName
   * @param reporter
   * @param notificationInterval - interval for reporter updates (in ms)
   * @param timeout - when the autoprogressor should stop reporting (in ms)
   */
  AutoProgressor(String logClassName, Reporter reporter,
      int notificationInterval, long timeout) {
    this.logClassName = logClassName;
    this.reporter = reporter;
    this.notificationInterval = notificationInterval;
    this.timeout = timeout;
  }

  public void go() {
    LOG.info("Running ReporterTask every " + notificationInterval
        + " miliseconds.");
    rpTimer = new Timer(true);


    ReporterTask rt = new ReporterTask(reporter);
    rpTimer.scheduleAtFixedRate(rt, 0, notificationInterval);

    if (timeout > 0) {
      srpTimer = new Timer(true);
      StopReporterTimerTask srt = new StopReporterTimerTask(rt);
      srpTimer.schedule(srt, timeout);
    }
  }
}
