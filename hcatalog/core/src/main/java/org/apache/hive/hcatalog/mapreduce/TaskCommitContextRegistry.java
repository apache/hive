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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.mapreduce;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

/**
 * Singleton Registry to track the commit of TaskAttempts.
 * Used to manage commits for Tasks that create dynamic-partitions.
 */
public class TaskCommitContextRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(TaskCommitContextRegistry.class);

  private static TaskCommitContextRegistry ourInstance = new TaskCommitContextRegistry();

  /**
   * Singleton instance getter.
   */
  public static TaskCommitContextRegistry getInstance() {
    return ourInstance;
  }

  /**
   * Implement this interface to register call-backs for committing TaskAttempts.
   */
  public static interface TaskCommitterProxy {

    /**
     * Call-back for Committer's abortTask().
     */
    public void abortTask(TaskAttemptContext context) throws IOException;

    /**
     * Call-back for Committer's abortTask().
     */
    public void commitTask(TaskAttemptContext context) throws IOException;
  }

  private HashMap<String, TaskCommitterProxy> taskCommitters
                    = new HashMap<String, TaskCommitterProxy>();

  /**
   * Trigger commit for TaskAttempt, as specified by the TaskAttemptContext argument.
   */
  public synchronized void commitTask(TaskAttemptContext context) throws IOException {
    String key = generateKey(context);
    if (!taskCommitters.containsKey(key)) {
      LOG.warn("No callback registered for TaskAttemptID:" + key + ". Skipping.");
      return;
    }

    try {
      LOG.info("Committing TaskAttempt:" + key);
      taskCommitters.get(key).commitTask(context);
    }
    catch (Throwable t) {
      throw new IOException("Could not clean up TaskAttemptID:" + key, t);
    }

  }

  private String generateKey(TaskAttemptContext context) throws IOException {
    String jobInfoString = context.getConfiguration().get(HCatConstants.HCAT_KEY_OUTPUT_INFO);
    if (StringUtils.isBlank(jobInfoString)) { // Avoid the NPE.
      throw new IOException("Could not retrieve OutputJobInfo for TaskAttempt " + context.getTaskAttemptID());
    }
    OutputJobInfo jobInfo = (OutputJobInfo) HCatUtil.deserialize(jobInfoString);
    return context.getTaskAttemptID().toString() + "@" + jobInfo.getLocation();
  }

  /**
   * Trigger abort for TaskAttempt, as specified by the TaskAttemptContext argument.
   */
  public synchronized void abortTask(TaskAttemptContext context) throws IOException {
    String key = generateKey(context);
    if (!taskCommitters.containsKey(key)) {
      LOG.warn("No callback registered for TaskAttemptID:" + key + ". Skipping.");
      return;
    }

    try {
      LOG.info("Aborting TaskAttempt:" + key);
      taskCommitters.get(key).abortTask(context);
    }
    catch (Throwable t) {
      throw new IOException("Could not clean up TaskAttemptID:" + key, t);
    }
  }

  /**
   * Method to register call-backs to control commits and aborts of TaskAttempts.
   * @param context The TaskAttemptContext instance for the task-attempt, identifying the output.
   * @param committer Instance of TaskCommitterProxy, to commit/abort a TaskAttempt.
   * @throws java.io.IOException On failure.
   */
  public synchronized void register(TaskAttemptContext context, TaskCommitterProxy committer) throws IOException {
    String key = generateKey(context);
    LOG.info("Registering committer for TaskAttemptID:" + key);
    if (taskCommitters.containsKey(key)) {
      LOG.warn("Replacing previous committer:" + committer);
    }
    taskCommitters.put(key, committer);
  }

  /**
   * Method to discard the committer call-backs for a specified TaskAttemptID.
   * @param context The TaskAttemptContext instance for the task-attempt, identifying the output.
   * @throws java.io.IOException On failure.
   */
  public synchronized void discardCleanupFor(TaskAttemptContext context) throws IOException {
    String key = generateKey(context);
    LOG.info("Discarding all cleanup for TaskAttemptID:" + key);
    if (!taskCommitters.containsKey(key)) {
      LOG.warn("No committer registered for TaskAttemptID:" + key);
    }
    else {
      taskCommitters.remove(key);
    }
  }

  // Hide constructor, for make benefit glorious Singleton.
  private TaskCommitContextRegistry() {
  }
}
