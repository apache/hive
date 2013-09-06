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

package org.apache.hcatalog.mapreduce;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.mapreduce.ProgressReporter} instead
 */
class ProgressReporter extends StatusReporter implements Reporter {

  private TaskInputOutputContext context = null;
  private TaskAttemptContext taskAttemptContext = null;

  public ProgressReporter(TaskAttemptContext context) {
    if (context instanceof TaskInputOutputContext) {
      this.context = (TaskInputOutputContext) context;
    } else {
      taskAttemptContext = context;
    }
  }

  @Override
  public void setStatus(String status) {
    if (context != null) {
      context.setStatus(status);
    }
  }

  @Override
  public Counters.Counter getCounter(Enum<?> name) {
    return (context != null) ? (Counters.Counter) context.getCounter(name) : null;
  }

  @Override
  public Counters.Counter getCounter(String group, String name) {
    return (context != null) ? (Counters.Counter) context.getCounter(group, name) : null;
  }

  @Override
  public void incrCounter(Enum<?> key, long amount) {
    if (context != null) {
      context.getCounter(key).increment(amount);
    }
  }

  @Override
  public void incrCounter(String group, String counter, long amount) {
    if (context != null) {
      context.getCounter(group, counter).increment(amount);
    }
  }

  @Override
  public InputSplit getInputSplit() throws UnsupportedOperationException {
    return null;
  }

  public float getProgress() {
    /* Required to build against 0.23 Reporter and StatusReporter. */
    /* TODO: determine the progress. */
    return 0.0f;
  }

  @Override
  public void progress() {
    if (context != null) {
      context.progress();
    } else {
      taskAttemptContext.progress();
    }
  }
}
