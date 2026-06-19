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

import java.io.IOException;

import org.apache.hadoop.mapred.Counters;

/**
 * TaskHandle.
 *
 */
public class TaskHandle {
  // The eventual goal is to monitor the progress of all the tasks, not only the
  // map reduce task.
  // The execute() method of the tasks will return immediately, and return a
  // task specific handle to
  // monitor the progress of that task.
  // Right now, the behavior is kind of broken, ExecDriver's execute method
  // calls progress - instead it should
  // be invoked by Driver
  public Counters getCounters() throws IOException {
    // default implementation
    return null;
  }
}
