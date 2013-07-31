/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.ptest.execution;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.hive.ptest.execution.ssh.RSyncCommand;
import org.apache.hive.ptest.execution.ssh.RSyncCommandExecutor;
import org.slf4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class MockRSyncCommandExecutor extends RSyncCommandExecutor {
  private final List<String> mCommands;
  private final Map<String, Queue<Integer>> mFailures;
  public MockRSyncCommandExecutor(Logger logger) {
    super(logger);
    mCommands = Lists.newArrayList();
    mFailures = Maps.newHashMap();
  }
  public List<String> getCommands() {
    return mCommands;
  }
  public synchronized void putFailure(String command, Integer... exitCodes) {
    Queue<Integer> queue = mFailures.get(command);
    if(queue == null) {
      queue = new LinkedList<Integer>();
      mFailures.put(command, queue);
    } else {
      queue = mFailures.get(command);
    }
    for(Integer exitCode : exitCodes) {
      queue.add(exitCode);
    }
  }
  @Override
  public synchronized void execute(RSyncCommand command) {
    String filePair = command.getLocalFile() + " " + command.getRemoteFile();
    mCommands.add(filePair);
    command.setOutput("");
    Queue<Integer> queue = mFailures.get(filePair);
    if(queue == null || queue.isEmpty()) {
      command.setExitCode(0);
    } else {
      command.setExitCode(queue.remove());
    }
  }


}
