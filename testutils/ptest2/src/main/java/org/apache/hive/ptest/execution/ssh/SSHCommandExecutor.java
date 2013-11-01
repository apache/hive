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
package org.apache.hive.ptest.execution.ssh;

import java.util.concurrent.TimeUnit;

import org.apache.hive.ptest.execution.Constants;
import org.apache.hive.ptest.execution.LocalCommand;
import org.apache.hive.ptest.execution.LocalCommand.CollectPolicy;
import org.apache.hive.ptest.execution.LocalCommandFactory;
import org.slf4j.Logger;

public class SSHCommandExecutor {

  private final Logger mLogger;
  private final LocalCommandFactory mLocalCommandFactory;
  private volatile boolean mShutdown;
  
  public SSHCommandExecutor(Logger logger, LocalCommandFactory localCommandFactory) {
    mLogger = logger;
    mShutdown = false;
    mLocalCommandFactory = localCommandFactory;
  }
  public SSHCommandExecutor(Logger logger) {
    this(logger, new LocalCommandFactory(logger));
  }
  /**
   * Execute the given command via the ssh command line tool. If the command
   * exits with status code 255 the command will be tries up to three times.
   */
  public void execute(SSHCommand command) {
    CollectPolicy collector = new CollectPolicy();
    try {
      String commandText = String.format("ssh -v -i %s -l %s %s '%s'", command.getPrivateKey(),
          command.getUser(), command.getHost(), command.getCommand());
      int attempts = 0;
      boolean retry;
      LocalCommand cmd;
      do {
        retry = false;
        cmd = mLocalCommandFactory.create(collector, commandText);
        if(mShutdown) {
          mLogger.warn("Shutting down command " + command);
          cmd.kill();
          command.setExitCode(Constants.EXIT_CODE_UNKNOWN);
          return;
        }
        if(attempts++ <= 3 && cmd.getExitCode() == Constants.EXIT_CODE_UNKNOWN) {
          mLogger.warn("Command exited with " + cmd.getExitCode() + ", will retry: " + command);
          retry = true;
          TimeUnit.SECONDS.sleep(5);
        }
      } while (!mShutdown && retry); // an error occurred, re-try
      command.setExitCode(cmd.getExitCode());
    } catch (Exception e) {
      if(command.getExitCode() == Constants.EXIT_CODE_SUCCESS) {
        command.setExitCode(Constants.EXIT_CODE_EXCEPTION);
      }
      command.setException(e);
    } finally {
      command.setOutput(collector.getOutput());
    }
  }
  boolean isShutdown() {
    return mShutdown;
  }
  public void shutdownNow() {
    this.mShutdown = true;
  }
}