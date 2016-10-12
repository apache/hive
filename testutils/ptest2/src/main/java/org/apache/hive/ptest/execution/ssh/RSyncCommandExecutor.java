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

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.hive.ptest.execution.Constants;
import org.apache.hive.ptest.execution.LocalCommand;
import org.apache.hive.ptest.execution.LocalCommandFactory;
import org.apache.hive.ptest.execution.LocalCommand.CollectPolicy;
import org.slf4j.Logger;


public class RSyncCommandExecutor {
  private final Logger mLogger;
  private final int mMaxRsyncThreads;
  private final LocalCommandFactory mLocalCommandFactory;
  private final Semaphore mSemaphore;
  private volatile boolean mShutdown;

  public RSyncCommandExecutor(Logger logger, int maxRsyncThreads, LocalCommandFactory localCommandFactory) {
    mLogger = logger;
    mMaxRsyncThreads = Math.min(Runtime.getRuntime().availableProcessors() * 5, maxRsyncThreads);
    mLocalCommandFactory = localCommandFactory;
    mSemaphore = new Semaphore(mMaxRsyncThreads);
    mShutdown = false;
    mLogger.info("RSyncCommandExecutor has " + mMaxRsyncThreads + " threads on " + Runtime.getRuntime()
      .availableProcessors() + " cpus");
  }

  /**
   * Execute the given RSync. If the command exits with a non-zero
   * exit status the command will be retried up to three times.
   */
  public void execute(RSyncCommand command) {
    CollectPolicy collector = new CollectPolicy();
    boolean release = false;
    try {
      mSemaphore.acquire();
      release = true;
      int attempts = 0;
      boolean retry;
      LocalCommand cmd;
      do {
        retry = false;
        if(command.getType() == RSyncCommand.Type.TO_LOCAL) {
          cmd = mLocalCommandFactory.create(collector,
              String.format("timeout 1h rsync -vaPe \"ssh -i %s\" --timeout 600 %s@%s:%s %s",
                  command.getPrivateKey(), command.getUser(), command.getHost(),
                  command.getRemoteFile(), command.getLocalFile()));
        } else if (command.getType() == RSyncCommand.Type.TO_LOCAL_NON_RECURSIVE) {
          cmd = mLocalCommandFactory.create(collector,
              String.format("timeout 1h rsync --exclude \"*/\" -vaPe \"ssh -i %s\" --timeout 600 %s@%s:%s %s",
                  command.getPrivateKey(), command.getUser(), command.getHost(),
                  command.getRemoteFile(), command.getLocalFile()));
        } else if(command.getType() == RSyncCommand.Type.FROM_LOCAL) {
          cmd = mLocalCommandFactory.create(collector,
              String.format("timeout 1h rsync -vaPe \"ssh -i %s\" --timeout 600 --delete --delete-during --force %s %s@%s:%s",
                  command.getPrivateKey(), command.getLocalFile(), command.getUser(), command.getHost(),
                  command.getRemoteFile()));
        } else {
          throw new UnsupportedOperationException(String.valueOf(command.getType()));
        }
        if(mShutdown) {
          mLogger.warn("Shutting down command " + command);
          cmd.kill();
          command.setExitCode(Constants.EXIT_CODE_UNKNOWN);
          return;
        }
        // 12 is timeout and 255 is unspecified error
        if(attempts++ <= 3 && cmd.getExitCode() != 0) {
          mLogger.warn("Command exited with " + cmd.getExitCode() + ", will retry: " + command);
          retry = true;
          TimeUnit.SECONDS.sleep(20);
        }
      } while (!mShutdown && retry); // an error occurred, re-try
      command.setExitCode(cmd.getExitCode());
    } catch (IOException e) {
      command.setException(e);
    } catch (InterruptedException e) {
      command.setException(e);
    } finally {
      if(release) {
        mSemaphore.release();
      }
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
