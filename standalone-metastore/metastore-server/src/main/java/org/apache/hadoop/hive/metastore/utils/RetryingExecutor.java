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

package org.apache.hadoop.hive.metastore.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryingExecutor<T> {
  private static Logger LOG = LoggerFactory.getLogger(RetryingExecutor.class);

  private final int maxRetries;
  private final long sleepInterval;
  private final Callable<T> command;
  private final List<Class<? extends Exception>> retriableException = new ArrayList<>();
  private int currentRetries = 0;
  private String commandName;

  public RetryingExecutor(int maxRetries, long sleepInterval, Callable<T> command) {
    this.maxRetries = maxRetries;
    this.sleepInterval = sleepInterval;
    this.command = command;
  }

  public RetryingExecutor<T> onRetry(Class<? extends Exception> ex) {
    this.retriableException.add(ex);
    return this;
  }

  public RetryingExecutor<T> commandName(String name) {
    this.commandName = name;
    return this;
  }

  public T run() throws MetaException {
    while (true) {
      try {
        return command.call();
      } catch (Exception e) {
        checkException(e);
        LOG.info("Attempting to retry the command:{} in {} out of {} retries",
            commandName, currentRetries, maxRetries, e);
        if (currentRetries >= maxRetries) {
          String message = "Couldn't finish the command: " + commandName +
              " because we reached the maximum of retries: " + maxRetries;
          LOG.error(message, e);
          throw new MetaException(message + " :: " + e.getMessage());
        }
        currentRetries++;
        try {
          Thread.sleep(sleepInterval);
        } catch (InterruptedException e1) {
          String msg = "Couldn't run the command: " + commandName + " in " + currentRetries +
              " retry, because the following error: ";
          LOG.error(msg, e1);
          throw new MetaException(msg + e1.getMessage());
        }
      }
    }
  }

  private void checkException(Exception e) throws MetaException {
    if (!retriableException.isEmpty() &&
        retriableException.stream().noneMatch(nex -> nex.isInstance(e))) {
      String message = "See a non-retriable exception, avoid to retry the command:" + commandName;
      LOG.info(message, e);
      throw new MetaException(message + " :: " + e.getMessage());
    }
  }

  public static class RetryException extends Exception {
    private static final long serialVersionUID = 1L;

    public RetryException(Exception ex) {
      super(ex);
    }

    public RetryException(String msg) {
      super(msg);
    }
  }

  public long getSleepInterval() {
    return sleepInterval;
  }
}