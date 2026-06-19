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
package org.apache.hadoop.hive.common.log;

import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Class used to redirect output read from a stream to a logger
 */
public class LogRedirector implements Runnable {

  private static final long MAX_ERR_LOG_LINES_FOR_RPC = 1000;

  public interface LogSourceCallback {
    boolean isAlive();
  }

  private final Logger logger;
  private final BufferedReader in;
  private final LogSourceCallback callback;
  private List<String> errLogs;
  private int numErrLogLines = 0;

  public LogRedirector(InputStream in, Logger logger, LogSourceCallback callback) {
    this.in = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
    this.callback = callback;
    this.logger = logger;
  }

  public LogRedirector(InputStream in, Logger logger, List<String> errLogs,
                       LogSourceCallback callback) {
    this.in = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
    this.errLogs = errLogs;
    this.callback = callback;
    this.logger = logger;
  }

  @Override
  public void run() {
    try {
      String line = null;
      while ((line = in.readLine()) != null) {
        logger.info(line);
        if (errLogs != null) {
          if (numErrLogLines++ < MAX_ERR_LOG_LINES_FOR_RPC) {
            errLogs.add(line);
          }
        }
      }
    } catch (IOException e) {
      if (callback.isAlive()) {
        logger.warn("I/O error in redirector thread.", e);
      } else {
        // When stopping the process we are redirecting from,
        // the streams might be closed during reading.
        // We should not log the related exceptions in a visible level
        // as they might mislead the user.
        logger.debug("I/O error in redirector thread while stopping the remote driver", e);
      }
    } catch (Exception e) {
      logger.warn("Error in redirector thread.", e);
    }
  }

  /**
   * Start the logredirector in a new thread
   * @param name name of the new thread
   * @param redirector redirector to start
   */
  public static void redirect(String name, LogRedirector redirector) {
    Thread thread = new Thread(redirector);
    thread.setName(name);
    thread.setDaemon(true);
    thread.start();
  }

}
