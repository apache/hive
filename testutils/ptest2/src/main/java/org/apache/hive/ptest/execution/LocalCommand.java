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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;

public class LocalCommand {

  private static final AtomicInteger localCommandCounter = new AtomicInteger(0);

  private final Logger logger;
  private final Process process;
  private final StreamReader streamReader;
  private Integer exitCode;
  private final int commandId;
  private final Stopwatch stopwatch = Stopwatch.createUnstarted();

  public LocalCommand(Logger logger, OutputPolicy outputPolicy, String command) throws IOException {
    this.commandId = localCommandCounter.incrementAndGet();
    this.logger = logger;
    logger.info("Starting LocalCommandId={}: {}", commandId, command);
    stopwatch.start();
    process = new ProcessBuilder().command(new String[] {"bash", "-c", command}).redirectErrorStream(true).start();
    streamReader = new StreamReader(outputPolicy, process.getInputStream());
    streamReader.setName("StreamReader-[" + command + "]");
    streamReader.setDaemon(true);
    streamReader.start();
  }

  public int getExitCode() throws InterruptedException {
    synchronized (process) {
      awaitProcessCompletion();
      return exitCode;
    }
  }

  private void awaitProcessCompletion() throws InterruptedException {
    synchronized (process) {
      if (exitCode == null) {
        exitCode = process.waitFor();
        if (stopwatch.isRunning()) {
          stopwatch.stop();
          logger.info("Finished LocalCommandId={}. ElapsedTime(ms)={}", commandId,
              stopwatch.elapsed(
                  TimeUnit.MILLISECONDS));
        }
      }
    }
  }

  public void kill() {
    synchronized (process) {
      process.destroy();
    }
  }

  public static interface OutputPolicy {
    public void handleOutput(String line);
    public void handleThrowable(Throwable throwable);
  }
  public static class CollectLogPolicy extends CollectPolicy {
    private final Logger logger;
    public CollectLogPolicy(Logger logger) {
      this.logger = logger;
    }
    @Override
    public void handleOutput(String line) {
      logger.info(line);
      output.append(line).append("\n");
    }
  }
  public static class CollectPolicy implements OutputPolicy {
    protected final StringBuilder output = new StringBuilder();
    protected Throwable throwable;
    @Override
    public void handleOutput(String line) {
      output.append(line).append("\n");
    }
    @Override
    public void handleThrowable(Throwable throwable) {
      if(throwable instanceof IOException &&
          "Stream closed".equals(throwable.getMessage())) {
        return;
      }
      this.throwable = throwable;
    }
    public String getOutput() {
      String result = output.toString();
      if(throwable != null) {
        throw new RuntimeException(result, throwable);
      }
      return result;
    }
  }

  private static class StreamReader extends Thread {
    private final BufferedReader input;
    private final OutputPolicy outputPolicy;
    public StreamReader(OutputPolicy outputPolicy, InputStream in) {
      this.outputPolicy = outputPolicy;
      this.input = new BufferedReader(new InputStreamReader(in));
    }
    @Override
    public void run() {
      try {
        String line;
        while((line = input.readLine()) != null) {
          outputPolicy.handleOutput(line);
        }
      } catch(Exception e) {
        outputPolicy.handleThrowable(e);
      } finally {
        try {
          input.close();
        } catch (IOException ignored) {

        }
      }
    }
  }
}
