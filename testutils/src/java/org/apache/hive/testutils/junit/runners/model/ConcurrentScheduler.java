/*
 * Copyright (c) 2009-2012, toby weston & tempus-fugit committers
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.testutils.junit.runners.model;

import static com.google.code.tempusfugit.concurrency.ExecutorServiceShutdown.shutdown;
import static com.google.code.tempusfugit.temporal.Duration.days;
import static java.lang.Boolean.TRUE;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;

import org.junit.runners.model.RunnerScheduler;

/**
 * Originally taken from com.google.code.tempusfugit.concurrency.ConcurrentScheduler
 */
public class ConcurrentScheduler implements RunnerScheduler {

  private final ExecutorService executor;
  private final OutputStream outputStream;

  public ConcurrentScheduler(ExecutorService executor) {
    this(executor, System.err);
  }

  public ConcurrentScheduler(ExecutorService executor, OutputStream outputStream) {
    this.executor = executor;
    this.outputStream = outputStream;
  }

  public void schedule(Runnable childStatement) {
    executor.submit(childStatement);
  }

  public void finished() {
    if (!successful(shutdown(executor).waitingForCompletion(days(365)))) {
      writeln(outputStream, "scheduler shutdown timed out before tests completed, "
          + "you may have executors hanging around...");
    }
  }

  private Boolean successful(Boolean completed) {
    return TRUE.equals(completed);
  }

  private void writeln(OutputStream stream, String string) {
    try {
      stream.write(string.getBytes());
      stream.write(System.getProperty("line.separator").getBytes());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
