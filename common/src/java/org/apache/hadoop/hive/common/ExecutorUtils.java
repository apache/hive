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
package org.apache.hadoop.hive.common;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class ExecutorUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorUtils.class);

  /**
   * Waiting for futures to finish and return any exceptions caught.
   * This implementation is borrowed from iceberg's Tasks.
   * @param futures
   * @return
   */
  public static Collection<Throwable> waitFor(Collection<Future<?>> futures) {
    while (true) {
      int numFinished = 0;
      for (Future<?> future : futures) {
        if (future.isDone()) {
          numFinished += 1;
        }
      }

      if (numFinished == futures.size()) {
        List<Throwable> uncaught = Lists.newArrayList();
        // all of the futures are done, get any uncaught exceptions
        for (Future<?> future : futures) {
          try {
            future.get();

          } catch (InterruptedException e) {
            LOG.warn("Interrupted while getting future results", e);
            for (Throwable t : uncaught) {
              e.addSuppressed(t);
            }
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);

          } catch (CancellationException e) {
            // ignore cancellations

          } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (Error.class.isInstance(cause)) {
              for (Throwable t : uncaught) {
                cause.addSuppressed(t);
              }
              throw (Error) cause;
            }

            if (cause != null) {
              uncaught.add(e);
            }

            LOG.warn("Task threw uncaught exception", cause);
          }
        }

        return uncaught;

      } else {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while waiting for tasks to finish", e);

          for (Future<?> future : futures) {
            future.cancel(true);
          }
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }
    }
  }
}
