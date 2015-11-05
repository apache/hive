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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the Deadline class used for long running requests.
 */
public class TestDeadline {

  private static final Logger LOG = LoggerFactory.getLogger(TestDeadline.class);
  private long timeout = 1000;
  private long resetTimeout = 200;
  private long duration = 500;
  private boolean isFailed;
  private String errorMsg;

  @Test
  public void testDeadline() throws Exception {
    isFailed = false;
    errorMsg = "";

    Thread threadTimeout = new Thread(createRunnable());
    threadTimeout.setDaemon(true);
    threadTimeout.start();
    threadTimeout.join(60000);

    if (isFailed) {
      Assert.fail(errorMsg);
    }
  }

  private Runnable createRunnable() {
    return new Runnable() {
      @Override
      public void run() {
        Deadline.registerIfNot(timeout);
        try {
          // normal
          start();
          try {
            Deadline.checkTimeout();
          } catch (MetaException e) {
            failInThread("should not timeout", e);
            return;
          }
          Deadline.stopTimer();

          // normal. Check stopTimer() works.
          start();
          try {
            Deadline.checkTimeout();
          } catch (MetaException e) {
            failInThread("should not timeout", e);
            return;
          }
          Deadline.stopTimer();

          // reset
          Deadline.resetTimeout(resetTimeout);

          // timeout
          start();
          try {
            Deadline.checkTimeout();
            failInThread("should timeout.", null);
            return;
          } catch (MetaException e) {
            if (e.getCause() instanceof DeadlineException) {
              Deadline.clear();
            } else {
              failInThread("new MetaException failed.", e);
              return;
            }
          }
          if (Deadline.getCurrentDeadline() != null) {
            failInThread("the threadlocal object should be removed after timeout.", null);
          }
        } catch (MetaException e) {
          failInThread("error happens in start, end, or reset. Check the exception.", e);
        }
      }
    };
  }

  private void start() throws MetaException {
    Deadline.startTimer("test");
    try {
      Thread.sleep(duration);
    } catch (InterruptedException e) {
      failInThread("Odd. Sleeping thread is interrupted.", e);
    }
  }

  private void failInThread(String msg, Exception e) {
    isFailed = true;

    if (e != null) {
      LOG.error(msg, e);
      errorMsg = msg + ": " + e.getMessage();
    } else {
      LOG.error(msg);
      errorMsg = msg;
    }
  }
}
