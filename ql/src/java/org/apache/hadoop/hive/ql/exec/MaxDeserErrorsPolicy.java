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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MaxDeserErrorsPolicy extends DeserErrorPolicy.Policy {

  private DeserializerErrsReporter daemon;
  private volatile boolean shouldStop = false;
  private AtomicLong deserializer_errors = new AtomicLong(0);
  private long maxDeserErrors;

  MaxDeserErrorsPolicy(Operator operator, Configuration hconf) {
    super(operator, hconf);
    this.daemon = new DeserializerErrsReporter();
    this.maxDeserErrors = HiveConf.getLongVar(hconf, HiveConf.ConfVars.MAXDESERERRORS);
  }

  private class DeserializerErrsReporter extends Thread {
    private long lastErrsCnt;
    private long sleepInterval;
    private boolean isStarted;

    DeserializerErrsReporter() {
      this.sleepInterval = HiveConf.getTimeVar(hconf,
          HiveConf.ConfVars.HIVE_REPORT_DESERERRORS_INTERVAL, TimeUnit.MILLISECONDS);
      this.isStarted = false;
      this.lastErrsCnt = 0;
    }

    void goes() {
      setName("DeserializationError-Reporter");
      setDaemon(true);
      start();
      isStarted = true;
    }

    boolean isStarted() {
      return isStarted;
    }

    @Override
    public void run() {
      while (!shouldStop) {
        try {
          reportDeserializerErrs();
          sleep(sleepInterval);
        } catch (InterruptedException e) {
          break;
        }
      }
      reportDeserializerErrs();
    }

    private void reportDeserializerErrs() {
      long errsCnt = deserializer_errors.get(), incr;
      if ((incr = errsCnt - lastErrsCnt) > 0) {
        operator.reporter.incrCounter(counterGroup, counterName, incr);
        lastErrsCnt = errsCnt;
      }
    }

  }
    @Override
  public void onDeserError(Exception e) throws HiveException {
    if (!daemon.isStarted() && shouldReport()) {
      // Start the daemon lazily only when deserialization errors happens
      daemon.goes();
    }

    if (deserializer_errors.incrementAndGet() > maxDeserErrors && !shouldReport()) {
      // reaches the limit in one map and unable to report the counter,
      // throw the exception instead
      throw new HiveException("Hive Runtime Error while processing writable"
          + ", deserialization errors: " + deserializer_errors.get()
          + ", max allowed: " + maxDeserErrors, e);
    }
  }

  @Override
  public void close(boolean abort) {
    if (daemon.isStarted) {
      daemon.isStarted = false;
      shouldStop = true;
      daemon.interrupt();
      if (!abort) {
        try {
          daemon.join(10000);
        } catch (InterruptedException e) {
          // ignore this
        }
      }
    } else {
      // No deserialization errors, but report the counter as well
      super.close(abort);
    }
  }

  @Override
  public long getErrorCount() {
    return deserializer_errors.get();
  }

}
