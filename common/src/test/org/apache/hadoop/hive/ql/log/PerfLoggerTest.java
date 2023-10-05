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

package org.apache.hadoop.hive.ql.log;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class PerfLoggerTest {
  private static void snooze(int ms) {
    try {
      Thread.currentThread().sleep(ms);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testBasic() {
    final PerfLogger pl = PerfLogger.getPerfLogger(null, true);
    pl.perfLogBegin("test", PerfLogger.COMPILE);
    snooze(100);
    pl.perfLogEnd("test", PerfLogger.COMPILE);
    long duration = pl.getDuration(PerfLogger.COMPILE);
    Assert.assertTrue(duration >= 100);
  }

  @Test
  public void testMT() throws InterruptedException {
    final PerfLogger pl = PerfLogger.getPerfLogger(null, true);
    // we run concurrently the getEndTimes and perfLogBegin/perfLogEnd:
    // on a Mac M1, this test fails easily if the perflogger maps are hashmaps
    ExecutorService executorService = Executors.newFixedThreadPool(64);
    // An executing threads counter
    AtomicInteger count = new AtomicInteger(0);
    // getEndTimes in a loop
    executorService.execute(() -> {
      PerfLogger.setPerfLogger(pl);
      try {
        count.incrementAndGet();
        snooze(100);
        for (int i = 0; i < 64; ++i) {
          snooze(50);
          Map<String, Long> et = pl.getEndTimes();
          Assert.assertNotNull(et);
        }
      } finally {
        count.decrementAndGet();
        synchronized (count) {
          count.notifyAll();
        }
      }
    });
    // 32 threads calling perLogBeing/perfLogEnd
    for(int t = 0; t < 31; ++t) {
      executorService.execute(() -> {
        try {
          int cnt = count.incrementAndGet();
          PerfLogger.setPerfLogger(pl);
          for (int i = 0; i < 64; ++i) {
            pl.perfLogBegin("test", PerfLogger.COMPILE + "_ "+  cnt + "_" + i);
            snooze(50);
            pl.perfLogEnd("test", PerfLogger.COMPILE + "_ "  + cnt + "_" + i);
          }
      } catch(Exception xany) {
        String msg = xany.getMessage();
      } finally {
          count.decrementAndGet();
          synchronized (count) {
            count.notifyAll();
          }
        }
    });
    }
    // wait for all threads to end
    while(count.get() != 0) {
      synchronized (count) {
        count.wait();
      }
    }
    executorService.shutdown();
  }
}
