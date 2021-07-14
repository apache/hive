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

package org.apache.hadoop.hive.ql.plan.mapping;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import com.google.common.io.Files;
import com.google.common.util.concurrent.MoreExecutors;

public class AA1 {

  static class ProcStatTicker extends Ticker {
    // ideally this should be sysconf(_SC_CLK_TCK) ; but it's aint easy to access that from java and its almost always 100Hz
    public static final int TICKS = 100;
    public static final long TICK_US = 1_000_000_000l / TICKS;

    @Override
    public long read() {
      try {
        String line = Files.toString(new File("/proc/self/stat"), Charsets.UTF_8);
        return Long.parseLong(line.split(" ")[13]) * TICK_US;
      } catch (IOException e) {
        throw new RuntimeException("unable to read stat", e);
      }
    }
  }

  public static void main(String[] args) throws InterruptedException {

    long t0;
    System.out.println(t0 = System.nanoTime());
    ExecutorService e;
    e = MoreExecutors.newDirectExecutorService();
    e = Executors.newFixedThreadPool(32);

    List<Worj> li = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      li.add(new Worj());
    }
    //    e.invokeAll(li);

    Stopwatch sw = Stopwatch.createStarted();
    ProcStatTicker ticker = new ProcStatTicker();
    Stopwatch sw2 = Stopwatch.createStarted(ticker);

    System.out.println("t0:" + ticker.read());
    e.invokeAll(li);
    //    for (int i = 0; i < 100; i++) {
    //      e.submit(new Worj());
    //    }
    e.shutdown();
    if (!e.awaitTermination(30, TimeUnit.SECONDS)) {
      throw new RuntimeException("timeou");
    }
    sw.stop();
    sw2.stop();
    long t1;
    System.out.println("t0:" + ticker.read());
    System.out.println(t1 = System.nanoTime());
    System.out.println(t1 - t0);
    System.out.println(sw);
    System.out.println(sw2);

  }

  static class Worj implements Callable<Integer> {

    @Override
    public Integer call() throws Exception {
      int k = 0;
      for (int i = 0; i < 10000; i++) {
        for (int j = 0; j < 30000; j++) {
          k ^= j + i;
        }
      }
      return k;
    }

  }
}
