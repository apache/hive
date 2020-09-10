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
package org.apache.hadoop.hive.ql.txn.compactor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class CompactorUtil {
  public static final String COMPACTOR = "compactor";

  public interface ThrowingRunnable<E extends Exception> {
    void run() throws E;

    static Runnable unchecked(ThrowingRunnable<?> r) {
      return () -> {
        try {
          r.run();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      };
    }
  }

  public static ThreadFactory createThreadFactory(String threadNameFormat) {
    return new ThreadFactoryBuilder()
      .setPriority(Thread.currentThread().getPriority())
      .setDaemon(Thread.currentThread().isDaemon())
      .setNameFormat(threadNameFormat)
      .build();
  }

  public static ExecutorService createExecutorWithThreadFactory(int threadCount, String threadNameFormat) {
    return Executors.newFixedThreadPool(threadCount, createThreadFactory(threadNameFormat));
  }
}
