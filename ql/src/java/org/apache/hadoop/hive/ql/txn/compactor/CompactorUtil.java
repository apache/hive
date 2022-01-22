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

import org.apache.hadoop.hive.common.StringableMap;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;

import static java.lang.String.format;

public class CompactorUtil {
  public static final String COMPACTOR = "compactor";
  static final String COMPACTOR_PREFIX = "compactor.";
  static final String MAPRED_QUEUE_NAME = "mapred.job.queue.name";

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

  public static ExecutorService createExecutorWithThreadFactory(int parallelism, String threadNameFormat) {
    return new ForkJoinPool(parallelism,
      pool -> {
        ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
        worker.setName(format(threadNameFormat, worker.getPoolIndex()));
        return worker;
      },
      null, false);
  }

  /**
   * Get the compactor queue name if it's defined.
   * @param conf global hive conf
   * @param ci compaction info object
   * @param table instance of table
   * @return name of the queue, can be null
   */
  static String getCompactorJobQueueName(HiveConf conf, CompactionInfo ci, Table table) {
    // Get queue name from the ci. This is passed through
    // ALTER TABLE table_name COMPACT 'major' WITH OVERWRITE TBLPROPERTIES('compactor.hive.compactor.job.queue'='some_queue')
    if (ci.properties != null) {
      StringableMap ciProperties = new StringableMap(ci.properties);
      String queueName = ciProperties.get(COMPACTOR_PREFIX + MAPRED_QUEUE_NAME);
      if (queueName != null && queueName.length() > 0) {
        return queueName;
      }
    }

    // Get queue name from the table properties
    String queueName = table.getParameters().get(COMPACTOR_PREFIX + MAPRED_QUEUE_NAME);
    if (queueName != null && queueName.length() > 0) {
      return queueName;
    }

    // Get queue name from global hive conf
    queueName = conf.get(HiveConf.ConfVars.COMPACTOR_JOB_QUEUE.varname);
    if (queueName != null && queueName.length() > 0) {
      return queueName;
    }
    return null;
  }
}
