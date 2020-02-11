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
package org.apache.hadoop.hive.ql.parse.repl.dump;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.PartitionIterable;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.FileOperations;
import org.apache.hadoop.hive.ql.plan.ExportWork.MmContext;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;

import static org.apache.hadoop.hive.ql.parse.repl.dump.TableExport.Paths;

/**
 * This class manages writing multiple partitions _data files simultaneously.
 * it has a blocking queue that stores partitions to be dumped via a producer thread.
 * it has a worker thread pool that reads of the queue to perform the various tasks.
 */
// TODO: this object is created once to call one method and then immediately destroyed.
//       So it's basically just a roundabout way to pass arguments to a static method. Simplify?
class PartitionExport {
  private final Paths paths;
  private final PartitionIterable partitionIterable;
  private final String distCpDoAsUser;
  private final HiveConf hiveConf;
  private final int nThreads;
  private final SessionState callersSession;
  private final MmContext mmCtx;

  private static final Logger LOG = LoggerFactory.getLogger(PartitionExport.class);
  private BlockingQueue<Partition> queue;

  PartitionExport(Paths paths, PartitionIterable partitionIterable, String distCpDoAsUser,
      HiveConf hiveConf, MmContext mmCtx) {
    this.paths = paths;
    this.partitionIterable = partitionIterable;
    this.distCpDoAsUser = distCpDoAsUser;
    this.hiveConf = hiveConf;
    this.mmCtx = mmCtx;
    this.nThreads = hiveConf.getIntVar(HiveConf.ConfVars.REPL_PARTITIONS_DUMP_PARALLELISM);
    this.queue = new ArrayBlockingQueue<>(2 * nThreads);
    this.callersSession = SessionState.get();
  }

  void write(final ReplicationSpec forReplicationSpec) throws InterruptedException, HiveException {
    List<Future<?>> futures = new LinkedList<>();
    ExecutorService producer = Executors.newFixedThreadPool(1,
        new ThreadFactoryBuilder().setNameFormat("partition-submitter-thread-%d").build());
    futures.add(producer.submit(() -> {
      SessionState.setCurrentSessionState(callersSession);
      for (Partition partition : partitionIterable) {
        try {
          queue.put(partition);
        } catch (InterruptedException e) {
          throw new RuntimeException(
              "Error while queuing up the partitions for export of data files", e);
        }
      }
    }));
    producer.shutdown();

    ThreadFactory namingThreadFactory =
        new ThreadFactoryBuilder().setNameFormat("partition-dump-thread-%d").build();
    ExecutorService consumer = Executors.newFixedThreadPool(nThreads, namingThreadFactory);

    while (!producer.isTerminated() || !queue.isEmpty()) {
      /*
      This is removed using a poll because there can be a case where there partitions iterator is empty
      but because both the producer and consumer are started simultaneously the while loop will execute
      because producer is not terminated but it wont produce anything so queue will be empty and then we
      should only wait for a specific time before continuing, as the next loop cycle will fail.
       */
      Partition partition = queue.poll(1, TimeUnit.SECONDS);
      if (partition == null) {
        continue;
      }
      LOG.debug("scheduling partition dump {}", partition.getName());
      futures.add(consumer.submit(() -> {
        String partitionName = partition.getName();
        String threadName = Thread.currentThread().getName();
        LOG.debug("Thread: {}, start partition dump {}", threadName, partitionName);
        try {
          // this the data copy
          List<Path> dataPathList = Utils.getDataPathList(partition.getDataLocation(),
                  forReplicationSpec, hiveConf);
          Path rootDataDumpDir = paths.partitionExportDir(partitionName);
          new FileOperations(dataPathList, rootDataDumpDir, distCpDoAsUser, hiveConf, mmCtx)
                  .export(forReplicationSpec);
          LOG.debug("Thread: {}, finish partition dump {}", threadName, partitionName);
        } catch (Exception e) {
          throw new RuntimeException(e.getMessage(), e);
        }
      }));
    }
    consumer.shutdown();
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (Exception e) {
        LOG.error("failed", e.getCause());
        throw new HiveException(e.getCause().getMessage(), e.getCause());
      }
    }
    // may be drive this via configuration as well.
    consumer.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
  }
}
