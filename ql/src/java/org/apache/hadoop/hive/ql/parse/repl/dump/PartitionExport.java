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
import org.apache.hadoop.hive.ql.exec.repl.util.FileList;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.PartitionIterable;
import org.apache.hadoop.hive.ql.parse.EximUtil.DataCopyPath;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.FileOperations;
import org.apache.hadoop.hive.ql.plan.ExportWork.MmContext;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;

import static org.apache.hadoop.hive.ql.parse.repl.dump.TableExport.Paths;

/**
 * This class manages writing multiple partitions _data files simultaneously.
 * It creates a worker thread pool. Each thread in pool is assigned the task
 * of dumping or writing each partition _data files in parallel manner.
 *
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

  PartitionExport(Paths paths, PartitionIterable partitionIterable, String distCpDoAsUser,
      HiveConf hiveConf, MmContext mmCtx) {
    this.paths = paths;
    this.partitionIterable = partitionIterable;
    this.distCpDoAsUser = distCpDoAsUser;
    this.hiveConf = hiveConf;
    this.mmCtx = mmCtx;
    this.nThreads = hiveConf.getIntVar(HiveConf.ConfVars.REPL_PARTITIONS_DUMP_PARALLELISM);
    this.callersSession = SessionState.get();
  }

  List<DataCopyPath> write(final ReplicationSpec forReplicationSpec, boolean isExportTask,
                                   FileList fileList, boolean dataCopyAtLoad)
          throws InterruptedException, HiveException {
    List<Future<?>> futures = new LinkedList<>();
    List<DataCopyPath> managedTableCopyPaths = new LinkedList<>();

    ThreadFactory namingThreadFactory =
        new ThreadFactoryBuilder().setNameFormat("partition-dump-thread-%d").build();
    ExecutorService consumer = Executors.newFixedThreadPool(nThreads, namingThreadFactory);

    for (Partition part : partitionIterable) {
      LOG.debug("scheduling partition dump {}", part.getName());
      Runnable r = () -> {
        String partitionName = part.getName();
        String threadName = Thread.currentThread().getName();
        LOG.debug("Thread: {}, start partition dump {}", threadName, partitionName);
        try {
          // Data Copy in case of ExportTask or when dataCopyAtLoad is true
          List<Path> dataPathList = Utils.getDataPathList(part.getDataLocation(),
              forReplicationSpec, hiveConf);
          Path rootDataDumpDir = isExportTask
              ? paths.partitionMetadataExportDir(partitionName) : paths.partitionDataExportDir(partitionName);
          new FileOperations(dataPathList, rootDataDumpDir, distCpDoAsUser, hiveConf, mmCtx)
              .export(isExportTask, dataCopyAtLoad);
          Path dataDumpDir = new Path(paths.dataExportRootDir(), partitionName);
          LOG.debug("Thread: {}, finish partition dump {}", threadName, partitionName);
          if (!(isExportTask || dataCopyAtLoad)) {
            fileList.add(new DataCopyPath(forReplicationSpec, part.getDataLocation(),
                dataDumpDir).convertToString());
          }
        } catch (Exception e) {
          throw new RuntimeException(e.getMessage(), e);
        }
      };
      futures.add(consumer.submit(r));
    }
    consumer.shutdown();
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (Exception e) {
        LOG.error("Partition dump thread failed", e.getCause());
        throw new HiveException(e.getCause().getMessage(), e.getCause());
      }
    }
    // may be drive this via configuration as well.
    consumer.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    return managedTableCopyPaths;
  }
}
