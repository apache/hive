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
package org.apache.hadoop.hive.ql.parse.repl.dump.events;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.AddPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.PartitionFiles;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.EximUtil;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Iterator;

import org.apache.hadoop.hive.ql.parse.repl.DumpType;

class AddPartitionHandler extends AbstractEventHandler {
  protected AddPartitionHandler(NotificationEvent notificationEvent) {
    super(notificationEvent);
  }

  @Override
  public void handle(Context withinContext) throws Exception {
    AddPartitionMessage apm = deserializer.getAddPartitionMessage(event.getMessage());
    LOG.info("Processing#{} ADD_PARTITION message : {}", fromEventId(), event.getMessage());
    Iterable<org.apache.hadoop.hive.metastore.api.Partition> ptns = apm.getPartitionObjs();
    if ((ptns == null) || (!ptns.iterator().hasNext())) {
      LOG.debug("Event#{} was an ADD_PTN_EVENT with no partitions");
      return;
    }
    org.apache.hadoop.hive.metastore.api.Table tobj = apm.getTableObj();
    if (tobj == null) {
      LOG.debug("Event#{} was a ADD_PTN_EVENT with no table listed");
      return;
    }

    final Table qlMdTable = new Table(tobj);
    Iterable<Partition> qlPtns = Iterables.transform(
        ptns,
        new Function<org.apache.hadoop.hive.metastore.api.Partition, Partition>() {
          @Nullable
          @Override
          public Partition apply(@Nullable org.apache.hadoop.hive.metastore.api.Partition input) {
            if (input == null) {
              return null;
            }
            try {
              return new Partition(qlMdTable, input);
            } catch (HiveException e) {
              throw new IllegalArgumentException(e);
            }
          }
        }
    );

    Path metaDataPath = new Path(withinContext.eventRoot, EximUtil.METADATA_NAME);
    EximUtil.createExportDump(
        metaDataPath.getFileSystem(withinContext.hiveConf),
        metaDataPath,
        qlMdTable,
        qlPtns,
        withinContext.replicationSpec);

    Iterator<PartitionFiles> partitionFilesIter = apm.getPartitionFilesIter().iterator();
    for (Partition qlPtn : qlPtns) {
      Iterable<String> files = partitionFilesIter.next().getFiles();
      if (files != null) {
        // encoded filename/checksum of files, write into _files
        try (BufferedWriter fileListWriter = writer(withinContext, qlPtn)) {
          for (String file : files) {
            fileListWriter.write(file);
            fileListWriter.newLine();
          }
        }
      }
    }
    withinContext.createDmd(this).write();
  }

  private BufferedWriter writer(Context withinContext, Partition qlPtn)
      throws IOException {
    Path ptnDataPath = new Path(withinContext.eventRoot, qlPtn.getName());
    FileSystem fs = ptnDataPath.getFileSystem(withinContext.hiveConf);
    Path filesPath = new Path(ptnDataPath, EximUtil.FILES_NAME);
    return new BufferedWriter(new OutputStreamWriter(fs.create(filesPath)));
  }

  @Override
  public DumpType dumpType() {
    return DumpType.EVENT_ADD_PARTITION;
  }
}
