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

package org.apache.hadoop.hive.ql.ddl.process;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Operation process of showing compactions.
 */
public class ShowCompactionsOperation extends DDLOperation<ShowCompactionsDesc> {
  public ShowCompactionsOperation(DDLOperationContext context, ShowCompactionsDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    SessionState sessionState = SessionState.get();
    // Call the metastore to get the status of all known compactions (completed get purged eventually)
    ShowCompactResponse rsp = context.getDb().showCompactions();

    // Write the results into the file
    try (DataOutputStream os = DDLUtils.getOutputStream(new Path(desc.getResFile()), context)) {
      // Write a header for cliDriver
      if (!sessionState.isHiveServerQuery()) {
        writeHeader(os);
      }

      if (rsp.getCompacts() != null) {
        for (ShowCompactResponseElement e : rsp.getCompacts()) {
          writeRow(os, e);
        }
      }
    } catch (IOException e) {
      LOG.warn("show compactions: ", e);
      return 1;
    }
    return 0;
  }

  private void writeHeader(DataOutputStream os) throws IOException {
    os.writeBytes("CompactionId");
    os.write(Utilities.tabCode);
    os.writeBytes("Database");
    os.write(Utilities.tabCode);
    os.writeBytes("Table");
    os.write(Utilities.tabCode);
    os.writeBytes("Partition");
    os.write(Utilities.tabCode);
    os.writeBytes("Type");
    os.write(Utilities.tabCode);
    os.writeBytes("State");
    os.write(Utilities.tabCode);
    os.writeBytes("Hostname");
    os.write(Utilities.tabCode);
    os.writeBytes("Worker");
    os.write(Utilities.tabCode);
    os.writeBytes("Start Time");
    os.write(Utilities.tabCode);
    os.writeBytes("Duration(ms)");
    os.write(Utilities.tabCode);
    os.writeBytes("HadoopJobId");
    os.write(Utilities.newLineCode);
  }

  private static final String NO_VAL = " --- ";

  private void writeRow(DataOutputStream os, ShowCompactResponseElement e) throws IOException {
    os.writeBytes(Long.toString(e.getId()));
    os.write(Utilities.tabCode);
    os.writeBytes(e.getDbname());
    os.write(Utilities.tabCode);
    os.writeBytes(e.getTablename());
    os.write(Utilities.tabCode);
    String part = e.getPartitionname();
    os.writeBytes(part == null ? NO_VAL : part);
    os.write(Utilities.tabCode);
    os.writeBytes(e.getType().toString());
    os.write(Utilities.tabCode);
    os.writeBytes(e.getState());
    os.write(Utilities.tabCode);
    String wid = e.getWorkerid();
    os.writeBytes(wid == null ? NO_VAL : wid.split("-")[0]);
    os.write(Utilities.tabCode);
    os.writeBytes(wid == null ? NO_VAL : wid.split("-")[1]);
    os.write(Utilities.tabCode);
    os.writeBytes(e.isSetStart() ? Long.toString(e.getStart()) : NO_VAL);
    os.write(Utilities.tabCode);
    os.writeBytes(e.isSetEndTime() ? Long.toString(e.getEndTime() - e.getStart()) : NO_VAL);
    os.write(Utilities.tabCode);
    os.writeBytes(e.isSetHadoopJobId() ?  e.getHadoopJobId() : NO_VAL);
    os.write(Utilities.newLineCode);
  }
}
