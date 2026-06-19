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

package org.apache.hadoop.hive.ql.ddl.process.show.transactions;

import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.TxnInfo;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Operation process of showing transactions.
 */
public class ShowTransactionsOperation extends DDLOperation<ShowTransactionsDesc> {
  public ShowTransactionsOperation(DDLOperationContext context, ShowTransactionsDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    SessionState sessionState = SessionState.get();
    // Call the metastore to get the currently queued and running compactions.
    GetOpenTxnsInfoResponse rsp = context.getDb().showTransactions();

    // Write the results into the file
    try (DataOutputStream os = ShowUtils.getOutputStream(new Path(desc.getResFile()), context)) {
      if (!sessionState.isHiveServerQuery()) {
        writeHeader(os);
      }

      for (TxnInfo txn : rsp.getOpen_txns()) {
        writeRow(os, txn);
      }
    } catch (IOException e) {
      LOG.warn("show transactions: ", e);
      return 1;
    }

    return 0;
  }

  private void writeHeader(DataOutputStream os) throws IOException {
    os.writeBytes("Transaction ID");
    os.write(Utilities.tabCode);
    os.writeBytes("Transaction State");
    os.write(Utilities.tabCode);
    os.writeBytes("Started Time");
    os.write(Utilities.tabCode);
    os.writeBytes("Last Heartbeat Time");
    os.write(Utilities.tabCode);
    os.writeBytes("User");
    os.write(Utilities.tabCode);
    os.writeBytes("Hostname");
    os.write(Utilities.newLineCode);
  }

  private void writeRow(DataOutputStream os, TxnInfo txn) throws IOException {
    os.writeBytes(Long.toString(txn.getId()));
    os.write(Utilities.tabCode);
    os.writeBytes(txn.getState().toString());
    os.write(Utilities.tabCode);
    os.writeBytes(Long.toString(txn.getStartedTime()));
    os.write(Utilities.tabCode);
    os.writeBytes(Long.toString(txn.getLastHeartbeatTime()));
    os.write(Utilities.tabCode);
    os.writeBytes(txn.getUser());
    os.write(Utilities.tabCode);
    os.writeBytes(txn.getHostname());
    os.write(Utilities.newLineCode);
  }
}
