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

import com.google.common.collect.Collections2;
import org.apache.hadoop.hive.metastore.api.GetAllWriteEventInfoRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.WriteEventInfo;
import org.apache.hadoop.hive.metastore.messaging.AbortTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAbortTxnMessage;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;

import java.util.ArrayList;
import java.util.List;

class AbortTxnHandler extends AbstractEventHandler<AbortTxnMessage> {

  AbortTxnHandler(NotificationEvent event) {
    super(event);
  }

  @Override
  AbortTxnMessage eventMessage(String stringRepresentation) {
    return deserializer.getAbortTxnMessage(stringRepresentation);
  }

  @Override
  public void handle(Context withinContext) throws Exception {
    if (!ReplUtils.includeAcidTableInDump(withinContext.hiveConf)) {
      return;
    }

    if (ReplUtils.filterTransactionOperations(withinContext.hiveConf)) {
      String contextDbName = StringUtils.normalizeIdentifier(withinContext.replScope.getDbName());
      JSONAbortTxnMessage abortMsg = (JSONAbortTxnMessage)eventMessage;
      if ((abortMsg.getDbsUpdated() == null) || !abortMsg.getDbsUpdated().contains(contextDbName)) {
        LOG.info("Filter out #{} ABORT_TXN message : {}", fromEventId(), eventMessageAsJSON);
        return;
      }
    }

    LOG.info("Processing#{} ABORT_TXN message : {}", fromEventId(), eventMessageAsJSON);
    DumpMetaData dmd = withinContext.createDmd(this);
    dmd.setPayload(eventMessageAsJSON);
    dmd.write();
  }

  @Override
  public DumpType dumpType() {
    return DumpType.EVENT_ABORT_TXN;
  }
}
