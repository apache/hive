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

import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.OpenTxnMessage;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;

class OpenTxnHandler extends AbstractEventHandler<OpenTxnMessage> {

  OpenTxnHandler(NotificationEvent event) {
    super(event);
  }

  @Override
  OpenTxnMessage eventMessage(String stringRepresentation) {
    return deserializer.getOpenTxnMessage(stringRepresentation);
  }

  @Override
  public void handle(Context withinContext) throws Exception {
    if (!ReplUtils.includeAcidTableInDump(withinContext.hiveConf)) {
      return;
    }

    if (ReplUtils.filterTransactionOperations(withinContext.hiveConf)) {
      return;
    }

    LOG.info("Processing#{} OPEN_TXN message : {}", fromEventId(), eventMessageAsJSON);
    DumpMetaData dmd = withinContext.createDmd(this);
    dmd.setPayload(eventMessageAsJSON);
    dmd.write();
  }

  @Override
  public DumpType dumpType() {
    return DumpType.EVENT_OPEN_TXN;
  }
}
