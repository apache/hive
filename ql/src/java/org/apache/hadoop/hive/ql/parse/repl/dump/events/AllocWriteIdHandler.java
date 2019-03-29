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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.AllocWriteIdMessage;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;

class AllocWriteIdHandler extends AbstractEventHandler<AllocWriteIdMessage> {
  AllocWriteIdHandler(NotificationEvent event) {
    super(event);
  }

  @Override
  AllocWriteIdMessage eventMessage(String stringRepresentation) {
    return deserializer.getAllocWriteIdMessage(stringRepresentation);
  }

  @Override
  public void handle(Context withinContext) throws Exception {
    LOG.info("Processing#{} ALLOC_WRITE_ID message : {}", fromEventId(), eventMessageAsJSON);

    // If we are bootstrapping ACID table during an incremental dump, the events corresponding to
    // these ACID tables are not dumped. Hence we do not need to allocate any writeId on the
    // target and hence we do not need to dump these events.
    if (withinContext.hiveConf.getBoolVar(HiveConf.ConfVars.REPL_BOOTSTRAP_ACID_TABLES)) {
      return;
    }

    // Also only for testing, we do not include ACID tables in the dump (and replicate) if config
    // says so.
    if (withinContext.hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST_REPL) &&
        !withinContext.hiveConf.getBoolVar(HiveConf.ConfVars.REPL_DUMP_INCLUDE_ACID_TABLES)) {
      return;
    }

    DumpMetaData dmd = withinContext.createDmd(this);
    dmd.setPayload(eventMessageAsJSON);
    dmd.write();
  }

  @Override
  public DumpType dumpType() {
    return DumpType.EVENT_ALLOC_WRITE_ID;
  }
}
