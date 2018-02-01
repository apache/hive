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
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.messaging.AddForeignKeyMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;

import java.util.ArrayList;
import java.util.List;

public class AddForeignKeyHandler extends AbstractConstraintEventHandler {
  AddForeignKeyHandler(NotificationEvent event) {
    super(event);
  }

  @Override
  public void handle(Context withinContext) throws Exception {
    LOG.debug("Processing#{} ADD_FOREIGNKEY_MESSAGE message : {}", fromEventId(),
        event.getMessage());
    if (shouldReplicate(withinContext)) {
      DumpMetaData dmd = withinContext.createDmd(this);

      AddForeignKeyMessage message = deserializer.getAddForeignKeyMessage(event.getMessage());
      List<SQLForeignKey> foreignKeys = message.getForeignKeys();
      ArrayList<SQLForeignKey> result = new ArrayList<>();
      for (SQLForeignKey fk : foreignKeys) {
        fk.setFktable_db(fk.getFktable_db().toLowerCase());
        fk.setFktable_name(fk.getFktable_name().toLowerCase());
        fk.setPktable_db(fk.getPktable_db().toLowerCase());
        fk.setPktable_name(fk.getPktable_name().toLowerCase());
        result.add(fk);
      }

      dmd.setPayload(MessageFactory.getInstance().buildAddForeignKeyMessage(result).toString());
      dmd.write();
    }
  }

  @Override
  public DumpType dumpType() {
    return DumpType.EVENT_ADD_FOREIGNKEY;
  }
}
