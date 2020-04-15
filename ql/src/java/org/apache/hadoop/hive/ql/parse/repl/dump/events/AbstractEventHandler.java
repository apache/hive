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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.MessageEncoder;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageEncoder;
import org.apache.hadoop.hive.ql.metadata.HiveFatalException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.repl.CopyUtils;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

abstract class AbstractEventHandler<T extends EventMessage> implements EventHandler {
  static final Logger LOG = LoggerFactory.getLogger(AbstractEventHandler.class);
  static final MessageEncoder jsonMessageEncoder = JSONMessageEncoder.getInstance();

  final NotificationEvent event;
  final MessageDeserializer deserializer;
  final String eventMessageAsJSON;
  final T eventMessage;

  AbstractEventHandler(NotificationEvent event) {
    this.event = event;
    try {
      deserializer = MessageFactory.getInstance(event.getMessageFormat()).getDeserializer();
    } catch (Exception e) {
      String message =
          "could not create appropriate messageFactory for format " + event.getMessageFormat();
      LOG.error(message, e);
      throw new IllegalStateException(message, e);
    }
    eventMessage = eventMessage(event.getMessage());
    eventMessageAsJSON = eventMessageAsJSON(eventMessage);
  }

  /**
   * This takes in the string representation of the message in the format as specified in rdbms backing metastore.
   */
  abstract T eventMessage(String stringRepresentation);

  private String eventMessageAsJSON(T eventMessage) {
    if (eventMessage == null) {
      // this will only happen in case DefaultHandler is invoked
      return null;
    }
    return jsonMessageEncoder.getSerializer().serialize(eventMessage);
  }

  @Override
  public long fromEventId() {
    return event.getEventId();
  }

  @Override
  public long toEventId() {
    return event.getEventId();
  }

  protected void writeFileEntry(Table table, Partition ptn, String file, Context withinContext)
          throws IOException, LoginException, MetaException, HiveFatalException {
    HiveConf hiveConf = withinContext.hiveConf;
    String distCpDoAsUser = hiveConf.getVar(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER);
    if (!Utils.shouldDumpMetaDataOnly(withinContext.hiveConf)) {
      Path dataPath = new Path(withinContext.eventRoot, EximUtil.DATA_PATH_NAME);
      if (table.isPartitioned()) {
        dataPath = new Path(dataPath, ptn.getName());
      }
      String[] decodedURISplits = ReplChangeManager.decodeFileUri(file);
      Path srcDataPath = new Path(decodedURISplits[0]);
      if (dataPath.toUri().getScheme() == null) {
        dataPath = new Path(srcDataPath.toUri().getScheme(), srcDataPath.toUri().getAuthority(), dataPath.toString());
      }
      List<ReplChangeManager.FileInfo> filePaths = new ArrayList<>();
      ReplChangeManager.FileInfo fileInfo = ReplChangeManager.getFileInfo(new Path(decodedURISplits[0]),
                  decodedURISplits[1], decodedURISplits[2], decodedURISplits[3], hiveConf);
      filePaths.add(fileInfo);
      FileSystem dstFs = dataPath.getFileSystem(hiveConf);
      CopyUtils copyUtils = new CopyUtils(distCpDoAsUser, hiveConf, dstFs);
      copyUtils.copyAndVerify(dataPath, filePaths, srcDataPath);
      copyUtils.renameFileCopiedFromCmPath(dataPath, dstFs, filePaths);
    }
  }
}
