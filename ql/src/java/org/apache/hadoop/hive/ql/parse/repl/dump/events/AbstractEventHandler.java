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
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.repl.CopyUtils;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginException;
import java.io.BufferedWriter;
import java.io.File;
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

  protected void writeFileEntry(String dbName, Table table, String file, BufferedWriter fileListWriter,
                                Context withinContext)
          throws IOException, LoginException, MetaException, HiveFatalException {
    HiveConf hiveConf = withinContext.hiveConf;
    String distCpDoAsUser = hiveConf.getVar(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER);
    if (!Utils.shouldDumpMetaDataOnly(table, withinContext.hiveConf)) {
      Path dataPath = new Path(withinContext.dumpRoot.toString(), EximUtil.DATA_PATH_NAME);
      List<ReplChangeManager.FileInfo> filePaths = new ArrayList<>();
      String[] decodedURISplits = ReplChangeManager.decodeFileUri(file);
      String srcDataFile = decodedURISplits[0];
      Path srcDataPath = new Path(srcDataFile);
      if (dataPath.toUri().getScheme() == null) {
        dataPath = new Path(srcDataPath.toUri().getScheme(), srcDataPath.toUri().getAuthority(), dataPath.toString());
      }
      String eventTblPath = event.getEventId() + File.separator + dbName + File.separator + table.getTableName();
      String srcDataFileRelativePath = null;
      if (srcDataFile.contains(table.getPath().toString())) {
        srcDataFileRelativePath = srcDataFile.substring(table.getPath().toString().length() + 1);
      } else if (decodedURISplits[3] == null) {
        srcDataFileRelativePath = srcDataPath.getName();
      } else {
        srcDataFileRelativePath = srcDataFileRelativePath + File.separator + srcDataPath.getName();
      }
      Path targetPath = new Path(dataPath, eventTblPath + File.separator + srcDataFileRelativePath);
      String encodedTargetPath = ReplChangeManager.encodeFileUri(
              targetPath.toString(), decodedURISplits[1], decodedURISplits[3]);
      ReplChangeManager.FileInfo f = ReplChangeManager.getFileInfo(new Path(decodedURISplits[0]),
                  decodedURISplits[1], decodedURISplits[2], decodedURISplits[3], hiveConf);
      filePaths.add(f);
      FileSystem dstFs = targetPath.getFileSystem(hiveConf);
      Path finalTargetPath = targetPath.getParent();
      if (decodedURISplits[3] != null) {
        finalTargetPath = finalTargetPath.getParent();
      }
      new CopyUtils(distCpDoAsUser, hiveConf, dstFs).copyAndVerify(finalTargetPath, filePaths, srcDataPath);
      fileListWriter.write(encodedTargetPath + "\n");
    }
  }
}
