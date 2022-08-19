/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.CreateFunctionMessage;
import org.apache.hadoop.hive.ql.metadata.HiveFatalException;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.repl.CopyUtils;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.FunctionSerializer;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.JsonWriter;

import org.apache.hadoop.hive.ql.parse.EximUtil.DataCopyPath;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class CreateFunctionHandler extends AbstractEventHandler<CreateFunctionMessage> {
  CreateFunctionHandler(NotificationEvent event) {
    super(event);
  }

  @Override
  CreateFunctionMessage eventMessage(String stringRepresentation) {
    return deserializer.getCreateFunctionMessage(stringRepresentation);
  }

  @Override
  public void handle(Context withinContext) throws Exception {
    Function functionObj = eventMessage.getFunctionObj();
    if (functionObj.getResourceUris() == null || functionObj.getResourceUris().isEmpty()) {
      LOG.info("Not replicating function: " + functionObj.getFunctionName() + " as it seems to have been created "
              + "without USING clause");
      return;
    }
    LOG.info("Processing#{} CREATE_FUNCTION message : {}", fromEventId(), eventMessageAsJSON);
    Path metadataPath = new Path(withinContext.eventRoot, EximUtil.METADATA_NAME);
    Path dataPath = new Path(withinContext.eventRoot, EximUtil.DATA_PATH_NAME);
    FileSystem fileSystem = metadataPath.getFileSystem(withinContext.hiveConf);
    boolean copyAtLoad = withinContext.hiveConf.getBoolVar(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET);
    List<DataCopyPath> functionBinaryCopyPaths = new ArrayList<>();
    try (JsonWriter jsonWriter = new JsonWriter(fileSystem, metadataPath)) {
      FunctionSerializer serializer = new FunctionSerializer(functionObj,
              dataPath, copyAtLoad, withinContext.hiveConf);
      serializer.writeTo(jsonWriter, withinContext.replicationSpec);
      functionBinaryCopyPaths.addAll(serializer.getFunctionBinaryCopyPaths());
    }
    withinContext.createDmd(this).write();
    copyFunctionBinaries(functionBinaryCopyPaths, withinContext.hiveConf);
  }

  private void copyFunctionBinaries(List<DataCopyPath> functionBinaryCopyPaths, HiveConf hiveConf)
          throws IOException, LoginException, HiveFatalException {
    if (!functionBinaryCopyPaths.isEmpty()) {
      String distCpDoAsUser = hiveConf.getVar(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER);
      List<ReplChangeManager.FileInfo> filePaths = new ArrayList<>();
      for (DataCopyPath funcBinCopyPath : functionBinaryCopyPaths) {
        String [] decodedURISplits = ReplChangeManager.decodeFileUri(funcBinCopyPath.getSrcPath().toString());
        ReplChangeManager.FileInfo fileInfo = ReplChangeManager.getFileInfo(new Path(decodedURISplits[0]),
                decodedURISplits[1], decodedURISplits[2], decodedURISplits[3], hiveConf);
        filePaths.add(fileInfo);
        Path destRoot = funcBinCopyPath.getTargetPath().getParent();
        FileSystem dstFs = destRoot.getFileSystem(hiveConf);
        CopyUtils copyUtils = new CopyUtils(distCpDoAsUser, hiveConf, dstFs);
        copyUtils.copyAndVerify(destRoot, filePaths, funcBinCopyPath.getSrcPath(), true, false);
        copyUtils.renameFileCopiedFromCmPath(destRoot, dstFs, filePaths);
      }
    }
  }

  @Override
  public DumpType dumpType() {
    return DumpType.EVENT_CREATE_FUNCTION;
  }
}
