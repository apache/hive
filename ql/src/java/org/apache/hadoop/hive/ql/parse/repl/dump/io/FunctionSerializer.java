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
package org.apache.hadoop.hive.ql.parse.repl.dump.io;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.PathBuilder;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FunctionSerializer implements JsonWriter.Serializer {
  public static final String FIELD_NAME = "function";
  private Function function;
  private HiveConf hiveConf;
  private Path functionDataRoot;
  private boolean copyAtLoad;
  private List<EximUtil.DataCopyPath> functionBinaryCopyPaths = new ArrayList<>();

  public FunctionSerializer(Function function, Path functionDataRoot, boolean copyAtLoad, HiveConf hiveConf) {
    this.hiveConf = hiveConf;
    this.function = function;
    this.functionDataRoot = functionDataRoot;
    this.copyAtLoad = copyAtLoad;
  }

  @Override
  public void writeTo(JsonWriter writer, ReplicationSpec additionalPropertiesProvider)
      throws SemanticException, IOException, MetaException {

    List<ResourceUri> resourceUris = new ArrayList<>();
    if (function.getResourceUris() != null) {
      for (ResourceUri uri : function.getResourceUris()) {
        Path inputPath = new Path(uri.getUri());
        if ("hdfs".equals(inputPath.toUri().getScheme())) {
          FileSystem fileSystem = inputPath.getFileSystem(hiveConf);
          Path qualifiedUri = PathBuilder.fullyQualifiedHDFSUri(inputPath, fileSystem);
          String checkSum = ReplChangeManager.checksumFor(qualifiedUri, fileSystem);
          String encodedSrcUri = ReplChangeManager.getInstance(hiveConf)
                  .encodeFileUri(qualifiedUri.toString(), checkSum, null);
          if (copyAtLoad) {
            if (hiveConf.getBoolVar(HiveConf.ConfVars.REPL_HA_DATAPATH_REPLACE_REMOTE_NAMESERVICE)) {
              encodedSrcUri = Utils.replaceNameserviceInEncodedURI(encodedSrcUri, hiveConf);
            }
            resourceUris.add(new ResourceUri(uri.getResourceType(), encodedSrcUri));
          } else {
            Path newBinaryPath = new Path(functionDataRoot, qualifiedUri.getName());
            resourceUris.add(new ResourceUri(uri.getResourceType(),newBinaryPath.toString()));
            functionBinaryCopyPaths.add(new EximUtil.DataCopyPath(additionalPropertiesProvider,
                    new Path(encodedSrcUri), newBinaryPath));
          }
        } else {
          resourceUris.add(uri);
        }
      }
    }
    Function copyObj = new Function(this.function);
    if (!resourceUris.isEmpty()) {
      assert resourceUris.size() == this.function.getResourceUris().size();
      copyObj.setResourceUris(resourceUris);
    }

    try {
      TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
      //This is required otherwise correct work object on repl load wont be created.
      writer.jsonGenerator.writeStringField(ReplicationSpec.KEY.REPL_SCOPE.toString(),
          "all");
      writer.jsonGenerator.writeStringField(ReplicationSpec.KEY.CURR_STATE_ID_SOURCE.toString(),
          additionalPropertiesProvider.getCurrentReplicationState());
      writer.jsonGenerator
          .writeStringField(FIELD_NAME, serializer.toString(copyObj));
    } catch (TException e) {
      throw new SemanticException(ErrorMsg.ERROR_SERIALIZE_METASTORE.getMsg(), e);
    }
  }

  public List<EximUtil.DataCopyPath> getFunctionBinaryCopyPaths() {
    return functionBinaryCopyPaths;
  }
}
