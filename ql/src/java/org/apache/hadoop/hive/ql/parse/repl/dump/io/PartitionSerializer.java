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

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;

import java.io.IOException;
import java.util.Map;

public class PartitionSerializer implements JsonWriter.Serializer {
  public static final String FIELD_NAME="partitions";
  private Partition partition;

  PartitionSerializer(Partition partition) {
    this.partition = partition;
  }

  @Override
  public void writeTo(JsonWriter writer, ReplicationSpec additionalPropertiesProvider)
      throws SemanticException, IOException {
    TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
    try {
      // Remove all the entries from the parameters which are added by repl tasks internally.
      Map<String, String> parameters = partition.getParameters();
      if (parameters != null) {
        parameters.entrySet()
                .removeIf(e -> e.getKey().equals(ReplUtils.REPL_CHECKPOINT_KEY));
      }

      if (additionalPropertiesProvider.isInReplicationScope()) {
        // Current replication state must be set on the Partition object only for bootstrap dump.
        // Event replication State will be null in case of bootstrap dump.
        if (additionalPropertiesProvider.getReplSpecType()
                != ReplicationSpec.Type.INCREMENTAL_DUMP) {
          partition.putToParameters(
                  ReplicationSpec.KEY.CURR_STATE_ID.toString(),
                  additionalPropertiesProvider.getCurrentReplicationState());
        }
        if (isPartitionExternal()) {
          // Replication destination will not be external
          partition.putToParameters("EXTERNAL", "FALSE");
        }
      }
      writer.jsonGenerator.writeString(serializer.toString(partition, UTF_8));
      writer.jsonGenerator.flush();
    } catch (TException e) {
      throw new SemanticException(ErrorMsg.ERROR_SERIALIZE_METASTORE.getMsg(), e);
    }
  }

  private boolean isPartitionExternal() {
    Map<String, String> params = partition.getParameters();
    return params.containsKey("EXTERNAL")
        && params.get("EXTERNAL").equalsIgnoreCase("TRUE");
  }
}
