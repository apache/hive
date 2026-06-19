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

import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class TableSerializer implements JsonWriter.Serializer {
  public static final String FIELD_NAME = "table";
  private static final Logger LOG = LoggerFactory.getLogger(TableSerializer.class);

  private final org.apache.hadoop.hive.ql.metadata.Table tableHandle;
  private final Iterable<Partition> partitions;
  private final HiveConf hiveConf;

  public TableSerializer(org.apache.hadoop.hive.ql.metadata.Table tableHandle,
      Iterable<Partition> partitions, HiveConf hiveConf) {
    this.tableHandle = tableHandle;
    this.partitions = partitions;
    this.hiveConf = hiveConf;
  }

  @Override
  public void writeTo(JsonWriter writer, ReplicationSpec additionalPropertiesProvider)
      throws SemanticException, IOException {
    if (!Utils.shouldReplicate(additionalPropertiesProvider, tableHandle,
            false, null, null, hiveConf)) {
      return;
    }

    Table tTable = updatePropertiesInTable(
        tableHandle.getTTable(), additionalPropertiesProvider
    );
    try {
      TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
      writer.jsonGenerator
          .writeStringField(FIELD_NAME, serializer.toString(tTable));
      writer.jsonGenerator.writeFieldName(PartitionSerializer.FIELD_NAME);
      writePartitions(writer, additionalPropertiesProvider);
    } catch (TException e) {
      throw new SemanticException(ErrorMsg.ERROR_SERIALIZE_METASTORE.getMsg(), e);
    }
  }

  private Table updatePropertiesInTable(Table table, ReplicationSpec additionalPropertiesProvider) {
    // Remove all the entries from the parameters which are added by repl tasks internally.
    Map<String, String> parameters = table.getParameters();
    if (parameters != null) {
      parameters.entrySet()
              .removeIf(e -> (e.getKey().equals(ReplConst.REPL_TARGET_DB_PROPERTY) ||
                      e.getKey().equals(ReplConst.REPL_FIRST_INC_PENDING_FLAG)));
    }

    if (additionalPropertiesProvider.isInReplicationScope()) {
      // Current replication state must be set on the Table object only for bootstrap dump.
      // Event replication State will be null in case of bootstrap dump.
      if (additionalPropertiesProvider.getReplSpecType()
              != ReplicationSpec.Type.INCREMENTAL_DUMP) {
        table.putToParameters(
                ReplicationSpec.KEY.CURR_STATE_ID_SOURCE.toString(),
                additionalPropertiesProvider.getCurrentReplicationState());
      }
    } else {
      // ReplicationSpec.KEY scopeKey = ReplicationSpec.KEY.REPL_SCOPE;
      // write(out, ",\""+ scopeKey.toString() +"\":\"" + replicationSpec.get(scopeKey) + "\"");
      // TODO: if we want to be explicit about this dump not being a replication dump, we can
      // uncomment this else section, but currently unneeded. Will require a lot of golden file
      // regen if we do so.
    }
    return table;
  }

  private void writePartitions(JsonWriter writer, ReplicationSpec additionalPropertiesProvider)
      throws SemanticException, IOException {
    writer.jsonGenerator.writeStartArray();
    if (partitions != null) {
      for (org.apache.hadoop.hive.ql.metadata.Partition partition : partitions) {
        new PartitionSerializer(partition.getTPartition())
            .writeTo(writer, additionalPropertiesProvider);
      }
    }
    writer.jsonGenerator.writeEndArray();
  }
}
