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

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;

import java.io.IOException;
import java.util.Map;

public class TableSerializer implements JsonWriter.Serializer {
  public static final String FIELD_NAME = "table";
  private final org.apache.hadoop.hive.ql.metadata.Table tableHandle;
  private final Iterable<Partition> partitions;

  public TableSerializer(org.apache.hadoop.hive.ql.metadata.Table tableHandle,
      Iterable<Partition> partitions) {
    this.tableHandle = tableHandle;
    this.partitions = partitions;
  }

  @Override
  public void writeTo(JsonWriter writer, ReplicationSpec additionalPropertiesProvider)
      throws SemanticException, IOException {
    if (cannotReplicateTable(additionalPropertiesProvider)) {
      return;
    }

    Table tTable = tableHandle.getTTable();
    tTable = addPropertiesToTable(tTable, additionalPropertiesProvider);
    try {
      TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
      writer.jsonGenerator
          .writeStringField(FIELD_NAME, serializer.toString(tTable, UTF_8));
      writer.jsonGenerator.writeFieldName(PartitionSerializer.FIELD_NAME);
      writePartitions(writer, additionalPropertiesProvider);
    } catch (TException e) {
      throw new SemanticException(ErrorMsg.ERROR_SERIALIZE_METASTORE.getMsg(), e);
    }
  }

  private boolean cannotReplicateTable(ReplicationSpec additionalPropertiesProvider) {
    return tableHandle == null || additionalPropertiesProvider.isNoop();
  }

  private Table addPropertiesToTable(Table table, ReplicationSpec additionalPropertiesProvider)
      throws SemanticException, IOException {
    if (additionalPropertiesProvider.isInReplicationScope()) {
      table.putToParameters(
            ReplicationSpec.KEY.CURR_STATE_ID.toString(),
            additionalPropertiesProvider.getCurrentReplicationState());
      if (isExternalTable(table)) {
          // Replication destination will not be external - override if set
        table.putToParameters("EXTERNAL", "FALSE");
        }
      if (isExternalTableType(table)) {
          // Replication dest will not be external - override if set
        table.setTableType(TableType.MANAGED_TABLE.toString());
        }
    } else {
      // ReplicationSpec.KEY scopeKey = ReplicationSpec.KEY.REPL_SCOPE;
      // write(out, ",\""+ scopeKey.toString() +"\":\"" + replicationSpec.get(scopeKey) + "\"");
      // TODO: if we want to be explicit about this dump not being a replication dump, we can
      // uncomment this else section, but currently unnneeded. Will require a lot of golden file
      // regen if we do so.
    }
    return table;
  }

  private boolean isExternalTableType(org.apache.hadoop.hive.metastore.api.Table table) {
    return table.isSetTableType()
        && table.getTableType().equalsIgnoreCase(TableType.EXTERNAL_TABLE.toString());
  }

  private boolean isExternalTable(org.apache.hadoop.hive.metastore.api.Table table) {
    Map<String, String> params = table.getParameters();
    return params.containsKey("EXTERNAL")
        && params.get("EXTERNAL").equalsIgnoreCase("TRUE");
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
