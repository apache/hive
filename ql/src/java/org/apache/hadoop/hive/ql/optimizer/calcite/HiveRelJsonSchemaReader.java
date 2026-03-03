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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.TableConstraintsInfo;
import org.apache.hadoop.hive.ql.parse.QueryTables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reads a JSON plan and converts it to a RelOptSchema by finding all tables
 * that are referenced in the plan. The schema is created exclusively from the
 * JSON input so any information that is not there will not be available.
 */
@InterfaceStability.Evolving
public final class HiveRelJsonSchemaReader {

  private HiveRelJsonSchemaReader() {
    throw new IllegalStateException("Utility class");
  }
  /**
   * Reads the schema from the JSON input using the specified configuration and type factory.
   */
  public static RelOptSchema read(String jsonInput, HiveConf conf, RelDataTypeFactory typeFactory) throws IOException {
    JsonNode node = new ObjectMapper().readTree(jsonInput);
    Map<List<String>, TableInfo> tables = new HashMap<>();
    for (JsonNode scan : node.findParents("table")) {
      List<String> names = new ArrayList<>();
      for (JsonNode n : scan.get("table")) {
        names.add(n.asText());
      }
      RelDataType type = readType(typeFactory, scan.get("rowType"));
      JsonNode rowNode = scan.get("rowCount");
      double rowCount = rowNode != null ? rowNode.asDouble() : 100.0;
      tables.put(names, new TableInfo(type, rowCount));
    }
    return new MapRelOptSchema(conf, typeFactory, tables);
  }

  private static RelDataType readType(RelDataTypeFactory typeFactory, JsonNode typeNode) throws IOException {
    ObjectMapper typeMapper = new ObjectMapper();
    Object value;
    if (typeNode.getNodeType() == JsonNodeType.OBJECT) {
      value = typeMapper.treeToValue(typeNode, Map.class);
    } else if (typeNode.getNodeType() == JsonNodeType.ARRAY) {
      value = typeMapper.treeToValue(typeNode, List.class);
    } else {
      throw new IllegalStateException();
    }
    return RelJson.create().toType(typeFactory, value);
  }

  private record TableInfo(RelDataType rowType, double rowCount) {
  }

  private static final class MapRelOptSchema implements RelOptSchema {
    private final HiveConf conf;
    private final RelDataTypeFactory typeFactory;
    private final Map<List<String>, TableInfo> tables;

    MapRelOptSchema(HiveConf conf, RelDataTypeFactory typeFactory, Map<List<String>, TableInfo> tables) {
      this.conf = conf;
      this.typeFactory = typeFactory;
      this.tables = tables;
    }

    @Override
    public RelOptTable getTableForMember(List<String> names) {
      TableInfo tableInfo = tables.get(names);
      if (tableInfo == null) {
        return null;
      }
      org.apache.hadoop.hive.metastore.api.Table mTable = new org.apache.hadoop.hive.metastore.api.Table();
      mTable.setDbName(names.get(0));
      mTable.setTableName(names.get(1));
      Table metadataTable = new Table(mTable);
      // Set info constraints as empty since we can't extract anything from JSON.
      // and we want to avoid lookups in the metastore that will anyways fail
      // since tables are not expected to exist.
      metadataTable.setTableConstraintsInfo(new TableConstraintsInfo());
      RelOptHiveTable optTable = new RelOptHiveTable(
          this,
          typeFactory,
          names,
          tableInfo.rowType,
          metadataTable,
          new ArrayList<>(),
          new ArrayList<>(),
          new ArrayList<>(),
          conf,
          new QueryTables(true),
          new HashMap<>(),
          new HashMap<>(),
          new AtomicInteger());
      optTable.setRowCount(tableInfo.rowCount);
      return optTable;
    }

    @Override
    public RelDataTypeFactory getTypeFactory() {
      return typeFactory;
    }

    @Override
    public void registerRules(RelOptPlanner planner) {
      // No need to register any rules in the planner
    }
  }
}
