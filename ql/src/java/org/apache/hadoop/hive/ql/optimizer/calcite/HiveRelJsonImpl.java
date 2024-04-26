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

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writer implementation for rel nodes that produces an output in json that is easily
 * parseable back into rel nodes.
 */
public class HiveRelJsonImpl extends RelJsonWriter {

  protected static final Logger LOG = LoggerFactory.getLogger(HiveRelJsonImpl.class);

  //~ Constructors -------------------------------------------------------------

  public HiveRelJsonImpl() {
    super();

    // Upgrade to Calcite 1.23.0 to remove this
    try {
      final Field fieldRelJson = RelJsonWriter.class.getDeclaredField("relJson");
      fieldRelJson.setAccessible(true);
      fieldRelJson.set(this, new HiveRelJson(jsonBuilder));
    } catch (IllegalAccessException | NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  //~ Methods ------------------------------------------------------------------

  @Override
  protected void explain_(RelNode rel, List<Pair<String, Object>> values) {
    super.explain_(rel, values);
    Map<String, Object> map = (Map<String, Object>) relList.get(relList.size() - 1);

    if (rel.getInputs().isEmpty()) {
      map.put("rowType", relJson.toJson(rel.getRowType()));
      // We also include partition columns information
      RelOptHiveTable table = (RelOptHiveTable) rel.getTable();
      if (table == null) {
        return;
      }

      List<Object> list = jsonBuilder.list();
      list.addAll(table.getHiveTableMD().getPartColNames());
      if (!list.isEmpty()) {
        map.put("partitionColumns", list);
      }

      list = table.getVirtualCols()
          .stream()
          .map(VirtualColumn::getName)
          .collect(Collectors.toList());

      if (!list.isEmpty()) {
        map.put("virtualColumns", list);
      }
    }
  }

  @Override
  public String asString() {
    final Map<String, Object> map = jsonBuilder.map();
    map.put("rels", relList);

    return new HiveJsonBuilder().toJsonString(map);
  }

}
