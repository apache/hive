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

package org.apache.hadoop.hive.ql.ddl.table.info.show.tables;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.ddl.ShowUtils.TextMetaDataTable;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.formatting.MapBuilder;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatUtils;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Formats SHOW TABLES results.
 */
public abstract class ShowTablesFormatter {
  public static ShowTablesFormatter getFormatter(HiveConf conf) {
    if (MetaDataFormatUtils.isJson(conf)) {
      return new JsonShowTablesFormatter();
    } else {
      return new TextShowTablesFormatter();
    }
  }

  public abstract void showTables(DataOutputStream out, List<String> tables) throws HiveException;

  abstract void showTablesExtended(DataOutputStream out, List<Table> tables) throws HiveException;

  // ------ Implementations ------

  static class JsonShowTablesFormatter extends ShowTablesFormatter {
    @Override
    public void showTables(DataOutputStream out, List<String> tables) throws HiveException {
      ShowUtils.asJson(out, MapBuilder.create().put("tables", tables).build());
    }

    @Override
    void showTablesExtended(DataOutputStream out, List<Table> tables) throws HiveException {
      if (tables.isEmpty()) {
        return;
      }

      List<Map<String, Object>> tableDataList = new ArrayList<>();
      for (Table table : tables) {
        Map<String, Object> tableData = ImmutableMap.of(
            "Table Name", table.getTableName(),
            "Table Type", table.getTableType().toString());
        tableDataList.add(tableData);
      }

      ShowUtils.asJson(out, ImmutableMap.of("tables", tableDataList));
    }
  }

  static class TextShowTablesFormatter extends ShowTablesFormatter {
    @Override
    public void showTables(DataOutputStream out, List<String> tables) throws HiveException {
      Iterator<String> iterTbls = tables.iterator();

      try {
        while (iterTbls.hasNext()) {
          // create a row per table name
          out.write(iterTbls.next().getBytes(StandardCharsets.UTF_8));
          out.write(Utilities.newLineCode);
        }
      } catch (IOException e) {
        throw new HiveException(e);
      }
    }

    @Override
    void showTablesExtended(DataOutputStream out, List<Table> tables) throws HiveException {
      if (tables.isEmpty()) {
        return;
      }

      try {
        TextMetaDataTable mdt = new TextMetaDataTable();
        if (!SessionState.get().isHiveServerQuery()) {
          mdt.addRow("# Table Name", "Table Type");
        }
        for (Table table : tables) {
          mdt.addRow(table.getTableName(), table.getTableType().toString());
        }
        // In case the query is served by HiveServer2, don't pad it with spaces,
        // as HiveServer2 output is consumed by JDBC/ODBC clients.
        out.write(mdt.renderTable(!SessionState.get().isHiveServerQuery()).getBytes(StandardCharsets.UTF_8));
      } catch (IOException e) {
        throw new HiveException(e);
      }
    }
  }
}
