/**
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

package org.apache.hadoop.hive.ql.hooks;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.BaseColumnInfo;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.Dependency;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.DependencyKey;
import org.apache.hadoop.hive.ql.hooks.conf.FBHiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Implementation of a post execute hook that simply prints out its parameters
 * to standard output.
 */
public class LineageHook implements PostExecute {

  ConnectionUrlFactory urlFactory = null;
  public LineageHook() throws Exception {
    HiveConf conf = new HiveConf(LineageHook.class);
    urlFactory = HookUtils.getUrlFactory(conf,
        FBHiveConf.CONNECTION_FACTORY,
        FBHiveConf.LINEAGE_CONNECTION_FACTORY,
        FBHiveConf.LINEAGE_MYSQL_TIER_VAR_NAME,
        FBHiveConf.LINEAGE_HOST_DATABASE_VAR_NAME);
  }

  @Override
  public void run(SessionState sess, Set<ReadEntity> inputs,
      Set<WriteEntity> outputs, LineageInfo linfo,
      UserGroupInformation ugi) throws Exception {

    HiveConf conf = sess.getConf();

    if (linfo != null) {

      Iterator<Map.Entry<DependencyKey, Dependency>> iter = linfo.entrySet().iterator();
      while(iter.hasNext()) {
        Map.Entry<DependencyKey, Dependency> it = iter.next();
        Dependency dep = it.getValue();
        DependencyKey depK = it.getKey();

        /**
         *  Generate json values of the following format:
         *
         *  {"value": {
         *     "type":"SIMPLE",
         *     "baseCols":[{
         *        "column":{
         *         "name":"col",
         *         "comment":"from serde",
         *         "type":"array<string>"
         *        },
         *        "tabAlias":{
         *          "alias":"athusoo_tmp",
         *          "table":{
         *            "dbName":"default",
         *            "tableName":"athusoo_tmp"
         *          }
         *        }
         *     }]
         *   },
         *  "key":{
         *    "fieldSchema":{
         *      "name":"col",
         *      "comment":"from deserializer",
         *      "type":"array<string>"
         *    },
         *    "dataContainer":{
         *      "isPartition":false,
         *      "table":{
         *        "dbName":"default",
         *        "tableName":"athusoo_tmp2"
         *      }
         *    }
         *  }
         *}
         */
        JSONObject out_json = new JSONObject();
        JSONObject depk_json = new JSONObject();
        JSONObject field_json = new JSONObject();

        field_json.put("name", depK.getFieldSchema().getName());
        field_json.put("type", depK.getFieldSchema().getType());
        field_json.put("comment", depK.getFieldSchema().getComment());
        depk_json.put("fieldSchema", field_json);

        JSONObject dc_json = new JSONObject();
        dc_json.put("isPartition", depK.getDataContainer().isPartition());
        JSONObject tab_json = new JSONObject();
        if (depK.getDataContainer().isPartition()) {
          JSONObject part_json = new JSONObject();
          Partition part = depK.getDataContainer().getPartition();
          part_json.put("values", part.getValues());

          tab_json.put("tableName", depK.getDataContainer().getTable().getTableName());
          tab_json.put("dbName", depK.getDataContainer().getTable().getDbName());
          JSONArray fs_array = new JSONArray();
          for (FieldSchema fs : depK.getDataContainer().getTable().getPartitionKeys()) {
            field_json = new JSONObject();
            field_json.put("name", fs.getName());
            field_json.put("type", fs.getType());
            field_json.put("comment", fs.getComment());

            fs_array.put(field_json);
          }
          tab_json.put("partitionKeys", fs_array);
          part_json.put("table", tab_json);
          dc_json.put("partition", part_json);
        }
        else {
          tab_json.put("tableName", depK.getDataContainer().getTable().getTableName());
          tab_json.put("dbName", depK.getDataContainer().getTable().getDbName());
          dc_json.put("table", tab_json);
        }
        depk_json.put("dataContainer", dc_json);
        out_json.put("key", depk_json);

        JSONObject dep_json = new JSONObject();
        dep_json.put("type", dep.getType().toString());
        dep_json.put("expr", dep.getExpr());
        JSONArray basecol_array = new JSONArray();
        for(BaseColumnInfo col: dep.getBaseCols()) {
          JSONObject col_json = new JSONObject();

          field_json = new JSONObject();
          // A column can be null in the case of aggregations like count(1)
          // where the value is dependent on the entire row.
          if (col.getColumn() != null) {
            field_json.put("name", col.getColumn().getName());
            field_json.put("type", col.getColumn().getType());
            field_json.put("comment", col.getColumn().getComment());
          }
          col_json.put("column", field_json);

          JSONObject tabAlias_json = new JSONObject();
          tabAlias_json.put("alias", col.getTabAlias().getAlias());

          tab_json = new JSONObject();
          tab_json.put("tableName", col.getTabAlias().getTable().getTableName());
          tab_json.put("dbName", col.getTabAlias().getTable().getDbName());

          tabAlias_json.put("table", tab_json);
          col_json.put("tabAlias", tabAlias_json);
          basecol_array.put(col_json);
        }
        dep_json.put("baseCols", basecol_array);
        out_json.put("value", dep_json);

        ArrayList<Object> sqlParams = new ArrayList<Object>();
        sqlParams.add(StringEscapeUtils.escapeJava(out_json.toString()));
        String sql = "insert into lineage_log set info = ?";

        HookUtils.runInsert(conf, urlFactory, sql, sqlParams, HookUtils
            .getSqlNumRetry(conf));
      }
    }
  }
}
