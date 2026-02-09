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
package org.apache.hadoop.hive.metastore;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.jdo.PersistenceManager;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;

import static org.apache.hadoop.hive.metastore.MetastoreDirectSqlUtils.executeWithArray;
import static org.apache.hadoop.hive.metastore.MetastoreDirectSqlUtils.extractSqlClob;
import static org.apache.hadoop.hive.metastore.MetastoreDirectSqlUtils.extractSqlLong;

/**
 * Shared helper to diff and apply parameter table changes (delete/update/insert) in batches.
 */
class DirectSqlUpdateParams extends DirectSqlBase {

  DirectSqlUpdateParams(PersistenceManager pm, DatabaseProduct dbType, int batchSize) {
    super(pm, dbType, batchSize);
  }

  void run(String paramTable, String idColumn, List<Long> ids, Map<Long, Optional<Map<String, String>>> newParamsOpt)
      throws MetaException {
    Map<Long, Map<String, String>> oldParams = getParams(paramTable, idColumn, ids);

    List<Pair<Long, String>> toDelete = new ArrayList<>();
    List<Pair<Long, Pair<String, String>>> toUpdate = new ArrayList<>();
    List<Pair<Long, Pair<String, String>>> toInsert = new ArrayList<>();

    for (Long id : ids) {
      Map<String, String> oldParam = oldParams.getOrDefault(id, new HashMap<>());
      Map<String, String> newParam = newParamsOpt.get(id).orElseGet(HashMap::new);

      for (Map.Entry<String, String> entry : oldParam.entrySet()) {
        String key = entry.getKey();
        String oldValue = entry.getValue();
        if (!newParam.containsKey(key)) {
          toDelete.add(Pair.of(id, key));
        } else if (!oldValue.equals(newParam.get(key))) {
          toUpdate.add(Pair.of(id, Pair.of(key, newParam.get(key))));
        }
      }
      for (Map.Entry<String, String> entry : newParam.entrySet()) {
        if (!oldParam.containsKey(entry.getKey())) {
          toInsert.add(Pair.of(id, Pair.of(entry.getKey(), entry.getValue())));
        }
      }
    }

    deleteParams(paramTable, idColumn, toDelete);
    updateParams(paramTable, idColumn, toUpdate);
    insertParams(paramTable, idColumn, toInsert);
  }

  private Map<Long, Map<String, String>> getParams(String paramTable, String idName, List<Long> ids)
      throws MetaException {
    Map<Long, Map<String, String>> idToParams = new HashMap<>();
    Batchable.runBatched(maxBatchSize, ids, new Batchable<>() {
      @Override
      public List<Object> run(List<Long> input) throws MetaException {
        String idList = MetaStoreDirectSql.getIdListForIn(input);
        String queryText = "select " + idName + ", \"PARAM_KEY\", \"PARAM_VALUE\" from " + paramTable +
            " where " + idName + " in (" + idList + ")";
        try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
          List<Object[]> sqlResult = executeWithArray(query.getInnerQuery(), null, queryText);
          for (Object[] row : sqlResult) {
            Long id = extractSqlLong(row[0]);
            String paramKey = extractSqlClob(row[1]);
            String paramVal = extractSqlClob(row[2]);
            idToParams.computeIfAbsent(id, key -> new HashMap<>()).put(paramKey, paramVal);
          }
        }
        return null;
      }
    });
    return idToParams;
  }

  private void deleteParams(String paramTable, String idColumn, List<Pair<Long, String>> deleteIdKeys)
      throws MetaException {
    String deleteStmt = "delete from " + paramTable + " where " + idColumn +  "=? and \"PARAM_KEY\"=?";
    int maxRows = dbType.getMaxRows(maxBatchSize, 2);
    updateWithStatement(statement -> Batchable.runBatched(maxRows, deleteIdKeys,
        new Batchable<Pair<Long, String>, Void>() {
          @Override
          public List<Void> run(List<Pair<Long, String>> input) throws SQLException {
            for (Pair<Long, String> pair : input) {
              statement.setLong(1, pair.getLeft());
              statement.setString(2, pair.getRight());
              statement.addBatch();
            }
            statement.executeBatch();
            return null;
          }
        }
    ), deleteStmt);
  }

  private void updateParams(String paramTable, String idColumn,
      List<Pair<Long, Pair<String, String>>> updateIdAndParams) throws MetaException {
    List<String> columns = List.of("\"PARAM_VALUE\"");
    List<String> conditionKeys = Arrays.asList(idColumn, "\"PARAM_KEY\"");
    String stmt = TxnUtils.createUpdatePreparedStmt(paramTable, columns, conditionKeys);
    int maxRows = dbType.getMaxRows(maxBatchSize, 3);
    updateWithStatement(statement -> Batchable.runBatched(maxRows, updateIdAndParams,
        new Batchable<>() {
          @Override
          public List<Object> run(List<Pair<Long, Pair<String, String>>> input) throws SQLException {
            for (Pair<Long, Pair<String, String>> pair : input) {
              statement.setString(1, pair.getRight().getRight());
              statement.setLong(2, pair.getLeft());
              statement.setString(3, pair.getRight().getLeft());
              statement.addBatch();
            }
            statement.executeBatch();
            return null;
          }
        }
    ), stmt);
  }

  private void insertParams(String paramTable, String idColumn,
      List<Pair<Long, Pair<String, String>>> addIdAndParams) throws MetaException {
    List<String> columns = Arrays.asList(idColumn, "\"PARAM_KEY\"", "\"PARAM_VALUE\"");
    String query = TxnUtils.createInsertPreparedStmt(paramTable, columns);
    int maxRows = dbType.getMaxRows(maxBatchSize, 3);
    updateWithStatement(statement -> Batchable.runBatched(maxRows, addIdAndParams,
        new Batchable<Pair<Long, Pair<String, String>>, Void>() {
          @Override
          public List<Void> run(List<Pair<Long, Pair<String, String>>> input) throws SQLException {
            for (Pair<Long, Pair<String, String>> pair : input) {
              statement.setLong(1, pair.getLeft());
              statement.setString(2, pair.getRight().getLeft());
              statement.setString(3, pair.getRight().getRight());
              statement.addBatch();
            }
            statement.executeBatch();
            return null;
          }
        }
    ), query);
  }

}
