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
package org.apache.hadoop.hive.metastore.txn.jdbc.functions;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.sql.Types;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class CleanupRecordsFunction implements TransactionalFunction<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(CleanupRecordsFunction.class);
  private static final EnumSet<HiveObjectType> HIVE_OBJECT_TYPES = 
      EnumSet.of(HiveObjectType.DATABASE, HiveObjectType.TABLE, HiveObjectType.PARTITION);

  @SuppressWarnings("squid:S3599")
  //language=SQL
  private static final Map<BiFunction<HiveObjectType, Boolean, Boolean>, String> DELETE_COMMANDS =
      new LinkedHashMap<BiFunction<HiveObjectType, Boolean, Boolean>, String>() {{
        put((hiveObjectType, keepTxnToWriteIdMetaData) -> HIVE_OBJECT_TYPES.contains(hiveObjectType),
            "DELETE FROM \"TXN_COMPONENTS\" WHERE " +
                "\"TC_DATABASE\" = :dbName AND " +
                "(\"TC_TABLE\" = :tableName OR :tableName IS NULL) AND " +
                "(\"TC_PARTITION\" = :partName OR :partName IS NULL)");
        put((hiveObjectType, keepTxnToWriteIdMetaData) -> HIVE_OBJECT_TYPES.contains(hiveObjectType),
            "DELETE FROM \"COMPLETED_TXN_COMPONENTS\" WHERE " +
                "(\"CTC_DATABASE\" = :dbName) AND " +
                "(\"CTC_TABLE\" = :tableName OR :tableName IS NULL) AND " +
                "(\"CTC_PARTITION\" = :partName OR :partName IS NULL)");
        put((hiveObjectType, keepTxnToWriteIdMetaData) -> HIVE_OBJECT_TYPES.contains(hiveObjectType),
            "DELETE FROM \"COMPACTION_QUEUE\" WHERE " +
                "\"CQ_DATABASE\" = :dbName AND " +
                "(\"CQ_TABLE\" = :tableName OR :tableName IS NULL) AND " +
                "(\"CQ_PARTITION\" = :partName OR :partName IS NULL) AND " +
                "(\"CQ_TXN_ID\" != :txnId OR :txnId IS NULL)");
        put((hiveObjectType, keepTxnToWriteIdMetaData) -> HIVE_OBJECT_TYPES.contains(hiveObjectType),
            "DELETE FROM \"COMPLETED_COMPACTIONS\" WHERE " +
                "\"CC_DATABASE\" = :dbName AND " +
                "(\"CC_TABLE\" = :tableName OR :tableName IS NULL) AND " +
                "(\"CC_PARTITION\" = :partName OR :partName IS NULL)");
        put((hiveObjectType, keepTxnToWriteIdMetaData) -> HiveObjectType.DATABASE.equals(hiveObjectType) ||
                (HiveObjectType.TABLE.equals(hiveObjectType) && !keepTxnToWriteIdMetaData),
            "DELETE FROM \"TXN_TO_WRITE_ID\" WHERE " +
                "\"T2W_DATABASE\" = :dbName AND " +
                "(\"T2W_TABLE\" = :tableName OR :tableName IS NULL)");
        put((hiveObjectType, keepTxnToWriteIdMetaData) -> HiveObjectType.DATABASE.equals(hiveObjectType) ||
                HiveObjectType.TABLE.equals(hiveObjectType) && !keepTxnToWriteIdMetaData,
            "DELETE FROM \"NEXT_WRITE_ID\" WHERE " +
                "\"NWI_DATABASE\" = :dbName AND " +
                "(\"NWI_TABLE\" = :tableName OR :tableName IS NULL)");
        put((hiveObjectType, keepTxnToWriteIdMetaData) -> HIVE_OBJECT_TYPES.contains(hiveObjectType),
            "DELETE FROM \"COMPACTION_METRICS_CACHE\" WHERE " +
                "\"CMC_DATABASE\" = :dbName AND " +
                "(\"CMC_TABLE\" = :tableName OR :tableName IS NULL) AND " +
                "(\"CMC_PARTITION\" = :partName OR :partName IS NULL)");
      }};

  private final HiveObjectType type;
  private final Database db;
  private final Table table;
  private final Iterator<Partition> partitionIterator;
  private final String defaultCatalog;
  private final boolean keepTxnToWriteIdMetaData;
  private final Long txnId;

  public CleanupRecordsFunction(HiveObjectType type, Database db, Table table, Iterator<Partition> partitionIterator,
                                String defaultCatalog, boolean keepTxnToWriteIdMetaData, Long txnId) {
    this.type = type;
    this.db = db;
    this.table = table;
    this.partitionIterator = partitionIterator;
    this.defaultCatalog = defaultCatalog;
    this.keepTxnToWriteIdMetaData = keepTxnToWriteIdMetaData;
    this.txnId = txnId;
  }

  @Override
  public Void execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    // cleanup should be done only for objects belonging to default catalog
    List<MapSqlParameterSource> paramSources = new ArrayList<>();

    switch (type) {
      case DATABASE: {
        if (!defaultCatalog.equals(db.getCatalogName())) {
          LOG.debug("Skipping cleanup because db: " + db.getName() + " belongs to catalog "
              + "other than default catalog: " + db.getCatalogName());
          return null;
        }
        paramSources.add(new MapSqlParameterSource()
            .addValue("dbName", db.getName().toLowerCase())
            .addValue("tableName", null, Types.VARCHAR)
            .addValue("partName", null, Types.VARCHAR)
            .addValue("txnId", txnId, Types.BIGINT));
        break;
      }
      case TABLE: {
        if (!defaultCatalog.equals(table.getCatName())) {
          LOG.debug("Skipping cleanup because table: {} belongs to catalog other than default catalog: {}",
              table.getTableName(), table.getCatName());
          return null;
        }
        paramSources.add(new MapSqlParameterSource()
            .addValue("dbName", table.getDbName().toLowerCase())
            .addValue("tableName", table.getTableName().toLowerCase(), Types.VARCHAR)
            .addValue("partName", null, Types.VARCHAR)
            .addValue("txnId", null, Types.BIGINT));
        break;
      }
      case PARTITION: {
        if (!defaultCatalog.equals(table.getCatName())) {
          LOG.debug("Skipping cleanup because partitions belong to catalog other than default catalog: {}",
              table.getCatName());
          return null;
        }
        List<FieldSchema> partCols = table.getPartitionKeys();  // partition columns
        List<String> partVals;                                  // partition values
        while (partitionIterator.hasNext()) {
          Partition partition = partitionIterator.next();
          partVals = partition.getValues();
          paramSources.add(new MapSqlParameterSource()
              .addValue("dbName", table.getDbName().toLowerCase())
              .addValue("tableName", table.getTableName().toLowerCase(), Types.VARCHAR)
              .addValue("partName", Warehouse.makePartName(partCols, partVals), Types.VARCHAR)
              .addValue("txnId", null, Types.BIGINT));
        }
      }
    }

    try {
      for (MapSqlParameterSource parameterSource : paramSources) {
        for (Map.Entry<BiFunction<HiveObjectType, Boolean, Boolean>, String> item : DELETE_COMMANDS.entrySet()) {
          if (item.getKey().apply(type, keepTxnToWriteIdMetaData)) {
            jdbcResource.getJdbcTemplate().update(item.getValue(), parameterSource);
          }
        }
      }
    } catch (DataAccessException e) {
      Throwable ex = e;
      do {
        String message = ex.getMessage();
        if (StringUtils.isNotBlank(message) && message.contains("does not exist")) {
          LOG.warn("Cannot perform cleanup since metastore table does not exist");
          return null;
        }
        ex = ex.getCause();
      } while (ex != null);
      throw e;
    }
    return null;
  }

}