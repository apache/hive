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
package org.apache.hadoop.hive.ql.queryhistory.repository;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.PartitionTransform;
import org.apache.hadoop.hive.ql.parse.TransformSpec;
import org.apache.hadoop.hive.ql.queryhistory.schema.Schema;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

public abstract class AbstractRepository implements QueryHistoryRepository {
  protected Logger LOG = LoggerFactory.getLogger(getClass());
  @VisibleForTesting
  HiveConf conf;
  protected Schema schema;
  private Warehouse warehouse;

  public void init(HiveConf conf, Schema schema) {
    this.conf = conf;
    this.schema = schema;
    try {
      this.warehouse = new Warehouse(conf);
    } catch (MetaException e) {
      throw new RuntimeException(e);
    }
    initializeSessionForTableCreation();

    try (Hive hive = Hive.get(conf)) {
      Database database = initDatabase(hive);
      Table table = initTable(hive, database);
      postInitTable(table);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void initializeSessionForTableCreation() {
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_QUERY_ID, QueryHistoryRepository.QUERY_ID_FOR_TABLE_CREATION);
    SessionState ss = SessionState.get(); // if there is a SessionState in the called thread, we can use that
    if (ss == null){
      ss = SessionState.start(conf);
    }
    ss.addQueryState(QueryHistoryRepository.QUERY_ID_FOR_TABLE_CREATION,
        new QueryState.Builder().withHiveConf(conf).build());
  }

  protected Database initDatabase(Hive hive) {
    Database db;
    try {
      db = hive.getDatabase(QUERY_HISTORY_DB_NAME);
      if (db == null) {
        LOG.warn("Database ({}) for query history table hasn't been found, auto-creating one", QUERY_HISTORY_DB_NAME);
        db = new Database();
        // TODO catalog. The Hive Query History functionality is currently limited to the default catalog.
        db.setCatalogName(getDefaultCatalog(conf));
        db.setName(QUERY_HISTORY_DB_NAME);
        String location = getDatabaseLocation(new Database());
        db = new Database(QUERY_HISTORY_DB_NAME, QUERY_HISTORY_DB_COMMENT,
            location, null);
        hive.createDatabase(db, false);
      }
      return db;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String getDatabaseLocation(Database db) throws Exception {
    return warehouse.getDefaultExternalDatabasePath(db).toUri().toString();
  }

  protected Table initTable(Hive hive, Database db) {
    Table table;
    try {
      table = hive.getTable(QUERY_HISTORY_DB_NAME, QUERY_HISTORY_TABLE_NAME, null, false);
      if (table == null) {
        LOG.info("Query history table ({}) isn't created yet", QUERY_HISTORY_TABLE_NAME);
        table = createTable(hive, db);
      }
      return table;
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract void postInitTable(Table table) throws Exception;

  /**
   * Supposed to create the query history table in metastore. It's only called from
   * initTable if it doesn't exist yet.
   */
  protected abstract Table createTable(Hive hive, Database db) throws HiveException;

  /**
   * While creating the table, getInitialTable is supposed to return a common table object,
   * which contains anything general that's not specific to QueryHistoryRepository subclasses.
   */
  protected Table createTableObject() {
    Table table = new Table(QUERY_HISTORY_DB_NAME, QUERY_HISTORY_TABLE_NAME);
    table.setProperty("EXTERNAL", "TRUE");
    table.setTableType(TableType.EXTERNAL_TABLE);

    List<FieldSchema> partCols = new ArrayList<>();

    partCols.addAll(schema.getPartCols());
    List<TransformSpec> spec = PartitionTransform.getPartitionTransformSpec(partCols);
    SessionStateUtil.addResourceOrThrow(conf, hive_metastoreConstants.PARTITION_TRANSFORM_SPEC, spec);

    return table;
  }
}
