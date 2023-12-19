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
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

public class OnRenameFunction implements TransactionalFunction<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(OnRenameFunction.class);
  
  //language=SQL
  private static final String[] UPDATE_COMMANNDS = new String[]{
      "UPDATE \"TXN_COMPONENTS\" SET " +
          "\"TC_PARTITION\" = COALESCE(:newPartName, \"TC_PARTITION\"), " +
          "\"TC_TABLE\" = COALESCE(:newTableName, \"TC_TABLE\"), " +
          "\"TC_DATABASE\" = COALESCE(:newDbName, \"TC_DATABASE\") WHERE " +
          "(\"TC_PARTITION\" = :oldPartName OR :oldPartName IS NULL) AND " +
          "(\"TC_TABLE\" = :oldTableName OR :oldTableName IS NULL) AND " +
          "(\"TC_DATABASE\" = :oldDbName OR :oldDbName IS NULL)",
      "UPDATE \"COMPLETED_TXN_COMPONENTS\" SET " +
          "\"CTC_PARTITION\" = COALESCE(:newPartName, \"CTC_PARTITION\"), " +
          "\"CTC_TABLE\" = COALESCE(:newTableName, \"CTC_TABLE\"), " +
          "\"CTC_DATABASE\" = COALESCE(:newDbName, \"CTC_DATABASE\") WHERE " +
          "(\"CTC_PARTITION\" = :oldPartName OR :oldPartName IS NULL) AND " +
          "(\"CTC_TABLE\" = :oldTableName OR :oldTableName IS NULL) AND " +
          "(\"CTC_DATABASE\" = :oldDbName OR :oldDbName IS NULL)",
      "UPDATE \"HIVE_LOCKS\" SET " +
          "\"HL_PARTITION\" = COALESCE(:newPartName, \"HL_PARTITION\"), " +
          "\"HL_TABLE\" = COALESCE(:newTableName, \"HL_TABLE\"), " +
          "\"HL_DB\" = COALESCE(:newDbName, \"HL_DB\") WHERE " +
          "(\"HL_PARTITION\" = :oldPartName OR :oldPartName IS NULL) AND " +
          "(\"HL_TABLE\" = :oldTableName OR :oldTableName IS NULL) AND " +
          "(\"HL_DB\" = :oldDbName OR :oldDbName IS NULL)",
      "UPDATE \"COMPACTION_QUEUE\" SET " +
          "\"CQ_PARTITION\" = COALESCE(:newPartName, \"CQ_PARTITION\"), " +
          "\"CQ_TABLE\" = COALESCE(:newTableName, \"CQ_TABLE\"), " +
          "\"CQ_DATABASE\" = COALESCE(:newDbName, \"CQ_DATABASE\") WHERE " +
          "(\"CQ_PARTITION\" = :oldPartName OR :oldPartName IS NULL) AND " +
          "(\"CQ_TABLE\" = :oldTableName OR :oldTableName IS NULL) AND " +
          "(\"CQ_DATABASE\" = :oldDbName OR :oldDbName IS NULL)",
      "UPDATE \"COMPLETED_COMPACTIONS\" SET " +
          "\"CC_PARTITION\" = COALESCE(:newPartName, \"CC_PARTITION\"), " +
          "\"CC_TABLE\" = COALESCE(:newTableName, \"CC_TABLE\"), " +
          "\"CC_DATABASE\" = COALESCE(:newDbName, \"CC_DATABASE\") WHERE " +
          "(\"CC_PARTITION\" = :oldPartName OR :oldPartName IS NULL) AND " +
          "(\"CC_TABLE\" = :oldTableName OR :oldTableName IS NULL) AND " +
          "(\"CC_DATABASE\" = :oldDbName OR :oldDbName IS NULL)",
      "UPDATE \"WRITE_SET\" SET " +
          "\"WS_PARTITION\" = COALESCE(:newPartName, \"WS_PARTITION\"), " +
          "\"WS_TABLE\" = COALESCE(:newTableName, \"WS_TABLE\"), " +
          "\"WS_DATABASE\" = COALESCE(:newDbName, \"WS_DATABASE\") WHERE " +
          "(\"WS_PARTITION\" = :oldPartName OR :oldPartName IS NULL) AND " +
          "(\"WS_TABLE\" = :oldTableName OR :oldTableName IS NULL) AND " +
          "(\"WS_DATABASE\" = :oldDbName OR :oldDbName IS NULL)",
      "UPDATE \"TXN_TO_WRITE_ID\" SET " +
          "\"T2W_TABLE\" = COALESCE(:newTableName, \"T2W_TABLE\"), " +
          "\"T2W_DATABASE\" = COALESCE(:newDbName, \"T2W_DATABASE\") WHERE " +
          "(\"T2W_TABLE\" = :oldTableName OR :oldTableName IS NULL) AND " +
          "(\"T2W_DATABASE\" = :oldDbName OR :oldDbName IS NULL)",
      "UPDATE \"NEXT_WRITE_ID\" SET " +
          "\"NWI_TABLE\" = COALESCE(:newTableName, \"NWI_TABLE\"), " +
          "\"NWI_DATABASE\" = COALESCE(:newDbName, \"NWI_DATABASE\") WHERE " +
          "(\"NWI_TABLE\" = :oldTableName OR :oldTableName IS NULL) AND " +
          "(\"NWI_DATABASE\" = :oldDbName OR :oldDbName IS NULL)",
      "UPDATE \"COMPACTION_METRICS_CACHE\" SET " +
          "\"CMC_PARTITION\" = COALESCE(:newPartName, \"CMC_PARTITION\"), " +
          "\"CMC_TABLE\" = COALESCE(:newTableName, \"CMC_TABLE\"), " +
          "\"CMC_DATABASE\" = COALESCE(:newDbName, \"CMC_DATABASE\") WHERE " +
          "(\"CMC_PARTITION\" = :oldPartName OR :oldPartName IS NULL) AND " +
          "(\"CMC_TABLE\" = :oldTableName OR :oldTableName IS NULL) AND " +
          "(\"CMC_DATABASE\" = :oldDbName OR :oldDbName IS NULL)",
  };

  private final String oldCatName;
  private final String oldDbName;
  private final String oldTabName;
  private final String oldPartName;
  private final String newCatName;
  private final String newDbName;
  private final String newTabName;
  private final String newPartName;

  public OnRenameFunction(String oldCatName, String oldDbName, String oldTabName, String oldPartName, 
                          String newCatName, String newDbName, String newTabName, String newPartName) {
    this.oldCatName = oldCatName;
    this.oldDbName = oldDbName;
    this.oldTabName = oldTabName;
    this.oldPartName = oldPartName;
    this.newCatName = newCatName;
    this.newDbName = newDbName;
    this.newTabName = newTabName;
    this.newPartName = newPartName;
  }

  @SuppressWarnings("squid:S2259")
  @Override
  public Void execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    String callSig = "onRename(" +
        oldCatName + "," + oldDbName + "," + oldTabName + "," + oldPartName + "," +
        newCatName + "," + newDbName + "," + newTabName + "," + newPartName + ")";

    if(newPartName != null) {
      assert oldPartName != null && oldTabName != null && oldDbName != null && oldCatName != null : callSig;
    }
    if(newTabName != null) {
      assert oldTabName != null && oldDbName != null && oldCatName != null : callSig;
    }
    if(newDbName != null) {
      assert oldDbName != null && oldCatName != null : callSig;
    }

    MapSqlParameterSource paramSource = new MapSqlParameterSource()
        .addValue("oldDbName", StringUtils.lowerCase(oldDbName))
        .addValue("newDbName", StringUtils.lowerCase(newDbName))
        .addValue("oldTableName", StringUtils.lowerCase(oldTabName))
        .addValue("newTableName", StringUtils.lowerCase(newTabName))
        .addValue("oldPartName", oldPartName)
        .addValue("newPartName", newPartName);
    try {
      for (String command : UPDATE_COMMANNDS) {
        jdbcResource.getJdbcTemplate().update(command, paramSource);
      }
    } catch (DataAccessException e) {
      //TODO: this seems to be very hacky, and as a result retry attempts won't happen, because DataAccessExceptions are
      // caught and either swallowed or wrapped in MetaException. Also, only a single test fails without this block:
      // org.apache.hadoop.hive.metastore.client.TestDatabases.testAlterDatabaseNotNullableFields
      // It may worth investigate if this catch block is really needed. 
      if (e.getMessage() != null && e.getMessage().contains("does not exist")) {
        LOG.warn("Cannot perform {} since metastore table does not exist", callSig);
      } else {
        throw new MetaException("Unable to " + callSig + ":" + org.apache.hadoop.util.StringUtils.stringifyException(e));
      }
    }
    return null;
  }

}
