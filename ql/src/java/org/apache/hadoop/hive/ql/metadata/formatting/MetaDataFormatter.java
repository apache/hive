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

package org.apache.hadoop.hive.ql.metadata.formatting;

import java.io.DataOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.ql.metadata.CheckConstraint;
import org.apache.hadoop.hive.ql.metadata.DefaultConstraint;
import org.apache.hadoop.hive.ql.metadata.ForeignKeyInfo;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.NotNullConstraint;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.PrimaryKeyInfo;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.UniqueConstraint;

/**
 * Interface to format table and index information.  We can format it
 * for human readability (lines of text) or for machine readability
 * (json).
 */
public interface MetaDataFormatter {
  /**
   * Write an error message.
   * @param sqlState if {@code null}, will be ignored
   */
  public void error(OutputStream out, String msg, int errorCode, String sqlState)
      throws HiveException;

  /**
   * @param sqlState if {@code null}, will be skipped in output
   * @param errorDetail usually string version of some Exception, if {@code null}, will be ignored
   */
  public void error(OutputStream out, String errorMessage, int errorCode, String sqlState, String errorDetail)
      throws HiveException;

  /**
   * Show a list of tables.
   */
  public void showTables(DataOutputStream out, Set<String> tables)
      throws HiveException;

  /**
   * Describe table.
   * @param out
   * @param colPath
   * @param tableName
   * @param tbl
   * @param part
   * @param cols
   * @param isFormatted - describe with formatted keyword
   * @param isExt
   * @param isPretty
   * @param isOutputPadded - if true, add spacing and indentation
   * @param colStats
   * @param fkInfo  foreign keys information
   * @param pkInfo  primary key information
   * @param ukInfo  unique constraint information
   * @param nnInfo  not null constraint information
   * @throws HiveException
   */
  public void describeTable(DataOutputStream out, String colPath,
      String tableName, Table tbl, Partition part, List<FieldSchema> cols,
      boolean isFormatted, boolean isExt,
      boolean isOutputPadded, List<ColumnStatisticsObj> colStats,
      PrimaryKeyInfo pkInfo, ForeignKeyInfo fkInfo,
      UniqueConstraint ukInfo, NotNullConstraint nnInfo, DefaultConstraint dInfo, CheckConstraint cInfo)
          throws HiveException;

  /**
   * Show the table status.
   */
  public void showTableStatus(DataOutputStream out,
      Hive db,
      HiveConf conf,
      List<Table> tbls,
      Map<String, String> part,
      Partition par)
          throws HiveException;

  /**
   * Show the table partitions.
   */
  public void showTablePartitions(DataOutputStream out,
      List<String> parts)
          throws HiveException;

  /**
   * Show the databases
   */
  public void showDatabases(DataOutputStream out, List<String> databases)
      throws HiveException;

  /**
   * Describe a database.
   */
  public void showDatabaseDescription (DataOutputStream out, String database, String comment,
      String location, String ownerName, String ownerType, Map<String, String> params)
          throws HiveException;

  void showResourcePlans(DataOutputStream out, List<WMResourcePlan> resourcePlans)
      throws HiveException;

  void showFullResourcePlan(DataOutputStream out, WMFullResourcePlan resourcePlan)
      throws HiveException;

  void showErrors(DataOutputStream out, WMValidateResourcePlanResponse errors)
      throws HiveException;
}

