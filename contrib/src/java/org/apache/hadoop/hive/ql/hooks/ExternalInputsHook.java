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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.conf.FBHiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContextImpl;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Implementation of a compile time hook that updates the inputs to include the managed objects
 * which have data external inputs are pointing to
 */
public class ExternalInputsHook extends AbstractSemanticAnalyzerHook {

  private static final int SQL_NUM_RETRIES = 3 ;
  private static final int RETRY_MAX_INTERVAL_SEC = 60;

  // Does nothing
  @Override
  public ASTNode preAnalyze(
    HiveSemanticAnalyzerHookContext context,
    ASTNode ast) throws SemanticException {
    //do nothing
    return ast;
  }

  // Updates the inputs to include managed tables/partitions whose data is pointed to by external
  // inputs
  @Override
  public void postAnalyze(
    HiveSemanticAnalyzerHookContext context,
    List<Task<? extends Serializable>> rootTasks) throws SemanticException {

    HiveSemanticAnalyzerHookContextImpl ctx = (HiveSemanticAnalyzerHookContextImpl)context;
    HiveConf conf = (HiveConf)ctx.getConf();

    Set<String> externalLocations = new HashSet<String>();

    for (ReadEntity input : ctx.getInputs()) {

      // If this input is either an external table or a partition in an external table, add its
      // location to the set of locations
      if (input.getTable().getTableType() == TableType.EXTERNAL_TABLE) {
        String location = null;
        try {
          location = input.getLocation().toString();
        } catch (Exception e) {
          throw new SemanticException("GetLocation failed", e);
        }

        // We assume all managed tables exist in /user/facebook/warehouse/
        // This helps to avoid having to look up if there are managed tables pointing to the data
        // being pointed to by scrape and scribe staging tables, which point to directories like
        // /user/facebook/scrape_staging (staging tables) and /user/facebook/scribe_staging
        // (current tables) and /tmp (signal tables)
        // We are also excluding inputs which are partitioned tables (without their partitions)
        // If the input is a partitioned table, it can only be a metadata change, and hence only
        // needs the external table, not the underlying managed table.  If any data was queried
        // the partition queried will also be in the inputs and we can get the managed
        // table/partition from this.
        if (location.contains("/user/facebook/warehouse/") &&
            (!input.getTable().isPartitioned() || input.getType() != ReadEntity.Type.TABLE)) {
          externalLocations.add(location);
        }
      }
    }

    // If there were some external inputs, get the managed tables/partitions whose data they
    // point to
    if (!externalLocations.isEmpty()) {
      // The 2 cases in the select are as follows:
      // d1.name, t1.tbl_name, p1.part_name
      // 1) The external entity's location is such that there are one or more partitions whose
      //    location is a subdirectory, this includes if the external entity's location is the same
      //    as the location of a partitioned table, in which case all partitions whose location has
      //    the table's location as a prefix will be returned, not the table (If the location of
      //    the table was ever changed, this means only the subset of partitions created after the
      //    location was changed will be included)
      // d2.name, t2.tbl_name, NULL
      // 2) The external entity's location is such that there is an unpartitioned whose location is
      //    a prefix.  In this case the table is returned.

      String sql = "SELECT IF(p1.part_name IS NOT NULL, d1.name, d2.name), " +
                   "       IF(p1.part_name IS NOT NULL, t1.tbl_name, t2.tbl_name), " +
                   "       p1.part_name " +
                   "FROM SDS s LEFT JOIN PARTITIONS p1 ON s.sd_id = p1.sd_id " +
                   "LEFT JOIN TBLS t1 ON t1.tbl_id = p1.tbl_id " +
                   "LEFT JOIN DBS d1 ON t1.db_id = d1.db_id " +
                   "LEFT JOIN TBLS t2 ON t2.sd_id = s.sd_id " +
                   "LEFT JOIN DBS d2 ON d2.db_id = t2.db_id " +
                   "LEFT JOIN PARTITION_KEYS k on t2.tbl_id = k.tbl_id " +
                   "WHERE ((p1.part_name IS NOT NULL AND t1.tbl_type = 'MANAGED_TABLE') OR " +
                   "       (p1.part_name IS NULL AND t2.tbl_type = 'MANAGED_TABLE' AND" +
                   "        k.tbl_id IS NULL)) AND (";

      List<Object> sqlParams = new ArrayList<Object>();

      boolean firstLocation = true;
      for (String location : externalLocations) {
        if (!firstLocation) {
          sql += "OR ";
        } else {
          firstLocation = false;
        }

        sql += "s.location LIKE ? ";
        sql += "OR s.location = ? ";
        // Adding the / ensures that we will only get locations which are subdirectories of the
        // external entities location, rather than just having it as a prefix
        sqlParams.add(location + "/%");
        // Also check if it is equal, in which case the final / will not be in the location or it
        // will be captured by the LIKE
        sqlParams.add(location);
      }

      sql += ");";
      ConnectionUrlFactory metastoreDbUrlFactory =
          HookUtils.getUrlFactory(
              conf,
              FBHiveConf.CONNECTION_FACTORY,
              FBHiveConf.METASTORE_CONNECTION_FACTORY,
              FBHiveConf.METASTORE_MYSQL_TIER_VAR_NAME,
              FBHiveConf.METASTORE_HOST_DATABASE_VAR_NAME);
      List<List<Object>> results = null;
      try {
        results = HookUtils.runInsertSelect(conf,
            metastoreDbUrlFactory, sql, sqlParams, false, SQL_NUM_RETRIES,
            RETRY_MAX_INTERVAL_SEC, false);
      } catch (Exception e) {
        throw new SemanticException("SQL query to retrieve names of managed tables/partitions " +
                                    "pointed to by externals failed", e);
      }

      // Construct a mapping to pass to updateInputs, the structure of the mapping is described in
      // updateInputs's method description
      Map<String[], List<String>> tableToPartitions = new HashMap<String[], List<String>>();

      for (List<Object> result : results) {

        String[] dbTable = {(String)result.get(0), (String)result.get(1)};
        if (!tableToPartitions.containsKey(dbTable)) {
          tableToPartitions.put(dbTable, new ArrayList<String>());
        }

        String partitionName = (String)result.get(2);
        if (partitionName != null) {
          tableToPartitions.get(dbTable).add(partitionName);
        }
      }

      try {
        updateInputs(ctx.getInputs(), tableToPartitions, ctx.getHive());
      } catch (HiveException e) {
        throw new SemanticException("Failed to retrieve managed Table(s)/Partition(s) mapped to " +
                                    "by externals from the metastore.", e);
      }
    }
  }

  /**
   * Given a set of inputs and a map from db/table name to a list of partition names, and an
   * instance of Hive it updates the inputs to include for each db/table name the partitions, or if
   * the list of partitions is empty, the table
   * @param inputs             A set of ReadEntities
   * @param tableToPartitions  A map, whose keys are arrays of strings of length 2, the first index
   *                           should correspond to the db name and the second to the table name,
   *                           the values are lists of Strings representing partition names, if the
   *                           list is empty it is assumed the table is unpartitioned
   * @param db                 An instance of Hive, used to connect to the metastore.
   * @throws HiveException
   */
  private void updateInputs(Set<ReadEntity> inputs,
                            Map<String[], List<String>> tableToPartitions, Hive db)
    throws HiveException {
    for (Entry<String[], List<String>> entry : tableToPartitions.entrySet()) {

      Table table = db.getTable(entry.getKey()[0], entry.getKey()[1]);

      if (entry.getValue().isEmpty()) {
        inputs.add(new ReadEntity(table));
      } else {
        List<Partition> partitions = db.getPartitionsByNames(table, entry.getValue());
        for (Partition partition : partitions) {
          inputs.add(new ReadEntity(partition));
        }
      }
    }
  }
}
