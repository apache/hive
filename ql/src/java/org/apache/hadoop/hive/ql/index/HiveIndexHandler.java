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

package org.apache.hadoop.hive.ql.index;

import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;

/**
 * HiveIndexHandler defines a pluggable interface for adding new index handlers
 * to Hive.
 */
public interface HiveIndexHandler extends Configurable {
  /**
   * Determines whether this handler implements indexes by creating an index
   * table.
   * 
   * @return true if index creation implies creation of an index table in Hive;
   *         false if the index representation is not stored in a Hive table
   */
  boolean usesIndexTable();

  /**
   * Requests that the handler validate an index definition and fill in
   * additional information about its stored representation.
   * 
   * @param baseTable
   *          the definition of the table being indexed
   * 
   * @param index
   *          the definition of the index being created
   * 
   * @param indexTable
   *          a partial definition of the index table to be used for storing the
   *          index representation, or null if usesIndexTable() returns false;
   *          the handler can augment the index's storage descriptor (e.g. with
   *          information about input/output format) and/or the index table's
   *          definition (typically with additional columns containing the index
   *          representation, e.g. pointers into HDFS).
   * 
   * @throw HiveException if the index definition is invalid with respect to
   *        either the base table or the supplied index table definition
   */
  void analyzeIndexDefinition(
      org.apache.hadoop.hive.metastore.api.Table baseTable,
      org.apache.hadoop.hive.metastore.api.Index index,
      org.apache.hadoop.hive.metastore.api.Table indexTable)
      throws HiveException;

  /**
   * Requests that the handler generate a plan for building the index; the plan
   * should read the base table and write out the index representation.
   * 
   * @param baseTable
   *          the definition of the table being indexed
   * 
   * @param index
   *          the definition of the index
   * 
   * @param indexTblPartitions
   *          list of index partitions
   * 
   * @param baseTblPartitions
   *          list of base table partitions with each element mirrors to the
   *          corresponding one in indexTblPartitions
   * 
   * @param indexTable
   *          the definition of the index table, or null if usesIndexTable()
   *          returns null
   * 
   * @param db
   * 
   * @return list of tasks to be executed in parallel for building the index
   * 
   * @throw HiveException if plan generation fails
   */
  List<Task<?>> generateIndexBuildTaskList(
      org.apache.hadoop.hive.ql.metadata.Table baseTbl,
      org.apache.hadoop.hive.metastore.api.Index index,
      List<Partition> indexTblPartitions, List<Partition> baseTblPartitions,
      org.apache.hadoop.hive.ql.metadata.Table indexTbl, Hive db)
      throws HiveException;

}