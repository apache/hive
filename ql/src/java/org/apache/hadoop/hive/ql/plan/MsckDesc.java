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
package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;

/**
 * MsckDesc.
 *
 */
public class MsckDesc extends DDLWork implements Serializable {

  private String tableName;
  private ArrayList<LinkedHashMap<String, String>> partSpecs;
  private String resFile;
  private boolean repairPartitions;
  private boolean addPartitions;
  private boolean dropPartitions;

  /**
   * For serialization use only.
   */
  public MsckDesc() {
  }

    /**
     * Description of a msck command.
     *
     * @param tableName
     *          Table to check, can be null.
     * @param partSpecs
     *          Partition specification, can be null.
     * @param resFile
     *          Where to save the output of the command
     * @param repairPartitions
     *          remove stale / add new partitions found during the check
     * @param addPartitions
     *          find partitions that are missing from metastore, and add them when repairPartitions
     *          is set to true
     * @param dropPartitions
     *          find stale partitions in metastore, and drop them when repairPartitions
     *          is set to true
     */
  public MsckDesc(String tableName, List<? extends Map<String, String>> partSpecs,
        Path resFile, boolean repairPartitions, boolean addPartitions, boolean dropPartitions) {
    super();
    this.tableName = tableName;
    this.partSpecs = new ArrayList<LinkedHashMap<String, String>>(partSpecs.size());
    for (Map<String, String> partSpec : partSpecs) {
      this.partSpecs.add(new LinkedHashMap<>(partSpec));
    }
    this.resFile = resFile.toString();
    this.repairPartitions = repairPartitions;
    this.addPartitions = addPartitions;
    this.dropPartitions = dropPartitions;
  }

  /**
   * @return the table to check
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName
   *          the table to check
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * @return partitions to check.
   */
  public ArrayList<LinkedHashMap<String, String>> getPartSpecs() {
    return partSpecs;
  }

  /**
   * @param partSpecs
   *          partitions to check.
   */
  public void setPartSpecs(ArrayList<LinkedHashMap<String, String>> partSpecs) {
    this.partSpecs = partSpecs;
  }

  /**
   * @return file to save command output to
   */
  public String getResFile() {
    return resFile;
  }

  /**
   * @param resFile
   *          file to save command output to
   */
  public void setResFile(String resFile) {
    this.resFile = resFile;
  }

  /**
   * @return remove stale / add new partitions found during the check
   */
  public boolean isRepairPartitions() {
    return repairPartitions;
  }

  /**
   * @return if missing partitions is to be found, and added with repair option
   */
  public boolean isAddPartitions() {
    return addPartitions;
  }

  /**
   * @return if stale partitions is to be found, and removed with repair option
   */
  public boolean isDropPartitions() {
    return dropPartitions;
  }

  /**
   * @param repairPartitions
   *          stale / add new partitions found during the check
   */
  public void setRepairPartitions(boolean repairPartitions) {
    this.repairPartitions = repairPartitions;
  }
}
