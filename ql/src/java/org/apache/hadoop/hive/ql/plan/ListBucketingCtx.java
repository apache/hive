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
package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.optimizer.listbucketingpruner.ListBucketingPrunerUtils;
/**
 * Context for list bucketing.
 * It's created in SemanticAnalyzer.genFileSinkPlan().
 * It's used in FileSinkOperator.processOp(), merging files, alter table ...concatenate etc.
 */
public class ListBucketingCtx implements Serializable {
  /**
   * default serialization ID.
   */
  private static final long serialVersionUID = 1L;
  private List<String> skewedColNames;
  private List<List<String>> skewedColValues;
  private Map<List<String>, String> lbLocationMap;
  private List<SkewedColumnPositionPair> rowSkewedIndex;
  private boolean isStoredAsSubDirectories;
  private String defaultKey;
  private String defaultDirName;
  private List<String> skewedValuesDirNames;

  public ListBucketingCtx() {
    rowSkewedIndex = new ArrayList<SkewedColumnPositionPair>();
    skewedValuesDirNames = new ArrayList<String>();
  }

  /**
   * @return the skewedColNames
   */
  public List<String> getSkewedColNames() {
    return skewedColNames;
  }

  /**
   * @param skewedColNames the skewedColNames to set
   */
  public void setSkewedColNames(List<String> skewedColNames) {
    this.skewedColNames = skewedColNames;
  }

  /**
   * @return the skewedColValues
   */
  public List<List<String>> getSkewedColValues() {
    return skewedColValues;
  }

  /**
   * @param skewedColValues the skewedColValues to set
   */
  public void setSkewedColValues(List<List<String>> skewedColValues) {
    this.skewedColValues = skewedColValues;
  }

  /**
   * @return the lbLocationMap
   */
  public Map<List<String>, String> getLbLocationMap() {
    return lbLocationMap;
  }

  /**
   * @param lbLocationMap the lbLocationMap to set
   */
  public void setLbLocationMap(Map<List<String>, String> lbLocationMap) {
    this.lbLocationMap = lbLocationMap;
  }

  /**
   * Match column in skewed column list and record position.
   * The position will be used in {@link FileSinkOperator} generateListBucketingDirName().
   * Note that skewed column name matches skewed value in order.
   *
   * @param rowSch
   */
  public void processRowSkewedIndex(RowSchema rowSch) {
    if ((this.skewedColNames != null) && (this.skewedColNames.size() > 0) && (rowSch != null)
        && (rowSch.getSignature() != null) && (rowSch.getSignature().size() > 0)) {
      List<ColumnInfo> cols = rowSch.getSignature();
      int hitNo = 0;
      for (int i = 0; i < cols.size(); i++) {
        int index = this.skewedColNames.indexOf(cols.get(i).getInternalName());
        if (index > -1) {
          hitNo++;
          SkewedColumnPositionPair pair = new SkewedColumnPositionPair(i, index);
          rowSkewedIndex.add(pair);
        }
      }
      assert (hitNo == this.skewedColNames.size()) : "RowSchema doesn't have all skewed columns."
          + "Skewed column: " + this.skewedColNames.toString() + ". Rowschema has columns: " + cols;
    }
  }

  /**
   * Calculate skewed value subdirectory directory which is used in
   * FileSinkOperator.java createKeyForStatsPublisher()
   * For example, create table test skewed by (key, value) on (('484','val_484')
   * stored as DIRECTORIES;
   * after the method, skewedValuesDirNames will contain 2 elements:
   * key=484/value=val_484
   * HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME/HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME
   */
  public void calculateSkewedValueSubDirList() {
    if (isSkewedStoredAsDir()) {
      for (List<String> value : this.skewedColValues) {
        skewedValuesDirNames.add(FileUtils.makeListBucketingDirName(this.skewedColNames, value));
      }
      // creat default dir
      skewedValuesDirNames.add(FileUtils.makeDefaultListBucketingDirName(
          this.skewedColNames,
          ListBucketingPrunerUtils.HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME));
    }
  }

  /**
   * @return the isStoredAsSubDirectories
   */
  public boolean isStoredAsSubDirectories() {
    return isStoredAsSubDirectories;
  }

  /**
   * @param isStoredAsSubDirectories the isStoredAsSubDirectories to set
   */
  public void setStoredAsSubDirectories(boolean isStoredAsSubDirectories) {
    this.isStoredAsSubDirectories = isStoredAsSubDirectories;
  }

  /**
   * @return the defaultKey
   */
  public String getDefaultKey() {
    return defaultKey;
  }

  /**
   * @param defaultKey the defaultKey to set
   */
  public void setDefaultKey(String defaultKey) {
    this.defaultKey = defaultKey;
  }

  /**
   * @return the defaultDirName
   */
  public String getDefaultDirName() {
    return defaultDirName;
  }

  /**
   * @param defaultDirName the defaultDirName to set
   */
  public void setDefaultDirName(String defaultDirName) {
    this.defaultDirName = defaultDirName;
  }

  /**
   * check if list bucketing is enabled.
   *
   * @param ctx
   * @return
   */
  public  boolean isSkewedStoredAsDir() {
    return (this.getSkewedColNames() != null)
        && (this.getSkewedColNames().size() > 0)
        && (this.getSkewedColValues() != null)
        && (this.getSkewedColValues().size() > 0)
        && (this.isStoredAsSubDirectories());
  }

  /**
   * Calculate list bucketing level.
   *
   * 0: not list bucketing
   * int: no. of skewed columns
   *
   * @param ctx
   * @return
   */
  public  int calculateListBucketingLevel() {
    int lbLevel = isSkewedStoredAsDir() ? this.getSkewedColNames().size() : 0;
    return lbLevel;
  }

  /**
   * @return the skewedValuesDirNames
   */
  public List<String> getSkewedValuesDirNames() {
    return skewedValuesDirNames;
  }

  /**
   * @param skewedValuesDirNames the skewedValuesDirNames to set
   */
  public void setSkewedValuesDirNames(List<String> skewedValuesDirNames) {
    this.skewedValuesDirNames = skewedValuesDirNames;
  }

  /**
   * @return the rowSkewedIndex
   */
  public List<SkewedColumnPositionPair> getRowSkewedIndex() {
    return rowSkewedIndex;
  }

  /**
   * @param rowSkewedIndex the rowSkewedIndex to set
   */
  public void setRowSkewedIndex(List<SkewedColumnPositionPair> rowSkewedIndex) {
    this.rowSkewedIndex = rowSkewedIndex;
  }
}


