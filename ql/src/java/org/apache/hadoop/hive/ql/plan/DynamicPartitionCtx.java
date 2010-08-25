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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.metadata.Table;

public class DynamicPartitionCtx implements Serializable {

  /**
   * default serialization ID
   */
  private static final long serialVersionUID = 1L;

  private Map<String, String> partSpec; // partSpec is an ORDERED hash map
  private int numDPCols;   // number of dynamic partition columns
  private int numSPCols;   // number of static partition columns
  private String spPath;   // path name corresponding to SP columns
  private String rootPath; // the root path DP columns paths start from
  private int numBuckets;  // number of buckets in each partition

  private Map<String, String> inputToDPCols; // mapping from input column names to DP columns

  private List<String> spNames; // sp column names
  private List<String> dpNames; // dp column names
  private String defaultPartName; // default partition name in case of null or empty value
  private int maxPartsPerNode;    // maximum dynamic partitions created per mapper/reducer

  public DynamicPartitionCtx() {
  }

  public DynamicPartitionCtx(Table tbl, Map<String, String> partSpec, String defaultPartName,
      int maxParts) {
    this.partSpec = partSpec;
    this.spNames = new ArrayList<String>();
    this.dpNames = new ArrayList<String>();
    this.numBuckets = 0;
    this.maxPartsPerNode = maxParts;
    this.defaultPartName = defaultPartName;

    for (Map.Entry<String, String> me: partSpec.entrySet()) {
      if (me.getValue() == null) {
        dpNames.add(me.getKey());
      } else {
        spNames.add(me.getKey());
      }
    }
    this.numDPCols = dpNames.size();
    this.numSPCols = spNames.size();
    this.inputToDPCols = new HashMap<String, String>();
    if (this.numSPCols > 0) {
      this.spPath = Warehouse.makeDynamicPartName(partSpec);
    } else {
      this.spPath = null;
    }
  }

  public DynamicPartitionCtx(DynamicPartitionCtx dp) {
    this.partSpec = dp.partSpec;
    this.numDPCols = dp.numDPCols;
    this.numSPCols = dp.numSPCols;
    this.spPath = dp.spPath;
    this.rootPath = dp.rootPath;
    this.numBuckets = dp.numBuckets;
    this.inputToDPCols = dp.inputToDPCols;
    this.spNames = dp.spNames;
    this.dpNames = dp.dpNames;
    this.defaultPartName = dp.defaultPartName;
    this.maxPartsPerNode = dp.maxPartsPerNode;
  }

  public void mapInputToDP(List<ColumnInfo> fs) {

      assert fs.size() == this.numDPCols: "input DP column size != numDPCols";

      Iterator<ColumnInfo> itr1 = fs.iterator();
      Iterator<String> itr2 = dpNames.iterator();

      while (itr1.hasNext() && itr2.hasNext()) {
        inputToDPCols.put(itr1.next().getInternalName(), itr2.next());
      }
  }

  public int getMaxPartitionsPerNode() {
    return this.maxPartsPerNode;
  }

  public void setMaxPartitionsPerNode(int maxParts) {
    this.maxPartsPerNode = maxParts;
  }

  public String getDefaultPartitionName() {
    return this.defaultPartName;
  }

  public void setDefaultPartitionName(String pname) {
    this.defaultPartName = pname;
  }

  public void setNumBuckets(int bk) {
    this.numBuckets = bk;
  }

  public int getNumBuckets() {
    return this.numBuckets;
  }

  public void setRootPath(String root) {
    this.rootPath = root;
  }

  public String getRootPath() {
    return this.rootPath;
  }

  public List<String> getDPColNames() {
    return this.dpNames;
  }

  public void setDPColNames(List<String> dp) {
    this.dpNames = dp;
  }

  public List<String> getSPColNames() {
    return this.spNames;
  }

  public void setPartSpec(Map<String, String> ps) {
    this.partSpec = ps;
  }

  public Map<String, String> getPartSpec() {
    return this.partSpec;
  }

  public void setSPColNames(List<String> sp) {
    this.spNames = sp;
  }

  public Map<String, String> getInputToDPCols() {
    return this.inputToDPCols;
  }

  public void setInputToDPCols(Map<String, String> map) {
    this.inputToDPCols = map;
  }

  public void setNumDPCols(int dp) {
    this.numDPCols = dp;
  }

  public int getNumDPCols() {
    return this.numDPCols;
  }

  public void setNumSPCols(int sp) {
    this.numSPCols = sp;
  }

  public int getNumSPCols() {
    return this.numSPCols;
  }

  public void setSPPath(String sp) {
    this.spPath = sp;
  }

  public String getSPPath() {
    return this.spPath;
  }
}
