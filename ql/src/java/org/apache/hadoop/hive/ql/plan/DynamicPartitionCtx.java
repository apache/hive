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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.ql.ddl.table.partition.PartitionUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class DynamicPartitionCtx implements Serializable {

  /**
   * default serialization ID
   */
  private static final long serialVersionUID = 1L;

  private Map<String, String> partSpec; // partSpec is an ORDERED hash map
  private int numDPCols;   // number of dynamic partition columns
  private int numSPCols;   // number of static partition columns
  private String spPath;   // path name corresponding to SP columns
  private Path rootPath; // the root path DP columns paths start from
  private int numBuckets;  // number of buckets in each partition

  private List<String> spNames; // sp column names
  private List<String> dpNames; // dp column names
  private String defaultPartName; // default partition name in case of null or empty value
  private int maxPartsPerNode;    // maximum dynamic partitions created per mapper/reducer
  private Pattern whiteListPattern;
  private boolean hasCustomSortExpr = false;
  /**
   * Expressions describing a custom way of sorting the table before write. Expressions can reference simple
   * column descriptions or a tree of expressions containing more columns and UDFs.
   * Can be useful for custom bucket/hash sorting.
   * A custom expression should be a lambda that is given the original column description expressions as per read
   * schema and returns a single expression. Example for simply just referencing column 3: cols -> cols.get(3).clone()
   */
  private transient List<Function<List<ExprNodeDesc>, ExprNodeDesc>> customSortExpressions;
  private transient List<Integer> customSortOrder;
  private transient List<Integer> customSortNullOrder;

  public DynamicPartitionCtx() {
  }

  /**
   * This constructor is used for partitioned CTAS. Basically we pass the name of
   * partitioned columns, which will all be dynamic partitions since the binding
   * is done after executing the query in the CTAS.
   */
  public DynamicPartitionCtx(List<String> partColNames, String defaultPartName,
      int maxParts) throws SemanticException {
    this.partSpec = new LinkedHashMap<>();
    this.spNames = new ArrayList<>();
    this.dpNames = new ArrayList<>();
    for (String colName : partColNames) {
      this.partSpec.put(colName, null);
      this.dpNames.add(colName);
    }
    this.numBuckets = 0;
    this.maxPartsPerNode = maxParts;
    this.defaultPartName = defaultPartName;

    this.numDPCols = dpNames.size();
    this.numSPCols = spNames.size();
    this.spPath = null;
    String confVal;
    try {
      confVal = Hive.get().getMetaConf(ConfVars.PARTITION_NAME_WHITELIST_PATTERN.getVarname());
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
    this.whiteListPattern = confVal == null || confVal.isEmpty() ? null : Pattern.compile(confVal);
    this.customSortExpressions = new LinkedList<>();
    this.customSortOrder = new LinkedList<>();
    this.customSortNullOrder = new LinkedList<>();
  }

  public DynamicPartitionCtx(Map<String, String> partSpec, int maxParts, String defaultPartitionName)
      throws SemanticException {
    this.partSpec = partSpec;
    this.spNames = new ArrayList<>();
    this.dpNames = new ArrayList<>();
    this.numBuckets = 0;
    this.maxPartsPerNode = maxParts;
    this.defaultPartName = defaultPartitionName;

    for (Map.Entry<String, String> me: partSpec.entrySet()) {
      if (me.getValue() == null) {
        dpNames.add(me.getKey());
      } else {
        spNames.add(me.getKey());
      }
    }
    this.numDPCols = dpNames.size();
    this.numSPCols = spNames.size();
    if (this.numSPCols > 0) {
      this.spPath = Warehouse.makeDynamicPartName(partSpec, defaultPartitionName);
    } else {
      this.spPath = null;
    }
    String confVal;
    try {
      confVal = Hive.get().getMetaConf(ConfVars.PARTITION_NAME_WHITELIST_PATTERN.getVarname());
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
    this.whiteListPattern = confVal == null || confVal.isEmpty() ? null : Pattern.compile(confVal);
    this.customSortExpressions = new LinkedList<>();
    this.customSortOrder = new LinkedList<>();
    this.customSortNullOrder = new LinkedList<>();
  }

  public DynamicPartitionCtx(DynamicPartitionCtx dp) {
    this.partSpec = dp.partSpec;
    this.numDPCols = dp.numDPCols;
    this.numSPCols = dp.numSPCols;
    this.spPath = dp.spPath;
    this.rootPath = dp.rootPath;
    this.numBuckets = dp.numBuckets;
    this.spNames = dp.spNames;
    this.dpNames = dp.dpNames;
    this.defaultPartName = dp.defaultPartName;
    this.maxPartsPerNode = dp.maxPartsPerNode;
    this.whiteListPattern = dp.whiteListPattern;
    this.customSortExpressions = new LinkedList<>();
    addCustomSortExpressions(dp.customSortExpressions);
    this.customSortOrder = dp.customSortOrder;
    this.customSortNullOrder = dp.customSortNullOrder;
  }

  public Pattern getWhiteListPattern() {
    return whiteListPattern;
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

  public void setRootPath(Path root) {
    this.rootPath = root;
  }

  public Path getRootPath() {
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

  public List<Function<List<ExprNodeDesc>, ExprNodeDesc>> getCustomSortExpressions() {
    return customSortExpressions;
  }

  public void addCustomSortExpressions(List<Function<List<ExprNodeDesc>, ExprNodeDesc>> customSortExpressions) {
    if (!CollectionUtils.isEmpty(customSortExpressions)) {
      this.hasCustomSortExpr = true;
      this.customSortExpressions.addAll(customSortExpressions);
    }
  }

  public List<Integer> getCustomSortOrder() {
    return customSortOrder;
  }

  public void setCustomSortOrder(List<Integer> customSortOrder) {
    this.customSortOrder = customSortOrder;
  }

  public List<Integer> getCustomSortNullOrder() {
    return customSortNullOrder;
  }

  public void setCustomSortNullOrder(List<Integer> customSortNullOrder) {
    this.customSortNullOrder = customSortNullOrder;
  }

  public boolean hasCustomSortExpression() {
    return hasCustomSortExpr;
  }
}
