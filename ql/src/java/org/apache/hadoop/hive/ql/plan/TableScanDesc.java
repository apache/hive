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

import org.apache.hadoop.hive.ql.exec.PTFUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;

/**
 * Table Scan Descriptor Currently, data is only read from a base source as part
 * of map-reduce framework. So, nothing is stored in the descriptor. But, more
 * things will be added here as table scan is invoked as part of local work.
 **/
@Explain(displayName = "TableScan")
public class TableScanDesc extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;

  static {
    PTFUtils.makeTransient(TableScanDesc.class, "filterObject", "referencedColumns", "tableMetadata");
  }

  private String alias;

  private List<VirtualColumn> virtualCols;
  private String statsAggKeyPrefix;   // stats publishing/aggregating key prefix

 /**
  * A list of the partition columns of the table.
  * Set by the semantic analyzer only in case of the analyze command.
  */
  private List<String> partColumns;

  /**
   * Used for split sampling (row count per split)
   * For example,
   *   select count(1) from ss_src2 tablesample (10 ROWS) s;
   * provides first 10 rows from all input splits
   */
  private int rowLimit = -1;

  /**
   * A boolean variable set to true by the semantic analyzer only in case of the analyze command.
   *
   */
  private boolean gatherStats;
  private boolean statsReliable;
  private int maxStatsKeyPrefixLength = -1;

  private ExprNodeGenericFuncDesc filterExpr;
  private transient Serializable filterObject;

  // Both neededColumnIDs and neededColumns should never be null.
  // When neededColumnIDs is an empty list,
  // it means no needed column (e.g. we do not need any column to evaluate
  // SELECT count(*) FROM t).
  private List<Integer> neededColumnIDs;
  private List<String> neededColumns;

  // all column names referenced including virtual columns. used in ColumnAccessAnalyzer
  private transient List<String> referencedColumns;

  public static final String FILTER_EXPR_CONF_STR =
    "hive.io.filter.expr.serialized";

  public static final String FILTER_TEXT_CONF_STR =
    "hive.io.filter.text";

  public static final String FILTER_OBJECT_CONF_STR =
    "hive.io.filter.object";

  // input file name (big) to bucket number
  private Map<String, Integer> bucketFileNameMapping;
  
  private boolean isMetadataOnly = false;

  private transient final Table tableMetadata;


  public TableScanDesc() {
    this(null, null);
  }

  @SuppressWarnings("nls")
  public TableScanDesc(Table tblMetadata) {
    this(null, tblMetadata);
  }

  public TableScanDesc(final String alias, Table tblMetadata) {
    this(alias, null, tblMetadata);
  }

  public TableScanDesc(final String alias, List<VirtualColumn> vcs, Table tblMetadata) {
    this.alias = alias;
    this.virtualCols = vcs;
    this.tableMetadata = tblMetadata;
  }

  @Override
  public Object clone() {
    List<VirtualColumn> vcs = new ArrayList<VirtualColumn>(getVirtualCols());
    return new TableScanDesc(getAlias(), vcs, this.tableMetadata);
  }

  @Explain(displayName = "alias")
  public String getAlias() {
    return alias;
  }

  @Explain(displayName = "filterExpr")
  public String getFilterExprString() {
    StringBuffer sb = new StringBuffer();
    PlanUtils.addExprToStringBuffer(filterExpr, sb);
    return sb.toString();
  }

  public ExprNodeGenericFuncDesc getFilterExpr() {
    return filterExpr;
  }

  public void setFilterExpr(ExprNodeGenericFuncDesc filterExpr) {
    this.filterExpr = filterExpr;
  }

  public Serializable getFilterObject() {
    return filterObject;
  }

  public void setFilterObject(Serializable filterObject) {
    this.filterObject = filterObject;
  }

  public void setNeededColumnIDs(List<Integer> neededColumnIDs) {
    this.neededColumnIDs = neededColumnIDs;
  }

  public List<Integer> getNeededColumnIDs() {
    return neededColumnIDs;
  }

  public void setNeededColumns(List<String> neededColumns) {
    this.neededColumns = neededColumns;
  }

  public List<String> getNeededColumns() {
    return neededColumns;
  }

  public void setReferencedColumns(List<String> referencedColumns) {
    this.referencedColumns = referencedColumns;
  }

  public List<String> getReferencedColumns() {
    return referencedColumns;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public void setPartColumns (List<String> partColumns) {
    this.partColumns = partColumns;
  }

  public List<String> getPartColumns () {
    return partColumns;
  }

  public void setGatherStats(boolean gatherStats) {
    this.gatherStats = gatherStats;
  }

  @Explain(displayName = "GatherStats", normalExplain = false)
  public boolean isGatherStats() {
    return gatherStats;
  }

  public List<VirtualColumn> getVirtualCols() {
    return virtualCols;
  }

  public void setVirtualCols(List<VirtualColumn> virtualCols) {
    this.virtualCols = virtualCols;
  }

  public void addVirtualCols(List<VirtualColumn> virtualCols) {
    this.virtualCols.addAll(virtualCols);
  }

  public boolean hasVirtualCols() {
    return virtualCols != null && !virtualCols.isEmpty();
  }

  public void setStatsAggPrefix(String k) {
    statsAggKeyPrefix = k;
  }

  @Explain(displayName = "Statistics Aggregation Key Prefix", normalExplain = false)
  public String getStatsAggPrefix() {
    return statsAggKeyPrefix;
  }

  public boolean isStatsReliable() {
    return statsReliable;
  }

  public void setStatsReliable(boolean statsReliable) {
    this.statsReliable = statsReliable;
  }

  public int getMaxStatsKeyPrefixLength() {
    return maxStatsKeyPrefixLength;
  }

  public void setMaxStatsKeyPrefixLength(int maxStatsKeyPrefixLength) {
    this.maxStatsKeyPrefixLength = maxStatsKeyPrefixLength;
  }

  public void setRowLimit(int rowLimit) {
    this.rowLimit = rowLimit;
  }

  public int getRowLimit() {
    return rowLimit;
  }

  @Explain(displayName = "Row Limit Per Split")
  public Integer getRowLimitExplain() {
    return rowLimit >= 0 ? rowLimit : null;
  }

  public Map<String, Integer> getBucketFileNameMapping() {
    return bucketFileNameMapping;
  }

  public void setBucketFileNameMapping(Map<String, Integer> bucketFileNameMapping) {
    this.bucketFileNameMapping = bucketFileNameMapping;
  }
  
  public void setIsMetadataOnly(boolean metadata_only) {
    isMetadataOnly = metadata_only;
  }

  public boolean getIsMetadataOnly() {
    return isMetadataOnly;
  }

  public Table getTableMetadata() {
    return tableMetadata;
  }
}
