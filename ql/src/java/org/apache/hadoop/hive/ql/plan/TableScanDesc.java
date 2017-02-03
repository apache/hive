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
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.TableSample;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.Explain.Vectorization;
import org.apache.hadoop.hive.serde.serdeConstants;

/**
 * Table Scan Descriptor Currently, data is only read from a base source as part
 * of map-reduce framework. So, nothing is stored in the descriptor. But, more
 * things will be added here as table scan is invoked as part of local work.
 **/
@Explain(displayName = "TableScan", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class TableScanDesc extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;

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
  private String tmpStatsDir;

  private ExprNodeGenericFuncDesc filterExpr;
  private transient Serializable filterObject;
  private String serializedFilterExpr;
  private String serializedFilterObject;


  // Both neededColumnIDs and neededColumns should never be null.
  // When neededColumnIDs is an empty list,
  // it means no needed column (e.g. we do not need any column to evaluate
  // SELECT count(*) FROM t).
  private List<Integer> neededColumnIDs;
  private List<String> neededColumns;
  private List<String> neededNestedColumnPaths;

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

  private boolean isAcidTable;

  private AcidUtils.AcidOperationalProperties acidOperationalProperties = null;

  private transient TableSample tableSample;

  private transient Table tableMetadata;

  private BitSet includedBuckets;

  private int numBuckets = -1;

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
    isAcidTable = AcidUtils.isAcidTable(this.tableMetadata);
    if (isAcidTable) {
      acidOperationalProperties = AcidUtils.getAcidOperationalProperties(this.tableMetadata);
    }
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

  @Explain(explainLevels = { Level.USER })
  public String getTbl() {
    StringBuffer sb = new StringBuffer();
    sb.append(this.tableMetadata.getCompleteName());
    sb.append("," + alias);
    if (isAcidTable()) {
      sb.append(", ACID table");
    }
    sb.append(",Tbl:");
    sb.append(this.statistics.getBasicStatsState());
    sb.append(",Col:");
    sb.append(this.statistics.getColumnStatsState());
    return sb.toString();
  }

  public boolean isAcidTable() {
    return isAcidTable;
  }

  public AcidUtils.AcidOperationalProperties getAcidOperationalProperties() {
    return acidOperationalProperties;
  }

  @Explain(displayName = "Output", explainLevels = { Level.USER })
  public List<String> getOutputColumnNames() {
    return this.neededColumns;
  }

  @Explain(displayName = "filterExpr")
  public String getFilterExprString() {
    return PlanUtils.getExprListString(Arrays.asList(filterExpr));
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

  public List<String> getNeededNestedColumnPaths() {
    return neededNestedColumnPaths;
  }

  public void setNeededNestedColumnPaths(List<String> neededNestedColumnPaths) {
    this.neededNestedColumnPaths = neededNestedColumnPaths;
  }

  public void setNeededColumns(List<String> neededColumns) {
    this.neededColumns = neededColumns;
  }

  public List<String> getNeededColumns() {
    return neededColumns;
  }

  @Explain(displayName = "Pruned Column Paths")
  public List<String> getPrunedColumnPaths() {
    List<String> result = new ArrayList<>();
    for (String p : neededNestedColumnPaths) {
      if (p.indexOf('.') >= 0) {
        result.add(p);
      }
    }
    return result;
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

  @Explain(displayName = "GatherStats", explainLevels = { Level.EXTENDED })
  public boolean isGatherStats() {
    return gatherStats;
  }

  public String getTmpStatsDir() {
    return tmpStatsDir;
  }

  public void setTmpStatsDir(String tmpStatsDir) {
    this.tmpStatsDir = tmpStatsDir;
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

  @Explain(displayName = "Statistics Aggregation Key Prefix", explainLevels = { Level.EXTENDED })
  public String getStatsAggPrefix() {
    return statsAggKeyPrefix;
  }

  public boolean isStatsReliable() {
    return statsReliable;
  }

  public void setStatsReliable(boolean statsReliable) {
    this.statsReliable = statsReliable;
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

  public void setTableMetadata(Table tableMetadata) {
    this.tableMetadata = tableMetadata;
  }

  public TableSample getTableSample() {
    return tableSample;
  }

  public void setTableSample(TableSample tableSample) {
    this.tableSample = tableSample;
  }

  public String getSerializedFilterExpr() {
    return serializedFilterExpr;
  }

  public void setSerializedFilterExpr(String serializedFilterExpr) {
    this.serializedFilterExpr = serializedFilterExpr;
  }

  public String getSerializedFilterObject() {
    return serializedFilterObject;
  }

  public void setSerializedFilterObject(String serializedFilterObject) {
    this.serializedFilterObject = serializedFilterObject;
  }

  public void setIncludedBuckets(BitSet bitset) {
    this.includedBuckets = bitset;
  }

  public BitSet getIncludedBuckets() {
    return this.includedBuckets;
  }

  @Explain(displayName = "buckets included", explainLevels = { Level.EXTENDED })
  public String getIncludedBucketExplain() {
    if (this.includedBuckets == null) {
      return null;
    }

    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i = 0; i < this.includedBuckets.size(); i++) {
      if (this.includedBuckets.get(i)) {
        sb.append(i);
        sb.append(',');
      }
    }
    sb.append(String.format("] of %d", numBuckets));
    return sb.toString();
  }

  public int getNumBuckets() {
    return numBuckets;
  }

  public void setNumBuckets(int numBuckets) {
    this.numBuckets = numBuckets;
  }

  public boolean isNeedSkipHeaderFooters() {
    boolean rtn = false;
    if (tableMetadata != null && tableMetadata.getTTable() != null) {
      Map<String, String> params = tableMetadata.getTTable().getParameters();
      if (params != null) {
        String skipHVal = params.get(serdeConstants.HEADER_COUNT);
        int hcount = skipHVal == null? 0 : Integer.parseInt(skipHVal);
        String skipFVal = params.get(serdeConstants.FOOTER_COUNT);
        int fcount = skipFVal == null? 0 : Integer.parseInt(skipFVal);
        rtn = (hcount != 0 || fcount !=0 );
      }
    }
    return rtn;
  }

  @Override
  @Explain(displayName = "properties", explainLevels = { Level.DEFAULT, Level.USER, Level.EXTENDED })
  public Map<String, String> getOpProps() {
    return opProps;
  }

  public class TableScanOperatorExplainVectorization extends OperatorExplainVectorization {

    private final TableScanDesc tableScanDesc;
    private final VectorTableScanDesc vectorTableScanDesc;

    public TableScanOperatorExplainVectorization(TableScanDesc tableScanDesc, VectorDesc vectorDesc) {
      // Native vectorization supported.
      super(vectorDesc, true);
      this.tableScanDesc = tableScanDesc;
      vectorTableScanDesc = (VectorTableScanDesc) vectorDesc;
    }

    @Explain(vectorization = Vectorization.EXPRESSION, displayName = "projectedOutputColumns", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getProjectedOutputColumns() {
      return Arrays.toString(vectorTableScanDesc.getProjectedOutputColumns());
    }
  }

  @Explain(vectorization = Vectorization.OPERATOR, displayName = "TableScan Vectorization", explainLevels = { Level.DEFAULT, Level.EXTENDED })
  public TableScanOperatorExplainVectorization getTableScanVectorization() {
    if (vectorDesc == null) {
      return null;
    }
    return new TableScanOperatorExplainVectorization(this, vectorDesc);
  }
}
