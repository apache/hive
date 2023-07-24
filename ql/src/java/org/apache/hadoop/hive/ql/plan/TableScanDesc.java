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

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.signature.Signature;
import org.apache.hadoop.hive.ql.parse.TableSample;
import org.apache.hadoop.hive.ql.plan.BaseWork.BaseExplainVectorization;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.Explain.Vectorization;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.mapred.TextInputFormat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Table Scan Descriptor Currently, data is only read from a base source as part
 * of map-reduce framework. So, nothing is stored in the descriptor. But, more
 * things will be added here as table scan is invoked as part of local work.
 **/
@Explain(displayName = "TableScan", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class TableScanDesc extends AbstractOperatorDesc implements IStatsGatherDesc {
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
  private Serializable filterObject;
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
  private List<String> referencedColumns;

  public static final String FILTER_EXPR_CONF_STR =
      "hive.io.filter.expr.serialized";

  public static final String FILTER_TEXT_CONF_STR =
      "hive.io.filter.text";

  public static final String FILTER_OBJECT_CONF_STR =
      "hive.io.filter.object";

  public static final String PARTITION_PRUNING_FILTER =
      "hive.io.pruning.filter";

  public static final String AS_OF_TIMESTAMP =
      "hive.io.as.of.timestamp";

  public static final String AS_OF_VERSION =
      "hive.io.as.of.version";

  public static final String FROM_VERSION =
      "hive.io.version.from";

  public static final String SNAPSHOT_REF =
      "hive.io.snapshot.ref";

  // input file name (big) to bucket number
  private Map<String, Integer> bucketFileNameMapping;

  private String dbName = null;
  private String tableName = null;

  private boolean isMetadataOnly = false;

  private boolean isTranscationalTable;

  private boolean vectorized;

  private AcidUtils.AcidOperationalProperties acidOperationalProperties = null;

  private TableScanOperator.ProbeDecodeContext probeDecodeContext = null;

  private TableSample tableSample;

  private Table tableMetadata;

  private BitSet includedBuckets;

  private int numBuckets = -1;

  private String asOfVersion = null;
  private String versionIntervalFrom = null;

  private String asOfTimestamp = null;

  private String snapshotRef = null;

  public TableScanDesc() {
    this(null, null);
  }

  @SuppressWarnings("nls")
  public TableScanDesc(Table tblMetadata) {
    this(null, tblMetadata);
  }

  public TableScanDesc(final String alias, Table tblMetadata) {
    this(alias, null, tblMetadata, null);
  }

  public TableScanDesc(final String alias, List<VirtualColumn> vcs, Table tblMetadata) {
    this(alias, vcs, tblMetadata, null);
  }

  public TableScanDesc(final String alias, List<VirtualColumn> vcs, Table tblMetadata,
      TableScanOperator.ProbeDecodeContext probeDecodeContext) {
    this.alias = alias;
    this.virtualCols = vcs;
    this.tableMetadata = tblMetadata;

    if (tblMetadata != null) {
      dbName = tblMetadata.getDbName();
      tableName = tblMetadata.getTableName();
      numBuckets = tblMetadata.getNumBuckets();
      asOfTimestamp = tblMetadata.getAsOfTimestamp();
      asOfVersion = tblMetadata.getAsOfVersion();
      versionIntervalFrom = tblMetadata.getVersionIntervalFrom();
      snapshotRef = tblMetadata.getSnapshotRef();
    }
    isTranscationalTable = AcidUtils.isTransactionalTable(this.tableMetadata);
    if (isTranscationalTable) {
      acidOperationalProperties = AcidUtils.getAcidOperationalProperties(this.tableMetadata);
    }
    this.probeDecodeContext = probeDecodeContext;
  }

  @Override
  public Object clone() {
    List<VirtualColumn> vcs = new ArrayList<VirtualColumn>(getVirtualCols());
    return new TableScanDesc(getAlias(), vcs, this.tableMetadata, this.probeDecodeContext);
  }

  @Explain(displayName = "alias")
  public String getAlias() {
    return alias;
  }

  @Signature
  public String getPredicateString() {
    if (filterExpr == null) {
      return null;
    }
    return PlanUtils.getExprListString(Arrays.asList(filterExpr));
  }

  @Explain(displayName = "table", jsonOnly = true)
  public String getTableName() {
    return this.tableName;
  }

  @Explain(displayName = "database", jsonOnly = true)
  public String getDatabaseName() {
    return this.dbName;
  }

  @Explain(displayName = "columns", jsonOnly = true)
  public List<String> getColumnNamesForExplain() {
    return this.neededColumns;
  }

  @Explain(displayName = "isTempTable", jsonOnly = true)
  public boolean isTemporary() {
    return tableMetadata.isTemporary();
  }

  @Explain(explainLevels = { Level.USER })
  public String getTbl() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.tableMetadata.getCompleteName());
    sb.append("," + alias);
    if (AcidUtils.isFullAcidTable(tableMetadata)) {
      sb.append(", ACID table");
    } else if (isTranscationalTable()) {
      sb.append(", transactional table");
    }
    sb.append(",Tbl:");
    sb.append(this.statistics.getBasicStatsState());
    sb.append(",Col:");
    sb.append(this.statistics.getColumnStatsState());
    return sb.toString();
  }

  public boolean isTranscationalTable() {
    return isTranscationalTable;
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
    if (filterExpr == null) {
      return null;
    }
    return PlanUtils.getExprListString(Arrays.asList(filterExpr));
  }

  // @Signature // XXX
  public ExprNodeGenericFuncDesc getFilterExpr() {
    return filterExpr;
  }

  public void setFilterExpr(ExprNodeGenericFuncDesc filterExpr) {
    this.filterExpr = filterExpr;
  }

  @Explain(displayName = "probeDecodeDetails", explainLevels = { Level.DEFAULT, Level.EXTENDED })
  public String getProbeDecodeString() {
    if (probeDecodeContext == null) {
      return null;
    }
    return probeDecodeContext.toString();
  }

  public void setProbeDecodeContext(TableScanOperator.ProbeDecodeContext probeDecodeContext) {
    this.probeDecodeContext = probeDecodeContext;
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

  @Override
  @Explain(displayName = "GatherStats", explainLevels = { Level.EXTENDED })
  @Signature
  public boolean isGatherStats() {
    return gatherStats;
  }

  @Override
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

  @Override
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

  @Signature
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

  @Signature
  public String getQualifiedTable() {
    return dbName + "." + tableName;
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
    if (tableMetadata != null && tableMetadata.getTTable() != null
        && TextInputFormat.class
        .isAssignableFrom(tableMetadata.getInputFormatClass())) {
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

  @Override public Map<String, String> getOpProps() {
    return opProps;
  }

  @Explain(displayName = "properties", explainLevels = { Level.DEFAULT, Level.USER, Level.EXTENDED })
  public Map<String, String> getOpPropsWithStorageHandlerProps() {
    HiveStorageHandler storageHandler = tableMetadata.getStorageHandler();
    return storageHandler == null
            ? opProps
            : storageHandler.getOperatorDescProperties(this, opProps);
  }

  @Explain(displayName = "As of version")
  public String getAsOfVersion() {
    return asOfVersion;
  }

  @Explain(displayName = "Version interval from")
  public String getVersionIntervalFrom() {
    return versionIntervalFrom;
  }

  @Explain(displayName = "As of timestamp")
  public String getAsOfTimestamp() {
    return asOfTimestamp;
  }

  @Explain(displayName = "Snapshot ref")
  public String getSnapshotRef() {
    return snapshotRef;
  }

  public class TableScanOperatorExplainVectorization extends OperatorExplainVectorization {

    private final TableScanDesc tableScanDesc;
    private final VectorTableScanDesc vectorTableScanDesc;

    public TableScanOperatorExplainVectorization(TableScanDesc tableScanDesc,
        VectorTableScanDesc vectorTableScanDesc) {
      // Native vectorization supported.
      super(vectorTableScanDesc, true);
      this.tableScanDesc = tableScanDesc;
      this.vectorTableScanDesc = vectorTableScanDesc;
    }

    @Explain(vectorization = Vectorization.DETAIL, displayName = "vectorizationSchemaColumns", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getSchemaColumns() {
      String[] projectedColumnNames = vectorTableScanDesc.getProjectedColumnNames();
      TypeInfo[] projectedColumnTypeInfos = vectorTableScanDesc.getProjectedColumnTypeInfos();

      // We currently include all data, partition, and any vectorization available
      // virtual columns in the VRB.
      final int size = projectedColumnNames.length;
      int[] projectionColumns = new int[size];
      for (int i = 0; i < size; i++) {
        projectionColumns[i] = i;
      }

      DataTypePhysicalVariation[] projectedColumnDataTypePhysicalVariations =
          vectorTableScanDesc.getProjectedColumnDataTypePhysicalVariations();

      return BaseExplainVectorization.getColumnAndTypes(
          projectionColumns,
          projectedColumnNames,
          projectedColumnTypeInfos,
          projectedColumnDataTypePhysicalVariations).toString();
    }
  }

  @Explain(vectorization = Vectorization.OPERATOR, displayName = "TableScan Vectorization", explainLevels = { Level.DEFAULT, Level.EXTENDED })
  public TableScanOperatorExplainVectorization getTableScanVectorization() {
    VectorTableScanDesc vectorTableScanDesc = (VectorTableScanDesc) getVectorDesc();
    if (vectorTableScanDesc == null) {
      return null;
    }
    return new TableScanOperatorExplainVectorization(this, vectorTableScanDesc);
  }

  /*
   * This TableScanDesc flag is strictly set by the Vectorizer class for vectorized MapWork
   * vertices.
   */
  public void setVectorized(boolean vectorized) {
    this.vectorized = vectorized;
  }

  public boolean isVectorized() {
    return vectorized;
  }

  @Override
  public boolean isSame(OperatorDesc other) {
    if (getClass().getName().equals(other.getClass().getName())) {
      TableScanDesc otherDesc = (TableScanDesc) other;
      return Objects.equals(getQualifiedTable(), otherDesc.getQualifiedTable()) &&
          ExprNodeDescUtils.isSame(getFilterExpr(), otherDesc.getFilterExpr()) &&
          getRowLimit() == otherDesc.getRowLimit() &&
          isGatherStats() == otherDesc.isGatherStats();
    }
    return false;
  }

  public boolean isFullAcidTable() {
    return isTranscationalTable() && !getAcidOperationalProperties().isInsertOnly();
  }
}
