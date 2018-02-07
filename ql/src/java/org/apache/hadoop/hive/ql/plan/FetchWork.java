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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.ListSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.parse.SplitSample;
import org.apache.hadoop.hive.ql.plan.BaseWork.BaseExplainVectorization;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.Explain.Vectorization;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * FetchWork.
 *
 */
@Explain(displayName = "Fetch Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED },
    vectorization = Vectorization.SUMMARY_PATH)
public class FetchWork implements Serializable {
  private static final long serialVersionUID = 1L;

  private Path tblDir;
  private TableDesc tblDesc;

  private ArrayList<Path> partDir;
  private ArrayList<PartitionDesc> partDesc;

  private Operator<?> source;
  private ListSinkOperator sink;

  private int limit;
  private int leastNumRows;

  private SplitSample splitSample;

  private transient List<List<Object>> rowsComputedFromStats;
  private transient StructObjectInspector statRowOI;

  /**
   * Serialization Null Format for the serde used to fetch data.
   */
  private String serializationNullFormat = "NULL";

  private boolean isHiveServerQuery;

  /**
   * Whether is a HiveServer query, and the destination table is
   * indeed written using ThriftJDBCBinarySerDe
   */
  private boolean isUsingThriftJDBCBinarySerDe = false;

  /**
   * Whether this FetchWork is returning a cached query result
   */
  private boolean isCachedResult = false;

  public boolean isHiveServerQuery() {
	return isHiveServerQuery;
  }

  public void setHiveServerQuery(boolean isHiveServerQuery) {
	this.isHiveServerQuery = isHiveServerQuery;
  }

  public boolean isUsingThriftJDBCBinarySerDe() {
	  return isUsingThriftJDBCBinarySerDe;
  }

  public void setIsUsingThriftJDBCBinarySerDe(boolean isUsingThriftJDBCBinarySerDe) {
	  this.isUsingThriftJDBCBinarySerDe = isUsingThriftJDBCBinarySerDe;
  }

  public FetchWork() {
  }

  public FetchWork(List<List<Object>> rowsComputedFromStats, StructObjectInspector statRowOI) {
    this.rowsComputedFromStats = rowsComputedFromStats;
    this.statRowOI = statRowOI;
  }

  public StructObjectInspector getStatRowOI() {
    return statRowOI;
  }

  public List<List<Object>> getRowsComputedUsingStats() {
    return rowsComputedFromStats;
  }

  public FetchWork(Path tblDir, TableDesc tblDesc) {
    this(tblDir, tblDesc, -1);
  }

  public FetchWork(Path tblDir, TableDesc tblDesc, int limit) {
    this.tblDir = tblDir;
    this.tblDesc = tblDesc;
    this.limit = limit;
  }

  public FetchWork(List<Path> partDir, List<PartitionDesc> partDesc, TableDesc tblDesc) {
    this(partDir, partDesc, tblDesc, -1);
  }

  public FetchWork(List<Path> partDir, List<PartitionDesc> partDesc,
      TableDesc tblDesc, int limit) {
    this.tblDesc = tblDesc;
    this.partDir = new ArrayList<Path>(partDir);
    this.partDesc = new ArrayList<PartitionDesc>(partDesc);
    this.limit = limit;
  }

  public void initializeForFetch(CompilationOpContext ctx) {
    if (source == null) {
      ListSinkDesc desc = new ListSinkDesc(serializationNullFormat);
      sink = (ListSinkOperator) OperatorFactory.get(ctx, desc);
      source = sink;
    }
  }

  public String getSerializationNullFormat() {
    return serializationNullFormat;
  }

  public void setSerializationNullFormat(String format) {
    serializationNullFormat = format;
  }

  public boolean isNotPartitioned() {
    return tblDir != null;
  }

  public boolean isPartitioned() {
    return tblDir == null;
  }

  /**
   * @return the tblDir
   */
  public Path getTblDir() {
    return tblDir;
  }

  /**
   * @param tblDir
   *          the tblDir to set
   */
  public void setTblDir(Path tblDir) {
    this.tblDir = tblDir;
  }

  /**
   * @return the tblDesc
   */
  public TableDesc getTblDesc() {
    return tblDesc;
  }

  /**
   * @param tblDesc
   *          the tblDesc to set
   */
  public void setTblDesc(TableDesc tblDesc) {
    this.tblDesc = tblDesc;
  }

  /**
   * @return the partDir
   */
  public ArrayList<Path> getPartDir() {
    return partDir;
  }

  /**
   * @param partDir
   *          the partDir to set
   */
  public void setPartDir(ArrayList<Path> partDir) {
    this.partDir = partDir;
  }

  /**
   * @return the partDesc
   */
  public ArrayList<PartitionDesc> getPartDesc() {
    return partDesc;
  }

  public List<Path> getPathLists() {
    return isPartitioned() ? partDir == null ?
        null : new ArrayList<Path>(partDir) : Arrays.asList(tblDir);
  }

  /**
   * Get Partition descriptors in sorted (ascending) order of partition directory
   *
   * @return the partDesc array list
   */
  @Explain(displayName = "Partition Description", explainLevels = { Level.EXTENDED })
  public ArrayList<PartitionDesc> getPartDescOrderedByPartDir() {
    ArrayList<PartitionDesc> partDescOrdered = partDesc;

    if (partDir != null && partDir.size() > 1) {
      if (partDesc == null || partDir.size() != partDesc.size()) {
        throw new RuntimeException(
            "Partition Directory list size doesn't match Partition Descriptor list size");
      }

      // Construct a sorted Map of Partition Dir - Partition Descriptor; ordering is based on
      // patition dir (map key)
      // Assumption: there is a 1-1 mapping between partition dir and partition descriptor lists
      TreeMap<Path, PartitionDesc> partDirToPartSpecMap = new TreeMap<Path, PartitionDesc>();
      for (int i = 0; i < partDir.size(); i++) {
        partDirToPartSpecMap.put(partDir.get(i), partDesc.get(i));
      }

      // Extract partition desc from sorted map (ascending order of part dir)
      partDescOrdered = new ArrayList<PartitionDesc>(partDirToPartSpecMap.values());
    }

    return partDescOrdered;
  }

  /**
   * @return the partDescs for paths
   */
  public List<PartitionDesc> getPartDescs(List<Path> paths) {
    List<PartitionDesc> parts = new ArrayList<PartitionDesc>(paths.size());
    for (Path path : paths) {
      parts.add(partDesc.get(partDir.indexOf(path.getParent())));
    }
    return parts;
  }

  /**
   * @param partDesc
   *          the partDesc to set
   */
  public void setPartDesc(ArrayList<PartitionDesc> partDesc) {
    this.partDesc = partDesc;
  }

  /**
   * @return the limit
   */
  @Explain(displayName = "limit", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public int getLimit() {
    return limit;
  }

  /**
   * @param limit
   *          the limit to set
   */
  public void setLimit(int limit) {
    this.limit = limit;
  }

  public int getLeastNumRows() {
    return leastNumRows;
  }

  public void setLeastNumRows(int leastNumRows) {
    this.leastNumRows = leastNumRows;
  }

  @Explain(displayName = "Processor Tree", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Operator<?> getSource() {
    return source;
  }

  public void setSource(Operator<?> source) {
    this.source = source;
  }

  public ListSinkOperator getSink() {
    return sink;
  }

  public void setSink(ListSinkOperator sink) {
    this.sink = sink;
  }

  public void setSplitSample(SplitSample splitSample) {
    this.splitSample = splitSample;
  }

  public SplitSample getSplitSample() {
    return splitSample;
  }

  @Override
  public String toString() {
    if (tblDir != null) {
      return new String("table = " + tblDir);
    }

    if (partDir == null) {
      return "null fetchwork";
    }

    String ret = "partition = ";
    for (Path part : partDir) {
      ret = ret.concat(part.toUri().toString());
    }

    return ret;
  }

  // -----------------------------------------------------------------------------------------------

  private boolean vectorizationExamined;

  public void setVectorizationExamined(boolean vectorizationExamined) {
    this.vectorizationExamined = vectorizationExamined;
  }

  public boolean getVectorizationExamined() {
    return vectorizationExamined;
  }

  public class FetchExplainVectorization {

    private final FetchWork fetchWork;

    public FetchExplainVectorization(FetchWork fetchWork) {
      this.fetchWork = fetchWork;
    }

    @Explain(vectorization = Vectorization.SUMMARY, displayName = "enabled", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public boolean enabled() {
      return false;
    }

    @Explain(vectorization = Vectorization.SUMMARY, displayName = "enabledConditionsNotMet", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> enabledConditionsNotMet() {
      return VectorizationCondition.getConditionsSupported(false);
    }
  }

  @Explain(vectorization = Vectorization.SUMMARY, displayName = "Fetch Vectorization", explainLevels = { Level.DEFAULT, Level.EXTENDED })
  public FetchExplainVectorization getMapExplainVectorization() {
    if (!getVectorizationExamined()) {
      return null;
    }
    return new FetchExplainVectorization(this);
  }
  @Explain(displayName = "Cached Query Result", displayOnlyOnTrue = true, explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean isCachedResult() {
    return isCachedResult;
  }

  public void setCachedResult(boolean isCachedResult) {
    this.isCachedResult = isCachedResult;
  }
}
