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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.ListSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.parse.SplitSample;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.Explain.Vectorization;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * FetchWork.
 */
@Explain(displayName = "Fetch Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED },
    vectorization = Vectorization.SUMMARY_PATH)
public class FetchWork implements Serializable {
  private static final long serialVersionUID = 1L;

  private final TableDesc tableDesc;
  private final Path tableDir;
  private final List<Path> partitionDirs;
  private final List<PartitionDesc> partitionDescs;
  private final List<List<Object>> rowsComputedFromStats;
  private final StructObjectInspector statRowOI;

  private int limit;
  private int leastNumRows;
  private Operator<?> source;
  private ListSinkOperator sink;
  private SplitSample splitSample;

  /**
   * Serialization Null Format for the serde used to fetch data.
   */
  private String serializationNullFormat = "NULL";

  private boolean isHiveServerQuery;

  /**
   * Whether is a HiveServer query, and the destination table is indeed written using ThriftJDBCBinarySerDe.
   */
  private boolean isUsingThriftJDBCBinarySerDe = false;

  /**
   * Whether this FetchWork is returning a cached query result.
   */
  private boolean isCachedResult = false;

  private Set<FileStatus> filesToFetch = null;

  public FetchWork(Path tableDir, TableDesc tableDesc) {
    this(tableDir, tableDesc, -1);
  }

  public FetchWork(Path tableDir, TableDesc tableDesc, int limit) {
    this(tableDesc, tableDir, null, null, null, null, limit);
  }

  public FetchWork(List<Path> partitionDirs, List<PartitionDesc> partitionDescs, TableDesc tableDesc) {
    this(tableDesc, null, partitionDirs, partitionDescs, null, null, -1);
  }

  public FetchWork(List<List<Object>> rowsComputedFromStats, StructObjectInspector statRowOI) {
    this(null, null, null, null, rowsComputedFromStats, statRowOI, -1);
  }

  private FetchWork(TableDesc tblDesc, Path tblDir, List<Path> partDir, List<PartitionDesc> partDesc,
      List<List<Object>> rowsComputedFromStats, StructObjectInspector statRowOI, int limit) {
    this.tableDesc = tblDesc;
    this.tableDir = tblDir;
    this.partitionDirs = partDir == null ? null : new ArrayList<Path>(partDir);
    this.partitionDescs = partDesc == null ? null : new ArrayList<PartitionDesc>(partDesc);
    this.rowsComputedFromStats = rowsComputedFromStats;
    this.statRowOI = statRowOI;
    this.limit = limit;
  }

  public TableDesc getTableDesc() {
    return tableDesc;
  }

  public Path getTableDir() {
    return tableDir;
  }

  public boolean isPartitioned() {
    return tableDir == null;
  }

  public boolean isNotPartitioned() {
    return tableDir != null;
  }

  public List<Path> getPartitionDirs() {
    return partitionDirs;
  }

  public List<PartitionDesc> getPartitionDescs() {
    return partitionDescs;
  }

  public List<List<Object>> getRowsComputedUsingStats() {
    return rowsComputedFromStats;
  }

  public StructObjectInspector getStatRowOI() {
    return statRowOI;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  @Explain(displayName = "limit", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public int getLimit() {
    return limit;
  }

  public void setLeastNumRows(int leastNumRows) {
    this.leastNumRows = leastNumRows;
  }

  public int getLeastNumRows() {
    return leastNumRows;
  }

  public void setSource(Operator<?> source) {
    this.source = source;
  }

  @Explain(displayName = "Processor Tree", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Operator<?> getSource() {
    return source;
  }

  public boolean isSourceTable() {
    return source != null && source instanceof TableScanOperator;
  }

  public void setSink(ListSinkOperator sink) {
    this.sink = sink;
  }

  public ListSinkOperator getSink() {
    return sink;
  }

  public void setSplitSample(SplitSample splitSample) {
    this.splitSample = splitSample;
  }

  public SplitSample getSplitSample() {
    return splitSample;
  }

  public void setSerializationNullFormat(String format) {
    serializationNullFormat = format;
  }

  public String getSerializationNullFormat() {
    return serializationNullFormat;
  }

  public void setHiveServerQuery(boolean isHiveServerQuery) {
    this.isHiveServerQuery = isHiveServerQuery;
  }

  public boolean isHiveServerQuery() {
    return isHiveServerQuery;
  }

  public void setIsUsingThriftJDBCBinarySerDe(boolean isUsingThriftJDBCBinarySerDe) {
    this.isUsingThriftJDBCBinarySerDe = isUsingThriftJDBCBinarySerDe;
  }

  public boolean isUsingThriftJDBCBinarySerDe() {
    return isUsingThriftJDBCBinarySerDe;
  }

  public void setCachedResult(boolean isCachedResult) {
    this.isCachedResult = isCachedResult;
  }

  @Explain(displayName = "Cached Query Result", displayOnlyOnTrue = true,
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean isCachedResult() {
    return isCachedResult;
  }

  public void setFilesToFetch(Set<FileStatus> filesToFetch) {
    this.filesToFetch = filesToFetch;
  }

  public Set<FileStatus> getFilesToFetch() {
    return filesToFetch;
  }

  public void initializeForFetch(CompilationOpContext ctx) {
    if (source == null) {
      ListSinkDesc desc = new ListSinkDesc(serializationNullFormat);
      sink = (ListSinkOperator) OperatorFactory.get(ctx, desc);
      source = sink;
    }
  }

  public List<Path> getPathLists() {
    return isPartitioned() ?
        partitionDirs == null ? null : new ArrayList<Path>(partitionDirs) :
        Arrays.asList(tableDir);
  }

  /**
   * Get Partition descriptors in sorted (ascending) order of partition directory.
   */
  @Explain(displayName = "Partition Description", explainLevels = { Level.EXTENDED })
  public List<PartitionDesc> getPartitionDescsOrderedByDir() {
    List<PartitionDesc> partDescOrdered = partitionDescs;

    if (partitionDirs != null && partitionDirs.size() > 1) {
      if (partitionDescs == null || partitionDirs.size() != partitionDescs.size()) {
        throw new RuntimeException(
            "Partition Directory list size doesn't match Partition Descriptor list size");
      }

      // Construct a sorted Map of Partition Dir - Partition Descriptor; ordering is based on patition dir (map key)
      // Assumption: there is a 1-1 mapping between partition dir and partition descriptor lists
      Map<Path, PartitionDesc> partDirToPartSpecMap = new TreeMap<Path, PartitionDesc>();
      for (int i = 0; i < partitionDirs.size(); i++) {
        partDirToPartSpecMap.put(partitionDirs.get(i), partitionDescs.get(i));
      }

      // Extract partition desc from sorted map (ascending order of part dir)
      partDescOrdered = new ArrayList<PartitionDesc>(partDirToPartSpecMap.values());
    }

    return partDescOrdered;
  }

  public List<PartitionDesc> getPartitionDescs(List<Path> paths) {
    List<PartitionDesc> parts = new ArrayList<PartitionDesc>(paths.size());
    for (Path path : paths) {
      parts.add(partitionDescs.get(partitionDirs.indexOf(path.getParent())));
    }
    return parts;
  }

  @Override
  public String toString() {
    if (tableDir != null) {
      return new String("table = " + tableDir);
    }

    if (partitionDirs == null) {
      return "null fetchwork";
    }

    String ret = "partition = ";
    for (Path part : partitionDirs) {
      ret = ret.concat(part.toUri().toString());
    }

    return ret;
  }
}
