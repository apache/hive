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

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.HashTableDummyOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedSupport.Support;
import org.apache.hadoop.hive.ql.parse.RuntimeValuesInfo;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.physical.VectorizerReason;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.Explain.Vectorization;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;


/**
 * BaseWork. Base class for any "work" that's being done on the cluster. Items like stats
 * gathering that are commonly used regardless of the type of work live here.
 */
@SuppressWarnings({"serial"})
public abstract class BaseWork extends AbstractOperatorDesc {
  protected static final Logger LOG = LoggerFactory.getLogger(BaseWork.class);

  // dummyOps is a reference to all the HashTableDummy operators in the
  // plan. These have to be separately initialized when we setup a task.
  // Their function is mainly as root ops to give the mapjoin the correct
  // schema info.
  List<HashTableDummyOperator> dummyOps;
  int tag = 0;
  private final List<String> sortColNames = new ArrayList<String>();

  private MapredLocalWork mrLocalWork;

  public BaseWork() {}

  public BaseWork(String name) {
    setName(name);
  }

  private boolean gatheringStats;

  private String name;

  /*
   * Vectorization.
   */

  // This will be true if a node was examined by the Vectorizer class.
  protected boolean vectorizationExamined;

  protected boolean vectorizationEnabled;

  protected VectorizedRowBatchCtx vectorizedRowBatchCtx;

  protected boolean useVectorizedInputFileFormat;

  protected Set<Support> inputFormatSupportSet;
  protected Set<Support> supportSetInUse;
  protected List<String> supportRemovedReasons;

  private transient VectorizerReason notVectorizedReason;

  private boolean groupByVectorOutput;
  private boolean allNative;
  private boolean usesVectorUDFAdaptor;

  protected long vectorizedVertexNum;
  protected int vectorizedTestingReducerBatchSize;

  private boolean isTestForcedVectorizationEnable;
  private boolean isTestVectorizationSuppressExplainExecutionMode;

  protected boolean llapMode = false;
  protected boolean uberMode = false;

  private int reservedMemoryMB = -1;  // default to -1 means we leave it up to Tez to decide

  // Used for value registry
  private Map<String, RuntimeValuesInfo> inputSourceToRuntimeValuesInfo =
          new HashMap<String, RuntimeValuesInfo>();

  public void setGatheringStats(boolean gatherStats) {
    this.gatheringStats = gatherStats;
  }

  public boolean isGatheringStats() {
    return this.gatheringStats;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<HashTableDummyOperator> getDummyOps() {
    return dummyOps;
  }

  public void setDummyOps(List<HashTableDummyOperator> dummyOps) {
    if (this.dummyOps != null && !this.dummyOps.isEmpty()
        && (dummyOps == null || dummyOps.isEmpty())) {
      LOG.info("Removing dummy operators from " + name + " " + this.getClass().getSimpleName());
    }
    this.dummyOps = dummyOps;
  }

  public void addDummyOp(HashTableDummyOperator dummyOp) {
    if (dummyOps == null) {
      dummyOps = new LinkedList<HashTableDummyOperator>();
    }
    dummyOps.add(dummyOp);
  }

  public abstract void replaceRoots(Map<Operator<?>, Operator<?>> replacementMap);

  public abstract Set<Operator<? extends OperatorDesc>> getAllRootOperators();
  public abstract Operator<? extends OperatorDesc> getAnyRootOperator();

  public Set<Operator<?>> getAllOperators() {

    Set<Operator<?>> returnSet = new LinkedHashSet<Operator<?>>();
    Set<Operator<?>> opSet = getAllRootOperators();
    Stack<Operator<?>> opStack = new Stack<Operator<?>>();

    // add all children
    opStack.addAll(opSet);

    while(!opStack.empty()) {
      Operator<?> op = opStack.pop();
      returnSet.add(op);
      if (op.getChildOperators() != null) {
        opStack.addAll(op.getChildOperators());
      }
    }

    return returnSet;
  }

  /**
   * Returns a set containing all leaf operators from the operator tree in this work.
   * @return a set containing all leaf operators in this operator tree.
   */
  public Set<Operator<? extends OperatorDesc>> getAllLeafOperators() {
    Set<Operator<?>> returnSet = new LinkedHashSet<Operator<?>>();
    Set<Operator<?>> opSet = getAllRootOperators();
    Stack<Operator<?>> opStack = new Stack<Operator<?>>();

    // add all children
    opStack.addAll(opSet);

    while (!opStack.empty()) {
      Operator<?> op = opStack.pop();
      if (op.getNumChild() == 0) {
        returnSet.add(op);
      }
      if (op.getChildOperators() != null) {
        opStack.addAll(op.getChildOperators());
      }
    }

    return returnSet;
  }

  public void setVectorizedVertexNum(long vectorizedVertexNum) {
    this.vectorizedVertexNum = vectorizedVertexNum;
  }

  public long getVectorizedVertexNum() {
    return vectorizedVertexNum;
  }

  public void setVectorizedTestingReducerBatchSize(int vectorizedTestingReducerBatchSize) {
    this.vectorizedTestingReducerBatchSize = vectorizedTestingReducerBatchSize;
  }

  public int getVectorizedTestingReducerBatchSize() {
    return vectorizedTestingReducerBatchSize;
  }

  // -----------------------------------------------------------------------------------------------

  public void setVectorizationExamined(boolean vectorizationExamined) {
    this.vectorizationExamined = vectorizationExamined;
  }

  public boolean getVectorizationExamined() {
    return vectorizationExamined;
  }

  public void setVectorizationEnabled(boolean vectorizationEnabled) {
    this.vectorizationEnabled = vectorizationEnabled;
  }

  public boolean getVectorizationEnabled() {
    return vectorizationEnabled;
  }

  /*
   * The vectorization context for creating the VectorizedRowBatch for the node.
   */
  public VectorizedRowBatchCtx getVectorizedRowBatchCtx() {
    return vectorizedRowBatchCtx;
  }

  public void setVectorizedRowBatchCtx(VectorizedRowBatchCtx vectorizedRowBatchCtx) {
    this.vectorizedRowBatchCtx = vectorizedRowBatchCtx;
  }

  public void setNotVectorizedReason(VectorizerReason notVectorizedReason) {
    this.notVectorizedReason = notVectorizedReason;
  }

  public VectorizerReason getNotVectorizedReason() {
    return notVectorizedReason;
  }

  public void setUsesVectorUDFAdaptor(boolean usesVectorUDFAdaptor) {
    this.usesVectorUDFAdaptor = usesVectorUDFAdaptor;
  }

  public boolean getUsesVectorUDFAdaptor() {
    return usesVectorUDFAdaptor;
  }

  public void setAllNative(boolean allNative) {
    this.allNative = allNative;
  }

  public boolean getAllNative() {
    return allNative;
  }

  public void setIsTestForcedVectorizationEnable(boolean isTestForcedVectorizationEnable) {
    this.isTestForcedVectorizationEnable = isTestForcedVectorizationEnable;
  }

  public boolean getIsTestForcedVectorizationEnable() {
    return isTestForcedVectorizationEnable;
  }

  public void setIsTestVectorizationSuppressExplainExecutionMode(
      boolean isTestVectorizationSuppressExplainExecutionMode) {
    this.isTestVectorizationSuppressExplainExecutionMode =
        isTestVectorizationSuppressExplainExecutionMode;
  }

  public boolean getIsTestVectorizationSuppressExplainExecutionMode() {
    return isTestVectorizationSuppressExplainExecutionMode;
  }

  public static class BaseExplainVectorization {

    private final BaseWork baseWork;

    public BaseExplainVectorization(BaseWork baseWork) {
      this.baseWork = baseWork;
    }

    public static List<String> getColumnAndTypes(
        int[] projectionColumns,
        String[] columnNames, TypeInfo[] typeInfos,
        DataTypePhysicalVariation[] dataTypePhysicalVariations) {
      final int size = columnNames.length;
      List<String> result = new ArrayList<String>(size);
      for (int i = 0; i < size; i++) {
        String displayString = projectionColumns[i] + ":" + columnNames[i] + ":" + typeInfos[i];
        if (dataTypePhysicalVariations != null &&
            dataTypePhysicalVariations[i] != DataTypePhysicalVariation.NONE) {
          displayString += "/" + dataTypePhysicalVariations[i].toString();
        }
        result.add(displayString);
      }
      return result;
    }

    @Explain(vectorization = Vectorization.SUMMARY, displayName = "enabled",
        explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public boolean enabled() {
      return baseWork.getVectorizationEnabled();
    }

    @Explain(vectorization = Vectorization.SUMMARY, displayName = "vectorized",
        explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public Boolean vectorized() {
      if (!baseWork.getVectorizationEnabled()) {
        return null;
      }
      return baseWork.getVectorMode();
    }

    @Explain(vectorization = Vectorization.SUMMARY, displayName = "notVectorizedReason",
        explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String notVectorizedReason() {
      if (!baseWork.getVectorizationEnabled() || baseWork.getVectorMode()) {
        return null;
      }
      VectorizerReason notVectorizedReason = baseWork.getNotVectorizedReason();
      if (notVectorizedReason ==  null) {
        return "Unknown";
      }
      return notVectorizedReason.toString();
    }

    @Explain(vectorization = Vectorization.SUMMARY, displayName = "allNative",
        explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public Boolean nativeVectorized() {
      if (!baseWork.getVectorMode()) {
        return null;
      }
      return baseWork.getAllNative();
    }

    public static List<String> getColumns(VectorizedRowBatchCtx vectorizedRowBatchCtx,
        int startIndex, int count) {
      String[] rowColumnNames = vectorizedRowBatchCtx.getRowColumnNames();
      TypeInfo[] rowColumnTypeInfos = vectorizedRowBatchCtx.getRowColumnTypeInfos();
      DataTypePhysicalVariation[]  dataTypePhysicalVariations =
          vectorizedRowBatchCtx.getRowdataTypePhysicalVariations();

      List<String> result = new ArrayList<String>(count);
      final int end = startIndex + count;
      for (int i = startIndex; i < end; i++) {
        String displayString = rowColumnNames[i] + ":" + rowColumnTypeInfos[i];
        if (dataTypePhysicalVariations != null &&
            dataTypePhysicalVariations[i] != DataTypePhysicalVariation.NONE) {
          displayString += "/" + dataTypePhysicalVariations[i].toString();
        }
        result.add(displayString);
      }
      return result;
    }

    public static String getScratchColumns(VectorizedRowBatchCtx vectorizedRowBatchCtx) {
      String[] scratchColumnTypeNames = vectorizedRowBatchCtx.getScratchColumnTypeNames();
      DataTypePhysicalVariation[] scratchDataTypePhysicalVariations =
          vectorizedRowBatchCtx.getScratchDataTypePhysicalVariations();
      final int size = scratchColumnTypeNames.length;
      List<String> result = new ArrayList<String>(size);
      for (int i = 0; i < size; i++) {
        String displayString = scratchColumnTypeNames[i];
        if (scratchDataTypePhysicalVariations != null &&
            scratchDataTypePhysicalVariations[i] != DataTypePhysicalVariation.NONE) {
          displayString += "/" + scratchDataTypePhysicalVariations[i].toString();
        }
        result.add(displayString);
      }
      return result.toString();
    }

    @Explain(vectorization = Vectorization.SUMMARY, displayName = "usesVectorUDFAdaptor",
        explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public Boolean usesVectorUDFAdaptor() {
      if (!baseWork.getVectorMode()) {
        return null;
      }
      return baseWork.getUsesVectorUDFAdaptor();
    }

    public static class RowBatchContextExplainVectorization {

      private final VectorizedRowBatchCtx vectorizedRowBatchCtx;

      public RowBatchContextExplainVectorization(VectorizedRowBatchCtx vectorizedRowBatchCtx) {
        this.vectorizedRowBatchCtx = vectorizedRowBatchCtx;
      }

      @Explain(vectorization = Vectorization.DETAIL, displayName = "dataColumns",
          explainLevels = { Level.DEFAULT, Level.EXTENDED })
      public List<String> getDataColumns() {
        return getColumns(
            vectorizedRowBatchCtx,
            0,
            vectorizedRowBatchCtx.getDataColumnCount());
      }

      @Explain(vectorization = Vectorization.DETAIL, displayName = "partitionColumns",
          explainLevels = { Level.DEFAULT, Level.EXTENDED })
      public List<String> getPartitionColumns() {
        return getColumns(
            vectorizedRowBatchCtx,
            vectorizedRowBatchCtx.getDataColumnCount(),
            vectorizedRowBatchCtx.getPartitionColumnCount());
      }

      @Explain(vectorization = Vectorization.DETAIL, displayName = "includeColumns",
          explainLevels = { Level.DEFAULT, Level.EXTENDED })
      public String getDataColumnNums() {
        int[] dataColumnNums = vectorizedRowBatchCtx.getDataColumnNums();
        if (dataColumnNums == null) {
          return null;
        }
        return Arrays.toString(dataColumnNums);
      }

      @Explain(vectorization = Vectorization.DETAIL, displayName = "dataColumnCount",
          explainLevels = { Level.DEFAULT, Level.EXTENDED })
      public int getDataColumnCount() {
        return vectorizedRowBatchCtx.getDataColumnCount();
      }

      @Explain(vectorization = Vectorization.DETAIL, displayName = "partitionColumnCount",
          explainLevels = { Level.DEFAULT, Level.EXTENDED })
      public int getPartitionColumnCount() {
        return vectorizedRowBatchCtx.getPartitionColumnCount();
      }

      @Explain(vectorization = Vectorization.DETAIL, displayName = "scratchColumnTypeNames",
          explainLevels = { Level.DEFAULT, Level.EXTENDED })
      public String getScratchColumnTypeNames() {
        return getScratchColumns(vectorizedRowBatchCtx);
      }

      @Explain(vectorization = Vectorization.DETAIL, displayName = "neededVirtualColumns",
          explainLevels = { Level.DEFAULT, Level.EXTENDED })
      public String getNeededVirtualColumns() {
        VirtualColumn[] neededVirtualColumns = vectorizedRowBatchCtx.getNeededVirtualColumns();
        if (neededVirtualColumns == null || neededVirtualColumns.length == 0) {
          return null;
        }
        return Arrays.toString(neededVirtualColumns);
      }

    }

    @Explain(vectorization = Vectorization.DETAIL, displayName = "rowBatchContext",
        explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public RowBatchContextExplainVectorization vectorizedRowBatchContext() {
      if (!baseWork.getVectorMode()) {
        return null;
      }
      return new RowBatchContextExplainVectorization(baseWork.getVectorizedRowBatchCtx());
    }
  }


  // -----------------------------------------------------------------------------------------------

  /**
   * @return the mapredLocalWork
   */
  @Explain(displayName = "Local Work", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public MapredLocalWork getMapRedLocalWork() {
    return mrLocalWork;
  }

  /**
   * @param mapLocalWork
   *          the mapredLocalWork to set
   */
  public void setMapRedLocalWork(final MapredLocalWork mapLocalWork) {
    this.mrLocalWork = mapLocalWork;
  }

  public void setUberMode(boolean uberMode) {
    this.uberMode = uberMode;
  }

  public boolean getUberMode() {
    return uberMode;
  }

  public void setLlapMode(boolean llapMode) {
    this.llapMode = llapMode;
  }

  public boolean getLlapMode() {
    return llapMode;
  }

  public int getReservedMemoryMB() {
    return reservedMemoryMB;
  }

  public void setReservedMemoryMB(int memoryMB) {
    reservedMemoryMB = memoryMB;
  }

  public void configureJobConf(JobConf job) {
    OperatorUtils.findOperators(getAllRootOperators(), FileSinkOperator.class).forEach(fs -> {
      LOG.debug("Configuring JobConf for table {}.{}", fs.getConf().getTableInfo().getDbName(),
          fs.getConf().getTableInfo().getTableName());
      PlanUtils.configureJobConf(fs.getConf().getTableInfo(), job);
    });
  }

  public void setTag(int tag) {
    this.tag = tag;
  }

  @Explain(displayName = "tag", explainLevels = { Level.USER })
  public int getTag() {
    return tag;
  }

  public void addSortCols(List<String> sortCols) {
    this.sortColNames.addAll(sortCols);
  }

  public List<String> getSortCols() {
    return sortColNames;
  }

  public Map<String, RuntimeValuesInfo> getInputSourceToRuntimeValuesInfo() {
    return inputSourceToRuntimeValuesInfo;
  }

  public void setInputSourceToRuntimeValuesInfo(
          String workName, RuntimeValuesInfo runtimeValuesInfo) {
    inputSourceToRuntimeValuesInfo.put(workName, runtimeValuesInfo);
  }
}
