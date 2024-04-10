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

package org.apache.hadoop.hive.ql.exec.vector.ptf;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.IntervalDayTimeColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContextRegion;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;
import org.apache.hadoop.hive.ql.plan.VectorPTFDesc;
import org.apache.hadoop.hive.ql.plan.VectorPTFInfo;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

/**
 * This class is native vectorized PTF operator class.
 */
public class VectorPTFOperator extends Operator<PTFDesc>
    implements VectorizationOperator, VectorizationContextRegion {

  private static final long serialVersionUID = 1L;
  private static final String CLASS_NAME = VectorPTFOperator.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  private int[] scratchColumnPositions;

  private VectorizationContext vContext;
  private VectorPTFDesc vectorDesc;

  /**
   * Information about our native vectorized PTF created by the Vectorizer class during
   * it decision process and useful for execution.
   */
  private VectorPTFInfo vectorPTFInfo;

  // This is the vectorized row batch description of the output of the native vectorized PTF
  // operator.  It is based on the incoming vectorization context.  Its projection may include
  // a mixture of input columns and new scratch columns (for the aggregation output).
  protected VectorizationContext vOutContext;

  private boolean isPartitionOrderBy;

  /**
   * PTF vector expressions.
   */

  private TypeInfo[] reducerBatchTypeInfos;

  private int[] outputProjectionColumnMap;
  private String[] outputColumnNames;
  private TypeInfo[] outputTypeInfos;
  private DataTypePhysicalVariation[] outputDataTypePhysicalVariations;

  private int evaluatorCount;
  private String[] evaluatorFunctionNames;

  private int[] orderColumnMap;
  private Type[] orderColumnVectorTypes;
  private VectorExpression[] orderExpressions;

  private int[] partitionColumnMap;
  private Type[] partitionColumnVectorTypes;
  private VectorExpression[] partitionExpressions;

  private int[] keyInputColumnMap;
  private int[] nonKeyInputColumnMap;

  // The above members are initialized by the constructor and must not be
  // transient.
  //---------------------------------------------------------------------------

  private transient boolean isLastGroupBatch;

  private transient VectorizedRowBatch overflowBatch;

  private transient VectorPTFGroupBatches groupBatches;

  private transient VectorPTFEvaluatorBase[] evaluators;

  private transient int[] streamingEvaluatorNums;

  private transient boolean allEvaluatorsAreStreaming;

  private transient boolean isFirstPartition;

  private transient boolean[] currentPartitionIsNull;
  private transient long[] currentPartitionLongs;
  private transient double[] currentPartitionDoubles;
  private transient byte[][] currentPartitionByteArrays;
  private transient int[] currentPartitionByteLengths;
  private transient HiveDecimalWritable[] currentPartitionDecimals;
  private transient Timestamp[] currentPartitionTimestamps;
  private transient HiveIntervalDayTime[] currentPartitionIntervalDayTimes;

  private transient int[] bufferedColumnMap;
  private transient TypeInfo[] bufferedTypeInfos;

  // For debug tracing: the name of the map or reduce task.
  private transient String taskName;

  // Debug display.
  private transient long batchCounter;

  //---------------------------------------------------------------------------

  /** Kryo ctor. */
  protected VectorPTFOperator() {
    super();
  }

  public VectorPTFOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  public VectorPTFOperator(CompilationOpContext ctx, OperatorDesc conf,
      VectorizationContext vContext, VectorDesc vectorDesc) throws HiveException {
    this(ctx);

    LOG.info("VectorPTF constructor");

    PTFDesc desc = (PTFDesc) conf;
    this.conf = desc;
    this.vectorDesc = (VectorPTFDesc) vectorDesc;
    vectorPTFInfo = this.vectorDesc.getVectorPTFInfo();
    this.vContext = vContext;

    reducerBatchTypeInfos = this.vectorDesc.getReducerBatchTypeInfos();

    isPartitionOrderBy = this.vectorDesc.getIsPartitionOrderBy();

    outputColumnNames = this.vectorDesc.getOutputColumnNames();
    outputTypeInfos = this.vectorDesc.getOutputTypeInfos();
    outputDataTypePhysicalVariations = this.vectorDesc.getOutputDataTypePhysicalVariations();
    outputProjectionColumnMap = vectorPTFInfo.getOutputColumnMap();

    /*
     * Create a new vectorization context to create a new projection, but keep
     * same output column manager must be inherited to track the scratch the columns.
     */
    vOutContext = new VectorizationContext(getName(), this.vContext);
    setupVOutContext();

    evaluatorFunctionNames = this.vectorDesc.getEvaluatorFunctionNames();
    evaluatorCount = evaluatorFunctionNames.length;

    orderColumnMap = vectorPTFInfo.getOrderColumnMap();
    orderColumnVectorTypes = vectorPTFInfo.getOrderColumnVectorTypes();
    orderExpressions = vectorPTFInfo.getOrderExpressions();

    partitionColumnMap = vectorPTFInfo.getPartitionColumnMap();
    partitionColumnVectorTypes = vectorPTFInfo.getPartitionColumnVectorTypes();
    partitionExpressions = vectorPTFInfo.getPartitionExpressions();

    keyInputColumnMap = vectorPTFInfo.getKeyInputColumnMap();
    nonKeyInputColumnMap = vectorPTFInfo.getNonKeyInputColumnMap();
  }

  /**
   * Setup the vectorized row batch description of the output of the native vectorized PTF
   * operator.  Use the output projection we previously built from a mixture of input
   * columns and new scratch columns.
   */
  protected void setupVOutContext() {
    vOutContext.resetProjectionColumns();
    final int count = outputColumnNames.length;
    for (int i = 0; i < count; ++i) {
      String columnName = outputColumnNames[i];
      int outputColumn = outputProjectionColumnMap[i];
      vOutContext.addProjectionColumn(columnName, outputColumn);
    }
  }

  /*
   * Allocate overflow batch columns by hand.
   */
  private static void allocateOverflowBatchColumnVector(VectorizedRowBatch overflowBatch,
      int outputColumn, String typeName, DataTypePhysicalVariation dataTypePhysicalVariation)
      throws HiveException {

    if (overflowBatch.cols[outputColumn] == null) {
      typeName = VectorizationContext.mapTypeNameSynonyms(typeName);

      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeName);

      overflowBatch.cols[outputColumn] =
          VectorizedBatchUtil.createColumnVector(typeInfo, dataTypePhysicalVariation);
    }
  }

  /*
   * Setup our 2nd batch with the same "column schema" as the output columns plus any scratch
   * columns since the overflow batch will get forwarded to children operators.
   *
   * Considering this query below:
   *
   * select p_mfgr, p_name,
   * count(*) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as cs1,
   * count(*) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as cs2
   * from vector_ptf_part_simple_orc;
   *
   * The overflow batch will have the following column structure:
   * [0] BytesColumnVector   -> p_mfgr (key)
   * [1] DateColumnVector    -> p_date (key, ordering col)
   * [2] BytesColumnVector   -> p_name (non-key)
   * [3] LongColumnVector    -> scratch
   * [4] LongColumnVector    -> scratch
   */
  @VisibleForTesting
  VectorizedRowBatch setupOverflowBatch(int firstOutputColumnIndex, String[] scratchColumnTypeNames,
      int[] outputProjectionColumnMap, TypeInfo[] outputTypeInfos) throws HiveException {

    int initialColumnCount = firstOutputColumnIndex;
    VectorizedRowBatch overflowBatch;

    int totalNumColumns = initialColumnCount + scratchColumnTypeNames.length;
    overflowBatch = new VectorizedRowBatch(totalNumColumns);

    // First, just allocate just the output columns we will be using.
    for (int i = 0; i < outputProjectionColumnMap.length; i++) {
      int outputColumn = outputProjectionColumnMap[i];
      String typeName = outputTypeInfos[i].getTypeName();
      allocateOverflowBatchColumnVector(overflowBatch, outputColumn, typeName,
          vOutContext.getDataTypePhysicalVariation(outputColumn));
    }

    // Now, add any scratch columns needed for children operators.
    int outputColumn = initialColumnCount;
    int s = 0;
    scratchColumnPositions = new int[scratchColumnTypeNames.length];
    for (String typeName : scratchColumnTypeNames) {
      allocateOverflowBatchColumnVector(overflowBatch, outputColumn, typeName,
          vOutContext.getDataTypePhysicalVariation(outputColumn));
      scratchColumnPositions[s] = outputColumn;
      outputColumn += 1;
      s += 1;
    }

    overflowBatch.projectedColumns = outputProjectionColumnMap;
    overflowBatch.projectionSize = outputProjectionColumnMap.length;

    overflowBatch.reset();

    return overflowBatch;
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);

    VectorExpression.doTransientInit(partitionExpressions, hconf);
    VectorExpression.doTransientInit(orderExpressions, hconf);

    if (LOG.isDebugEnabled()) {
      // Determine the name of our map or reduce task for debug tracing.
      BaseWork work = Utilities.getMapWork(hconf);
      if (work == null) {
        work = Utilities.getReduceWork(hconf);
      }
      taskName = work.getName();
    }

    final int partitionKeyCount = vectorDesc.getPartitionExprNodeDescs().length;
    currentPartitionIsNull = new boolean[partitionKeyCount];
    currentPartitionLongs = new long[partitionKeyCount];
    currentPartitionDoubles = new double[partitionKeyCount];
    currentPartitionByteArrays = new byte[partitionKeyCount][];
    currentPartitionByteLengths = new int[partitionKeyCount];
    currentPartitionDecimals = new HiveDecimalWritable[partitionKeyCount];
    currentPartitionTimestamps = new Timestamp[partitionKeyCount];
    currentPartitionIntervalDayTimes = new HiveIntervalDayTime[partitionKeyCount];

    /*
     * Setup the overflow batch.
     */
    overflowBatch = setupOverflowBatch(vContext.firstOutputColumnIndex(),
        vOutContext.getScratchColumnTypeNames(), outputProjectionColumnMap, outputTypeInfos);

    evaluators = VectorPTFDesc.getEvaluators(vectorDesc, vectorPTFInfo);
    for (VectorPTFEvaluatorBase evaluator : evaluators) {
      evaluator.setNullsLast(HiveConf.getBoolVar(hconf, HiveConf.ConfVars.HIVE_DEFAULT_NULLS_LAST));
    }

    streamingEvaluatorNums = VectorPTFDesc.getStreamingEvaluatorNums(evaluators);

    allEvaluatorsAreStreaming = (streamingEvaluatorNums.length == evaluatorCount);

    groupBatches = new VectorPTFGroupBatches(
        hconf, vectorDesc.getVectorizedPTFMaxMemoryBufferingBatchCount());

    initBufferedColumns();
    initExpressionColumns();
    int [] keyWithoutOrderColumnMap = determineKeyColumnsWithoutOrderColumns(keyInputColumnMap, orderColumnMap);
    groupBatches.init(
        evaluators,
        outputProjectionColumnMap,
        bufferedColumnMap,
        bufferedTypeInfos,
        orderColumnMap,
        keyWithoutOrderColumnMap,
        overflowBatch);

    isFirstPartition = true;

    batchCounter = 0;
  }

  @Override
  public void setNextVectorBatchGroupStatus(boolean isLastGroupBatch) throws HiveException {
    this.isLastGroupBatch = isLastGroupBatch;
  }

  /**
   * We are processing a batch from reduce processor that is only for one reducer key or PTF group.
   *
   * For a simple OVER (PARTITION BY column) or OVER (ORDER BY column), the reduce processor's
   * group key is the partition or order by key.
   *
   * For an OVER (PARTITION BY column1, ORDER BY column2), the reduce-shuffle group key is
   * the combination of the partition column1 and the order by column2.  In this case, this method
   * has to watch for changes in the partition and reset the group aggregations.
   *
   * The reduce processor calls setNextVectorBatchGroupStatus beforehand to tell us whether the
   * batch supplied to our process method is the last batch for the group key, or not.  This helps
   * us intelligently process the batch.
   */
  @Override
  public void process(Object row, int tag) throws HiveException {
    VectorizedRowBatch batch = (VectorizedRowBatch) row;

    for (VectorExpression orderExpression : orderExpressions) {
      orderExpression.evaluate(batch);
    }

    if (partitionExpressions != null) {
      for (VectorExpression partitionExpression : partitionExpressions) {
        partitionExpression.evaluate(batch);
      }
    }

    if (isFirstPartition) {
      isFirstPartition = false;
      setCurrentPartition(batch);
    } else if (isPartitionChanged(batch)) {
      /*
       * We should take care of evaluating the previous partition, but only if not all the
       * evaluators are streaming. If they are all streaming, are supposed to put their results into
       * the column vectors on the fly (in a streaming manner), this is handled later on by calling
       * groupBatches.evaluateStreamingGroupBatch for every single batch.
       */
      if (!allEvaluatorsAreStreaming){
        finishPartition(getPartitionKey());
      }
      setCurrentPartition(batch);
      groupBatches.resetEvaluators();
    }

    if (allEvaluatorsAreStreaming) {
      // We can process this batch immediately.
      groupBatches.evaluateStreamingGroupBatch(batch, isLastGroupBatch);
      forward(batch, null);
    } else {
      // only collecting the batch for later evaluation
      groupBatches.bufferGroupBatch(batch, isLastGroupBatch);
    }

    // If we are only processing a PARTITION BY and isLastGroupBatch, reset our evaluators.
    if (!isPartitionOrderBy && isLastGroupBatch) {
      groupBatches.resetEvaluators();
    }
  }

  private void initBufferedColumns() {
    /*
     * If we have more than one group key batch, we will buffer their contents.
     * We don't buffer the partitioning column since it's a constant for the whole partition.
     * We buffer:
     * 1. order columns
     * 2. the non-key input columns
     * 3. any streaming columns that will already have their output values
     * 4. scratch columns (as they're used by input expressions of evaluators)
     */
    int bufferedColumnCount = orderColumnMap.length + nonKeyInputColumnMap.length
        + streamingEvaluatorNums.length + scratchColumnPositions.length;
    this.bufferedColumnMap = new int[bufferedColumnCount];
    this.bufferedTypeInfos = new TypeInfo[bufferedColumnCount];

    int orderColumnCount = orderColumnMap.length;
    int nonKeyInputColumnCount = nonKeyInputColumnMap.length;
    int streamingEvaluatorCount = streamingEvaluatorNums.length;
    /*
     * In scenario 4. order column is not in keyInputColumnMap and is not referred in
     * reducerBatchTypeInfos, so we need to figure the TypeInfo out in an alternative way
     * (columnVectorTypeToTypeInfo). This is a workaround, maybe a compile-time solution would be
     * better.
     */
    for (int i = 0; i < orderColumnMap.length; i++) {
      final int columnNum = orderColumnMap[i];
      bufferedColumnMap[i] = columnNum;
      bufferedTypeInfos[i] = reducerBatchTypeInfos.length <= columnNum
        ? columnVectorTypeToTypeInfo(orderColumnVectorTypes[i]) : reducerBatchTypeInfos[columnNum];
    }

    for (int i = 0; i < nonKeyInputColumnMap.length; i++) {
      final int columnNum = nonKeyInputColumnMap[i];
      // ensure offsets in buffered map: [[order cols] [non-key input cols]]
      final int bufferedMapIndex = orderColumnCount + i;
      bufferedColumnMap[bufferedMapIndex] = columnNum;
      bufferedTypeInfos[bufferedMapIndex] = reducerBatchTypeInfos[columnNum];
    }

    for (int i = 0; i < streamingEvaluatorCount; i++) {
      final int streamingEvaluatorNum = streamingEvaluatorNums[i];
      // ensure offsets in buffered map: [[order cols] [non-key input cols] [streaming cols]]
      final int bufferedMapIndex = orderColumnCount + nonKeyInputColumnCount + i;
      bufferedColumnMap[bufferedMapIndex] = outputProjectionColumnMap[streamingEvaluatorNum];
      bufferedTypeInfos[bufferedMapIndex] = outputTypeInfos[streamingEvaluatorNum];
    }

    for (int i = 0; i < scratchColumnPositions.length; i++) {
      final int bufferedMapIndex = orderColumnCount + nonKeyInputColumnCount + streamingEvaluatorCount + i;
      bufferedColumnMap[bufferedMapIndex] = scratchColumnPositions[i];
      bufferedTypeInfos[bufferedMapIndex] = TypeInfoUtils.getTypeInfoFromTypeString(vOutContext.getScratchColumnTypeNames()[i]);
    }
  }

  private void initExpressionColumns() {
    for (int i = 0; i < evaluators.length; i++) {
      VectorPTFEvaluatorBase evaluator = evaluators[i];
      /*
       * Non-streaming evaluators work on buffered batches, we need to adapt them. Before PTF
       * bounded start vectorization (HIVE-24761), VectorExpression.outputColumnNum was closed and
       * VectorExpression.inputColumnNum didn't even exist (even though the vast majority of
       * VectorExpression subclasses use at least 1 input column). Since VectorPTFOperator and
       * VectorPTFGroupBatches work on modified batches (by not storing all the columns, and having
       * ordering columns first), the expressions planned in compile-time won't work with the
       * original config (column layout). It would make sense to move this logic to compile time,
       * because here in runtime, a very simple mapping (bufferedColumnMap) is used, so it might be
       * used. However, vectorized expression compilation affects many layers of code (having
       * VectorizationContext as the common scope), and moving the calculation of bufferedColumnMap
       * and this override logic to compile-time would create much more complicated behavior there
       * (probably involving hacking most of the time, or maybe a great re-design) just because of
       * the optimized column layout of the PTF vectorization.
       */
      if (!evaluator.streamsResult()) {
        evaluator.inputColumnNum = IntStream.range(0, bufferedColumnMap.length)
            .filter(j -> bufferedColumnMap[j] == evaluator.inputColumnNum).findFirst()
            .orElseGet(() -> evaluator.inputColumnNum);

        if (evaluator.inputVecExpr != null) {
          for (int j = 0; j <  evaluator.inputVecExpr.inputColumnNum.length; j++){
            final int jj = j; // need a final in stream filter
            evaluator.inputVecExpr.inputColumnNum[jj] = IntStream.range(0, bufferedColumnMap.length)
                .filter(k -> bufferedColumnMap[k] == evaluator.inputVecExpr.inputColumnNum[jj])
                .findFirst().orElseGet(() -> evaluator.inputVecExpr.inputColumnNum[jj]);
          }
          evaluator.inputVecExpr.outputColumnNum = IntStream.range(0, bufferedColumnMap.length)
              .filter(j -> bufferedColumnMap[j] == evaluator.inputVecExpr.outputColumnNum)
              .findFirst().orElseGet(() -> evaluator.inputVecExpr.outputColumnNum);
        }
        evaluator.mapCustomColumns(bufferedColumnMap);
      }
    }
  }

  /**
   * Let's say we have a typical ordering scenario where is 1 order column:
   * keyInputColumnMap: [0, 1] (-> key cols contain order cols)
   * orderColumnMap: [1]
   * The method returns the non-ordering key columns: [0]
   *
   * This is typically needed when while forwarding batches and want to fill
   * only partitioning but non-ordering columns from buffered batches to the overflow batch,
   * as the buffered batches already contain ordering column which is used for range calculation.
   *
   * Possible scenarios:
   * 1. explicit partitioning and ordering column
   * select p_mfgr, p_name, p_retailprice, count(*) over(partition by p_mfgr order by p_date)
   * keyInputColumnMap: [0, 1]
   * orderColumnMap: [1]
   *
   * 2a. no explicit ordering col
   * select p_mfgr, p_name, p_retailprice, count(*) over(partition by p_mfgr)
   * keyInputColumnMap: [0]
   * orderColumnMap: [0]
   *
   * 2b. no explicit ordering col, another column (keyInput is 0th again)
   * select p_mfgr, p_name, p_retailprice, count(*) over(partition by p_name)
   * keyInputColumnMap: [0]
   * orderColumnMap: [0]
   *
   * 3. no explicit partitioning col (= constant partition expression)
   * select p_mfgr, p_name, p_retailprice, count(*) over(order by p_date)
   * keyInputColumnMap: [1]
   * orderColumnMap: [1]
   *
   * 4. constant present on the partitioning column
   * select p_mfgr, p_name, p_retailprice, count(*) over(partition by p_mfgr)
   * from vector_ptf_part_simple_orc where p_mfgr = "Manufacturer#1";
   * partitionExpressions: [ConstantVectorExpression(val Manufacturer#1) -> 6:string]
   * orderExpressions: [ConstantVectorExpression(val Manufacturer#1) -> 7:string]
   * keyInputColumnMap: []
   * orderColumnMap: [7]
   * @param keyInputColumnMap
   * @param orderColumnMap
   * @return
   */
  private int[] determineKeyColumnsWithoutOrderColumns(int[] keyInputColumnMap,
      int[] orderColumnMap) {
    List<Integer> keyColumnsWithoutOrderColumns =
        IntStream.of(keyInputColumnMap).boxed().collect(Collectors.toList());
    List<Integer> orderColumns = IntStream.of(orderColumnMap).boxed().collect(Collectors.toList());
    keyColumnsWithoutOrderColumns.removeAll(orderColumns);

    return Ints.toArray(keyColumnsWithoutOrderColumns);
  }

  private static TypeInfo columnVectorTypeToTypeInfo(Type type) {
    switch (type) {
    case DOUBLE:
      return TypeInfoUtils.getTypeInfoFromTypeString(serdeConstants.DOUBLE_TYPE_NAME);
    case BYTES:
      return TypeInfoUtils.getTypeInfoFromTypeString(serdeConstants.STRING_TYPE_NAME);
    case DECIMAL:
      return TypeInfoUtils.getTypeInfoFromTypeString(serdeConstants.DECIMAL_TYPE_NAME);
    case TIMESTAMP:
      return TypeInfoUtils.getTypeInfoFromTypeString(serdeConstants.TIMESTAMP_TYPE_NAME);
    case LONG:
      return TypeInfoUtils.getTypeInfoFromTypeString(serdeConstants.INT_TYPE_NAME);
    default:
      throw new RuntimeException("Cannot convert column vector type: '" + type + "' to TypeInfo");
    }
  }

  private Object[] getPartitionKey() {
    final int count = partitionColumnMap.length;
    Object[] key = new Object[count];

    for (int i = 0; i < count; i++) {
      if (currentPartitionIsNull[i]) {
        key[i] = null;
        continue;
      }
      switch (partitionColumnVectorTypes[i]) {
      case LONG:
        key[i] = currentPartitionLongs[i];
        break;
      case DOUBLE:
        key[i] = currentPartitionDoubles[i];
        break;
      case BYTES:
        key[i] = new byte[currentPartitionByteLengths[i]];
        System.arraycopy(currentPartitionByteArrays[i], 0, key[i], 0,
            currentPartitionByteLengths[i]);
        break;
      case DECIMAL:
        key[i] = currentPartitionDecimals[i];
        break;
      case TIMESTAMP:
        key[i] = currentPartitionTimestamps[i];
        break;
      case INTERVAL_DAY_TIME:
        key[i] = currentPartitionIntervalDayTimes[i];
        break;
      default:
        throw new RuntimeException(
            "Unexpected column vector type " + partitionColumnVectorTypes[i]);
      }
    }

    return key;
  }

  private void finishPartition(Object[] partitionKey) throws HiveException {
    /*
     * Last group batch.
     *
     * Take the (non-streaming) group aggregation values and write output columns for all
     * rows of every batch of the group.  As each group batch is finished being written, they are
     * forwarded to the next operator.
     */
    groupBatches.finishPartition();
    groupBatches.fillGroupResultsAndForward(this, partitionKey);
    groupBatches.cleanupPartition();
  }

  private boolean isPartitionChanged(VectorizedRowBatch batch) {

    final int count = partitionColumnMap.length;
    for (int i = 0; i < count; i++) {
      ColumnVector colVector = batch.cols[partitionColumnMap[i]];

      // Vector reduce key (i.e. partition) columns are repeated -- so we test element 0.

      final boolean isNull = !colVector.noNulls && colVector.isNull[0];
      final boolean currentIsNull = currentPartitionIsNull[i];

      if (isNull != currentIsNull) {
        return true;
      }
      if (isNull) {
        // NULL does equal NULL here.
        continue;
      }

      switch (partitionColumnVectorTypes[i]) {
      case LONG:
        if (currentPartitionLongs[i] != ((LongColumnVector) colVector).vector[0]) {
          return true;
        }
        break;
      case DOUBLE:
        if (currentPartitionDoubles[i] != ((DoubleColumnVector) colVector).vector[0]) {
          return true;
        }
        break;
      case BYTES:
        {
          BytesColumnVector byteColVector = (BytesColumnVector) colVector;
          byte[] bytes = byteColVector.vector[0];
          final int start = byteColVector.start[0];
          final int length = byteColVector.length[0];
          if (!StringExpr.equal(
              bytes, start, length,
              currentPartitionByteArrays[i], 0, currentPartitionByteLengths[i])) {
            return true;
          }
        }
        break;
      case DECIMAL:
        if (!currentPartitionDecimals[i].equals(((DecimalColumnVector) colVector).vector[0])) {
          return true;
        }
        break;
      case TIMESTAMP:
        if (((TimestampColumnVector) colVector).compareTo(0, currentPartitionTimestamps[i]) != 0) {
          return true;
        }
        break;
      case INTERVAL_DAY_TIME:
        if (((IntervalDayTimeColumnVector) colVector).compareTo(0, currentPartitionIntervalDayTimes[i]) != 0) {
          return true;
        }
        break;
      default:
        throw new RuntimeException("Unexpected column vector type " + partitionColumnVectorTypes[i]);
      }
    }
    return false;
  }

  private void setCurrentPartition(VectorizedRowBatch batch) {

    final int count = partitionColumnMap.length;
    for (int i = 0; i < count; i++) {
      ColumnVector colVector = batch.cols[partitionColumnMap[i]];

      // Partition columns are repeated -- so we test element 0.

      final boolean isNull = !colVector.noNulls && colVector.isNull[0];
      currentPartitionIsNull[i] = isNull;

      if (isNull) {
        continue;
      }

      switch (partitionColumnVectorTypes[i]) {
      case LONG:
        currentPartitionLongs[i] = ((LongColumnVector) colVector).vector[0];
        break;
      case DOUBLE:
        currentPartitionDoubles[i] = ((DoubleColumnVector) colVector).vector[0];
        break;
      case BYTES:
        {
          BytesColumnVector byteColVector = (BytesColumnVector) colVector;
          byte[] bytes = byteColVector.vector[0];
          final int start = byteColVector.start[0];
          final int length = byteColVector.length[0];
          if (currentPartitionByteArrays[i] == null || currentPartitionByteLengths[i] < length) {
            currentPartitionByteArrays[i] = Arrays.copyOfRange(bytes, start, start + length);
          } else {
            System.arraycopy(bytes, start, currentPartitionByteArrays[i], 0, length);
          }
          currentPartitionByteLengths[i] = length;
        }
        break;
      case DECIMAL:
        if (currentPartitionDecimals[i] == null) {
          currentPartitionDecimals[i] = new HiveDecimalWritable();
        }
        currentPartitionDecimals[i].set(((DecimalColumnVector) colVector).vector[0]);
        break;
      case TIMESTAMP:
        if (currentPartitionTimestamps[i] == null) {
          currentPartitionTimestamps[i] = new Timestamp(0);
        }
        ((TimestampColumnVector) colVector).timestampUpdate(currentPartitionTimestamps[i], 0);
        break;
      case INTERVAL_DAY_TIME:
        if (currentPartitionIntervalDayTimes[i] == null) {
          currentPartitionIntervalDayTimes[i] = new HiveIntervalDayTime();
        }
        ((IntervalDayTimeColumnVector) colVector).intervalDayTimeUpdate(currentPartitionIntervalDayTimes[i], 0);
        break;
      default:
        throw new RuntimeException("Unexpected column vector type " + partitionColumnVectorTypes[i]);
      }
    }
  }

  /**
   * This package visible method can be called from VectorPTFGroupBatches. The usage of
   * vectorForward instead of forward is important in order to count the runtime rows (EXPLAIN
   * ANALYZE) correctly.
   *
   * @param batch
   * @throws HiveException
   */
  void forwardBatch(VectorizedRowBatch batch) throws HiveException {
    super.vectorForward(batch);
  }

  @Override
  protected void closeOp(boolean abort) throws HiveException {
    /*
     * Why would finishPartition be skipped here?
     * 1. abort: obviously
     * 2. allEvaluatorsAreStreaming: if all evaluators are streaming, we already evaluated
     * 3. isFirstPartition: if it's true, we haven't seen any records/batches in the operator
     */
    if (!abort && !allEvaluatorsAreStreaming && !isFirstPartition){
      finishPartition(getPartitionKey());
    }
    super.closeOp(abort);
  }

  /**
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "PTF";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.PTF;
  }

  @Override
  public VectorizationContext getOutputVectorizationContext() {
    return vOutContext;
  }

  @Override
  public VectorizationContext getInputVectorizationContext() {
    return vContext;
  }

  @Override
  public VectorDesc getVectorDesc() {
    return vectorDesc;
  }
}