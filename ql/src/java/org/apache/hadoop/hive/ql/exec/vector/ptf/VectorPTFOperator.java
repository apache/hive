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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
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
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;
import org.apache.hadoop.hive.ql.plan.VectorPTFDesc;
import org.apache.hadoop.hive.ql.plan.VectorPTFInfo;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is native vectorized PTF operator class.
 */
public class VectorPTFOperator extends Operator<PTFDesc>
    implements VectorizationOperator, VectorizationContextRegion {

  private static final long serialVersionUID = 1L;
  private static final String CLASS_NAME = VectorPTFOperator.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

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

  private int evaluatorCount;
  private String[] evaluatorFunctionNames;
  private WindowFrameDef[] evaluatorWindowFrameDefs;
  private VectorExpression[] evaluatorInputExpressions;
  private Type[] evaluatorInputColumnVectorTypes;

  private ExprNodeDesc[] orderExprNodeDescs;
  private int[] orderColumnMap;
  private Type[] orderColumnVectorTypes;
  private VectorExpression[] orderExpressions;

  private ExprNodeDesc[] partitionExprNodeDescs;
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
    outputProjectionColumnMap = vectorPTFInfo.getOutputColumnMap();

    /*
     * Create a new vectorization context to create a new projection, but keep
     * same output column manager must be inherited to track the scratch the columns.
     */
    vOutContext = new VectorizationContext(getName(), this.vContext);
    setupVOutContext();

    evaluatorFunctionNames = this.vectorDesc.getEvaluatorFunctionNames();
    evaluatorCount = evaluatorFunctionNames.length;
    evaluatorWindowFrameDefs = this.vectorDesc.getEvaluatorWindowFrameDefs();
    evaluatorInputExpressions = vectorPTFInfo.getEvaluatorInputExpressions();
    evaluatorInputColumnVectorTypes = vectorPTFInfo.getEvaluatorInputColumnVectorTypes();

    orderExprNodeDescs = this.vectorDesc.getOrderExprNodeDescs();
    orderColumnMap = vectorPTFInfo.getOrderColumnMap();
    orderColumnVectorTypes = vectorPTFInfo.getOrderColumnVectorTypes();
    orderExpressions = vectorPTFInfo.getOrderExpressions();

    partitionExprNodeDescs = this.vectorDesc.getPartitionExprNodeDescs();
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
  private void allocateOverflowBatchColumnVector(VectorizedRowBatch overflowBatch, int outputColumn,
              String typeName) throws HiveException {

    if (overflowBatch.cols[outputColumn] == null) {
      typeName = VectorizationContext.mapTypeNameSynonyms(typeName);

      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeName);

      overflowBatch.cols[outputColumn] = VectorizedBatchUtil.createColumnVector(typeInfo);
    }
  }

  /*
   * Setup our 2nd batch with the same "column schema" as the output columns plus any scratch
   * columns since the overflow batch will get forwarded to children operators.
   */
  protected VectorizedRowBatch setupOverflowBatch() throws HiveException {

    int initialColumnCount = vContext.firstOutputColumnIndex();
    VectorizedRowBatch overflowBatch;

    int totalNumColumns = initialColumnCount + vOutContext.getScratchColumnTypeNames().length;
    overflowBatch = new VectorizedRowBatch(totalNumColumns);

    // First, just allocate just the output columns we will be using.
    for (int i = 0; i < outputProjectionColumnMap.length; i++) {
      int outputColumn = outputProjectionColumnMap[i];
      String typeName = outputTypeInfos[i].getTypeName();
      allocateOverflowBatchColumnVector(overflowBatch, outputColumn, typeName);
    }

    // Now, add any scratch columns needed for children operators.
    int outputColumn = initialColumnCount;
    for (String typeName : vOutContext.getScratchColumnTypeNames()) {
      allocateOverflowBatchColumnVector(overflowBatch, outputColumn++, typeName);
    }

    overflowBatch.projectedColumns = outputProjectionColumnMap;
    overflowBatch.projectionSize = outputProjectionColumnMap.length;

    overflowBatch.reset();

    return overflowBatch;
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);

    if (LOG.isDebugEnabled()) {
      // Determine the name of our map or reduce task for debug tracing.
      BaseWork work = Utilities.getMapWork(hconf);
      if (work == null) {
        work = Utilities.getReduceWork(hconf);
      }
      taskName = work.getName();
    }

    if (!isPartitionOrderBy) {
      currentPartitionIsNull = null;
      currentPartitionLongs = null;
      currentPartitionDoubles = null;
      currentPartitionByteArrays = null;
      currentPartitionByteLengths = null;
      currentPartitionDecimals = null;
      currentPartitionTimestamps = null;
      currentPartitionIntervalDayTimes = null;
    } else {
      final int partitionKeyCount = vectorDesc.getPartitionExprNodeDescs().length;
      currentPartitionIsNull = new boolean[partitionKeyCount];
      currentPartitionLongs = new long[partitionKeyCount];
      currentPartitionDoubles = new double[partitionKeyCount];
      currentPartitionByteArrays = new byte[partitionKeyCount][];
      currentPartitionByteLengths = new int[partitionKeyCount];
      currentPartitionDecimals = new HiveDecimalWritable[partitionKeyCount];
      currentPartitionTimestamps = new Timestamp[partitionKeyCount];
      currentPartitionIntervalDayTimes = new HiveIntervalDayTime[partitionKeyCount];
    }

    evaluators = VectorPTFDesc.getEvaluators(vectorDesc, vectorPTFInfo);

    streamingEvaluatorNums = VectorPTFDesc.getStreamingEvaluatorNums(evaluators);

    allEvaluatorsAreStreaming = (streamingEvaluatorNums.length == evaluatorCount);

    /*
     * Setup the overflow batch.
     */
    overflowBatch = setupOverflowBatch();

    groupBatches = new VectorPTFGroupBatches(
        hconf, vectorDesc.getVectorizedPTFMaxMemoryBufferingBatchCount());
    groupBatches.init(
        reducerBatchTypeInfos,
        evaluators,
        outputProjectionColumnMap,
        outputTypeInfos,
        keyInputColumnMap,
        nonKeyInputColumnMap,
        streamingEvaluatorNums,
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

     if (isPartitionOrderBy) {

      // Check for PARTITION BY key change when we have ORDER BY keys.
      if (isFirstPartition) {
        isFirstPartition = false;
        setCurrentPartition(batch);
      } else if (isPartitionChanged(batch)) {
        setCurrentPartition(batch);
        groupBatches.resetEvaluators();
      }
    }

    if (allEvaluatorsAreStreaming) {

      // We can process this batch immediately.
      groupBatches.evaluateStreamingGroupBatch(batch, isLastGroupBatch);
      forward(batch, null);

    } else {

      // Evaluate the aggregation functions over the group batch.
      groupBatches.evaluateGroupBatch(batch, isLastGroupBatch);

      if (!isLastGroupBatch) {

        // The group spans a VectorizedRowBatch.  Swap the relevant columns into our batch buffers,
        // or write the batch to temporary storage.
        groupBatches.bufferGroupBatch(batch);
        return;
      }

      /*
       * Last group batch.
       *
       * Take the (non-streaming) group aggregation values and write output columns for all
       * rows of every batch of the group.  As each group batch is finished being written, they are
       * forwarded to the next operator.
       */
      groupBatches.fillGroupResultsAndForward(this, batch);
    }

    // If we are only processing a PARTITION BY, reset our evaluators.
    if (!isPartitionOrderBy) {
      groupBatches.resetEvaluators();
    }
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

  @Override
  public void forward(Object row, ObjectInspector rowInspector) throws HiveException {
    super.forward(row, rowInspector);
  }

  @Override
  protected void closeOp(boolean abort) throws HiveException {
    super.closeOp(abort);

    // We do not try to finish and flush an in-progress group because correct values require the
    // last group batch.
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