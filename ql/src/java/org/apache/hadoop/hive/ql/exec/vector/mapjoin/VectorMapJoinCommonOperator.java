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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.HashTableLoaderFactory;
import org.apache.hadoop.hive.ql.exec.HashTableLoader;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnMapping;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnOutputMapping;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnSourceMapping;
import org.apache.hadoop.hive.ql.exec.vector.VectorCopyRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorDeserializeRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContextRegion;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.optimized.VectorMapJoinOptimizedCreateHashTable;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTable;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastHashTableLoader;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableImplementationType;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinInfo;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinaryDeserializeRead;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import com.google.common.base.Preconditions;

/**
 * This class is common operator class for native vectorized map join.
 *
 * It contain common initialization logic.
 *
 * It is used by both inner and outer joins.
 */
public abstract class VectorMapJoinCommonOperator extends MapJoinOperator implements VectorizationContextRegion {
  private static final long serialVersionUID = 1L;

  //------------------------------------------------------------------------------------------------

  private static final String CLASS_NAME = VectorMapJoinCommonOperator.class.getName();
private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  protected abstract String getLoggingPrefix();

  // For debug tracing: information about the map or reduce task, operator, operator class, etc.
  protected transient String loggingPrefix;

  protected String getLoggingPrefix(String className) {
    if (loggingPrefix == null) {
      initLoggingPrefix(className);
    }
    return loggingPrefix;
  }

  protected void initLoggingPrefix(String className) {
    loggingPrefix = className;
  }

  //------------------------------------------------------------------------------------------------

  protected VectorMapJoinDesc vectorDesc;

  protected VectorMapJoinInfo vectorMapJoinInfo;

  // Whether this operator is an outer join.
  protected boolean isOuterJoin;

  // Position of the *single* native vector map join small table.
  protected byte posSingleVectorMapJoinSmallTable;

  // The incoming vectorization context.  It describes the input big table vectorized row batch.
  protected VectorizationContext vContext;

  // This is the vectorized row batch description of the output of the native vectorized map join
  // operator.  It is based on the incoming vectorization context.  Its projection may include
  // a mixture of input big table columns and new scratch columns.
  protected VectorizationContext vOutContext;

  // The output column projection of the vectorized row batch.  And, the type infos of the output
  // columns.
  protected int[] outputProjection;
  protected TypeInfo[] outputTypeInfos;

  // These are the vectorized batch expressions for filtering, key expressions, and value
  // expressions.
  protected VectorExpression[] bigTableFilterExpressions;
  protected VectorExpression[] bigTableKeyExpressions;
  protected VectorExpression[] bigTableValueExpressions;

  // This is map of which vectorized row batch columns are the big table key columns.  Since
  // we may have key expressions that produce new scratch columns, we need a mapping.
  // And, we have their type infos.
  protected int[] bigTableKeyColumnMap;
  protected String[] bigTableKeyColumnNames;
  protected TypeInfo[] bigTableKeyTypeInfos;

  // Similarly, this is map of which vectorized row batch columns are the big table value columns.
  // Since we may have value expressions that produce new scratch columns, we need a mapping.
  // And, we have their type infos.
  protected int[] bigTableValueColumnMap;
  protected String[] bigTableValueColumnNames;
  protected TypeInfo[] bigTableValueTypeInfos;

  // This is a mapping of which big table columns (input and key/value expressions) will be
  // part of the big table portion of the join output result.
  protected VectorColumnOutputMapping bigTableRetainedMapping;

  // This is a mapping of which keys will be copied from the big table (input and key expressions)
  // to the small table result portion of the output for outer join.
  protected VectorColumnOutputMapping bigTableOuterKeyMapping;

  // This is a mapping of the values in the small table hash table that will be copied to the
  // small table result portion of the output.  That is, a mapping of the LazyBinary field order
  // to output batch scratch columns for the small table portion.
  protected VectorColumnSourceMapping smallTableMapping;

  protected VectorColumnSourceMapping projectionMapping;

  // These are the output columns for the small table and the outer small table keys.
  protected int[] smallTableOutputVectorColumns;
  protected int[] bigTableOuterKeyOutputVectorColumns;

  // These are the columns in the big and small table that are ByteColumnVector columns.
  // We create data buffers for these columns so we can copy strings into those columns by value.
  protected int[] bigTableByteColumnVectorColumns;
  protected int[] smallTableByteColumnVectorColumns;

  // The above members are initialized by the constructor and must not be
  // transient.
  //---------------------------------------------------------------------------

  // The threshold where we should use a repeating vectorized row batch optimization for
  // generating join output results.
  protected transient boolean useOverflowRepeatedThreshold;
  protected transient int overflowRepeatedThreshold;

  // A helper object that efficiently copies the big table columns that are for the big table
  // portion of the join output.
  protected transient VectorCopyRow bigTableRetainedVectorCopy;

  // A helper object that efficiently copies the big table key columns (input or key expressions)
  // that appear in the small table portion of the join output for outer joins.
  protected transient VectorCopyRow bigTableVectorCopyOuterKeys;

  // This helper object deserializes LazyBinary format small table values into columns of a row
  // in a vectorized row batch.
  protected transient VectorDeserializeRow<LazyBinaryDeserializeRead> smallTableVectorDeserializeRow;

  // This a 2nd batch with the same "column schema" as the big table batch that can be used to
  // build join output results in.  If we can create some join output results in the big table
  // batch, we will for better efficiency (i.e. avoiding copying).  Otherwise, we will use the
  // overflow batch.
  protected transient VectorizedRowBatch overflowBatch;

  // A scratch batch that will be used to play back big table rows that were spilled
  // to disk for the Hybrid Grace hash partitioning.
  protected transient VectorizedRowBatch spillReplayBatch;

  // Whether the native vectorized map join operator has performed its common setup.
  protected transient boolean needCommonSetup;

  // Whether the native vectorized map join operator has performed its
  // native vector map join hash table setup.
  protected transient boolean needHashTableSetup;

  // The small table hash table for the native vectorized map join operator.
  protected transient VectorMapJoinHashTable vectorMapJoinHashTable;

  /** Kryo ctor. */
  protected VectorMapJoinCommonOperator() {
    super();
  }

  public VectorMapJoinCommonOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  public VectorMapJoinCommonOperator(CompilationOpContext ctx,
      VectorizationContext vContext, OperatorDesc conf) throws HiveException {
    super(ctx);

    MapJoinDesc desc = (MapJoinDesc) conf;
    this.conf = desc;
    vectorDesc = (VectorMapJoinDesc) desc.getVectorDesc();
    vectorMapJoinInfo = vectorDesc.getVectorMapJoinInfo();
    Preconditions.checkState(vectorMapJoinInfo != null);

    this.vContext = vContext;

    /*
     * Create a new vectorization context to create a new projection, but keep
     * same output column manager must be inherited to track the scratch the columns.
     */
    vOutContext = new VectorizationContext(getName(), this.vContext);

    order = desc.getTagOrder();
    posBigTable = (byte) desc.getPosBigTable();
    posSingleVectorMapJoinSmallTable = (order[0] == posBigTable ? order[1] : order[0]);
    isOuterJoin = !desc.getNoOuterJoin();

    Map<Byte, List<ExprNodeDesc>> filterExpressions = desc.getFilters();
    bigTableFilterExpressions = vContext.getVectorExpressions(filterExpressions.get(posBigTable),
        VectorExpressionDescriptor.Mode.FILTER);

    bigTableKeyColumnMap = vectorMapJoinInfo.getBigTableKeyColumnMap();
    bigTableKeyColumnNames = vectorMapJoinInfo.getBigTableKeyColumnNames();
    bigTableKeyTypeInfos = vectorMapJoinInfo.getBigTableKeyTypeInfos();
    bigTableKeyExpressions = vectorMapJoinInfo.getBigTableKeyExpressions();

    bigTableValueColumnMap = vectorMapJoinInfo.getBigTableValueColumnMap();
    bigTableValueColumnNames = vectorMapJoinInfo.getBigTableValueColumnNames();
    bigTableValueTypeInfos = vectorMapJoinInfo.getBigTableValueTypeInfos();
    bigTableValueExpressions = vectorMapJoinInfo.getBigTableValueExpressions();

    bigTableRetainedMapping = vectorMapJoinInfo.getBigTableRetainedMapping();

    bigTableOuterKeyMapping =  vectorMapJoinInfo.getBigTableOuterKeyMapping();

    smallTableMapping = vectorMapJoinInfo.getSmallTableMapping();

    projectionMapping = vectorMapJoinInfo.getProjectionMapping();

    determineCommonInfo(isOuterJoin);
  }

  protected void determineCommonInfo(boolean isOuter) throws HiveException {

    bigTableOuterKeyOutputVectorColumns = bigTableOuterKeyMapping.getOutputColumns();
    smallTableOutputVectorColumns = smallTableMapping.getOutputColumns();

    // Which big table and small table columns are ByteColumnVector and need have their data buffer
    // to be manually reset for some join result processing?

    bigTableByteColumnVectorColumns = getByteColumnVectorColumns(bigTableOuterKeyMapping);

    smallTableByteColumnVectorColumns = getByteColumnVectorColumns(smallTableMapping);

    outputProjection = projectionMapping.getOutputColumns();
    outputTypeInfos = projectionMapping.getTypeInfos();

    if (isLogDebugEnabled) {
      int[] orderDisplayable = new int[order.length];
      for (int i = 0; i < order.length; i++) {
        orderDisplayable[i] = (int) order[i];
      }
      LOG.debug(getLoggingPrefix() + " VectorMapJoinCommonOperator constructor order " + Arrays.toString(orderDisplayable));
      LOG.debug(getLoggingPrefix() + " VectorMapJoinCommonOperator constructor posBigTable " + (int) posBigTable);
      LOG.debug(getLoggingPrefix() + " VectorMapJoinCommonOperator constructor posSingleVectorMapJoinSmallTable " + (int) posSingleVectorMapJoinSmallTable);

      LOG.debug(getLoggingPrefix() + " VectorMapJoinCommonOperator constructor bigTableKeyColumnMap " + Arrays.toString(bigTableKeyColumnMap));
      LOG.debug(getLoggingPrefix() + " VectorMapJoinCommonOperator constructor bigTableKeyColumnNames " + Arrays.toString(bigTableKeyColumnNames));
      LOG.debug(getLoggingPrefix() + " VectorMapJoinCommonOperator constructor bigTableKeyTypeInfos " + Arrays.toString(bigTableKeyTypeInfos));

      LOG.debug(getLoggingPrefix() + " VectorMapJoinCommonOperator constructor bigTableValueColumnMap " + Arrays.toString(bigTableValueColumnMap));
      LOG.debug(getLoggingPrefix() + " VectorMapJoinCommonOperator constructor bigTableValueColumnNames " + Arrays.toString(bigTableValueColumnNames));
      LOG.debug(getLoggingPrefix() + " VectorMapJoinCommonOperator constructor bigTableValueTypeNames " + Arrays.toString(bigTableValueTypeInfos));

      LOG.debug(getLoggingPrefix() + " VectorMapJoinCommonOperator constructor bigTableRetainedMapping " + bigTableRetainedMapping.toString());

      LOG.debug(getLoggingPrefix() + " VectorMapJoinCommonOperator constructor bigTableOuterKeyMapping " + bigTableOuterKeyMapping.toString());

      LOG.debug(getLoggingPrefix() + " VectorMapJoinCommonOperator constructor smallTableMapping " + smallTableMapping.toString());

      LOG.debug(getLoggingPrefix() + " VectorMapJoinCommonOperator constructor bigTableByteColumnVectorColumns " + Arrays.toString(bigTableByteColumnVectorColumns));
      LOG.debug(getLoggingPrefix() + " VectorMapJoinCommonOperator constructor smallTableByteColumnVectorColumns " + Arrays.toString(smallTableByteColumnVectorColumns));

      LOG.debug(getLoggingPrefix() + " VectorMapJoinCommonOperator constructor outputProjection " + Arrays.toString(outputProjection));
      LOG.debug(getLoggingPrefix() + " VectorMapJoinCommonOperator constructor outputTypeInfos " + Arrays.toString(outputTypeInfos));
    }

    setupVOutContext(conf.getOutputColumnNames());
  }

  /**
   * Determine from a mapping which columns are BytesColumnVector columns.
   */
  private int[] getByteColumnVectorColumns(VectorColumnMapping mapping) {
    // Search mapping for any strings and return their output columns.
    ArrayList<Integer> list = new ArrayList<Integer>();
    int count = mapping.getCount();
    int[] outputColumns = mapping.getOutputColumns();
    TypeInfo[] typeInfos = mapping.getTypeInfos();
    for (int i = 0; i < count; i++) {
      int outputColumn = outputColumns[i];
      String typeName = typeInfos[i].getTypeName();
      if (VectorizationContext.isStringFamily(typeName)) {
        list.add(outputColumn);
      }
    }
    return ArrayUtils.toPrimitive(list.toArray(new Integer[0]));
  }

  /**
   * Setup the vectorized row batch description of the output of the native vectorized map join
   * operator.  Use the output projection we previously built from a mixture of input big table
   * columns and new scratch columns.
   */
  protected void setupVOutContext(List<String> outputColumnNames) {
    if (isLogDebugEnabled) {
      LOG.debug(getLoggingPrefix() + " VectorMapJoinCommonOperator constructor outputColumnNames " + outputColumnNames);
    }
    if (outputColumnNames.size() != outputProjection.length) {
      throw new RuntimeException("Output column names " + outputColumnNames + " length and output projection " + Arrays.toString(outputProjection) + " / " + Arrays.toString(outputTypeInfos) + " length mismatch");
    }
    vOutContext.resetProjectionColumns();
    for (int i = 0; i < outputColumnNames.size(); ++i) {
      String columnName = outputColumnNames.get(i);
      int outputColumn = outputProjection[i];
      vOutContext.addProjectionColumn(columnName, outputColumn);

      if (isLogDebugEnabled) {
        LOG.debug(getLoggingPrefix() + " VectorMapJoinCommonOperator constructor addProjectionColumn " + i + " columnName " + columnName + " outputColumn " + outputColumn);
      }
    }
  }

  /**
   * This override lets us substitute our own fast vectorized hash table loader.
   */
  @Override
  protected HashTableLoader getHashTableLoader(Configuration hconf) {
    VectorMapJoinDesc vectorDesc = (VectorMapJoinDesc) conf.getVectorDesc();
    HashTableImplementationType hashTableImplementationType = vectorDesc.hashTableImplementationType();
    HashTableLoader hashTableLoader;
    switch (vectorDesc.hashTableImplementationType()) {
    case OPTIMIZED:
      // Use the Tez hash table loader.
      hashTableLoader = HashTableLoaderFactory.getLoader(hconf);
      break;
    case FAST:
      // Use our specialized hash table loader.
      hashTableLoader = HiveConf.getVar(
          hconf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("spark") ?
          HashTableLoaderFactory.getLoader(hconf) : new VectorMapJoinFastHashTableLoader();
      break;
    default:
      throw new RuntimeException("Unknown vector map join hash table implementation type " + hashTableImplementationType.name());
    }
    return hashTableLoader;
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);

    /*
     * Get configuration parameters.
     */
    overflowRepeatedThreshold = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVE_VECTORIZATION_MAPJOIN_NATIVE_OVERFLOW_REPEATED_THRESHOLD);
    useOverflowRepeatedThreshold = (overflowRepeatedThreshold >= 0);


    /*
     * Create our vectorized copy row and deserialize row helper objects.
     */
    if (smallTableMapping.getCount() > 0) {
      smallTableVectorDeserializeRow =
          new VectorDeserializeRow<LazyBinaryDeserializeRead>(
              new LazyBinaryDeserializeRead(
                  smallTableMapping.getTypeInfos(),
                  /* useExternalBuffer */ true));
      smallTableVectorDeserializeRow.init(smallTableMapping.getOutputColumns());
    }

    if (bigTableRetainedMapping.getCount() > 0) {
      bigTableRetainedVectorCopy = new VectorCopyRow();
      bigTableRetainedVectorCopy.init(bigTableRetainedMapping);
    }

    if (bigTableOuterKeyMapping.getCount() > 0) {
      bigTableVectorCopyOuterKeys = new VectorCopyRow();
      bigTableVectorCopyOuterKeys.init(bigTableOuterKeyMapping);
    }

    /*
     * Setup the overflow batch.
     */
    overflowBatch = setupOverflowBatch();

    needCommonSetup = true;
    needHashTableSetup = true;

    if (isLogDebugEnabled) {
      int[] currentScratchColumns = vOutContext.currentScratchColumns();
      LOG.debug(getLoggingPrefix() + " VectorMapJoinCommonOperator initializeOp currentScratchColumns " + Arrays.toString(currentScratchColumns));

      StructObjectInspector structOutputObjectInspector = (StructObjectInspector) outputObjInspector;
      List<? extends StructField> fields = structOutputObjectInspector.getAllStructFieldRefs();
      int i = 0;
      for (StructField field : fields) {
        LOG.debug(getLoggingPrefix() + " VectorMapJoinCommonOperator initializeOp " + i + " field " + field.getFieldName() + " type " + field.getFieldObjectInspector().getTypeName());
        i++;
      }
    }
  }

  @Override
  protected void completeInitializationOp(Object[] os) throws HiveException {
    // setup mapJoinTables and serdes
    super.completeInitializationOp(os);

    VectorMapJoinDesc vectorDesc = (VectorMapJoinDesc) conf.getVectorDesc();
    HashTableImplementationType hashTableImplementationType = vectorDesc.hashTableImplementationType();
    switch (vectorDesc.hashTableImplementationType()) {
    case OPTIMIZED:
      {
        // Create our vector map join optimized hash table variation *above* the
        // map join table container.
        vectorMapJoinHashTable = VectorMapJoinOptimizedCreateHashTable.createHashTable(conf,
                mapJoinTables[posSingleVectorMapJoinSmallTable]);
      }
      break;

    case FAST:
      {
        // Get our vector map join fast hash table variation from the
        // vector map join table container.
        VectorMapJoinTableContainer vectorMapJoinTableContainer =
                (VectorMapJoinTableContainer) mapJoinTables[posSingleVectorMapJoinSmallTable];
        vectorMapJoinHashTable = vectorMapJoinTableContainer.vectorMapJoinHashTable();
      }
      break;
    default:
      throw new RuntimeException("Unknown vector map join hash table implementation type " + hashTableImplementationType.name());
    }
    LOG.info("Using " + vectorMapJoinHashTable.getClass().getSimpleName() + " from " + this.getClass().getSimpleName());
  }

  /*
   * Setup our 2nd batch with the same "column schema" as the big table batch that can be used to
   * build join output results in.
   */
  protected VectorizedRowBatch setupOverflowBatch() throws HiveException {

    int initialColumnCount = vContext.firstOutputColumnIndex();
    VectorizedRowBatch overflowBatch;

    int totalNumColumns = initialColumnCount + vOutContext.getScratchColumnTypeNames().length;
    overflowBatch = new VectorizedRowBatch(totalNumColumns);

    // First, just allocate just the projection columns we will be using.
    for (int i = 0; i < outputProjection.length; i++) {
      int outputColumn = outputProjection[i];
      String typeName = outputTypeInfos[i].getTypeName();
      allocateOverflowBatchColumnVector(overflowBatch, outputColumn, typeName);
    }

    // Now, add any scratch columns needed for children operators.
    int outputColumn = initialColumnCount;
    for (String typeName : vOutContext.getScratchColumnTypeNames()) {
      allocateOverflowBatchColumnVector(overflowBatch, outputColumn++, typeName);
    }

    overflowBatch.projectedColumns = outputProjection;
    overflowBatch.projectionSize = outputProjection.length;

    overflowBatch.reset();

    return overflowBatch;
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

      if (isLogDebugEnabled) {
        LOG.debug(getLoggingPrefix() + " VectorMapJoinCommonOperator initializeOp overflowBatch outputColumn " + outputColumn + " class " + overflowBatch.cols[outputColumn].getClass().getSimpleName());
      }
    }
  }

  /*
   * Common one time setup by native vectorized map join operator's processOp.
   */
  protected void commonSetup(VectorizedRowBatch batch) throws HiveException {

    if (isLogDebugEnabled) {
      LOG.debug("VectorMapJoinInnerCommonOperator commonSetup begin...");
      displayBatchColumns(batch, "batch");
      displayBatchColumns(overflowBatch, "overflowBatch");
    }

    // Make sure big table BytesColumnVectors have room for string values in the overflow batch...
    for (int column: bigTableByteColumnVectorColumns) {
      BytesColumnVector bytesColumnVector = (BytesColumnVector) overflowBatch.cols[column];
      bytesColumnVector.initBuffer();
    }

    // Make sure small table BytesColumnVectors have room for string values in the big table and
    // overflow batchs...
    for (int column: smallTableByteColumnVectorColumns) {
      BytesColumnVector bytesColumnVector = (BytesColumnVector) batch.cols[column];
      bytesColumnVector.initBuffer();
      bytesColumnVector = (BytesColumnVector) overflowBatch.cols[column];
      bytesColumnVector.initBuffer();
    }

    // Setup a scratch batch that will be used to play back big table rows that were spilled
    // to disk for the Hybrid Grace hash partitioning.
    spillReplayBatch = VectorizedBatchUtil.makeLike(batch);
  }

  protected void displayBatchColumns(VectorizedRowBatch batch, String batchName) {
    LOG.debug(getLoggingPrefix() + " VectorMapJoinCommonOperator commonSetup " + batchName + " column count " + batch.numCols);
    for (int column = 0; column < batch.numCols; column++) {
      LOG.debug(getLoggingPrefix() + " VectorMapJoinCommonOperator commonSetup " + batchName + "     column " + column + " type " + (batch.cols[column] == null ? "NULL" : batch.cols[column].getClass().getSimpleName()));
    }
  }

  @Override
  public OperatorType getType() {
    return OperatorType.MAPJOIN;
  }

  @Override
  public VectorizationContext getOuputVectorizationContext() {
    return vOutContext;
  }
}
