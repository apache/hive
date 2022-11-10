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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.HashTableLoaderFactory;
import org.apache.hadoop.hive.ql.exec.HashTableLoader;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnMapping;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnOutputMapping;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnSourceMapping;
import org.apache.hadoop.hive.ql.exec.vector.VectorCopyRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorDeserializeRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContextRegion;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.optimized.VectorMapJoinOptimizedCreateHashTable;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTable;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastHashTableLoader;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableImplementationType;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKind;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.VectorMapJoinVariation;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinInfo;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinaryDeserializeRead;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * This class is common operator class for native vectorized map join.
 *
 * It contain common initialization logic.
 *
 * It is used by both inner and outer joins.
 */
public abstract class VectorMapJoinCommonOperator extends MapJoinOperator
    implements VectorizationOperator, VectorizationContextRegion {
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

  protected VectorMapJoinVariation vectorMapJoinVariation;
  protected HashTableKind hashTableKind;
  protected HashTableKeyType hashTableKeyType;

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

  /*
   * NOTE:
   *    The Big Table key columns are from the key expressions.
   *    The Big Table value columns are from the getExpr(posBigTable) expressions.
   *    Any calculations needed for those will be scratch columns.
   *
   *    The Small Table key and value output columns are scratch columns.
   *
   * Big Table Retain Column Map / TypeInfos:
   *    Any Big Table Batch columns that will be in the output result.
   *    0, 1, ore more Column Nums and TypeInfos
   *
   * Non Outer Small Table Key Mapping:
   *    For non-[FULL] OUTER MapJoin, when Big Table key columns are not retained for the output
   *    result but are needed for the Small Table output result, they are put in this mapping
   *    as they are required for copying rows to the overflow batch.
   *
   * Outer Small Table Key Mapping
   *    For [FULL] OUTER MapJoin, the mapping for any Small Table key columns needed for the
   *    output result from the Big Table key columns.  The Big Table keys cannot be projected since
   *    on NOMATCH there must be a physical column present to hold the non-match NULL.
   *
   * Full Outer Small Table Key Mapping
   *    For FULL OUTER MapJoin, the mapping from any needed Small Table key columns to their area
   *    in the output result.
   *
   *    For deserializing a FULL OUTER non-match Small Table key into the output result.
   *    Can be partial or empty if some or all Small Table key columns are not retained.
   *
   * Small Table Value Mapping
   *    The mapping from Small Table value columns to their area in the output result.
   *
   *    For deserializing Small Table value into the output result.
   *
   *    It is the Small Table value index to output column numbers and TypeInfos.
   *    That is, a mapping of the LazyBinary field order to output batch scratch columns for the
   *       small table portion.
   *    Or, to use the output column nums for OUTER Small Table value NULLs.
   *
   */
  protected int[] bigTableRetainColumnMap;
  protected TypeInfo[] bigTableRetainTypeInfos;

  protected int[] nonOuterSmallTableKeyColumnMap;
  protected TypeInfo[] nonOuterSmallTableKeyTypeInfos;

  protected VectorColumnOutputMapping outerSmallTableKeyMapping;

  protected VectorColumnSourceMapping fullOuterSmallTableKeyMapping;

  protected VectorColumnSourceMapping smallTableValueMapping;

  // The MapJoin output result projection for both the Big Table input batch and the overflow batch.
  protected VectorColumnSourceMapping projectionMapping;

  // These are the output columns for the small table and the outer small table keys.
  protected int[] outerSmallTableKeyColumnMap;
  protected int[] smallTableValueColumnMap;

  // These are the columns in the big and small table that are ByteColumnVector columns.
  // We create data buffers for these columns so we can copy strings into those columns by value.
  protected int[] bigTableByteColumnVectorColumns;
  protected int[] nonOuterSmallTableKeyByteColumnVectorColumns;
  protected int[] outerSmallTableKeyByteColumnVectorColumns;
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

  // This helper object deserializes BinarySortable format small table keys into columns of a row
  // in a vectorized row batch.
  protected int[] allSmallTableKeyColumnNums;
  protected boolean[] allSmallTableKeyColumnIncluded;
  protected transient VectorDeserializeRow<BinarySortableDeserializeRead> smallTableKeyOuterVectorDeserializeRow;

  protected transient VectorCopyRow nonOuterSmallTableKeyVectorCopy;

  // UNDONE
  // A helper object that efficiently copies the big table key columns (input or key expressions)
  // that appear in the small table portion of the join output.
  protected transient VectorCopyRow outerSmallTableKeyVectorCopy;

  // This helper object deserializes LazyBinary format small table values into columns of a row
  // in a vectorized row batch.
  protected transient VectorDeserializeRow<LazyBinaryDeserializeRead> smallTableValueVectorDeserializeRow;

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

  // Whether the native vectorized map join operator has performed its first batch setup.
  protected transient boolean needFirstBatchSetup;

  // Whether the native vectorized map join operator has performed its
  // native vector map join hash table setup.
  protected transient boolean needHashTableSetup;

  // The small table hash table for the native vectorized map join operator.
  protected transient VectorMapJoinHashTable vectorMapJoinHashTable;

  protected transient long batchCounter;
  protected transient long rowCounter;

  /** Kryo ctor. */
  protected VectorMapJoinCommonOperator() {
    super();
  }

  public VectorMapJoinCommonOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  public VectorMapJoinCommonOperator(CompilationOpContext ctx, OperatorDesc conf,
      VectorizationContext vContext, VectorDesc vectorDesc) throws HiveException {
    super(ctx);

    MapJoinDesc desc = (MapJoinDesc) conf;
    this.conf = desc;
    this.vectorDesc = (VectorMapJoinDesc) vectorDesc;
    vectorMapJoinInfo = this.vectorDesc.getVectorMapJoinInfo();
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

    vectorMapJoinVariation = this.vectorDesc.getVectorMapJoinVariation();
    hashTableKind = this.vectorDesc.getHashTableKind();
    hashTableKeyType = this.vectorDesc.getHashTableKeyType();

    bigTableKeyColumnMap = vectorMapJoinInfo.getBigTableKeyColumnMap();
    bigTableKeyColumnNames = vectorMapJoinInfo.getBigTableKeyColumnNames();
    bigTableKeyTypeInfos = vectorMapJoinInfo.getBigTableKeyTypeInfos();
    bigTableKeyExpressions = vectorMapJoinInfo.getSlimmedBigTableKeyExpressions();

    bigTableValueColumnMap = vectorMapJoinInfo.getBigTableValueColumnMap();
    bigTableValueColumnNames = vectorMapJoinInfo.getBigTableValueColumnNames();
    bigTableValueTypeInfos = vectorMapJoinInfo.getBigTableValueTypeInfos();
    bigTableValueExpressions = vectorMapJoinInfo.getSlimmedBigTableValueExpressions();

    bigTableFilterExpressions = vectorMapJoinInfo.getBigTableFilterExpressions();

    bigTableRetainColumnMap = vectorMapJoinInfo.getBigTableRetainColumnMap();
    bigTableRetainTypeInfos = vectorMapJoinInfo.getBigTableRetainTypeInfos();

    nonOuterSmallTableKeyColumnMap = vectorMapJoinInfo.getNonOuterSmallTableKeyColumnMap();
    nonOuterSmallTableKeyTypeInfos = vectorMapJoinInfo.getNonOuterSmallTableKeyTypeInfos();

    outerSmallTableKeyMapping = vectorMapJoinInfo.getOuterSmallTableKeyMapping();

    fullOuterSmallTableKeyMapping = vectorMapJoinInfo.getFullOuterSmallTableKeyMapping();

    smallTableValueMapping = vectorMapJoinInfo.getSmallTableValueMapping();

    projectionMapping = vectorMapJoinInfo.getProjectionMapping();

    determineCommonInfo(isOuterJoin);
  }

  protected void determineCommonInfo(boolean isOuter) throws HiveException {

    outerSmallTableKeyColumnMap = outerSmallTableKeyMapping.getOutputColumns();

    smallTableValueColumnMap = smallTableValueMapping.getOutputColumns();

    // Which big table and small table columns are ByteColumnVector and need have their data buffer
    // to be manually reset for some join result processing?

    bigTableByteColumnVectorColumns =
        getByteColumnVectorColumns(bigTableRetainColumnMap, bigTableRetainTypeInfos);

    nonOuterSmallTableKeyByteColumnVectorColumns =
        getByteColumnVectorColumns(nonOuterSmallTableKeyColumnMap, nonOuterSmallTableKeyTypeInfos);

    outerSmallTableKeyByteColumnVectorColumns =
        getByteColumnVectorColumns(outerSmallTableKeyMapping);

    smallTableByteColumnVectorColumns =
        getByteColumnVectorColumns(smallTableValueMapping);

    outputProjection = projectionMapping.getOutputColumns();
    outputTypeInfos = projectionMapping.getTypeInfos();

    if (LOG.isInfoEnabled()) {
      int[] orderDisplayable = new int[order.length];
      for (int i = 0; i < order.length; i++) {
        orderDisplayable[i] = (int) order[i];
      }
      LOG.info(getLoggingPrefix() + " order " +
          Arrays.toString(orderDisplayable));
      LOG.info(getLoggingPrefix() + " posBigTable " +
          (int) posBigTable);
      LOG.info(getLoggingPrefix() + " posSingleVectorMapJoinSmallTable " +
          (int) posSingleVectorMapJoinSmallTable);

      LOG.info(getLoggingPrefix() + " bigTableKeyColumnMap " +
          Arrays.toString(bigTableKeyColumnMap));
      LOG.info(getLoggingPrefix() + " bigTableKeyColumnNames " +
          Arrays.toString(bigTableKeyColumnNames));
      LOG.info(getLoggingPrefix() + " bigTableKeyTypeInfos " +
          Arrays.toString(bigTableKeyTypeInfos));

      LOG.info(getLoggingPrefix() + " bigTableValueColumnMap " +
          Arrays.toString(bigTableValueColumnMap));
      LOG.info(getLoggingPrefix() + " bigTableValueColumnNames " +
          Arrays.toString(bigTableValueColumnNames));
      LOG.info(getLoggingPrefix() + " bigTableValueTypeNames " +
          Arrays.toString(bigTableValueTypeInfos));

      LOG.info(getLoggingPrefix() + " getBigTableRetainColumnMap " +
          Arrays.toString(bigTableRetainColumnMap));
      LOG.info(getLoggingPrefix() + " bigTableRetainTypeInfos " +
          Arrays.toString(bigTableRetainTypeInfos));

      LOG.info(getLoggingPrefix() + " nonOuterSmallTableKeyColumnMap " +
          Arrays.toString(nonOuterSmallTableKeyColumnMap));
      LOG.info(getLoggingPrefix() + " nonOuterSmallTableKeyTypeInfos " +
          Arrays.toString(nonOuterSmallTableKeyTypeInfos));

      LOG.info(getLoggingPrefix() + " outerSmallTableKeyMapping " +
          outerSmallTableKeyMapping.toString());

      LOG.info(getLoggingPrefix() + " fullOuterSmallTableKeyMapping " +
          fullOuterSmallTableKeyMapping.toString());

      LOG.info(getLoggingPrefix() + " smallTableValueMapping " +
          smallTableValueMapping.toString());

      LOG.info(getLoggingPrefix() + " bigTableByteColumnVectorColumns " +
          Arrays.toString(bigTableByteColumnVectorColumns));
      LOG.info(getLoggingPrefix() + " smallTableByteColumnVectorColumns " +
          Arrays.toString(smallTableByteColumnVectorColumns));

      LOG.info(getLoggingPrefix() + " outputProjection " +
          Arrays.toString(outputProjection));
      LOG.info(getLoggingPrefix() + " outputTypeInfos " +
          Arrays.toString(outputTypeInfos));

      LOG.info(getLoggingPrefix() + " mapJoinDesc.getKeysString " +
          conf.getKeysString());
      if (conf.getValueIndices() != null) {
        for (Entry<Byte, int[]> entry : conf.getValueIndices().entrySet()) {
          LOG.info(getLoggingPrefix() + " mapJoinDesc.getValueIndices +"
              + (int) entry.getKey() + " " + Arrays.toString(entry.getValue()));
        }
      }
      LOG.info(getLoggingPrefix() + " mapJoinDesc.getExprs " +
          conf.getExprs().toString());
      LOG.info(getLoggingPrefix() + " mapJoinDesc.getRetainList " +
          conf.getRetainList().toString());

    }

    setupVOutContext(conf.getOutputColumnNames());
  }

  /**
   * Determine from a mapping which columns are BytesColumnVector columns.
   */
  private int[] getByteColumnVectorColumns(VectorColumnMapping mapping) {
    return getByteColumnVectorColumns(mapping.getOutputColumns(), mapping.getTypeInfos());
  }

  private int[] getByteColumnVectorColumns(int[] outputColumns, TypeInfo[] typeInfos) {

    // Search mapping for any strings and return their output columns.
    ArrayList<Integer> list = new ArrayList<Integer>();
    final int count = outputColumns.length;
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
    if (LOG.isDebugEnabled()) {
      LOG.debug(getLoggingPrefix() + " outputColumnNames " + outputColumnNames);
    }
    if (outputColumnNames.size() != outputProjection.length) {
      throw new RuntimeException("Output column names " + outputColumnNames +
          " length and output projection " + Arrays.toString(outputProjection) +
          " / " + Arrays.toString(outputTypeInfos) + " length mismatch");
    }
    vOutContext.resetProjectionColumns();
    for (int i = 0; i < outputColumnNames.size(); ++i) {
      String columnName = outputColumnNames.get(i);
      int outputColumn = outputProjection[i];
      vOutContext.addProjectionColumn(columnName, outputColumn);

      if (LOG.isDebugEnabled()) {
        LOG.debug(getLoggingPrefix() + " addProjectionColumn " + i + " columnName " + columnName +
            " outputColumn " + outputColumn);
      }
    }
  }

  /**
   * This override lets us substitute our own fast vectorized hash table loader.
   */
  @Override
  protected HashTableLoader getHashTableLoader(Configuration hconf) {
    HashTableImplementationType hashTableImplementationType = vectorDesc.getHashTableImplementationType();
    HashTableLoader hashTableLoader;
    switch (vectorDesc.getHashTableImplementationType()) {
    case OPTIMIZED:
      // Use the Tez hash table loader.
      hashTableLoader = HashTableLoaderFactory.getLoader(hconf);
      break;
    case FAST:
      // Use our specialized hash table loader.
      hashTableLoader = new VectorMapJoinFastHashTableLoader();
      break;
    default:
      throw new RuntimeException("Unknown vector map join hash table implementation type " + hashTableImplementationType.name());
    }
    return hashTableLoader;
  }

  /*
   * Do FULL OUTER MapJoin operator initialization.
   */
  private void initializeFullOuterObjects() throws HiveException {

    // The Small Table key type jnfo is the same as Big Table's.
    TypeInfo[] smallTableKeyTypeInfos = bigTableKeyTypeInfos;
    final int allKeysSize = smallTableKeyTypeInfos.length;

    /*
     * The VectorMapJoinFullOuter{Long|MultiKey|String}Operator outputs 0, 1, or more
     * Small Key columns in the join result.
     */
    allSmallTableKeyColumnNums = new int[allKeysSize];
    Arrays.fill(allSmallTableKeyColumnNums, -1);
    allSmallTableKeyColumnIncluded = new boolean[allKeysSize];

    final int outputKeysSize = fullOuterSmallTableKeyMapping.getCount();
    int[] outputKeyNums = fullOuterSmallTableKeyMapping.getInputColumns();
    int[] outputKeyOutputColumns = fullOuterSmallTableKeyMapping.getOutputColumns();
    for (int i = 0; i < outputKeysSize; i++) {
      final int outputKeyNum = outputKeyNums[i];
      allSmallTableKeyColumnNums[outputKeyNum] = outputKeyOutputColumns[i];
      allSmallTableKeyColumnIncluded[outputKeyNum] = true;
    }

    if (hashTableKeyType == HashTableKeyType.MULTI_KEY &&
        outputKeysSize > 0) {

      smallTableKeyOuterVectorDeserializeRow =
          new VectorDeserializeRow<BinarySortableDeserializeRead>(BinarySortableDeserializeRead.with(
                          smallTableKeyTypeInfos, true, getConf().getKeyTblDesc().getProperties()));
      smallTableKeyOuterVectorDeserializeRow.init(
          allSmallTableKeyColumnNums, allSmallTableKeyColumnIncluded);
    }
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {

    super.initializeOp(hconf);

    VectorExpression.doTransientInit(bigTableFilterExpressions, hconf);
    VectorExpression.doTransientInit(bigTableKeyExpressions, hconf);
    VectorExpression.doTransientInit(bigTableValueExpressions, hconf);
    VectorExpression.doTransientInit(bigTableValueExpressions, hconf);

    /*
     * Get configuration parameters.
     */
    overflowRepeatedThreshold = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVE_VECTORIZATION_MAPJOIN_NATIVE_OVERFLOW_REPEATED_THRESHOLD);
    useOverflowRepeatedThreshold = (overflowRepeatedThreshold >= 0);


    /*
     * Create our vectorized copy row and deserialize row helper objects.
     */
    if (vectorMapJoinVariation == VectorMapJoinVariation.FULL_OUTER) {
      initializeFullOuterObjects();
    }

    if (smallTableValueMapping.getCount() > 0) {
      smallTableValueVectorDeserializeRow =
          new VectorDeserializeRow<LazyBinaryDeserializeRead>(
              new LazyBinaryDeserializeRead(
                  smallTableValueMapping.getTypeInfos(),
                  /* useExternalBuffer */ true));
      smallTableValueVectorDeserializeRow.init(smallTableValueMapping.getOutputColumns());
    }

    if (bigTableRetainColumnMap.length > 0) {
      bigTableRetainedVectorCopy = new VectorCopyRow();
      bigTableRetainedVectorCopy.init(
          bigTableRetainColumnMap, bigTableRetainTypeInfos);
    }

    if (nonOuterSmallTableKeyColumnMap.length > 0) {
      nonOuterSmallTableKeyVectorCopy = new VectorCopyRow();
      nonOuterSmallTableKeyVectorCopy.init(
          nonOuterSmallTableKeyColumnMap, nonOuterSmallTableKeyTypeInfos);
    }

    if (outerSmallTableKeyMapping.getCount() > 0) {
      outerSmallTableKeyVectorCopy = new VectorCopyRow();
      outerSmallTableKeyVectorCopy.init(outerSmallTableKeyMapping);
    }

    /*
     * Setup the overflow batch.
     */
    overflowBatch = setupOverflowBatch();

    needCommonSetup = true;
    needFirstBatchSetup = true;
    needHashTableSetup = true;

    if (LOG.isDebugEnabled()) {
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

    if (isTestingNoHashTableLoad) {
      return;
    }

    MapJoinTableContainer mapJoinTableContainer =
        mapJoinTables[posSingleVectorMapJoinSmallTable];

    setUpHashTable();
  }

  @VisibleForTesting
  @Override
  public void setTestMapJoinTableContainer(int posSmallTable,
      MapJoinTableContainer testMapJoinTableContainer,
      MapJoinTableContainerSerDe mapJoinTableContainerSerDe) {

    mapJoinTables[posSingleVectorMapJoinSmallTable] = testMapJoinTableContainer;

    setUpHashTable();
  }

  private void setUpHashTable() {

    HashTableImplementationType hashTableImplementationType = vectorDesc.getHashTableImplementationType();
    switch (vectorDesc.getHashTableImplementationType()) {
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
      allocateOverflowBatchColumnVector(overflowBatch, outputColumn, typeName, vOutContext.getDataTypePhysicalVariation(outputColumn));
    }

    // Now, add any scratch columns needed for children operators.
    int outputColumn = initialColumnCount;
    for (String typeName : vOutContext.getScratchColumnTypeNames()) {
      allocateOverflowBatchColumnVector(overflowBatch, outputColumn, typeName, vOutContext.getDataTypePhysicalVariation(outputColumn++));
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
                                                 String typeName,
                                                 DataTypePhysicalVariation dataTypePhysicalVariation) {

    if (overflowBatch.cols[outputColumn] == null) {
      typeName = VectorizationContext.mapTypeNameSynonyms(typeName);

      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeName);

      overflowBatch.cols[outputColumn] = VectorizedBatchUtil.createColumnVector(typeInfo, dataTypePhysicalVariation);

      if (LOG.isDebugEnabled()) {
        LOG.debug(getLoggingPrefix() + " VectorMapJoinCommonOperator initializeOp overflowBatch outputColumn " + outputColumn + " class " + overflowBatch.cols[outputColumn].getClass().getSimpleName());
      }
    }
  }

  /*
   * Common one time setup for Native Vector MapJoin operator.
   */
  protected void commonSetup() throws HiveException {

    /*
     * Make sure big table BytesColumnVectors have room for string values in the overflow batch...
     */
    for (int column: bigTableByteColumnVectorColumns) {
      BytesColumnVector bytesColumnVector = (BytesColumnVector) overflowBatch.cols[column];
      bytesColumnVector.initBuffer();
    }

    for (int column : nonOuterSmallTableKeyByteColumnVectorColumns) {
      BytesColumnVector bytesColumnVector = (BytesColumnVector) overflowBatch.cols[column];
      bytesColumnVector.initBuffer();
    }

    for (int column : outerSmallTableKeyByteColumnVectorColumns) {
      BytesColumnVector bytesColumnVector = (BytesColumnVector) overflowBatch.cols[column];
      bytesColumnVector.initBuffer();
    }

    for (int column: smallTableByteColumnVectorColumns) {
      BytesColumnVector bytesColumnVector = (BytesColumnVector) overflowBatch.cols[column];
      bytesColumnVector.initBuffer();
    }

    batchCounter = 0;
    rowCounter = 0;
  }

  /*
   * Common one time setup by native vectorized map join operator's first batch.
   */
  public void firstBatchSetup(VectorizedRowBatch batch) throws HiveException {
    // Make sure small table BytesColumnVectors have room for string values in the big table and
    // overflow batchs...
    for (int column: smallTableByteColumnVectorColumns) {
      BytesColumnVector bytesColumnVector = (BytesColumnVector) batch.cols[column];
      bytesColumnVector.initBuffer();
    }

    // Setup a scratch batch that will be used to play back big table rows that were spilled
    // to disk for the Hybrid Grace hash partitioning.
    spillReplayBatch = VectorizedBatchUtil.makeLike(batch);
  }

  /*
   * Perform any Native Vector MapJoin operator specific hash table setup.
   */
  public void hashTableSetup() throws HiveException {
  }

  /*
   * Perform the Native Vector MapJoin operator work.
   */
  public abstract void processBatch(VectorizedRowBatch batch) throws HiveException;

  /*
   * Common process method for all Native Vector MapJoin operators.
   *
   * Do common initialization work and invoke the override-able common setup methods.
   *
   * Then, invoke the processBatch override method to do the operator work.
   */
  @Override
  public void process(Object row, int tag) throws HiveException {

    VectorizedRowBatch batch = (VectorizedRowBatch) row;
    alias = (byte) tag;

    if (needCommonSetup) {

      // Our one time process method initialization.
      commonSetup();

      needCommonSetup = false;
    }

    if (needFirstBatchSetup) {

      // Our one time first-batch method initialization.
      firstBatchSetup(batch);

      needFirstBatchSetup = false;
    }

    if (needHashTableSetup) {

      // Setup our hash table specialization.  It will be the first time the process
      // method is called, or after a Hybrid Grace reload.

      hashTableSetup();

      needHashTableSetup = false;
    }

    batchCounter++;

    if (batch.size == 0) {
      return;
    }

    rowCounter += batch.size;

    processBatch(batch);
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
  public VectorizationContext getInputVectorizationContext() {
    return vContext;
  }

  @Override
  public VectorDesc getVectorDesc() {
    return vectorDesc;
  }

  @Override
  public VectorizationContext getOutputVectorizationContext() {
    return vOutContext;
  }
}
