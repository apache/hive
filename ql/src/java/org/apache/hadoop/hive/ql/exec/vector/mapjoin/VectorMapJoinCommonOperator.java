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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.HashTableLoaderFactory;
import org.apache.hadoop.hive.ql.exec.HashTableLoader;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer.ReusableGetAdaptor;
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
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.optimized.VectorMapJoinOptimizedCreateHashTable;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTable;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastHashTableLoader;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableImplementationType;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinaryDeserializeRead;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * This class is common operator class for native vectorized map join.
 *
 * It contain common initialization logic.
 *
 * It is used by both inner and outer joins.
 */
public abstract class VectorMapJoinCommonOperator extends MapJoinOperator implements VectorizationContextRegion {
  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog(VectorMapJoinCommonOperator.class.getName());

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

  // The output column projection of the vectorized row batch.  And, the type names of the output
  // columns.
  protected int[] outputProjection;
  protected String[] outputTypeNames;

  // These are the vectorized batch expressions for filtering, key expressions, and value
  // expressions.
  protected VectorExpression[] bigTableFilterExpressions;
  protected VectorExpression[] bigTableKeyExpressions;
  protected VectorExpression[] bigTableValueExpressions;

  // This is map of which vectorized row batch columns are the big table key columns.  Since
  // we may have key expressions that produce new scratch columns, we need a mapping.
  // And, we have their type names.
  protected int[] bigTableKeyColumnMap;
  protected ArrayList<String> bigTableKeyTypeNames;

  // Similarly, this is map of which vectorized row batch columns are the big table value columns.
  // Since we may have value expressions that produce new scratch columns, we need a mapping.
  // And, we have their type names.
  protected int[] bigTableValueColumnMap;
  protected ArrayList<String> bigTableValueTypeNames;

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

  // For debug tracing: the name of the map or reduce task.
  protected transient String taskName;

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
  protected transient VectorDeserializeRow smallTableVectorDeserializeRow;

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

  public VectorMapJoinCommonOperator() {
    super();
  }

  public VectorMapJoinCommonOperator(VectorizationContext vContext, OperatorDesc conf)
          throws HiveException {
    super();

    MapJoinDesc desc = (MapJoinDesc) conf;
    this.conf = desc;

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

    List<ExprNodeDesc> keyDesc = desc.getKeys().get(posBigTable);
    bigTableKeyExpressions = vContext.getVectorExpressions(keyDesc);

    // Since a key expression can be a calculation and the key will go into a scratch column,
    // we need the mapping and type information.
    bigTableKeyColumnMap = new int[bigTableKeyExpressions.length];
    bigTableKeyTypeNames = new ArrayList<String>();
    boolean onlyColumns = true;
    for (int i = 0; i < bigTableKeyColumnMap.length; i++) {
      VectorExpression ve = bigTableKeyExpressions[i];
      if (!IdentityExpression.isColumnOnly(ve)) {
        onlyColumns = false;
      }
      bigTableKeyTypeNames.add(keyDesc.get(i).getTypeString());
      bigTableKeyColumnMap[i] = ve.getOutputColumn();
    }
    if (onlyColumns) {
      bigTableKeyExpressions = null;
    }

    List<ExprNodeDesc> bigTableExprs = desc.getExprs().get(posBigTable);
    bigTableValueExpressions = vContext.getVectorExpressions(bigTableExprs);

    /*
     * Similarly, we need a mapping since a value expression can be a calculation and the value
     * will go into a scratch column.
     */
    bigTableValueColumnMap = new int[bigTableValueExpressions.length];
    bigTableValueTypeNames = new ArrayList<String>();
    onlyColumns = true;
    for (int i = 0; i < bigTableValueColumnMap.length; i++) {
      VectorExpression ve = bigTableValueExpressions[i];
      if (!IdentityExpression.isColumnOnly(ve)) {
        onlyColumns = false;
      }
      bigTableValueTypeNames.add(bigTableExprs.get(i).getTypeString());
      bigTableValueColumnMap[i] = ve.getOutputColumn();
    }
    if (onlyColumns) {
      bigTableValueExpressions = null;
    }

    determineCommonInfo(isOuterJoin);
  }

  protected void determineCommonInfo(boolean isOuter) {

    bigTableRetainedMapping = new VectorColumnOutputMapping("Big Table Retained Mapping");

    bigTableOuterKeyMapping = new VectorColumnOutputMapping("Big Table Outer Key Mapping");

    // The order of the fields in the LazyBinary small table value must be used, so
    // we use the source ordering flavor for the mapping.
    smallTableMapping = new VectorColumnSourceMapping("Small Table Mapping");

    // We use a mapping object here so we can build the projection in any order and
    // get the ordered by 0 to n-1 output columns at the end.
    //
    // Also, to avoid copying a big table key into the small table result area for inner joins,
    // we reference it with the projection so there can be duplicate output columns
    // in the projection.
    VectorColumnSourceMapping projectionMapping = new VectorColumnSourceMapping("Projection Mapping");

    /*
     * Gather up big and small table output result information from the MapJoinDesc.
     */
    List<Integer> bigTableRetainList = conf.getRetainList().get(posBigTable);
    int bigTableRetainSize = bigTableRetainList.size();

    int[] smallTableIndices;
    int smallTableIndicesSize;
    List<ExprNodeDesc> smallTableExprs = conf.getExprs().get(posSingleVectorMapJoinSmallTable);
    if (conf.getValueIndices() != null && conf.getValueIndices().get(posSingleVectorMapJoinSmallTable) != null) {
      smallTableIndices = conf.getValueIndices().get(posSingleVectorMapJoinSmallTable);
      smallTableIndicesSize = smallTableIndices.length;
    } else {
      smallTableIndices = null;
      smallTableIndicesSize = 0;
    }

    List<Integer> smallTableRetainList = conf.getRetainList().get(posSingleVectorMapJoinSmallTable);
    int smallTableRetainSize = smallTableRetainList.size();

    int smallTableResultSize = 0;
    if (smallTableIndicesSize > 0) {
      smallTableResultSize = smallTableIndicesSize;
    } else if (smallTableRetainSize > 0) {
      smallTableResultSize = smallTableRetainSize;
    }

    /*
     * Determine the big table retained mapping first so we can optimize out (with
     * projection) copying inner join big table keys in the subsequent small table results section.
     */
    int nextOutputColumn = (order[0] == posBigTable ? 0 : smallTableResultSize);
    for (int i = 0; i < bigTableRetainSize; i++) {

      // Since bigTableValueExpressions may do a calculation and produce a scratch column, we
      // need to map to the right batch column.

      int retainColumn = bigTableRetainList.get(i);
      int batchColumnIndex = bigTableValueColumnMap[retainColumn];
      String typeName = bigTableValueTypeNames.get(i);

      // With this map we project the big table batch to make it look like an output batch.
      projectionMapping.add(nextOutputColumn, batchColumnIndex, typeName);

      // Collect columns we copy from the big table batch to the overflow batch.
      if (!bigTableRetainedMapping.containsOutputColumn(batchColumnIndex)) {
        // Tolerate repeated use of a big table column.
        bigTableRetainedMapping.add(batchColumnIndex, batchColumnIndex, typeName);
      }

      nextOutputColumn++;
    }

    /*
     * Now determine the small table results.
     */
    int firstSmallTableOutputColumn;
    firstSmallTableOutputColumn = (order[0] == posBigTable ? bigTableRetainSize : 0);
    int smallTableOutputCount = 0;
    nextOutputColumn = firstSmallTableOutputColumn;

    // Small table indices has more information (i.e. keys) than retain, so use it if it exists...
    if (smallTableIndicesSize > 0) {
      smallTableOutputCount = smallTableIndicesSize;

      for (int i = 0; i < smallTableIndicesSize; i++) {
        if (smallTableIndices[i] >= 0) {

          // Zero and above numbers indicate a big table key is needed for
          // small table result "area".

          int keyIndex = smallTableIndices[i];

          // Since bigTableKeyExpressions may do a calculation and produce a scratch column, we
          // need to map the right column.
          int batchKeyColumn = bigTableKeyColumnMap[keyIndex];
          String typeName = bigTableKeyTypeNames.get(keyIndex);

          if (!isOuter) {

            // Optimize inner join keys of small table results.

            // Project the big table key into the small table result "area".
            projectionMapping.add(nextOutputColumn, batchKeyColumn, typeName);

            if (!bigTableRetainedMapping.containsOutputColumn(batchKeyColumn)) {
              // If necessary, copy the big table key into the overflow batch's small table
              // result "area".
              bigTableRetainedMapping.add(batchKeyColumn, batchKeyColumn, typeName);
            }
          } else {

            // For outer joins, since the small table key can be null when there is no match,
            // we must have a physical (scratch) column for those keys.  We cannot use the
            // projection optimization used by inner joins above.

            int scratchColumn = vOutContext.allocateScratchColumn(typeName);
            projectionMapping.add(nextOutputColumn, scratchColumn, typeName);

            bigTableRetainedMapping.add(batchKeyColumn, scratchColumn, typeName);

            bigTableOuterKeyMapping.add(batchKeyColumn, scratchColumn, typeName);
          }
        } else {

          // Negative numbers indicate a column to be (deserialize) read from the small table's
          // LazyBinary value row.
          int smallTableValueIndex = -smallTableIndices[i] - 1;

          String typeName = smallTableExprs.get(i).getTypeString();

          // Make a new big table scratch column for the small table value.
          int scratchColumn = vOutContext.allocateScratchColumn(typeName);
          projectionMapping.add(nextOutputColumn, scratchColumn, typeName);

          smallTableMapping.add(smallTableValueIndex, scratchColumn, typeName);
        }
        nextOutputColumn++;
      }
    } else if (smallTableRetainSize > 0) {
      smallTableOutputCount = smallTableRetainSize;

      // Only small table values appear in join output result.

      for (int i = 0; i < smallTableRetainSize; i++) {
        int smallTableValueIndex = smallTableRetainList.get(i);

        // Make a new big table scratch column for the small table value.
        String typeName = smallTableExprs.get(i).getTypeString();
        int scratchColumn = vOutContext.allocateScratchColumn(typeName);

        projectionMapping.add(nextOutputColumn, scratchColumn, typeName);

        smallTableMapping.add(smallTableValueIndex, scratchColumn, typeName);
        nextOutputColumn++;
      }
    }

    // Convert dynamic arrays and maps to simple arrays.

    bigTableRetainedMapping.finalize();

    bigTableOuterKeyMapping.finalize();

    smallTableMapping.finalize();

    bigTableOuterKeyOutputVectorColumns = bigTableOuterKeyMapping.getOutputColumns();
    smallTableOutputVectorColumns = smallTableMapping.getOutputColumns();

    // Which big table and small table columns are ByteColumnVector and need have their data buffer
    // to be manually reset for some join result processing?

    bigTableByteColumnVectorColumns = getByteColumnVectorColumns(bigTableOuterKeyMapping);

    smallTableByteColumnVectorColumns = getByteColumnVectorColumns(smallTableMapping);

    projectionMapping.finalize();

    // Verify we added an entry for each output.
    assert projectionMapping.isSourceSequenceGood();

    outputProjection = projectionMapping.getOutputColumns();
    outputTypeNames = projectionMapping.getTypeNames();

    if (LOG.isDebugEnabled()) {
      int[] orderDisplayable = new int[order.length];
      for (int i = 0; i < order.length; i++) {
        orderDisplayable[i] = (int) order[i];
      }
      LOG.debug(taskName + ", " + getOperatorId() + " VectorMapJoinCommonOperator constructor order " + Arrays.toString(orderDisplayable));
      LOG.debug(taskName + ", " + getOperatorId() + " VectorMapJoinCommonOperator constructor posBigTable " + (int) posBigTable);
      LOG.debug(taskName + ", " + getOperatorId() + " VectorMapJoinCommonOperator constructor posSingleVectorMapJoinSmallTable " + (int) posSingleVectorMapJoinSmallTable);

      LOG.debug(taskName + ", " + getOperatorId() + " VectorMapJoinCommonOperator constructor bigTableKeyColumnMap " + Arrays.toString(bigTableKeyColumnMap));
      LOG.debug(taskName + ", " + getOperatorId() + " VectorMapJoinCommonOperator constructor bigTableKeyTypeNames " + bigTableKeyTypeNames);

      LOG.debug(taskName + ", " + getOperatorId() + " VectorMapJoinCommonOperator constructor bigTableValueColumnMap " + Arrays.toString(bigTableValueColumnMap));
      LOG.debug(taskName + ", " + getOperatorId() + " VectorMapJoinCommonOperator constructor bigTableValueTypeNames " + bigTableValueTypeNames);

      LOG.debug(taskName + ", " + getOperatorId() + " VectorMapJoinCommonOperator constructor smallTableIndices " + Arrays.toString(smallTableIndices));
      LOG.debug(taskName + ", " + getOperatorId() + " VectorMapJoinCommonOperator constructor smallTableRetainList " + smallTableRetainList);

      LOG.debug(taskName + ", " + getOperatorId() + " VectorMapJoinCommonOperator constructor firstSmallTableOutputColumn " + firstSmallTableOutputColumn);
      LOG.debug(taskName + ", " + getOperatorId() + " VectorMapJoinCommonOperator constructor smallTableOutputCount " + smallTableOutputCount);

      LOG.debug(taskName + ", " + getOperatorId() + " VectorMapJoinCommonOperator constructor bigTableRetainedMapping " + bigTableRetainedMapping.toString());

      LOG.debug(taskName + ", " + getOperatorId() + " VectorMapJoinCommonOperator constructor bigTableOuterKeyMapping " + bigTableOuterKeyMapping.toString());

      LOG.debug(taskName + ", " + getOperatorId() + " VectorMapJoinCommonOperator constructor smallTableMapping " + smallTableMapping.toString());

      LOG.debug(taskName + ", " + getOperatorId() + " VectorMapJoinCommonOperator constructor bigTableByteColumnVectorColumns " + Arrays.toString(bigTableByteColumnVectorColumns));
      LOG.debug(taskName + ", " + getOperatorId() + " VectorMapJoinCommonOperator constructor smallTableByteColumnVectorColumns " + Arrays.toString(smallTableByteColumnVectorColumns));

      LOG.debug(taskName + ", " + getOperatorId() + " VectorMapJoinCommonOperator constructor outputProjection " + Arrays.toString(outputProjection));
      LOG.debug(taskName + ", " + getOperatorId() + " VectorMapJoinCommonOperator constructor outputTypeNames " + Arrays.toString(outputTypeNames));
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
    String[] typeNames = mapping.getTypeNames();
    for (int i = 0; i < count; i++) {
      int outputColumn = outputColumns[i];
      String typeName = typeNames[i];
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
      LOG.debug(taskName + ", " + getOperatorId() + " VectorMapJoinCommonOperator constructor outputColumnNames " + outputColumnNames);
    }
    if (outputColumnNames.size() != outputProjection.length) {
      throw new RuntimeException("Output column names " + outputColumnNames + " length and output projection " + Arrays.toString(outputProjection) + " / " + Arrays.toString(outputTypeNames) + " length mismatch");
    }
    vOutContext.resetProjectionColumns();
    for (int i = 0; i < outputColumnNames.size(); ++i) {
      String columnName = outputColumnNames.get(i);
      int outputColumn = outputProjection[i];
      vOutContext.addProjectionColumn(columnName, outputColumn);

      if (LOG.isDebugEnabled()) {
        LOG.debug(taskName + ", " + getOperatorId() + " VectorMapJoinCommonOperator constructor addProjectionColumn " + i + " columnName " + columnName + " outputColumn " + outputColumn);
      }
    }
  }

  /**
   * This override lets us substitute our own fast vectorized hash table loader.
   */
  @Override
  protected HashTableLoader getHashTableLoader(Configuration hconf) {

    VectorMapJoinDesc vectorDesc = conf.getVectorDesc();
    HashTableImplementationType hashTableImplementationType = vectorDesc.hashTableImplementationType();
    HashTableLoader hashTableLoader;
    switch (vectorDesc.hashTableImplementationType()) {
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

  @Override
  protected Collection<Future<?>> initializeOp(Configuration hconf) throws HiveException {
    Collection<Future<?>> result = super.initializeOp(hconf);

    if (LOG.isDebugEnabled()) {
      // Determine the name of our map or reduce task for debug tracing.
      BaseWork work = Utilities.getMapWork(hconf);
      if (work == null) {
        work = Utilities.getReduceWork(hconf);
      }
      taskName = work.getName();
    }

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
      smallTableVectorDeserializeRow = new VectorDeserializeRow(
                      new LazyBinaryDeserializeRead(
                          VectorizedBatchUtil.primitiveTypeInfosFromTypeNames(
                              smallTableMapping.getTypeNames())));
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

    if (LOG.isDebugEnabled()) {
      int[] currentScratchColumns = vOutContext.currentScratchColumns();
      LOG.debug(taskName + ", " + getOperatorId() + " VectorMapJoinCommonOperator initializeOp currentScratchColumns " + Arrays.toString(currentScratchColumns));

      StructObjectInspector structOutputObjectInspector = (StructObjectInspector) outputObjInspector;
      List<? extends StructField> fields = structOutputObjectInspector.getAllStructFieldRefs();
      int i = 0;
      for (StructField field : fields) {
        LOG.debug("VectorMapJoinInnerBigOnlyCommonOperator initializeOp " + i + " field " + field.getFieldName() + " type " + field.getFieldObjectInspector().getTypeName());
        i++;
      }
    }

    return result;
  }

  @Override
  protected Pair<MapJoinTableContainer[], MapJoinTableContainerSerDe[]> loadHashTable(
      ExecMapperContext mapContext, MapredContext mrContext) throws HiveException {

    Pair<MapJoinTableContainer[], MapJoinTableContainerSerDe[]> pair;

    VectorMapJoinDesc vectorDesc = conf.getVectorDesc();
    HashTableImplementationType hashTableImplementationType = vectorDesc.hashTableImplementationType();
    HashTableLoader hashTableLoader;
    switch (vectorDesc.hashTableImplementationType()) {
    case OPTIMIZED:
      {
        // Using Tez's HashTableLoader, create either a MapJoinBytesTableContainer or
        // HybridHashTableContainer.
        pair = super.loadHashTable(mapContext, mrContext);

        // Create our vector map join optimized hash table variation *above* the
        // map join table container.
        MapJoinTableContainer[] mapJoinTableContainers = pair.getLeft();
        vectorMapJoinHashTable = VectorMapJoinOptimizedCreateHashTable.createHashTable(conf,
                mapJoinTableContainers[posSingleVectorMapJoinSmallTable]);
      }
      break;

    case FAST:
      {
        // Use our VectorMapJoinFastHashTableLoader to create a VectorMapJoinTableContainer.
        pair = super.loadHashTable(mapContext, mrContext);

        // Get our vector map join fast hash table variation from the
        // vector map join table container.
        MapJoinTableContainer[] mapJoinTableContainers = pair.getLeft();
        VectorMapJoinTableContainer vectorMapJoinTableContainer =
                (VectorMapJoinTableContainer) mapJoinTableContainers[posSingleVectorMapJoinSmallTable];
        vectorMapJoinHashTable = vectorMapJoinTableContainer.vectorMapJoinHashTable();
      }
      break;
    default:
      throw new RuntimeException("Unknown vector map join hash table implementation type " + hashTableImplementationType.name());
    }

    return pair;
  }

  /*
   * Setup our 2nd batch with the same "column schema" as the big table batch that can be used to
   * build join output results in.
   */
  protected VectorizedRowBatch setupOverflowBatch() throws HiveException {
    VectorizedRowBatch overflowBatch;

    Map<Integer, String> scratchColumnTypeMap = vOutContext.getScratchColumnTypeMap();
    int maxColumn = 0;
    for (int i = 0; i < outputProjection.length; i++) {
      int outputColumn = outputProjection[i];
      if (maxColumn < outputColumn) {
        maxColumn = outputColumn;
      }
    }
    for (int outputColumn : scratchColumnTypeMap.keySet()) {
      if (maxColumn < outputColumn) {
        maxColumn = outputColumn;
      }
    }
    overflowBatch = new VectorizedRowBatch(maxColumn + 1);

    // First, just allocate just the projection columns we will be using.
    for (int i = 0; i < outputProjection.length; i++) {
      int outputColumn = outputProjection[i];
      String typeName = outputTypeNames[i];
      allocateOverflowBatchColumnVector(overflowBatch, outputColumn, typeName);
    }

    // Now, add any scratch columns needed for children operators.
    for (int outputColumn : scratchColumnTypeMap.keySet()) {
      String typeName = scratchColumnTypeMap.get(outputColumn);
      allocateOverflowBatchColumnVector(overflowBatch, outputColumn, typeName);
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

      String columnVectorTypeName;

      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeName);
      Type columnVectorType = VectorizationContext.getColumnVectorTypeFromTypeInfo(typeInfo);

      switch (columnVectorType) {
      case LONG:
        columnVectorTypeName = "long";
        break;

      case DOUBLE:
        columnVectorTypeName = "double";
        break;

      case BYTES:
        columnVectorTypeName = "string";
        break;

      case DECIMAL:
        columnVectorTypeName = typeName;  // Keep precision and scale.
        break;

      default:
        throw new HiveException("Unexpected column vector type " + columnVectorType);
      }

      overflowBatch.cols[outputColumn] = VectorizedRowBatchCtx.allocateColumnVector(columnVectorTypeName, VectorizedRowBatch.DEFAULT_SIZE);

      if (LOG.isDebugEnabled()) {
        LOG.debug(taskName + ", " + getOperatorId() + " VectorMapJoinCommonOperator initializeOp overflowBatch outputColumn " + outputColumn + " class " + overflowBatch.cols[outputColumn].getClass().getSimpleName());
      }
    }
  }

  /*
   * Common one time setup by native vectorized map join operator's processOp.
   */
  protected void commonSetup(VectorizedRowBatch batch) throws HiveException {

    if (LOG.isDebugEnabled()) {
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
    LOG.debug("commonSetup " + batchName + " column count " + batch.numCols);
    for (int column = 0; column < batch.numCols; column++) {
      LOG.debug("commonSetup " + batchName + "     column " + column + " type " + (batch.cols[column] == null ? "NULL" : batch.cols[column].getClass().getSimpleName()));
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