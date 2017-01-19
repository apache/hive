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

package org.apache.hadoop.hive.ql.exec.vector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.AbstractMapOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.VectorPartitionDesc;
import org.apache.hadoop.hive.ql.plan.VectorPartitionDesc.VectorMapOperatorReadType;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.fast.DeserializeRead;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.fast.LazySimpleDeserializeRead;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinaryDeserializeRead;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;

/*
 *
 * The vectorized MapOperator.
 *
 * There are 3 modes of reading for vectorization:
 *
 *   1) One for the Vectorized Input File Format which returns VectorizedRowBatch as the row.
 *
 *   2) One for using VectorDeserializeRow to deserialize each row into the VectorizedRowBatch.
 *      Currently, these Input File Formats:
 *        TEXTFILE
 *        SEQUENCEFILE
 *
 *   3) And one using the regular partition deserializer to get the row object and assigning
 *      the row object into the VectorizedRowBatch with VectorAssignRow.
 *      This picks up Input File Format not supported by the other two.
 */
public class VectorMapOperator extends AbstractMapOperator {

  private static final long serialVersionUID = 1L;

  /*
   * Overall information on this vectorized Map operation.
   */
  private transient HashMap<String, VectorPartitionContext> fileToPartitionContextMap;

  private transient Operator<? extends OperatorDesc> oneRootOperator;

  private transient TypeInfo tableStructTypeInfo;
  private transient StandardStructObjectInspector tableStandardStructObjectInspector;

  private transient TypeInfo[] tableRowTypeInfos;

  private transient int[] dataColumnNums;

  private transient StandardStructObjectInspector neededStandardStructObjectInspector;

  private transient VectorizedRowBatchCtx batchContext;
              // The context for creating the VectorizedRowBatch for this Map node that
              // the Vectorizer class determined.

  /*
   * A different batch for vectorized Input File Format readers so they can do their work
   * overlapped with work of the row collection that vector/row deserialization does.  This allows
   * the partitions to mix modes (e.g. for us to flush the previously batched rows on file change).
   */
  private transient VectorizedRowBatch vectorizedInputFileFormatBatch;

  /*
   * This batch is only used by vector/row deserializer readers.
   */
  private transient VectorizedRowBatch deserializerBatch;

  private transient long batchCounter;

  private transient int dataColumnCount;
  private transient int partitionColumnCount;
  private transient Object[] partitionValues;

  private transient boolean[] dataColumnsToIncludeTruncated;

  /*
   * The following members have context information for the current partition file being read.
   */
  private transient VectorMapOperatorReadType currentReadType;
  private transient VectorPartitionContext currentVectorPartContext;
                  // Current vector map operator read type and context.

  private transient int currentDataColumnCount;
                  // The number of data columns that the current reader will return.
                  // Only applicable for vector/row deserialization.

  private transient DeserializeRead currentDeserializeRead;
  private transient VectorDeserializeRow currentVectorDeserializeRow;
                  // When we are doing vector deserialization, these are the fast deserializer and
                  // the vector row deserializer.

  private Deserializer currentPartDeserializer;
  private StructObjectInspector currentPartRawRowObjectInspector;
  private VectorAssignRow currentVectorAssign;
                  // When we are doing row deserialization, these are the regular deserializer,
                  // partition object inspector, and vector row assigner.

  /*
   * The abstract context for the 3 kinds of vectorized reading.
   */
  protected abstract class VectorPartitionContext {

    protected final PartitionDesc partDesc;

    String tableName;
    String partName;

    /*
     * Initialization here is adapted from MapOperator.MapOpCtx.initObjectInspector method.
     */
    private VectorPartitionContext(PartitionDesc partDesc) {
      this.partDesc = partDesc;

      TableDesc td = partDesc.getTableDesc();

      // Use table properties in case of unpartitioned tables,
      // and the union of table properties and partition properties, with partition
      // taking precedence, in the case of partitioned tables
      Properties overlayedProps =
          SerDeUtils.createOverlayedProperties(td.getProperties(), partDesc.getProperties());

      Map<String, String> partSpec = partDesc.getPartSpec();

      tableName = String.valueOf(overlayedProps.getProperty("name"));
      partName = String.valueOf(partSpec);

    }

    public PartitionDesc getPartDesc() {
      return partDesc;
    }

    /*
     * Override this for concrete initialization.
     */
    public abstract void init(Configuration hconf)
        throws SerDeException, Exception;

    /*
     * How many data columns is the partition reader actually supplying?
     */
    public abstract int getReaderDataColumnCount();
  }

  /*
   * Context for reading a Vectorized Input File Format.
   */
  protected class VectorizedInputFileFormatPartitionContext extends VectorPartitionContext {

    private VectorizedInputFileFormatPartitionContext(PartitionDesc partDesc) {
      super(partDesc);
    }

    public void init(Configuration hconf) {
    }

    @Override
    public int getReaderDataColumnCount() {
      throw new RuntimeException("Not applicable");
    }
  }

  /*
   * Context for using VectorDeserializeRow to deserialize each row from the Input File Format
   * into the VectorizedRowBatch.
   */
  protected class VectorDeserializePartitionContext extends VectorPartitionContext {

    // This helper object deserializes known deserialization / input file format combination into
    // columns of a row in a vectorized row batch.
    private VectorDeserializeRow vectorDeserializeRow;

    private DeserializeRead deserializeRead;

    private int readerColumnCount;

    private VectorDeserializePartitionContext(PartitionDesc partDesc) {
      super(partDesc);
    }

    public VectorDeserializeRow getVectorDeserializeRow() {
      return vectorDeserializeRow;
    }

    DeserializeRead getDeserializeRead() {
      return deserializeRead;
    }

    @Override
    public int getReaderDataColumnCount() {
      return readerColumnCount;
    }

    public void init(Configuration hconf)
        throws SerDeException, HiveException {
      VectorPartitionDesc vectorPartDesc = partDesc.getVectorPartitionDesc();

      // This type information specifies the data types the partition needs to read.
      TypeInfo[] dataTypeInfos = vectorPartDesc.getDataTypeInfos();

      // We need to provide the minimum number of columns to be read so
      // LazySimpleDeserializeRead's separator parser does not waste time.
      //
      Preconditions.checkState(dataColumnsToIncludeTruncated != null);
      TypeInfo[] minimalDataTypeInfos;
      if (dataColumnsToIncludeTruncated.length < dataTypeInfos.length) {
        minimalDataTypeInfos =
            Arrays.copyOf(dataTypeInfos, dataColumnsToIncludeTruncated.length);
      } else {
        minimalDataTypeInfos = dataTypeInfos;
      }

      readerColumnCount = minimalDataTypeInfos.length;

      switch (vectorPartDesc.getVectorDeserializeType()) {
      case LAZY_SIMPLE:
        {
          LazySerDeParameters simpleSerdeParams =
              new LazySerDeParameters(hconf, partDesc.getTableDesc().getProperties(),
                  LazySimpleSerDe.class.getName());

          LazySimpleDeserializeRead lazySimpleDeserializeRead =
              new LazySimpleDeserializeRead(
                  minimalDataTypeInfos,
                  /* useExternalBuffer */ true,
                  simpleSerdeParams);

          vectorDeserializeRow =
              new VectorDeserializeRow<LazySimpleDeserializeRead>(lazySimpleDeserializeRead);

          // Initialize with data row type conversion parameters.
          vectorDeserializeRow.initConversion(tableRowTypeInfos, dataColumnsToIncludeTruncated);

          deserializeRead = lazySimpleDeserializeRead;
        }
        break;

      case LAZY_BINARY:
        {
          LazyBinaryDeserializeRead lazyBinaryDeserializeRead =
              new LazyBinaryDeserializeRead(
                  dataTypeInfos,
                  /* useExternalBuffer */ true);

          vectorDeserializeRow =
              new VectorDeserializeRow<LazyBinaryDeserializeRead>(lazyBinaryDeserializeRead);

          // Initialize with data row type conversion parameters.
          vectorDeserializeRow.initConversion(tableRowTypeInfos, dataColumnsToIncludeTruncated);

          deserializeRead = lazyBinaryDeserializeRead;
        }
        break;

      default:
        throw new RuntimeException(
            "Unexpected vector deserialize row type " + vectorPartDesc.getVectorDeserializeType().name());
      }
    }
  }

  /*
   * Context for reading using the regular partition deserializer to get the row object and
   * assigning the row object into the VectorizedRowBatch with VectorAssignRow
   */
  protected class RowDeserializePartitionContext extends VectorPartitionContext {

    private Deserializer partDeserializer;
    private StructObjectInspector partRawRowObjectInspector;
    private VectorAssignRow vectorAssign;

    private int readerColumnCount;

    private RowDeserializePartitionContext(PartitionDesc partDesc) {
      super(partDesc);
    }

    public Deserializer getPartDeserializer() {
      return partDeserializer;
    }

    public StructObjectInspector getPartRawRowObjectInspector() {
      return partRawRowObjectInspector;
    }

    public VectorAssignRow getVectorAssign() {
      return vectorAssign;
    }

    @Override
    public int getReaderDataColumnCount() {
      return readerColumnCount;
    }

    public void init(Configuration hconf)
        throws Exception {
      VectorPartitionDesc vectorPartDesc = partDesc.getVectorPartitionDesc();

      partDeserializer = partDesc.getDeserializer(hconf);

      if (partDeserializer instanceof OrcSerde) {

        // UNDONE: We need to get the table schema inspector from self-describing Input File
        //         Formats like ORC.  Modify the ORC serde instead?  For now, this works.

        partRawRowObjectInspector =
            (StructObjectInspector) OrcStruct.createObjectInspector(tableStructTypeInfo);

      } else {
        partRawRowObjectInspector =
            (StructObjectInspector) partDeserializer.getObjectInspector();
      }

      TypeInfo[] dataTypeInfos = vectorPartDesc.getDataTypeInfos();

      vectorAssign = new VectorAssignRow();

      // Initialize with data type conversion parameters.
      readerColumnCount =
          vectorAssign.initConversion(dataTypeInfos, tableRowTypeInfos, dataColumnsToIncludeTruncated);
    }
  }

  public VectorPartitionContext createAndInitPartitionContext(PartitionDesc partDesc,
      Configuration hconf)
          throws SerDeException, Exception {

    VectorPartitionDesc vectorPartDesc = partDesc.getVectorPartitionDesc();
    VectorPartitionContext vectorPartitionContext;
    VectorMapOperatorReadType vectorMapOperatorReadType =
        vectorPartDesc.getVectorMapOperatorReadType();

    if (vectorMapOperatorReadType == VectorMapOperatorReadType.VECTOR_DESERIALIZE ||
        vectorMapOperatorReadType == VectorMapOperatorReadType.ROW_DESERIALIZE) {
      // Verify hive.exec.schema.evolution is true or we have an ACID table so we are producing
      // the table schema from ORC.  The Vectorizer class assures this.
      boolean isAcid =
          AcidUtils.isTablePropertyTransactional(partDesc.getTableDesc().getProperties());
      Preconditions.checkState(Utilities.isSchemaEvolutionEnabled(hconf, isAcid));
    }

    switch (vectorMapOperatorReadType) {
    case VECTORIZED_INPUT_FILE_FORMAT:
      vectorPartitionContext = new VectorizedInputFileFormatPartitionContext(partDesc);
      break;

    case VECTOR_DESERIALIZE:
      vectorPartitionContext = new VectorDeserializePartitionContext(partDesc);
      break;

    case ROW_DESERIALIZE:
      vectorPartitionContext = new RowDeserializePartitionContext(partDesc);
      break;

    default:
      throw new RuntimeException("Unexpected vector MapOperator read type " +
          vectorMapOperatorReadType.name());
    }

    vectorPartitionContext.init(hconf);

    return vectorPartitionContext;
  }

  private void determineDataColumnsToIncludeTruncated() {

    Preconditions.checkState(batchContext != null);
    Preconditions.checkState(dataColumnNums != null);

    boolean[] columnsToInclude = new boolean[dataColumnCount];;
    final int count = dataColumnNums.length;
    int columnNum = -1;
    for (int i = 0; i < count; i++) {
      columnNum = dataColumnNums[i];
      Preconditions.checkState(columnNum < dataColumnCount);
      columnsToInclude[columnNum] = true;
    }

    if (columnNum == -1) {
      dataColumnsToIncludeTruncated = new boolean[0];
    } else {
      dataColumnsToIncludeTruncated = Arrays.copyOf(columnsToInclude, columnNum + 1);
    }
  }

  /** Kryo ctor. */
  public VectorMapOperator() {
    super();
  }

  public VectorMapOperator(CompilationOpContext ctx) {
    super(ctx);
  }


  /*
   * This is the same as the setChildren method below but for empty tables.
   */
  @Override
  public void initEmptyInputChildren(List<Operator<?>> children, Configuration hconf)
    throws SerDeException, Exception {

    // Get the single TableScanOperator.  Vectorization only supports one input tree.
    Preconditions.checkState(children.size() == 1);
    oneRootOperator = children.get(0);

    internalSetChildren(hconf);
  }

  @Override
  public void setChildren(Configuration hconf) throws Exception {

    // Get the single TableScanOperator.  Vectorization only supports one input tree.
    Iterator<Operator<? extends OperatorDesc>> aliasToWorkIterator =
        conf.getAliasToWork().values().iterator();
    oneRootOperator = aliasToWorkIterator.next();
    Preconditions.checkState(!aliasToWorkIterator.hasNext());

    internalSetChildren(hconf);
  }

  /*
   * Create information for vector map operator.
   * The member oneRootOperator has been set.
   */
  private void internalSetChildren(Configuration hconf) throws Exception {

    // The setupPartitionContextVars uses the prior read type to flush the prior deserializerBatch,
    // so set it here to none.
    currentReadType = VectorMapOperatorReadType.NONE;

    batchContext = conf.getVectorizedRowBatchCtx();
    /*
     * Use a different batch for vectorized Input File Format readers so they can do their work
     * overlapped with work of the row collection that vector/row deserialization does.  This allows
     * the partitions to mix modes (e.g. for us to flush the previously batched rows on file change).
     */
    vectorizedInputFileFormatBatch =
        batchContext.createVectorizedRowBatch();
    conf.setVectorizedRowBatch(vectorizedInputFileFormatBatch);

    /*
     * This batch is used by vector/row deserializer readers.
     */
    deserializerBatch = batchContext.createVectorizedRowBatch();

    batchCounter = 0;

    dataColumnCount = batchContext.getDataColumnCount();
    partitionColumnCount = batchContext.getPartitionColumnCount();
    partitionValues = new Object[partitionColumnCount];

    dataColumnNums = batchContext.getDataColumnNums();
    Preconditions.checkState(dataColumnNums != null);

    // Form a truncated boolean include array for our vector/row deserializers.
    determineDataColumnsToIncludeTruncated();

    /*
     * Create table related objects
     */
    final String[] rowColumnNames = batchContext.getRowColumnNames();
    final TypeInfo[] rowColumnTypeInfos = batchContext.getRowColumnTypeInfos();
    tableStructTypeInfo =
        TypeInfoFactory.getStructTypeInfo(
            Arrays.asList(rowColumnNames),
            Arrays.asList(rowColumnTypeInfos));
    tableStandardStructObjectInspector =
        (StandardStructObjectInspector)
            TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(tableStructTypeInfo);

    tableRowTypeInfos = batchContext.getRowColumnTypeInfos();

    /*
     * NOTE: We do not alter the projectedColumns / projectionSize of the batches to just be
     * the included columns (+ partition columns).
     *
     * For now, we need to model the object inspector rows because there are still several
     * vectorized operators that use them.
     *
     * We need to continue to model the Object[] as having null objects for not included columns
     * until the following has been fixed:
     *    o When we have to output a STRUCT for AVG we switch to row GroupBy operators.
     *    o Some variations of VectorMapOperator, VectorReduceSinkOperator, VectorFileSinkOperator
     *      use the row super class to process rows.
     */

    /*
     * The Vectorizer class enforces that there is only one TableScanOperator, so
     * we don't need the more complicated multiple root operator mapping that MapOperator has.
     */
    fileToPartitionContextMap = new HashMap<String, VectorPartitionContext>();

    // Temporary map so we only create one partition context entry.
    HashMap<PartitionDesc, VectorPartitionContext> partitionContextMap =
        new HashMap<PartitionDesc, VectorPartitionContext>();

    for (Map.Entry<Path, ArrayList<String>> entry : conf.getPathToAliases().entrySet()) {
      Path path = entry.getKey();
      PartitionDesc partDesc = conf.getPathToPartitionInfo().get(path);

      VectorPartitionContext vectorPartitionContext;
      if (!partitionContextMap.containsKey(partDesc)) {
        vectorPartitionContext = createAndInitPartitionContext(partDesc, hconf);
        partitionContextMap.put(partDesc, vectorPartitionContext);
      } else {
        vectorPartitionContext = partitionContextMap.get(partDesc);
      }

      fileToPartitionContextMap.put(path.toString(), vectorPartitionContext);
    }

    // Create list of one.
    List<Operator<? extends OperatorDesc>> children =
        new ArrayList<Operator<? extends OperatorDesc>>();
    children.add(oneRootOperator);

    setChildOperators(children);
  }

  @Override
  public void initializeMapOperator(Configuration hconf) throws HiveException {
    super.initializeMapOperator(hconf);

    oneRootOperator.initialize(hconf, new ObjectInspector[] {tableStandardStructObjectInspector});
  }

  public void initializeContexts() throws HiveException {
    Path fpath = getExecContext().getCurrentInputPath();
    String nominalPath = getNominalPath(fpath);
    setupPartitionContextVars(nominalPath);
  }

  // Find context for current input file
  @Override
  public void cleanUpInputFileChangedOp() throws HiveException {
    super.cleanUpInputFileChangedOp();
    Path fpath = getExecContext().getCurrentInputPath();
    String nominalPath = getNominalPath(fpath);

    setupPartitionContextVars(nominalPath);

    // Add alias, table name, and partitions to hadoop conf so that their
    // children will inherit these
    oneRootOperator.setInputContext(currentVectorPartContext.tableName,
        currentVectorPartContext.partName);
  }

  /*
   * Setup the context for reading from the next partition file.
   */
  private void setupPartitionContextVars(String nominalPath) throws HiveException {

    currentVectorPartContext = fileToPartitionContextMap.get(nominalPath);
    PartitionDesc partDesc = currentVectorPartContext.getPartDesc();
    VectorPartitionDesc vectorPartDesc = partDesc.getVectorPartitionDesc();
    currentReadType = vectorPartDesc.getVectorMapOperatorReadType();

    /*
     * Setup for 3 different kinds of vectorized reading supported:
     *
     *   1) Read the Vectorized Input File Format which returns VectorizedRowBatch as the row.
     *
     *   2) Read using VectorDeserializeRow to deserialize each row into the VectorizedRowBatch.
     *
     *   3) And read using the regular partition deserializer to get the row object and assigning
     *      the row object into the VectorizedRowBatch with VectorAssignRow.
     */
    if (currentReadType == VectorMapOperatorReadType.VECTORIZED_INPUT_FILE_FORMAT) {

      /*
       * The Vectorized Input File Format reader is responsible for setting the partition column
       * values, resetting and filling in the batch, etc.
       */

      /*
       * Clear all the reading variables.
       */
      currentDataColumnCount = 0;

      currentDeserializeRead = null;
      currentVectorDeserializeRow = null;

      currentPartDeserializer = null;
      currentPartRawRowObjectInspector = null;
      currentVectorAssign = null;

    } else {

      /*
       * We will get "regular" single rows from the Input File Format reader that we will need
       * to {vector|row} deserialize.
       */
      Preconditions.checkState(
          currentReadType == VectorMapOperatorReadType.VECTOR_DESERIALIZE ||
          currentReadType == VectorMapOperatorReadType.ROW_DESERIALIZE);

      if (deserializerBatch.size > 0) {

        /*
         * Clear out any rows in the batch from previous partition since we are going to change
         * the repeating partition column values.
         */
        batchCounter++;
        oneRootOperator.process(deserializerBatch, 0);
        deserializerBatch.reset();
        if (oneRootOperator.getDone()) {
          setDone(true);
          return;
        }

      }

      /*
       * For this particular file, how many columns will we actually read?
       */
      currentDataColumnCount = currentVectorPartContext.getReaderDataColumnCount();

      if (currentDataColumnCount < dataColumnCount) {

        /*
         * Default any additional data columns to NULL once for the file (if they are present).
         */
        for (int i = currentDataColumnCount; i < dataColumnCount; i++) {
          ColumnVector colVector = deserializerBatch.cols[i];
          if (colVector != null) {
            colVector.isNull[0] = true;
            colVector.noNulls = false;
            colVector.isRepeating = true;
          }
        }
      }

      if (batchContext.getPartitionColumnCount() > 0) {

        /*
         * The partition columns are set once for the partition and are marked repeating.
         */
        VectorizedRowBatchCtx.getPartitionValues(batchContext, partDesc, partitionValues);
        batchContext.addPartitionColsToBatch(deserializerBatch, partitionValues);
      }

      /*
       * Set or clear the rest of the reading variables based on {vector|row} deserialization.
       */
      switch (currentReadType) {
      case VECTOR_DESERIALIZE:
        {
          VectorDeserializePartitionContext vectorDeserPartContext =
              (VectorDeserializePartitionContext) currentVectorPartContext;

          // Set ours.
          currentDeserializeRead = vectorDeserPartContext.getDeserializeRead();
          currentVectorDeserializeRow = vectorDeserPartContext.getVectorDeserializeRow();

          // Clear the other ones.
          currentPartDeserializer = null;
          currentPartRawRowObjectInspector = null;
          currentVectorAssign = null;

        }
        break;

      case ROW_DESERIALIZE:
        {
          RowDeserializePartitionContext rowDeserPartContext =
              (RowDeserializePartitionContext) currentVectorPartContext;

          // Clear the other ones.
          currentDeserializeRead = null;
          currentVectorDeserializeRow = null;

          // Set ours.
          currentPartDeserializer = rowDeserPartContext.getPartDeserializer();
          currentPartRawRowObjectInspector = rowDeserPartContext.getPartRawRowObjectInspector();
          currentVectorAssign = rowDeserPartContext.getVectorAssign();
        }
        break;

      default:
        throw new RuntimeException("Unexpected VectorMapOperator read type " +
            currentReadType.name());
      }
    }
  }

  @Override
  public Deserializer getCurrentDeserializer() {
    // Not applicable.
    return null;
  }

  @Override
  public void process(Writable value) throws HiveException {

    // A mapper can span multiple files/partitions.
    // The VectorPartitionContext need to be changed if the input file changed
    ExecMapperContext context = getExecContext();
    if (context != null && context.inputFileChanged()) {
      // The child operators cleanup if input file has changed
      cleanUpInputFileChanged();
    }
    if (!oneRootOperator.getDone()) {

      /*
       * 3 different kinds of vectorized reading supported:
       *
       *   1) Read the Vectorized Input File Format which returns VectorizedRowBatch as the row.
       *
       *   2) Read using VectorDeserializeRow to deserialize each row into the VectorizedRowBatch.
       *
       *   3) And read using the regular partition deserializer to get the row object and assigning
       *      the row object into the VectorizedRowBatch with VectorAssignRow.
       */
      try {
        if (currentReadType == VectorMapOperatorReadType.VECTORIZED_INPUT_FILE_FORMAT) {

          /*
           * The Vectorized Input File Format reader has already set the partition column
           * values, reset and filled in the batch, etc.
           *
           * We pass the VectorizedRowBatch through here.
           */
          batchCounter++;
          if (value != null) {
            numRows += ((VectorizedRowBatch) value).size;
          }
          oneRootOperator.process(value, 0);
          if (oneRootOperator.getDone()) {
            setDone(true);
            return;
          }

        } else {

          /*
           * We have a "regular" single rows from the Input File Format reader that we will need
           * to deserialize.
           */
          Preconditions.checkState(
              currentReadType == VectorMapOperatorReadType.VECTOR_DESERIALIZE ||
              currentReadType == VectorMapOperatorReadType.ROW_DESERIALIZE);

          if (deserializerBatch.size == deserializerBatch.DEFAULT_SIZE) {
            numRows += deserializerBatch.size;

            /*
             * Feed current full batch to operator tree.
             */
            batchCounter++;
            oneRootOperator.process(deserializerBatch, 0);

            /**
             * Only reset the current data columns.  Not any data columns defaulted to NULL
             * because they are not present in the partition, and not partition columns.
             */
            for (int c = 0; c < currentDataColumnCount; c++) {
              ColumnVector colVector = deserializerBatch.cols[c];
              if (colVector != null) {
                colVector.reset();
                colVector.init();
              }
            }
            deserializerBatch.selectedInUse = false;
            deserializerBatch.size = 0;
            deserializerBatch.endOfFile = false;

            if (oneRootOperator.getDone()) {
              setDone(true);
              return;
            }
          }

          /*
           * Do the {vector|row} deserialization of the one row into the VectorizedRowBatch.
           */
          switch (currentReadType) {
          case VECTOR_DESERIALIZE:
            {
              BinaryComparable binComp = (BinaryComparable) value;
              currentDeserializeRead.set(binComp.getBytes(), 0, binComp.getLength());

              // Deserialize and append new row using the current batch size as the index.
              try {
                currentVectorDeserializeRow.deserialize(
                    deserializerBatch, deserializerBatch.size++);
              } catch (Exception e) {
                throw new HiveException(
                    "\nDeserializeRead detail: " +
                        currentVectorDeserializeRow.getDetailedReadPositionString(),
                    e);
              }
            }
            break;

          case ROW_DESERIALIZE:
            {
              Object deserialized = currentPartDeserializer.deserialize(value);

              // Note: Regardless of what the Input File Format returns, we have determined
              // with VectorAppendRow.initConversion that only currentDataColumnCount columns
              // have values we want.
              //
              // Any extra columns needed by the table schema were set to repeating null
              // in the batch by setupPartitionContextVars.

              // Convert input row to standard objects.
              List<Object> standardObjects = new ArrayList<Object>();
              ObjectInspectorUtils.copyToStandardObject(standardObjects, deserialized,
                  currentPartRawRowObjectInspector, ObjectInspectorCopyOption.WRITABLE);
              if (standardObjects.size() < currentDataColumnCount) {
                throw new HiveException("Input File Format returned row with too few columns");
              }

              // Append the deserialized standard object row using the current batch size
              // as the index.
              currentVectorAssign.assignRow(deserializerBatch, deserializerBatch.size++,
                  standardObjects, currentDataColumnCount);
            }
            break;

          default:
            throw new RuntimeException("Unexpected vector MapOperator read type " +
                currentReadType.name());
          }
        }
      } catch (Exception e) {
        throw new HiveException("Hive Runtime Error while processing row ", e);
      }
    }
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    throw new HiveException("Hive 2 Internal error: should not be called!");
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    if (!abort && oneRootOperator != null && !oneRootOperator.getDone() &&
        currentReadType != VectorMapOperatorReadType.VECTORIZED_INPUT_FILE_FORMAT) {
      if (deserializerBatch.size > 0) {
        numRows += deserializerBatch.size;
        batchCounter++;
        oneRootOperator.process(deserializerBatch, 0);
        deserializerBatch.size = 0;
      }
    }
    super.closeOp(abort);
  }

  @Override
  public String getName() {
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "MAP";
  }

  @Override
  public OperatorType getType() {
    return null;
  }
}
