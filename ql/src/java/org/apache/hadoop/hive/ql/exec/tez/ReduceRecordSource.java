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
package org.apache.hadoop.hive.ql.exec.tez;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.CommonMergeJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorDeserializeRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinaryDeserializeRead;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.tez.runtime.library.api.KeyValuesReader;

/**
 * Process input from tez LogicalInput and write output - for a map plan
 * Just pump the records through the query plan.
 */
@SuppressWarnings("deprecation")
public class ReduceRecordSource implements RecordSource {

  public static final Log l4j = LogFactory.getLog(ReduceRecordSource.class);

  private static final String CLASS_NAME = ReduceRecordSource.class.getName();

  private byte tag;

  private boolean abort = false;

  private Deserializer inputKeyDeserializer;

  // Input value serde needs to be an array to support different SerDe
  // for different tags
  private SerDe inputValueDeserializer;

  private TableDesc keyTableDesc;
  private TableDesc valueTableDesc;

  private ObjectInspector rowObjectInspector;
  private Operator<?> reducer;

  private Object keyObject = null;
  private BytesWritable groupKey;

  private boolean vectorized = false;

  private VectorDeserializeRow keyBinarySortableDeserializeToRow;

  private VectorDeserializeRow valueLazyBinaryDeserializeToRow;

  private VectorizedRowBatchCtx batchContext;
  private VectorizedRowBatch batch;

  // number of columns pertaining to keys in a vectorized row batch
  private int firstValueColumnOffset;
  private final int BATCH_SIZE = VectorizedRowBatch.DEFAULT_SIZE;

  private StructObjectInspector keyStructInspector;
  private StructObjectInspector valueStructInspectors;

  /* this is only used in the error code path */
  private List<VectorExpressionWriter> valueStringWriters;

  private KeyValuesReader reader;

  private boolean handleGroupKey;

  private ObjectInspector valueObjectInspector;

  private final PerfLogger perfLogger = PerfLogger.getPerfLogger();

  private Iterable<Object> valueWritables;
  
  private final GroupIterator groupIterator = new GroupIterator();

  void init(JobConf jconf, Operator<?> reducer, boolean vectorized, TableDesc keyTableDesc,
      TableDesc valueTableDesc, KeyValuesReader reader, boolean handleGroupKey, byte tag,
      Map<Integer, String> vectorScratchColumnTypeMap)
      throws Exception {

    ObjectInspector keyObjectInspector;

    this.reducer = reducer;
    this.vectorized = vectorized;
    this.keyTableDesc = keyTableDesc;
    this.reader = reader;
    this.handleGroupKey = handleGroupKey;
    this.tag = tag;

    try {
      inputKeyDeserializer = ReflectionUtils.newInstance(keyTableDesc
          .getDeserializerClass(), null);
      SerDeUtils.initializeSerDe(inputKeyDeserializer, null, keyTableDesc.getProperties(), null);
      keyObjectInspector = inputKeyDeserializer.getObjectInspector();

      if(vectorized) {
        keyStructInspector = (StructObjectInspector) keyObjectInspector;
        firstValueColumnOffset = keyStructInspector.getAllStructFieldRefs().size();
      }

      // We should initialize the SerDe with the TypeInfo when available.
      this.valueTableDesc = valueTableDesc;
      inputValueDeserializer = (SerDe) ReflectionUtils.newInstance(
          valueTableDesc.getDeserializerClass(), null);
      SerDeUtils.initializeSerDe(inputValueDeserializer, null,
          valueTableDesc.getProperties(), null);
      valueObjectInspector = inputValueDeserializer.getObjectInspector();

      ArrayList<ObjectInspector> ois = new ArrayList<ObjectInspector>();

      if(vectorized) {
        /* vectorization only works with struct object inspectors */
        valueStructInspectors = (StructObjectInspector) valueObjectInspector;

        final int totalColumns = firstValueColumnOffset +
            valueStructInspectors.getAllStructFieldRefs().size();
        valueStringWriters = new ArrayList<VectorExpressionWriter>(totalColumns);
        valueStringWriters.addAll(Arrays
            .asList(VectorExpressionWriterFactory
                .genVectorStructExpressionWritables(keyStructInspector)));
        valueStringWriters.addAll(Arrays
            .asList(VectorExpressionWriterFactory
                .genVectorStructExpressionWritables(valueStructInspectors)));

        /*
         * The row object inspector used by ReduceWork needs to be a **standard**
         * struct object inspector, not just any struct object inspector.
         */
        ArrayList<String> colNames = new ArrayList<String>();
        List<? extends StructField> fields = keyStructInspector.getAllStructFieldRefs();
        for (StructField field: fields) {
          colNames.add(Utilities.ReduceField.KEY.toString() + "." + field.getFieldName());
          ois.add(field.getFieldObjectInspector());
        }
        fields = valueStructInspectors.getAllStructFieldRefs();
        for (StructField field: fields) {
          colNames.add(Utilities.ReduceField.VALUE.toString() + "." + field.getFieldName());
          ois.add(field.getFieldObjectInspector());
        }
        rowObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(colNames, ois);

        batchContext = new VectorizedRowBatchCtx();
        batchContext.init(vectorScratchColumnTypeMap, (StructObjectInspector) rowObjectInspector);
        batch = batchContext.createVectorizedRowBatch();

        // Setup vectorized deserialization for the key and value.
        BinarySortableSerDe binarySortableSerDe = (BinarySortableSerDe) inputKeyDeserializer;

        keyBinarySortableDeserializeToRow =
                  new VectorDeserializeRow(
                        new BinarySortableDeserializeRead(
                                  VectorizedBatchUtil.primitiveTypeInfosFromStructObjectInspector(
                                      keyStructInspector),
                                  binarySortableSerDe.getSortOrders()));
        keyBinarySortableDeserializeToRow.init(0);

        final int valuesSize = valueStructInspectors.getAllStructFieldRefs().size();
        if (valuesSize > 0) {
          valueLazyBinaryDeserializeToRow =
                  new VectorDeserializeRow(
                        new LazyBinaryDeserializeRead(
                                  VectorizedBatchUtil.primitiveTypeInfosFromStructObjectInspector(
                                       valueStructInspectors)));
          valueLazyBinaryDeserializeToRow.init(firstValueColumnOffset);

          // Create data buffers for value bytes column vectors.
          for (int i = firstValueColumnOffset; i < batch.numCols; i++) {
            ColumnVector colVector = batch.cols[i];
            if (colVector instanceof BytesColumnVector) {
              BytesColumnVector bytesColumnVector = (BytesColumnVector) colVector;
              bytesColumnVector.initBuffer();
            }
          }
        }
      } else {
        ois.add(keyObjectInspector);
        ois.add(valueObjectInspector);
        rowObjectInspector =
            ObjectInspectorFactory.getStandardStructObjectInspector(Utilities.reduceFieldNameList,
                ois);
      }
    } catch (Throwable e) {
      abort = true;
      if (e instanceof OutOfMemoryError) {
        // Don't create a new object if we are already out of memory
        throw (OutOfMemoryError) e;
      } else {
        throw new RuntimeException("Reduce operator initialization failed", e);
      }
    }
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_INIT_OPERATORS);
  }
  
  @Override
  public final boolean isGrouped() {
    return vectorized;
  }

  @Override
  public boolean pushRecord() throws HiveException {

    if (vectorized) {
      return pushRecordVector();
    }

    if (groupIterator.hasNext()) {
      // if we have records left in the group we push one of those
      groupIterator.next();
      return true;
    }

    try {
      if (!reader.next()) {
        return false;
      }

      BytesWritable keyWritable = (BytesWritable) reader.getCurrentKey();
      valueWritables = reader.getCurrentValues();

      //Set the key, check if this is a new group or same group
      try {
        keyObject = inputKeyDeserializer.deserialize(keyWritable);
      } catch (Exception e) {
        throw new HiveException("Hive Runtime Error: Unable to deserialize reduce input key from "
            + Utilities.formatBinaryString(keyWritable.getBytes(), 0, keyWritable.getLength())
            + " with properties " + keyTableDesc.getProperties(), e);
      }

      if (handleGroupKey && !keyWritable.equals(this.groupKey)) {
        // If a operator wants to do some work at the beginning of a group
        if (groupKey == null) { // the first group
          this.groupKey = new BytesWritable();
        } else {
          // If a operator wants to do some work at the end of a group
          reducer.endGroup();
        }

        groupKey.set(keyWritable.getBytes(), 0, keyWritable.getLength());
        reducer.startGroup();
        reducer.setGroupKeyObject(keyObject);
      }

      groupIterator.initialize(valueWritables, keyObject, tag);
      if (groupIterator.hasNext()) {
        groupIterator.next(); // push first record of group
      }
      return true;
    } catch (Throwable e) {
      abort = true;
      if (e instanceof OutOfMemoryError) {
        // Don't create a new object if we are already out of memory
        throw (OutOfMemoryError) e;
      } else {
        l4j.fatal(StringUtils.stringifyException(e));
        throw new RuntimeException(e);
      }
    }
  }

  private Object deserializeValue(BytesWritable valueWritable, byte tag)
      throws HiveException {

    try {
      return inputValueDeserializer.deserialize(valueWritable);
    } catch (SerDeException e) {
      throw new HiveException(
          "Hive Runtime Error: Unable to deserialize reduce input value (tag="
              + tag
              + ") from "
          + Utilities.formatBinaryString(valueWritable.getBytes(), 0, valueWritable.getLength())
          + " with properties " + valueTableDesc.getProperties(), e);
    }
  }

  private class GroupIterator {
    private final List<Object> row = new ArrayList<Object>(Utilities.reduceFieldNameList.size());
    private List<Object> passDownKey = null;
    private Iterator<Object> values;
    private byte tag;
    private Object keyObject;

    public void initialize(Iterable<Object> values, Object keyObject, byte tag) {
      this.passDownKey = null;
      this.values = values.iterator();
      this.tag = tag;
      this.keyObject = keyObject;
    }

    public boolean hasNext() {
      return values != null && values.hasNext();
    }

    public void next() throws HiveException {
      row.clear();
      Object value = values.next();
      BytesWritable valueWritable = (BytesWritable) value;

      if (passDownKey == null) {
        row.add(this.keyObject);
      } else {
        row.add(passDownKey.get(0));
      }
      if ((passDownKey == null) && (reducer instanceof CommonMergeJoinOperator)) {
        passDownKey =
            (List<Object>) ObjectInspectorUtils.copyToStandardObject(row,
                reducer.getInputObjInspectors()[tag], ObjectInspectorCopyOption.WRITABLE);
        row.remove(0);
        row.add(0, passDownKey.get(0));
      }

      row.add(deserializeValue(valueWritable, tag));

      try {
        reducer.process(row, tag);
      } catch (Exception e) {
        String rowString = null;
        try {
          rowString = SerDeUtils.getJSONString(row, rowObjectInspector);
        } catch (Exception e2) {
          rowString = "[Error getting row data with exception "
              + StringUtils.stringifyException(e2) + " ]";
        }
        throw new HiveException("Hive Runtime Error while processing row (tag="
            + tag + ") " + rowString, e);
      }
    }
  }

  private boolean pushRecordVector() {
    try {
      if (!reader.next()) {
        return false;
      }

      BytesWritable keyWritable = (BytesWritable) reader.getCurrentKey();
      valueWritables = reader.getCurrentValues();

      // Check if this is a new group or same group
      if (handleGroupKey && !keyWritable.equals(this.groupKey)) {
        // If a operator wants to do some work at the beginning of a group
        if (groupKey == null) { // the first group
          this.groupKey = new BytesWritable();
        } else {
          // If a operator wants to do some work at the end of a group
          reducer.endGroup();
        }

        groupKey.set(keyWritable.getBytes(), 0, keyWritable.getLength());
        reducer.startGroup();
      }

      processVectorGroup(keyWritable, valueWritables, tag);
      return true;
    } catch (Throwable e) {
      abort = true;
      if (e instanceof OutOfMemoryError) {
        // Don't create a new object if we are already out of memory
        throw (OutOfMemoryError) e;
      } else {
        l4j.fatal(StringUtils.stringifyException(e));
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * @param values
   * @return true if it is not done and can take more inputs
   */
  private void processVectorGroup(BytesWritable keyWritable,
          Iterable<Object> values, byte tag) throws HiveException, IOException {

    // Deserialize key into vector row columns.
    // Since we referencing byte column vector byte arrays by reference, we don't need
    // a data buffer.
    byte[] keyBytes = keyWritable.getBytes();
    int keyLength = keyWritable.getLength();
    keyBinarySortableDeserializeToRow.setBytes(keyBytes, 0, keyLength);
    keyBinarySortableDeserializeToRow.deserializeByValue(batch, 0);
    for(int i = 0; i < firstValueColumnOffset; i++) {
      VectorizedBatchUtil.setRepeatingColumn(batch, i);
    }

    int rowIdx = 0;
    try {
      for (Object value : values) {
        if (valueLazyBinaryDeserializeToRow != null) {
          // Deserialize value into vector row columns.
          BytesWritable valueWritable = (BytesWritable) value;
          byte[] valueBytes = valueWritable.getBytes();
          int valueLength = valueWritable.getLength();
          valueLazyBinaryDeserializeToRow.setBytes(valueBytes, 0, valueLength);
          valueLazyBinaryDeserializeToRow.deserializeByValue(batch, rowIdx);
        }
        rowIdx++;
        if (rowIdx >= BATCH_SIZE) {
          VectorizedBatchUtil.setBatchSize(batch, rowIdx);
          reducer.process(batch, tag);

          // Reset just the value columns and value buffer.
          for (int i = firstValueColumnOffset; i < batch.numCols; i++) {
            // Note that reset also resets the data buffer for bytes column vectors.
            batch.cols[i].reset();
          }
          rowIdx = 0;
        }
      }
      if (rowIdx > 0) {
        // Flush final partial batch.
        VectorizedBatchUtil.setBatchSize(batch, rowIdx);
        reducer.process(batch, tag);
      }
      batch.reset();
    } catch (Exception e) {
      String rowString = null;
      try {
        /* batch.toString depends on this */
        batch.setValueWriters(valueStringWriters
            .toArray(new VectorExpressionWriter[0]));
        rowString = batch.toString();
      } catch (Exception e2) {
        rowString = "[Error getting row data with exception "
            + StringUtils.stringifyException(e2) + " ]";
      }
      throw new HiveException("Hive Runtime Error while processing vector batch (tag="
          + tag + ") " + rowString, e);
    }
  }

  boolean close() throws Exception {
    try {
      if (handleGroupKey && groupKey != null) {
        // If a operator wants to do some work at the end of a group
        reducer.endGroup();
      }
    } catch (Exception e) {
      if (!abort) {
        // signal new failure to map-reduce
        throw new RuntimeException("Hive Runtime Error while closing operators: "
            + e.getMessage(), e);
      }
    }
    return abort;
  }

  public ObjectInspector getObjectInspector() {
    return rowObjectInspector;
  }
}
