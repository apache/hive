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

package org.apache.hadoop.hive.ql.exec.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper.ReportStats;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorDeserializeRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinaryDeserializeRead;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;

/**
 * Clone from ExecReducer, it is the bridge between the spark framework and
 * the Hive operator pipeline at execution time. It's main responsibilities are:
 *
 * - Load and setup the operator pipeline from XML
 * - Run the pipeline by transforming key, value pairs to records and forwarding them to the operators
 * - Sending start and end group messages to separate records with same key from one another
 * - Catch and handle errors during execution of the operators.
 *
 */
public class SparkReduceRecordHandler extends SparkRecordHandler {

  private static final Logger LOG = LoggerFactory.getLogger(SparkReduceRecordHandler.class);

  // Input value serde needs to be an array to support different SerDe
  // for different tags
  private final Deserializer[] inputValueDeserializer = new Deserializer[Byte.MAX_VALUE];
  private final Object[] valueObject = new Object[Byte.MAX_VALUE];
  private final List<Object> row = new ArrayList<Object>(Utilities.reduceFieldNameList.size());

  // TODO: move to DynamicSerDe when it's ready
  private Deserializer inputKeyDeserializer;
  private Operator<?> reducer;
  private boolean isTagged = false;
  private TableDesc keyTableDesc;
  private TableDesc[] valueTableDesc;
  private ObjectInspector[] rowObjectInspector;
  private boolean vectorized = false;

  private VectorDeserializeRow<BinarySortableDeserializeRead> keyBinarySortableDeserializeToRow;

  private VectorDeserializeRow<LazyBinaryDeserializeRead> valueLazyBinaryDeserializeToRow;

  private VectorizedRowBatch batch;
  private long batchBytes = 0;
  private boolean handleGroupKey = true;  // For now.

  private DataOutputBuffer buffer;

  // number of columns pertaining to keys in a vectorized row batch
  private int firstValueColumnOffset;

  private static final int BATCH_SIZE = VectorizedRowBatch.DEFAULT_SIZE;
  private static final int BATCH_BYTES = VectorizedRowBatch.DEFAULT_BYTES;

  // runtime objects
  private transient Object keyObject;
  private transient BytesWritable groupKey;

  private StructObjectInspector keyStructInspector;
  private StructObjectInspector valueStructInspector;
  /* this is only used in the error code path */
  private MapredLocalWork localWork = null;

  @Override
  @SuppressWarnings("unchecked")
  public void init(JobConf job, OutputCollector output, Reporter reporter) throws Exception {
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.SPARK_INIT_OPERATORS);
    super.init(job, output, reporter);

    rowObjectInspector = new ObjectInspector[Byte.MAX_VALUE];
    ObjectInspector[] valueObjectInspector = new ObjectInspector[Byte.MAX_VALUE];
    ObjectInspector keyObjectInspector;

    ReduceWork gWork = Utilities.getReduceWork(job);

    reducer = gWork.getReducer();
    vectorized = gWork.getVectorMode();
    reducer.setParentOperators(null); // clear out any parents as reducer is the
    // root
    isTagged = gWork.getNeedsTagging();
    try {
      keyTableDesc = gWork.getKeyDesc();
      inputKeyDeserializer = ReflectionUtils.newInstance(keyTableDesc
        .getDeserializerClass(), null);
      SerDeUtils.initializeSerDe(inputKeyDeserializer, null, keyTableDesc.getProperties(), null);
      keyObjectInspector = inputKeyDeserializer.getObjectInspector();
      valueTableDesc = new TableDesc[gWork.getTagToValueDesc().size()];

      if (vectorized) {
        final int maxTags = gWork.getTagToValueDesc().size();

        // CONSIDER: Cleaning up this code and eliminating the arrays.  Vectorization only handles
        // one operator tree.
        Preconditions.checkState(maxTags == 1);

        keyStructInspector = (StructObjectInspector) keyObjectInspector;
        firstValueColumnOffset = keyStructInspector.getAllStructFieldRefs().size();

        buffer = new DataOutputBuffer();
      }

      for (int tag = 0; tag < gWork.getTagToValueDesc().size(); tag++) {
        // We should initialize the SerDe with the TypeInfo when available.
        valueTableDesc[tag] = gWork.getTagToValueDesc().get(tag);
        inputValueDeserializer[tag] = ReflectionUtils.newInstance(
            valueTableDesc[tag].getDeserializerClass(), null);
        SerDeUtils.initializeSerDe(inputValueDeserializer[tag], null,
            valueTableDesc[tag].getProperties(), null);
        valueObjectInspector[tag] = inputValueDeserializer[tag].getObjectInspector();

        ArrayList<ObjectInspector> ois = new ArrayList<ObjectInspector>();

        if (vectorized) {
          /* vectorization only works with struct object inspectors */
          valueStructInspector = (StructObjectInspector) valueObjectInspector[tag];

          final int totalColumns = firstValueColumnOffset
              + valueStructInspector.getAllStructFieldRefs().size();

          rowObjectInspector[tag] = Utilities.constructVectorizedReduceRowOI(keyStructInspector,
              valueStructInspector);
          batch = gWork.getVectorizedRowBatchCtx().createVectorizedRowBatch();

          // Setup vectorized deserialization for the key and value.
          BinarySortableSerDe binarySortableSerDe = (BinarySortableSerDe) inputKeyDeserializer;

          keyBinarySortableDeserializeToRow =
                    new VectorDeserializeRow<BinarySortableDeserializeRead>(
                          new BinarySortableDeserializeRead(
                                    VectorizedBatchUtil.typeInfosFromStructObjectInspector(
                                        keyStructInspector),
                                    /* useExternalBuffer */ true,
                                    binarySortableSerDe.getSortOrders(),
                                    binarySortableSerDe.getNullMarkers(),
                                    binarySortableSerDe.getNotNullMarkers()));
          keyBinarySortableDeserializeToRow.init(0);

          final int valuesSize = valueStructInspector.getAllStructFieldRefs().size();
          if (valuesSize > 0) {
            valueLazyBinaryDeserializeToRow =
                    new VectorDeserializeRow<LazyBinaryDeserializeRead>(
                          new LazyBinaryDeserializeRead(
                              VectorizedBatchUtil.typeInfosFromStructObjectInspector(
                                         valueStructInspector),
                              /* useExternalBuffer */ true));
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
          ois.add(valueObjectInspector[tag]);
          //reducer.setGroupKeyObjectInspector(keyObjectInspector);
          rowObjectInspector[tag] = ObjectInspectorFactory.getStandardStructObjectInspector(
              Utilities.reduceFieldNameList, ois);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    ExecMapperContext execContext = new ExecMapperContext(job);
    localWork = gWork.getMapRedLocalWork();
    execContext.setJc(jc);
    execContext.setLocalWork(localWork);
    reducer.passExecContext(execContext);

    reducer.setReporter(rp);
    OperatorUtils.setChildrenCollector(
        Arrays.<Operator<? extends OperatorDesc>>asList(reducer), output);

    // initialize reduce operator tree
    try {
      LOG.info(reducer.dump(0));
      reducer.initialize(jc, rowObjectInspector);

      if (localWork != null) {
        for (Operator<? extends OperatorDesc> dummyOp : localWork.getDummyParentOp()) {
          dummyOp.setExecContext(execContext);
          dummyOp.initialize(jc, null);
        }
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
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.SPARK_INIT_OPERATORS);
  }

  /**
   * A reusable dummy iterator that has only one value.
   *
   */
  private static class DummyIterator implements Iterator<Object> {
    private boolean done = false;
    private Object value = null;

    public void setValue(Object v) {
      this.value = v;
      done = false;
    }

    @Override
    public boolean hasNext() {
      return !done;
    }

    @Override
    public Object next() {
      done = true;
      return value;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Iterator.remove() is not implemented/supported");
    }
  }

  private DummyIterator dummyIterator = new DummyIterator();

  /**
   * Process one row using a dummy iterator.  Or, add row to vector batch.
   */
  @Override
  public void processRow(Object key, final Object value) throws IOException {
    if (!anyRow) {
      anyRow = true;
    }
    if (vectorized) {
      processVectorRow(key, value);
    } else {
      dummyIterator.setValue(value);
      processRow(key, dummyIterator);
    }
  }



  @Override
  public <E> void processRow(Object key, Iterator<E> values) throws IOException {
    if (!anyRow) {
      anyRow = true;
    }
    if (vectorized) {
      processVectorRows(key, values);
      return;
    }

    if (reducer.getDone()) {
      return;
    }

    try {
      BytesWritable keyWritable = (BytesWritable) key;
      byte tag = 0;
      if (isTagged) {
        // remove the tag from key coming out of reducer
        // and store it in separate variable.
        // make a copy for multi-insert with join case as Spark re-uses input key from same parent
        int size = keyWritable.getSize() - 1;
        tag = keyWritable.get()[size];
        keyWritable = new BytesWritable(keyWritable.getBytes(), size);
        keyWritable.setSize(size);
      }

      if (!keyWritable.equals(groupKey)) {
        // If a operator wants to do some work at the beginning of a group
        if (groupKey == null) { // the first group
          groupKey = new BytesWritable();
        } else {
          // If a operator wants to do some work at the end of a group
          LOG.trace("End Group");
          reducer.endGroup();
        }

        try {
          keyObject = inputKeyDeserializer.deserialize(keyWritable);
        } catch (Exception e) {
          throw new HiveException(
            "Hive Runtime Error: Unable to deserialize reduce input key from "
              + Utilities.formatBinaryString(keyWritable.get(), 0,
              keyWritable.getSize()) + " with properties "
              + keyTableDesc.getProperties(), e);
        }

        groupKey.set(keyWritable.get(), 0, keyWritable.getSize());
        LOG.trace("Start Group");
        reducer.setGroupKeyObject(keyObject);
        reducer.startGroup();
      }
      processKeyValues(values, tag);

    } catch (Throwable e) {
      abort = true;
      Utilities.setReduceWork(jc, null);
      if (e instanceof OutOfMemoryError) {
        // Don't create a new object if we are already out of memory
        throw (OutOfMemoryError) e;
      } else {
        String msg = "Fatal error: " + e;
        LOG.error(msg, e);
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * @param values
   * @return true if it is not done and can take more inputs
   */
  private <E> boolean processKeyValues(Iterator<E> values, byte tag) throws HiveException {
    while (values.hasNext()) {
      BytesWritable valueWritable = (BytesWritable) values.next();
      try {
        valueObject[tag] = inputValueDeserializer[tag].deserialize(valueWritable);
      } catch (SerDeException e) {
        throw new HiveException(
          "Hive Runtime Error: Unable to deserialize reduce input value (tag="
            + tag
            + ") from "
            + Utilities.formatBinaryString(valueWritable.get(), 0,
            valueWritable.getSize()) + " with properties "
            + valueTableDesc[tag].getProperties(), e);
      }
      row.clear();
      row.add(keyObject);
      row.add(valueObject[tag]);
      if (LOG.isInfoEnabled()) {
        logMemoryInfo();
      }
      try {
        reducer.process(row, tag);
      } catch (Exception e) {
        String rowString = null;
        try {
          rowString = SerDeUtils.getJSONString(row, rowObjectInspector[tag]);
        } catch (Exception e2) {
          rowString = "[Error getting row data with exception "
            + StringUtils.stringifyException(e2) + " ]";
        }
        throw new HiveException("Error while processing row (tag="
          + tag + ") " + rowString, e);
      }
    }
    return true; // give me more
  }

  private <E> void processVectorRows(Object key, Iterator<E> values) throws IOException {
    if (reducer.getDone()) {
      return;
    }
    while (values.hasNext()) {
      processVectorRow(key, values.next());
    }
  }

  private void processVectorRow(Object key, final Object value) throws IOException {
    BytesWritable keyWritable = (BytesWritable) key;
    BytesWritable valueWritable = (BytesWritable) value;

    try {

      if (handleGroupKey) {
        final boolean isKeyChange;
        if (groupKey == null) {

          // The first group.
          isKeyChange = true;
          groupKey = new BytesWritable();
        } else {
          isKeyChange = !keyWritable.equals(groupKey);
        }

        if (isKeyChange) {

          // Flush current group batch as last batch of group.
          if (batch.size > 0) {

            // Indicate last batch of current group.
            reducer.setNextVectorBatchGroupStatus(/* isLastGroupBatch */ true);

            // Forward; reset key and value columns.
            forwardBatch(/* resetValueColumnsOnly */ false);
          }

          // Deserialize group key into vector row columns.
          byte[] keyBytes = keyWritable.getBytes();
          int keyLength = keyWritable.getLength();

          groupKey.set(keyBytes, 0, keyLength);

          keyBinarySortableDeserializeToRow.setBytes(keyBytes, 0, keyLength);
          try {
            keyBinarySortableDeserializeToRow.deserialize(batch, 0);
          } catch (Exception e) {
            throw new HiveException(
                "\nDeserializeRead details: " +
                    keyBinarySortableDeserializeToRow.getDetailedReadPositionString(),
                e);
          }

          // And, mark group keys as repeating.
          for(int i = 0; i < firstValueColumnOffset; i++) {
            VectorizedBatchUtil.setRepeatingColumn(batch, i);
          }
        }

        // Can we add to current batch?
        if (batch.size >= batch.getMaxSize() ||
            batch.size > 0 && batchBytes >= BATCH_BYTES) {

          // We have a row for current group, so we indicate not the last batch.
          reducer.setNextVectorBatchGroupStatus(/* isLastGroupBatch */ false);

          // Batch is full or using too much space.
          forwardBatch(/* resetValueColumnsOnly */ true);
        }

        if (valueLazyBinaryDeserializeToRow != null) {
          // Deserialize value into vector row columns.
          byte[] valueBytes = valueWritable.getBytes();
          int valueLength = valueWritable.getLength();
          batchBytes += valueLength;

          valueLazyBinaryDeserializeToRow.setBytes(valueBytes, 0, valueLength);
          valueLazyBinaryDeserializeToRow.deserialize(batch, batch.size);
        }
        batch.size++;
      } else {

        // No group key.

        // Can we add to current batch?
        if (batch.size >= batch.getMaxSize() ||
            batch.size > 0 && batchBytes >= BATCH_BYTES) {

          // Batch is full or using too much space.
          forwardBatch(/* resetValueColumnsOnly */ false);
        }

        // Deserialize key into vector row columns.
        byte[] keyBytes = keyWritable.getBytes();
        int keyLength = keyWritable.getLength();

        keyBinarySortableDeserializeToRow.setBytes(keyBytes, 0, keyLength);
        try {
          keyBinarySortableDeserializeToRow.deserialize(batch, 0);
        } catch (Exception e) {
          throw new HiveException(
              "\nDeserializeRead details: " +
                  keyBinarySortableDeserializeToRow.getDetailedReadPositionString(),
              e);
        }

        if (valueLazyBinaryDeserializeToRow != null) {
          // Deserialize value into vector row columns.
          byte[] valueBytes = valueWritable.getBytes();
          int valueLength = valueWritable.getLength();

          batchBytes += valueLength;

          valueLazyBinaryDeserializeToRow.setBytes(valueBytes, 0, valueLength);
          valueLazyBinaryDeserializeToRow.deserialize(batch, batch.size);
        }
        batch.size++;
      }
    } catch (Throwable e) {
      abort = true;
      if (e instanceof OutOfMemoryError) {
        // Don't create a new object if we are already out of memory
        throw (OutOfMemoryError) e;
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  private void forwardBatch(boolean resetValueColumnsOnly) throws HiveException {
    reducer.process(batch, 0);

    if (resetValueColumnsOnly) {
      // Reset just the value columns and value buffer.
      for (int i = firstValueColumnOffset; i < batch.numCols; i++) {
        // Note that reset also resets the data buffer for bytes column vectors.
        batch.cols[i].reset();
      }
      batch.size = 0;
    } else {
      // Reset key and value columns; and batch.size
      batch.reset();
    }

    batchBytes = 0;
    if (LOG.isInfoEnabled()) {
      logMemoryInfo();
    }
  }

  private Object deserializeValue(BytesWritable valueWritable, byte tag) throws HiveException {
    try {
      return inputValueDeserializer[tag].deserialize(valueWritable);
    } catch (SerDeException e) {
      throw new HiveException("Error: Unable to deserialize reduce input value (tag="
        + tag + ") from "
        + Utilities.formatBinaryString(valueWritable.getBytes(), 0, valueWritable.getLength())
        + " with properties " + valueTableDesc[tag].getProperties(), e);
    }
  }

  @Override
  public void close() {

    // No row was processed
    if (!anyRow) {
      LOG.trace("Close called without any rows processed");
    }

    try {
      if (vectorized) {
        if (batch.size > 0) {

          if (handleGroupKey) {
            // Indicate last batch of current group.
            reducer.setNextVectorBatchGroupStatus(/* isLastGroupBatch */ true);
          }

          forwardBatch(/* resetValueColumnsOnly */ false);
        }
      } else {
        if (groupKey != null) {
          // If a operator wants to do some work at the end of a group
          LOG.trace("End Group");
          reducer.endGroup();
        }
      }
      if (LOG.isInfoEnabled()) {
        logCloseInfo();
      }

      reducer.close(abort);

      if (localWork != null) {
        for (Operator<? extends OperatorDesc> dummyOp : localWork.getDummyParentOp()) {
          dummyOp.close(abort);
        }
      }

      ReportStats rps = new ReportStats(rp, jc);
      reducer.preorderMap(rps);

    } catch (Exception e) {
      if (!abort) {
        // signal new failure to map-reduce
        LOG.error("Hit error while closing operators - failing tree");
        throw new RuntimeException("Hive Runtime Error while closing operators: "
          + e.getMessage(), e);
      }
    } finally {
      MapredContext.close();
      Utilities.clearWorkMap(jc);
    }
  }

  @Override
  public boolean getDone() {
    return reducer.getDone();
  }

  public static String displayBytes(byte[] bytes, int start, int length) {
    StringBuilder sb = new StringBuilder();
    for (int i = start; i < start + length; i++) {
      sb.append(String.format("\\%03d", (int) (bytes[i] & 0xff)));
    }
    return sb.toString();
  }
}
