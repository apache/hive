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

package org.apache.hadoop.hive.ql.exec.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper.ReportStats;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

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

  private static final Log LOG = LogFactory.getLog(SparkReduceRecordHandler.class);
  private static final String PLAN_KEY = "__REDUCE_PLAN__";

  // Input value serde needs to be an array to support different SerDe
  // for different tags
  private final Deserializer[] inputValueDeserializer = new Deserializer[Byte.MAX_VALUE];
  private final Object[] valueObject = new Object[Byte.MAX_VALUE];
  private final List<Object> row = new ArrayList<Object>(Utilities.reduceFieldNameList.size());
  private final boolean isLogInfoEnabled = LOG.isInfoEnabled();

  // TODO: move to DynamicSerDe when it's ready
  private Deserializer inputKeyDeserializer;
  private Operator<?> reducer;
  private boolean isTagged = false;
  private TableDesc keyTableDesc;
  private TableDesc[] valueTableDesc;
  private ObjectInspector[] rowObjectInspector;
  private boolean vectorized = false;

  // runtime objects
  private transient Object keyObject;
  private transient BytesWritable groupKey;

  private DataOutputBuffer buffer;
  private VectorizedRowBatch[] batches;
  // number of columns pertaining to keys in a vectorized row batch
  private int keysColumnOffset;
  private static final int BATCH_SIZE = VectorizedRowBatch.DEFAULT_SIZE;
  private StructObjectInspector keyStructInspector;
  private StructObjectInspector[] valueStructInspectors;
  /* this is only used in the error code path */
  private List<VectorExpressionWriter>[] valueStringWriters;
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
        keyStructInspector = (StructObjectInspector) keyObjectInspector;
        batches = new VectorizedRowBatch[maxTags];
        valueStructInspectors = new StructObjectInspector[maxTags];
        valueStringWriters = new List[maxTags];
        keysColumnOffset = keyStructInspector.getAllStructFieldRefs().size();
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
          valueStructInspectors[tag] = (StructObjectInspector) valueObjectInspector[tag];

          batches[tag] = VectorizedBatchUtil.constructVectorizedRowBatch(keyStructInspector,
              valueStructInspectors[tag]);
          final int totalColumns = keysColumnOffset
              + valueStructInspectors[tag].getAllStructFieldRefs().size();
          valueStringWriters[tag] = new ArrayList<VectorExpressionWriter>(totalColumns);
          valueStringWriters[tag].addAll(Arrays.asList(VectorExpressionWriterFactory
              .genVectorStructExpressionWritables(keyStructInspector)));
          valueStringWriters[tag].addAll(Arrays.asList(VectorExpressionWriterFactory
              .genVectorStructExpressionWritables(valueStructInspectors[tag])));

          /*
           * The row object inspector used by ReduceWork needs to be a
           * **standard** struct object inspector, not just any struct object
           * inspector.
           */
          ArrayList<String> colNames = new ArrayList<String>();
          List<? extends StructField> fields = keyStructInspector.getAllStructFieldRefs();
          for (StructField field : fields) {
            colNames.add(Utilities.ReduceField.KEY.toString() + "." + field.getFieldName());
            ois.add(field.getFieldObjectInspector());
          }
          fields = valueStructInspectors[tag].getAllStructFieldRefs();
          for (StructField field : fields) {
            colNames.add(Utilities.ReduceField.VALUE.toString() + "." + field.getFieldName());
            ois.add(field.getFieldObjectInspector());
          }
          rowObjectInspector[tag] = ObjectInspectorFactory.getStandardStructObjectInspector(
              colNames, ois);
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

  @Override
  public void processRow(Object key, Object value) throws IOException {
    throw new UnsupportedOperationException("Do not support this method in SparkReduceRecordHandler.");
  }

  @Override
  public <E> void processRow(Object key, Iterator<E> values) throws IOException {
    if (reducer.getDone()) {
      return;
    }

    try {
      BytesWritable keyWritable = (BytesWritable) key;
      byte tag = 0;
      if (isTagged) {
        // remove the tag from key coming out of reducer
        // and store it in separate variable.
        int size = keyWritable.getSize() - 1;
        tag = keyWritable.get()[size];
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
      /* this.keyObject passed via reference */
      if (vectorized) {
        processVectors(values, tag);
      } else {
        processKeyValues(values, tag);
      }

    } catch (Throwable e) {
      abort = true;
      Utilities.setReduceWork(jc, null);
      if (e instanceof OutOfMemoryError) {
        // Don't create a new object if we are already out of memory
        throw (OutOfMemoryError) e;
      } else {
        String msg = "Fatal error: " + e;
        LOG.fatal(msg, e);
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
      if (isLogInfoEnabled) {
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

  /**
   * @param values
   * @return true if it is not done and can take more inputs
   */
  private <E> boolean processVectors(Iterator<E> values, byte tag) throws HiveException {
    VectorizedRowBatch batch = batches[tag];
    batch.reset();

    /* deserialize key into columns */
    VectorizedBatchUtil.addRowToBatchFrom(keyObject, keyStructInspector, 0, 0, batch, buffer);
    for (int i = 0; i < keysColumnOffset; i++) {
      VectorizedBatchUtil.setRepeatingColumn(batch, i);
    }

    int rowIdx = 0;
    try {
      while (values.hasNext()) {
        /* deserialize value into columns */
        BytesWritable valueWritable = (BytesWritable) values.next();
        Object valueObj = deserializeValue(valueWritable, tag);

        VectorizedBatchUtil.addRowToBatchFrom(valueObj, valueStructInspectors[tag], rowIdx,
            keysColumnOffset, batch, buffer);
        rowIdx++;
        if (rowIdx >= BATCH_SIZE) {
          VectorizedBatchUtil.setBatchSize(batch, rowIdx);
          reducer.process(batch, tag);
          rowIdx = 0;
          if (isLogInfoEnabled) {
            logMemoryInfo();
          }
        }
      }
      if (rowIdx > 0) {
        VectorizedBatchUtil.setBatchSize(batch, rowIdx);
        reducer.process(batch, tag);
      }
      if (isLogInfoEnabled) {
        logMemoryInfo();
      }
    } catch (Exception e) {
      String rowString = null;
      try {
        /* batch.toString depends on this */
        batch.setValueWriters(valueStringWriters[tag].toArray(new VectorExpressionWriter[0]));
        rowString = batch.toString();
      } catch (Exception e2) {
        rowString = "[Error getting row data with exception " + StringUtils.stringifyException(e2)
          + " ]";
      }
      throw new HiveException("Error while processing vector batch (tag=" + tag + ") "
        + rowString, e);
    }
    return true; // give me more
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
    if (oc == null) {
      LOG.trace("Close called without any rows processed");
    }

    try {
      if (groupKey != null) {
        // If a operator wants to do some work at the end of a group
        LOG.trace("End Group");
        reducer.endGroup();
      }
      if (isLogInfoEnabled) {
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
      Utilities.clearWorkMap();
    }
  }

  @Override
  public boolean getDone() {
    return reducer.getDone();
  }
}
