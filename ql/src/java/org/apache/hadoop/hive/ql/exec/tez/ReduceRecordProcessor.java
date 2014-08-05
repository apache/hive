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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.HashTableDummyOperator;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper.ReportStats;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.exec.tez.TezProcessor.TezKVOutputCollector;
import org.apache.hadoop.hive.ql.exec.tez.tools.InputMerger;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
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
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.tez.mapreduce.processor.MRTaskReporter;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.TezProcessorContext;
import org.apache.tez.runtime.library.api.KeyValuesReader;

/**
 * Process input from tez LogicalInput and write output - for a map plan
 * Just pump the records through the query plan.
 */
public class ReduceRecordProcessor  extends RecordProcessor{

  private static final String REDUCE_PLAN_KEY = "__REDUCE_PLAN__";

  public static final Log l4j = LogFactory.getLog(ReduceRecordProcessor.class);
  private final ExecMapperContext execContext = new ExecMapperContext();
  private boolean abort = false;
  private Deserializer inputKeyDeserializer;

  // Input value serde needs to be an array to support different SerDe
  // for different tags
  private final SerDe[] inputValueDeserializer = new SerDe[Byte.MAX_VALUE];

  TableDesc keyTableDesc;
  TableDesc[] valueTableDesc;

  ObjectInspector[] rowObjectInspector;
  private Operator<?> reducer;
  private boolean isTagged = false;

  private Object keyObject = null;
  private BytesWritable groupKey;

  private ReduceWork redWork;

  private boolean vectorized = false;

  List<Object> row = new ArrayList<Object>(Utilities.reduceFieldNameList.size());

  private DataOutputBuffer buffer;
  private VectorizedRowBatch[] batches;
  // number of columns pertaining to keys in a vectorized row batch
  private int keysColumnOffset;
  private final int BATCH_SIZE = VectorizedRowBatch.DEFAULT_SIZE;
  private StructObjectInspector keyStructInspector;
  private StructObjectInspector[] valueStructInspectors;
  /* this is only used in the error code path */
  private List<VectorExpressionWriter>[] valueStringWriters;

  @Override
  void init(JobConf jconf, TezProcessorContext processorContext, MRTaskReporter mrReporter,
      Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs) throws Exception {
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_INIT_OPERATORS);
    super.init(jconf, processorContext, mrReporter, inputs, outputs);

    ObjectCache cache = ObjectCacheFactory.getCache(jconf);

    rowObjectInspector = new ObjectInspector[Byte.MAX_VALUE];
    ObjectInspector[] valueObjectInspector = new ObjectInspector[Byte.MAX_VALUE];
    ObjectInspector keyObjectInspector;

    redWork = (ReduceWork) cache.retrieve(REDUCE_PLAN_KEY);
    if (redWork == null) {
      redWork = Utilities.getReduceWork(jconf);
      cache.cache(REDUCE_PLAN_KEY, redWork);
    } else {
      Utilities.setReduceWork(jconf, redWork);
    }

    reducer = redWork.getReducer();
    reducer.setParentOperators(null); // clear out any parents as reducer is the
    // root
    isTagged = redWork.getNeedsTagging();
    vectorized = redWork.getVectorModeOn() != null;

    try {
      keyTableDesc = redWork.getKeyDesc();
      inputKeyDeserializer = (SerDe) ReflectionUtils.newInstance(keyTableDesc
          .getDeserializerClass(), null);
      SerDeUtils.initializeSerDe(inputKeyDeserializer, null, keyTableDesc.getProperties(), null);
      keyObjectInspector = inputKeyDeserializer.getObjectInspector();
      reducer.setGroupKeyObjectInspector(keyObjectInspector);
      valueTableDesc = new TableDesc[redWork.getTagToValueDesc().size()];

      if(vectorized) {
        final int maxTags = redWork.getTagToValueDesc().size();
        keyStructInspector = (StructObjectInspector)keyObjectInspector;
        batches = new VectorizedRowBatch[maxTags];
        valueStructInspectors = new StructObjectInspector[maxTags];
        valueStringWriters = (List<VectorExpressionWriter>[])new List[maxTags];
        keysColumnOffset = keyStructInspector.getAllStructFieldRefs().size();
        buffer = new DataOutputBuffer();
      }

      for (int tag = 0; tag < redWork.getTagToValueDesc().size(); tag++) {
        // We should initialize the SerDe with the TypeInfo when available.
        valueTableDesc[tag] = redWork.getTagToValueDesc().get(tag);
        inputValueDeserializer[tag] = (SerDe) ReflectionUtils.newInstance(
            valueTableDesc[tag].getDeserializerClass(), null);
        SerDeUtils.initializeSerDe(inputValueDeserializer[tag], null,
                                   valueTableDesc[tag].getProperties(), null);
        valueObjectInspector[tag] = inputValueDeserializer[tag]
            .getObjectInspector();

        ArrayList<ObjectInspector> ois = new ArrayList<ObjectInspector>();

        if(vectorized) {
          /* vectorization only works with struct object inspectors */
          valueStructInspectors[tag] = (StructObjectInspector)valueObjectInspector[tag];

          batches[tag] = VectorizedBatchUtil.constructVectorizedRowBatch(keyStructInspector,
              valueStructInspectors[tag]);
          final int totalColumns = keysColumnOffset +
              valueStructInspectors[tag].getAllStructFieldRefs().size();
          valueStringWriters[tag] = new ArrayList<VectorExpressionWriter>(totalColumns);
          valueStringWriters[tag].addAll(Arrays
              .asList(VectorExpressionWriterFactory
                  .genVectorStructExpressionWritables(keyStructInspector)));
          valueStringWriters[tag].addAll(Arrays
              .asList(VectorExpressionWriterFactory
                  .genVectorStructExpressionWritables(valueStructInspectors[tag])));

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
          fields = valueStructInspectors[tag].getAllStructFieldRefs();
          for (StructField field: fields) {
            colNames.add(Utilities.ReduceField.VALUE.toString() + "." + field.getFieldName());
            ois.add(field.getFieldObjectInspector());
          }
          rowObjectInspector[tag] = ObjectInspectorFactory
                  .getStandardStructObjectInspector(colNames, ois);
        } else {
          ois.add(keyObjectInspector);
          ois.add(valueObjectInspector[tag]);
          rowObjectInspector[tag] = ObjectInspectorFactory
                  .getStandardStructObjectInspector(Utilities.reduceFieldNameList, ois);
        }

      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    MapredContext.init(false, new JobConf(jconf));
    ((TezContext)MapredContext.get()).setInputs(inputs);

    // initialize reduce operator tree
    try {
      l4j.info(reducer.dump(0));
      reducer.initialize(jconf, rowObjectInspector);

      // Initialization isn't finished until all parents of all operators
      // are initialized. For broadcast joins that means initializing the
      // dummy parent operators as well.
      List<HashTableDummyOperator> dummyOps = redWork.getDummyOps();
      if (dummyOps != null) {
        for (Operator<? extends OperatorDesc> dummyOp : dummyOps){
          dummyOp.setExecContext(execContext);
          dummyOp.initialize(jconf, null);
        }
      }

      // set output collector for any reduce sink operators in the pipeline.
      List<Operator<? extends OperatorDesc>> children = new LinkedList<Operator<? extends OperatorDesc>>();
      children.add(reducer);
      if (dummyOps != null) {
        children.addAll(dummyOps);
      }
      createOutputMap();
      OperatorUtils.setChildrenCollector(children, outMap);

      reducer.setReporter(reporter);
      MapredContext.get().setReporter(reporter);

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
  void run() throws Exception {
    List<LogicalInput> shuffleInputs = getShuffleInputs(inputs);
    if (shuffleInputs != null) {
      l4j.info("Waiting for ShuffleInputs to become ready");
      processorContext.waitForAllInputsReady(new ArrayList<Input>(shuffleInputs));
    }

    for (Entry<String, LogicalOutput> outputEntry : outputs.entrySet()) {
      l4j.info("Starting Output: " + outputEntry.getKey());
      outputEntry.getValue().start();
      ((TezKVOutputCollector) outMap.get(outputEntry.getKey())).initialize();
    }

    KeyValuesReader kvsReader;
    try {
      if(shuffleInputs.size() == 1){
        //no merging of inputs required
        kvsReader = (KeyValuesReader) shuffleInputs.get(0).getReader();
      }else {
        //get a sort merged input
        kvsReader = new InputMerger(shuffleInputs);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }

    while(kvsReader.next()){
      Object key = kvsReader.getCurrentKey();
      Iterable<Object> values = kvsReader.getCurrentValues();
      boolean needMore = processRows(key, values);
      if(!needMore){
        break;
      }
    }

  }

  /**
   * Get the inputs that should be streamed through reduce plan.
   * @param inputs
   * @return
   */
  private List<LogicalInput> getShuffleInputs(Map<String, LogicalInput> inputs) {
    //the reduce plan inputs have tags, add all inputs that have tags
    Map<Integer, String> tag2input = redWork.getTagToInput();
    ArrayList<LogicalInput> shuffleInputs = new ArrayList<LogicalInput>();
    for(String inpStr : tag2input.values()){
      shuffleInputs.add((LogicalInput)inputs.get(inpStr));
    }
    return shuffleInputs;
  }

  /**
   * @param key
   * @param values
   * @return true if it is not done and can take more inputs
   */
  private boolean processRows(Object key, Iterable<Object> values) {
    if(reducer.getDone()){
      //done - no more records needed
      return false;
    }

    // reset the execContext for each new row
    execContext.resetRow();

    try {
      BytesWritable keyWritable = (BytesWritable) key;
      byte tag = 0;

      if (isTagged) {
        // remove the tag from key coming out of reducer
        // and store it in separate variable.
        int size = keyWritable.getLength() - 1;
        tag = keyWritable.getBytes()[size];
        keyWritable.setSize(size);
      }

      //Set the key, check if this is a new group or same group
      if (!keyWritable.equals(this.groupKey)) {
        // If a operator wants to do some work at the beginning of a group
        if (groupKey == null) { // the first group
          this.groupKey = new BytesWritable();
        } else {
          // If a operator wants to do some work at the end of a group
          if(isLogTraceEnabled) {
            l4j.trace("End Group");
          }
          reducer.endGroup();
        }

        try {
          this.keyObject = inputKeyDeserializer.deserialize(keyWritable);
        } catch (Exception e) {
          throw new HiveException(
              "Hive Runtime Error: Unable to deserialize reduce input key from "
              + Utilities.formatBinaryString(keyWritable.getBytes(), 0,
              keyWritable.getLength()) + " with properties "
              + keyTableDesc.getProperties(), e);
        }
        groupKey.set(keyWritable.getBytes(), 0, keyWritable.getLength());
        if (isLogTraceEnabled) {
          l4j.trace("Start Group");
        }
        reducer.setGroupKeyObject(keyObject);
        reducer.startGroup();
      }
      /* this.keyObject passed via reference */
      if(vectorized) {
        return processVectors(values, tag);
      } else {
        return processKeyValues(values, tag);
      }
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

  private Object deserializeValue(BytesWritable valueWritable, byte tag) throws HiveException {
    try {
      return inputValueDeserializer[tag].deserialize(valueWritable);
    } catch (SerDeException e) {
      throw new HiveException(
          "Hive Runtime Error: Unable to deserialize reduce input value (tag="
              + tag
              + ") from "
              + Utilities.formatBinaryString(valueWritable.getBytes(), 0,
                  valueWritable.getLength()) + " with properties "
              + valueTableDesc[tag].getProperties(), e);
    }
  }

  /**
   * @param values
   * @return true if it is not done and can take more inputs
   */
  private boolean processKeyValues(Iterable<Object> values, byte tag) throws HiveException {

    for (Object value : values) {
      BytesWritable valueWritable = (BytesWritable) value;

      row.clear();
      row.add(this.keyObject);
      row.add(deserializeValue(valueWritable, tag));

      try {
        reducer.processOp(row, tag);
      } catch (Exception e) {
        String rowString = null;
        try {
          rowString = SerDeUtils.getJSONString(row, rowObjectInspector[tag]);
        } catch (Exception e2) {
          rowString = "[Error getting row data with exception "
              + StringUtils.stringifyException(e2) + " ]";
        }
        throw new HiveException("Hive Runtime Error while processing row (tag="
            + tag + ") " + rowString, e);
      }
      if (isLogInfoEnabled) {
        logProgress();
      }
    }
    return true; //give me more
  }

  /**
   * @param values
   * @return true if it is not done and can take more inputs
   */
  private boolean processVectors(Iterable<Object> values, byte tag) throws HiveException {
    VectorizedRowBatch batch = batches[tag];
    batch.reset();

    /* deserialize key into columns */
    VectorizedBatchUtil.addRowToBatchFrom(keyObject, keyStructInspector,
        0, 0, batch, buffer);
    for(int i = 0; i < keysColumnOffset; i++) {
      VectorizedBatchUtil.setRepeatingColumn(batch, i);
    }

    int rowIdx = 0;
    try {
      for (Object value : values) {
        /* deserialize value into columns */
        BytesWritable valueWritable = (BytesWritable) value;
        Object valueObj = deserializeValue(valueWritable, tag);

        VectorizedBatchUtil.addRowToBatchFrom(valueObj, valueStructInspectors[tag],
            rowIdx, keysColumnOffset, batch, buffer);
        rowIdx++;
        if (rowIdx >= BATCH_SIZE) {
          VectorizedBatchUtil.setBatchSize(batch, rowIdx);
          reducer.processOp(batch, tag);
          rowIdx = 0;
          if (isLogInfoEnabled) {
            logProgress();
          }
        }
      }
      if (rowIdx > 0) {
        VectorizedBatchUtil.setBatchSize(batch, rowIdx);
        reducer.processOp(batch, tag);
      }
      if (isLogInfoEnabled) {
        logProgress();
      }
    } catch (Exception e) {
      String rowString = null;
      try {
        /* batch.toString depends on this */
        batch.setValueWriters(valueStringWriters[tag]
            .toArray(new VectorExpressionWriter[0]));
        rowString = batch.toString();
      } catch (Exception e2) {
        rowString = "[Error getting row data with exception "
            + StringUtils.stringifyException(e2) + " ]";
      }
      throw new HiveException("Hive Runtime Error while processing vector batch (tag="
          + tag + ") " + rowString, e);
    }
    return true; // give me more
  }

  @Override
  void close(){
    // check if there are IOExceptions
    if (!abort) {
      abort = execContext.getIoCxt().getIOExceptions();
    }

    try {
      if (groupKey != null) {
        // If a operator wants to do some work at the end of a group
        if(isLogTraceEnabled) {
          l4j.trace("End Group");
        }
        reducer.endGroup();
      }
      if (isLogInfoEnabled) {
        logCloseInfo();
      }

      reducer.close(abort);

      // Need to close the dummyOps as well. The operator pipeline
      // is not considered "closed/done" unless all operators are
      // done. For broadcast joins that includes the dummy parents.
      List<HashTableDummyOperator> dummyOps = redWork.getDummyOps();
      if (dummyOps != null) {
        for (Operator<? extends OperatorDesc> dummyOp : dummyOps){
          dummyOp.close(abort);
        }
      }
      ReportStats rps = new ReportStats(reporter);
      reducer.preorderMap(rps);

    } catch (Exception e) {
      if (!abort) {
        // signal new failure to map-reduce
        l4j.error("Hit error while closing operators - failing tree");
        throw new RuntimeException("Hive Runtime Error while closing operators: "
            + e.getMessage(), e);
      }
    } finally {
      Utilities.clearWorkMap();
      MapredContext.close();
    }
  }

}
