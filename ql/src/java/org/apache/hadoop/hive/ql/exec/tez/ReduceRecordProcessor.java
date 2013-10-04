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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper.reportStats;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.tez.mapreduce.processor.MRTaskReporter;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.input.ShuffledMergedInput;

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

  List<Object> row = new ArrayList<Object>(Utilities.reduceFieldNameList.size());

  @Override
  void init(JobConf jconf, MRTaskReporter mrReporter, Map<String, LogicalInput> inputs,
      OutputCollector out){
    super.init(jconf, mrReporter, inputs, out);

    ObjectCache cache = ObjectCacheFactory.getCache(jconf);

    rowObjectInspector = new ObjectInspector[Byte.MAX_VALUE];
    ObjectInspector[] valueObjectInspector = new ObjectInspector[Byte.MAX_VALUE];
    ObjectInspector keyObjectInspector;

    ReduceWork redWork = (ReduceWork) cache.retrieve(REDUCE_PLAN_KEY);
    if (redWork == null) {
      redWork = Utilities.getReduceWork(jconf);
      cache.cache(REDUCE_PLAN_KEY, redWork);
    }

    reducer = redWork.getReducer();
    reducer.setParentOperators(null); // clear out any parents as reducer is the
    // root
    isTagged = redWork.getNeedsTagging();
    try {
      keyTableDesc = redWork.getKeyDesc();
      inputKeyDeserializer = (SerDe) ReflectionUtils.newInstance(keyTableDesc
          .getDeserializerClass(), null);
      inputKeyDeserializer.initialize(null, keyTableDesc.getProperties());
      keyObjectInspector = inputKeyDeserializer.getObjectInspector();
      valueTableDesc = new TableDesc[redWork.getTagToValueDesc().size()];
      for (int tag = 0; tag < redWork.getTagToValueDesc().size(); tag++) {
        // We should initialize the SerDe with the TypeInfo when available.
        valueTableDesc[tag] = redWork.getTagToValueDesc().get(tag);
        inputValueDeserializer[tag] = (SerDe) ReflectionUtils.newInstance(
            valueTableDesc[tag].getDeserializerClass(), null);
        inputValueDeserializer[tag].initialize(null, valueTableDesc[tag]
            .getProperties());
        valueObjectInspector[tag] = inputValueDeserializer[tag]
            .getObjectInspector();

        ArrayList<ObjectInspector> ois = new ArrayList<ObjectInspector>();
        ois.add(keyObjectInspector);
        ois.add(valueObjectInspector[tag]);
        rowObjectInspector[tag] = ObjectInspectorFactory
            .getStandardStructObjectInspector(Utilities.reduceFieldNameList, ois);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    MapredContext.init(false, new JobConf(jconf));

    // initialize reduce operator tree
    try {
      l4j.info(reducer.dump(0));
      reducer.initialize(jconf, rowObjectInspector);
    } catch (Throwable e) {
      abort = true;
      if (e instanceof OutOfMemoryError) {
        // Don't create a new object if we are already out of memory
        throw (OutOfMemoryError) e;
      } else {
        throw new RuntimeException("Reduce operator initialization failed", e);
      }
    }

    reducer.setOutputCollector(out);
    reducer.setReporter(reporter);
    MapredContext.get().setReporter(reporter);


  }

  @Override
  void run() throws IOException{
    if (inputs.size() != 1) {
      throw new IllegalArgumentException("ReduceRecordProcessor expects single input"
          + ", inputCount=" + inputs.size());
    }

    //TODO - changes this for joins
    ShuffledMergedInput in = (ShuffledMergedInput)inputs.values().iterator().next();
    KeyValuesReader reader = in.getReader();

    //process records until done
    while(reader.next()){
      Object key = reader.getCurrentKey();
      Iterable<Object> values = reader.getCurrentValues();
      boolean needMore = processKeyValues(key, values);
      if(!needMore){
        break;
      }
    }
  }

  /**
   * @param key
   * @param values
   * @return true if it is not done and can take more inputs
   */
  private boolean processKeyValues(Object key, Iterable<Object> values) {
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
        int size = keyWritable.getSize() - 1;
        tag = keyWritable.get()[size];
        keyWritable.setSize(size);
      }

      //Set the key, check if this is a new group or same group
      if (!keyWritable.equals(groupKey)) {
        // If a operator wants to do some work at the beginning of a group
        if (groupKey == null) { // the first group
          groupKey = new BytesWritable();
        } else {
          // If a operator wants to do some work at the end of a group
          l4j.trace("End Group");
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
        l4j.trace("Start Group");
        reducer.startGroup();
        reducer.setGroupKeyObject(keyObject);
      }

      //process all the values we have for this key
      Iterator<Object> valuesIt = values.iterator();
      while (valuesIt.hasNext()) {
        BytesWritable valueWritable = (BytesWritable) valuesIt.next();
        Object valueObj;
        try {
          valueObj = inputValueDeserializer[tag].deserialize(valueWritable);
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
        row.add(valueObj);

        try {
          reducer.process(row, tag);
        } catch (Exception e) {
          String rowString = null;
          try {
            rowString = SerDeUtils.getJSONString(row, rowObjectInspector[tag]);
          } catch (Exception e2) {
            rowString = "[Error getting row data with exception " +
                  StringUtils.stringifyException(e2) + " ]";
          }
          throw new HiveException("Hive Runtime Error while processing row (tag="
              + tag + ") " + rowString, e);
        }
        if (isLogInfoEnabled) {
          logProgress();
        }
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
    return true; //give me more
  }

  @Override
  void close(){
    // check if there are IOExceptions
    if (!abort) {
      abort = execContext.getIoCxt().getIOExceptions();
    }
    // No row was processed
    if (out == null) {
      l4j.trace("Close called no row");
    }

    try {
      if (groupKey != null) {
        // If a operator wants to do some work at the end of a group
        l4j.trace("End Group");
        reducer.endGroup();
      }
      if (isLogInfoEnabled) {
        logCloseInfo();
      }

      reducer.close(abort);
      reportStats rps = new reportStats(reporter);
      reducer.preorderMap(rps);

    } catch (Exception e) {
      if (!abort) {
        // signal new failure to map-reduce
        l4j.error("Hit error while closing operators - failing tree");
        throw new RuntimeException("Hive Runtime Error while closing operators: "
            + e.getMessage(), e);
      }
    } finally {
      MapredContext.close();
    }
  }

}
