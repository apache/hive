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

package org.apache.hadoop.hive.ql.exec.mr;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper.ReportStats;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/**
 * ExecReducer is the generic Reducer class for Hive. Together with ExecMapper it is
 * the bridge between the map-reduce framework and the Hive operator pipeline at
 * execution time. It's main responsibilities are:
 *
 * - Load and setup the operator pipeline from XML
 * - Run the pipeline by transforming key, value pairs to records and forwarding them to the operators
 * - Sending start and end group messages to separate records with same key from one another
 * - Catch and handle errors during execution of the operators.
 *
 */
public class ExecReducer extends MapReduceBase implements Reducer {

  private static final Logger LOG = LoggerFactory.getLogger("ExecReducer");
  private static final String PLAN_KEY = "__REDUCE_PLAN__";

  // Input value serde needs to be an array to support different SerDe
  // for different tags
  private final Deserializer[] inputValueDeserializer = new Deserializer[Byte.MAX_VALUE];
  private final Object[] valueObject = new Object[Byte.MAX_VALUE];
  private final List<Object> row = new ArrayList<Object>(Utilities.reduceFieldNameList.size());

  // TODO: move to DynamicSerDe when it's ready
  private Deserializer inputKeyDeserializer;
  private JobConf jc;
  private OutputCollector<?, ?> oc;
  private Operator<?> reducer;
  private Reporter rp;
  private boolean abort = false;
  private boolean isTagged = false;
  private TableDesc keyTableDesc;
  private TableDesc[] valueTableDesc;
  private ObjectInspector[] rowObjectInspector;

  // runtime objects
  private transient Object keyObject;
  private transient BytesWritable groupKey;

  @Override
  public void configure(JobConf job) {
    rowObjectInspector = new ObjectInspector[Byte.MAX_VALUE];
    ObjectInspector[] valueObjectInspector = new ObjectInspector[Byte.MAX_VALUE];
    ObjectInspector keyObjectInspector;

    if (LOG.isInfoEnabled()) {
      try {
        LOG.info("conf classpath = "
            + Arrays.asList(((URLClassLoader) job.getClassLoader()).getURLs()));
        LOG.info("thread classpath = "
            + Arrays.asList(((URLClassLoader) Thread.currentThread()
            .getContextClassLoader()).getURLs()));
      } catch (Exception e) {
        LOG.info("cannot get classpath: " + e.getMessage());
      }
    }
    jc = job;

    ReduceWork gWork = Utilities.getReduceWork(job);

    reducer = gWork.getReducer();
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
      for (int tag = 0; tag < gWork.getTagToValueDesc().size(); tag++) {
        // We should initialize the SerDe with the TypeInfo when available.
        valueTableDesc[tag] = gWork.getTagToValueDesc().get(tag);
        inputValueDeserializer[tag] = ReflectionUtils.newInstance(
            valueTableDesc[tag].getDeserializerClass(), null);
        SerDeUtils.initializeSerDe(inputValueDeserializer[tag], null,
                                   valueTableDesc[tag].getProperties(), null);
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

    MapredContext.init(false, new JobConf(jc));

    // initialize reduce operator tree
    try {
      LOG.info(reducer.dump(0));
      reducer.initialize(jc, rowObjectInspector);
    } catch (Throwable e) {
      abort = true;
      if (e instanceof OutOfMemoryError) {
        // Don't create a new object if we are already out of memory
        throw (OutOfMemoryError) e;
      } else {
        throw new RuntimeException("Reduce operator initialization failed", e);
      }
    }
  }

  @Override
  public void reduce(Object key, Iterator values, OutputCollector output,
      Reporter reporter) throws IOException {
    if (reducer.getDone()) {
      return;
    }
    if (oc == null) {
      // propagate reporter and output collector to all operators
      oc = output;
      rp = reporter;
      reducer.setReporter(rp);
      MapredContext.get().setReporter(reporter);
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
          if (LOG.isTraceEnabled()) {
            LOG.trace("End Group");
          }
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
        if (LOG.isTraceEnabled()) {
          LOG.trace("Start Group");
        }
        reducer.startGroup();
        reducer.setGroupKeyObject(keyObject);
      }
      // System.err.print(keyObject.toString());
      while (values.hasNext()) {
        BytesWritable valueWritable = (BytesWritable) values.next();
        // System.err.print(who.getHo().toString());
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
      }

    } catch (Throwable e) {
      abort = true;
      if (e instanceof OutOfMemoryError) {
        // Don't create a new object if we are already out of memory
        throw (OutOfMemoryError) e;
      } else {
        LOG.error(StringUtils.stringifyException(e));
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void close() {

    // No row was processed
    if (oc == null && LOG.isTraceEnabled()) {
      LOG.trace("Close called without any rows processed");
    }

    try {
      if (groupKey != null) {
        // If a operator wants to do some work at the end of a group
        if (LOG.isTraceEnabled()) {
          LOG.trace("End Group");
        }
        reducer.endGroup();
      }

      reducer.close(abort);
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
}
