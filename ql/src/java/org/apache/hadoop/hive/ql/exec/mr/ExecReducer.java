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

package org.apache.hadoop.hive.ql.exec.mr;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper.reportStats;
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
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/**
 * ExecReducer is the generic Reducer class for Hive. Together with ExecMapper it is
 * the bridge between the map-reduce framework and the Hive operator pipeline at
 * execution time. It's main responsabilities are:
 *
 * - Load and setup the operator pipeline from XML
 * - Run the pipeline by transforming key, value pairs to records and forwarding them to the operators
 * - Sending start and end group messages to separate records with same key from one another
 * - Catch and handle errors during execution of the operators.
 *
 */
public class ExecReducer extends MapReduceBase implements Reducer {

  private static final String PLAN_KEY = "__REDUCE_PLAN__";

  private JobConf jc;
  private OutputCollector<?, ?> oc;
  private Operator<?> reducer;
  private Reporter rp;
  private boolean abort = false;
  private boolean isTagged = false;
  private long cntr = 0;
  private long nextCntr = 1;

  public static final Log l4j = LogFactory.getLog("ExecReducer");
  private boolean isLogInfoEnabled = false;

  // used to log memory usage periodically
  private MemoryMXBean memoryMXBean;

  // TODO: move to DynamicSerDe when it's ready
  private Deserializer inputKeyDeserializer;
  // Input value serde needs to be an array to support different SerDe
  // for different tags
  private final SerDe[] inputValueDeserializer = new SerDe[Byte.MAX_VALUE];

  TableDesc keyTableDesc;
  TableDesc[] valueTableDesc;

  ObjectInspector[] rowObjectInspector;

  @Override
  public void configure(JobConf job) {
    rowObjectInspector = new ObjectInspector[Byte.MAX_VALUE];
    ObjectInspector[] valueObjectInspector = new ObjectInspector[Byte.MAX_VALUE];
    ObjectInspector keyObjectInspector;

    // Allocate the bean at the beginning -
    memoryMXBean = ManagementFactory.getMemoryMXBean();
    l4j.info("maximum memory = " + memoryMXBean.getHeapMemoryUsage().getMax());

    isLogInfoEnabled = l4j.isInfoEnabled();

    try {
      l4j.info("conf classpath = "
          + Arrays.asList(((URLClassLoader) job.getClassLoader()).getURLs()));
      l4j.info("thread classpath = "
          + Arrays.asList(((URLClassLoader) Thread.currentThread()
          .getContextClassLoader()).getURLs()));
    } catch (Exception e) {
      l4j.info("cannot get classpath: " + e.getMessage());
    }
    jc = job;

    ObjectCache cache = ObjectCacheFactory.getCache(jc);
    ReduceWork gWork = (ReduceWork) cache.retrieve(PLAN_KEY);
    if (gWork == null) {
      gWork = Utilities.getReduceWork(job);
      cache.cache(PLAN_KEY, gWork);
    } else {
      Utilities.setReduceWork(job, gWork);
    }

    reducer = gWork.getReducer();
    reducer.setParentOperators(null); // clear out any parents as reducer is the
    // root
    isTagged = gWork.getNeedsTagging();
    try {
      keyTableDesc = gWork.getKeyDesc();
      inputKeyDeserializer = (SerDe) ReflectionUtils.newInstance(keyTableDesc
          .getDeserializerClass(), null);
      inputKeyDeserializer.initialize(null, keyTableDesc.getProperties());
      keyObjectInspector = inputKeyDeserializer.getObjectInspector();
      valueTableDesc = new TableDesc[gWork.getTagToValueDesc().size()];
      for (int tag = 0; tag < gWork.getTagToValueDesc().size(); tag++) {
        // We should initialize the SerDe with the TypeInfo when available.
        valueTableDesc[tag] = gWork.getTagToValueDesc().get(tag);
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

    MapredContext.init(false, new JobConf(jc));

    // initialize reduce operator tree
    try {
      l4j.info(reducer.dump(0));
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

  private Object keyObject;
  private final Object[] valueObject = new Object[Byte.MAX_VALUE];

  private BytesWritable groupKey;

  List<Object> row = new ArrayList<Object>(Utilities.reduceFieldNameList.size());

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
        if (isLogInfoEnabled) {
          cntr++;
          if (cntr == nextCntr) {
            long used_memory = memoryMXBean.getHeapMemoryUsage().getUsed();
            l4j.info("ExecReducer: processing " + cntr
                + " rows: used memory = " + used_memory);
            nextCntr = getNextCntr(cntr);
          }
        }
        try {
          reducer.processOp(row, tag);
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
        l4j.fatal(StringUtils.stringifyException(e));
        throw new RuntimeException(e);
      }
    }
  }

  private long getNextCntr(long cntr) {
    // A very simple counter to keep track of number of rows processed by the
    // reducer. It dumps
    // every 1 million times, and quickly before that
    if (cntr >= 1000000) {
      return cntr + 1000000;
    }

    return 10 * cntr;
  }

  @Override
  public void close() {

    // No row was processed
    if (oc == null) {
      l4j.trace("Close called no row");
    }

    try {
      if (groupKey != null) {
        // If a operator wants to do some work at the end of a group
        l4j.trace("End Group");
        reducer.endGroup();
      }
      if (isLogInfoEnabled) {
        l4j.info("ExecReducer: processed " + cntr + " rows: used memory = "
            + memoryMXBean.getHeapMemoryUsage().getUsed());
      }

      reducer.close(abort);
      reportStats rps = new reportStats(rp);
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
      Utilities.clearWorkMap();
    }
  }
}
