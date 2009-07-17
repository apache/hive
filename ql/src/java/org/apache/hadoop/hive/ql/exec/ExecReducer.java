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

package org.apache.hadoop.hive.ql.exec;

import java.io.*;
import java.net.URLClassLoader;
import java.util.*;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.exec.ExecMapper.reportStats;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

public class ExecReducer extends MapReduceBase implements Reducer {

  private JobConf jc;
  private OutputCollector<?,?> oc;
  private Operator<?> reducer;
  private Reporter rp;
  private boolean abort = false;
  private boolean isTagged = false;
  private long cntr = 0;
  private long nextCntr = 1;

  private static String [] fieldNames;
  public static final Log l4j = LogFactory.getLog("ExecReducer");

  // TODO: move to DynamicSerDe when it's ready
  private Deserializer inputKeyDeserializer;
  // Input value serde needs to be an array to support different SerDe 
  // for different tags
  private SerDe[] inputValueDeserializer = new SerDe[Byte.MAX_VALUE];
  static {
    ArrayList<String> fieldNameArray =  new ArrayList<String> ();
    for(Utilities.ReduceField r: Utilities.ReduceField.values()) {
      fieldNameArray.add(r.toString());
    }
    fieldNames = fieldNameArray.toArray(new String [0]);
  }

  public void configure(JobConf job) {
    ObjectInspector[] rowObjectInspector = new ObjectInspector[Byte.MAX_VALUE];
    ObjectInspector[] valueObjectInspector = new ObjectInspector[Byte.MAX_VALUE];
    ObjectInspector keyObjectInspector;
    try {
      l4j.info("conf classpath = " 
          + Arrays.asList(((URLClassLoader)job.getClassLoader()).getURLs()));
      l4j.info("thread classpath = " 
          + Arrays.asList(((URLClassLoader)Thread.currentThread().getContextClassLoader()).getURLs()));
    } catch (Exception e) {
      l4j.info("cannot get classpath: " + e.getMessage());
    }
    jc = job;
    mapredWork gWork = Utilities.getMapRedWork(job);
    reducer = gWork.getReducer();
    reducer.setParentOperators(null); // clear out any parents as reducer is the root
    isTagged = gWork.getNeedsTagging();
    try {
      tableDesc keyTableDesc = gWork.getKeyDesc();
      inputKeyDeserializer = (SerDe)ReflectionUtils.newInstance(keyTableDesc.getDeserializerClass(), null);
      inputKeyDeserializer.initialize(null, keyTableDesc.getProperties());
      keyObjectInspector = inputKeyDeserializer.getObjectInspector();
      for(int tag=0; tag<gWork.getTagToValueDesc().size(); tag++) {
        // We should initialize the SerDe with the TypeInfo when available.
        tableDesc valueTableDesc = gWork.getTagToValueDesc().get(tag);
        inputValueDeserializer[tag] = (SerDe)ReflectionUtils.newInstance(valueTableDesc.getDeserializerClass(), null);
        inputValueDeserializer[tag].initialize(null, valueTableDesc.getProperties());
        valueObjectInspector[tag] = inputValueDeserializer[tag].getObjectInspector();
        
        ArrayList<ObjectInspector> ois = new ArrayList<ObjectInspector>();
        ois.add(keyObjectInspector);
        ois.add(valueObjectInspector[tag]);
        ois.add(PrimitiveObjectInspectorFactory.writableByteObjectInspector);
        rowObjectInspector[tag] = ObjectInspectorFactory.getStandardStructObjectInspector(
            Arrays.asList(fieldNames), ois);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
    //initialize reduce operator tree
    try {
      l4j.info(reducer.dump(0));
      reducer.initialize(jc, rowObjectInspector);
    } catch (Throwable e) {
      abort = true;
      if (e instanceof OutOfMemoryError) {
        // Don't create a new object if we are already out of memory 
        throw (OutOfMemoryError) e; 
      } else {
        throw new RuntimeException ("Reduce operator initialization failed", e);
      }
    }
  }

  private Object keyObject;
  private Object[] valueObject = new Object[Byte.MAX_VALUE];
  
  private BytesWritable groupKey;
  
  ArrayList<Object> row = new ArrayList<Object>(3);
  ByteWritable tag = new ByteWritable();
  public void reduce(Object key, Iterator values,
                     OutputCollector output,
                     Reporter reporter) throws IOException {

    if(oc == null) {
      // propagete reporter and output collector to all operators
      oc = output;
      rp = reporter;
      reducer.setOutputCollector(oc);
      reducer.setReporter(rp);
    }

    try {
      BytesWritable keyWritable = (BytesWritable)key;
      tag.set((byte)0);
      if (isTagged) {
        // remove the tag
        int size = keyWritable.getSize() - 1;
        tag.set(keyWritable.get()[size]); 
        keyWritable.setSize(size);
      }
      
      if (!keyWritable.equals(groupKey)) {
        // If a operator wants to do some work at the beginning of a group
        if (groupKey == null) {
          groupKey = new BytesWritable();
        } else {
          // If a operator wants to do some work at the end of a group
          l4j.trace("End Group");
          reducer.endGroup();
        }
        groupKey.set(keyWritable.get(), 0, keyWritable.getSize());
        l4j.trace("Start Group");
        reducer.startGroup();
      }
      try {
        keyObject = inputKeyDeserializer.deserialize(keyWritable);
      } catch (Exception e) {
        throw new HiveException(e);
      }
      // System.err.print(keyObject.toString());
      while (values.hasNext()) {
        Writable valueWritable = (Writable) values.next();
        //System.err.print(who.getHo().toString());
        try {
          valueObject[tag.get()] = inputValueDeserializer[tag.get()].deserialize(valueWritable);
        } catch (SerDeException e) {
          throw new HiveException(e);
        }
        row.clear();
        row.add(keyObject);
        row.add(valueObject[tag.get()]);
        // The tag is not used any more, we should remove it.
        row.add(tag);
        cntr++;
        if (cntr == nextCntr) {
          l4j.info("ExecReducer: processing " + cntr + " rows");
          nextCntr = getNextCntr(cntr);
        }
        reducer.process(row, tag.get());
      }

    } catch (Throwable e) {
      abort = true;
      if (e instanceof OutOfMemoryError) {
        // Don't create a new object if we are already out of memory 
        throw (OutOfMemoryError) e; 
      } else {
        throw new IOException (e);
      }
    }
  }

  private long getNextCntr(long cntr) {
    // A very simple counter to keep track of number of rows processed by the reducer. It dumps
    // every 1 million times, and quickly before that
    if (cntr >= 1000000)
      return cntr + 1000000;
    
    return 10 * cntr;
  }

  public void close() {

    // No row was processed
    if(oc == null) {
      l4j.trace("Close called no row");
    }

    try {
      if (groupKey != null) {
        // If a operator wants to do some work at the end of a group
        l4j.trace("End Group");
        reducer.endGroup();
      }
      l4j.info("ExecReducer: processed " + cntr + " rows");
      reducer.close(abort);
      reportStats rps = new reportStats (rp);
      reducer.preorderMap(rps);
      return;
    } catch (Exception e) {
      if(!abort) {
        // signal new failure to map-reduce
        l4j.error("Hit error while closing operators - failing tree");
        throw new RuntimeException ("Error while closing operators: " + e.getMessage());
      }
    }
  }
}
