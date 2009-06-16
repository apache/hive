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
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.mapredLocalWork;
import org.apache.hadoop.hive.ql.plan.fetchWork;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class ExecMapper extends MapReduceBase implements Mapper {

  private MapOperator mo;
  private Map<String, FetchOperator> fetchOperators;
  private OutputCollector oc;
  private JobConf jc;
  private boolean abort = false;
  private Reporter rp;
  public static final Log l4j = LogFactory.getLog("ExecMapper");
  private static boolean done;

  private void init() {
    mo = null;
    fetchOperators = null;
    oc = null;
    jc = null;
    abort = false;
    rp = null;
  }
  
  public void configure(JobConf job) {
    try {
      init();
      jc = job;
      mapredWork mrwork = Utilities.getMapRedWork(job);
      mo = new MapOperator();
      mo.setConf(mrwork);
      mapredLocalWork mlo = mrwork.getMapLocalWork();
      if (mlo != null) {
        fetchOperators = new HashMap<String, FetchOperator>();
        Map<String, fetchWork> aliasToFetchWork = mlo.getAliasToFetchWork();
        Iterator<Map.Entry<String, fetchWork>> fetchWorkSet = aliasToFetchWork
            .entrySet().iterator();
        while (fetchWorkSet.hasNext()) {
          Map.Entry<String, fetchWork> entry = fetchWorkSet.next();
          String alias = entry.getKey();
          fetchWork fWork = entry.getValue();
          fetchOperators.put(alias, new FetchOperator(fWork, job));
          l4j.info("fetchoperator for " + alias + " initialized");
        }
      }

      // we don't initialize the operator until we have set the output collector
    } catch (Exception e) {
      // Bail out ungracefully - we should never hit
      // this here - but would have hit it in SemanticAnalyzer
      throw new RuntimeException(e);
    }
  }

  public void map(Object key, Object value,
                  OutputCollector output,
                  Reporter reporter) throws IOException {
    if(oc == null) {
      try {
        oc = output;
        mo.setOutputCollector(oc);
        mo.initialize(jc, reporter, null);
        if (fetchOperators != null) {
          mapredWork mrwork = Utilities.getMapRedWork(jc);
          mapredLocalWork localWork = mrwork.getMapLocalWork();
          Iterator<Map.Entry<String, FetchOperator>> fetchOps = fetchOperators.entrySet().iterator();
          while (fetchOps.hasNext()) {
            Map.Entry<String, FetchOperator> entry = fetchOps.next();
            String alias = entry.getKey();
            FetchOperator fetchOp = entry.getValue();
            Operator<? extends Serializable> forwardOp = localWork.getAliasToWork().get(alias); 
            // All the operators need to be initialized before process
            forwardOp.initialize(jc, reporter, new ObjectInspector[]{fetchOp.getOutputObjectInspector()});
          }

          fetchOps = fetchOperators.entrySet().iterator();
          while (fetchOps.hasNext()) {
            Map.Entry<String, FetchOperator> entry = fetchOps.next();
            String alias = entry.getKey();
            FetchOperator fetchOp = entry.getValue();
            Operator<? extends Serializable> forwardOp = localWork.getAliasToWork().get(alias); 

            while (true) {
              InspectableObject row = fetchOp.getNextRow();
              if (row == null) {
                break;
              }

              forwardOp.process(row.o, row.oi, 0);
            }
          }
        }

        rp = reporter;
      } catch (Throwable e) {
        abort = true;
        e.printStackTrace();
        if (e instanceof OutOfMemoryError) {
          // Don't create a new object if we are already out of memory 
          throw (OutOfMemoryError) e; 
        } else {
          throw new RuntimeException ("Map operator initialization failed", e);
        }
      }
    }

    try {
      if (mo.getDone())
        done = true;
      else
        // Since there is no concept of a group, we don't invoke startGroup/endGroup for a mapper
        mo.process((Writable)value);
    } catch (Throwable e) {
      abort = true;
      e.printStackTrace();
      if (e instanceof OutOfMemoryError) {
        // Don't create a new object if we are already out of memory 
        throw (OutOfMemoryError) e; 
      } else {
        throw new RuntimeException (e.getMessage(), e);
      }
    }
  }

  public void close() {
    // No row was processed
    if(oc == null) {
      try {
        l4j.trace("Close called no row");
        mo.initialize(jc, null, null);
        rp = null;
      } catch (Throwable e) {
        abort = true;
        e.printStackTrace();
        if (e instanceof OutOfMemoryError) {
          // Don't create a new object if we are already out of memory 
          throw (OutOfMemoryError) e; 
        } else {
          throw new RuntimeException ("Map operator close failed during initialize", e);
        }
      }
    }

    // detecting failed executions by exceptions thrown by the operator tree
    // ideally hadoop should let us know whether map execution failed or not
    try {
      mo.close(abort);
      if (fetchOperators != null) {
        mapredWork mrwork = Utilities.getMapRedWork(jc);
        mapredLocalWork localWork = mrwork.getMapLocalWork();
        Iterator<Map.Entry<String, FetchOperator>> fetchOps = fetchOperators.entrySet().iterator();
        while (fetchOps.hasNext()) {
          Map.Entry<String, FetchOperator> entry = fetchOps.next();
          String alias = entry.getKey();
          Operator<? extends Serializable> forwardOp = localWork.getAliasToWork().get(alias); 
          forwardOp.close(abort);
        }
      }

      reportStats rps = new reportStats (rp);
      mo.preorderMap(rps);
      return;
    } catch (Exception e) {
      if(!abort) {
        // signal new failure to map-reduce
        l4j.error("Hit error while closing operators - failing tree");
        throw new RuntimeException ("Error while closing operators", e);
      }
    }
  }

  public static boolean getDone() {
    return done;
  }

  public static class reportStats implements Operator.OperatorFunc {
    Reporter rp;
    public reportStats (Reporter rp) {
      this.rp = rp;
    }
    public void func(Operator op) {
      Map<Enum, Long> opStats = op.getStats();
      for(Map.Entry<Enum, Long> e: opStats.entrySet()) {
          if(this.rp != null) {
              rp.incrCounter(e.getKey(), e.getValue());
          }
      }
    }
  }
}
