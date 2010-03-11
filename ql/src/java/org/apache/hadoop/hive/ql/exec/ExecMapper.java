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

import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork.BucketMapJoinContext;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/**
 * ExecMapper.
 *
 */
public class ExecMapper extends MapReduceBase implements Mapper {

  private MapOperator mo;
  private Map<String, FetchOperator> fetchOperators;
  private OutputCollector oc;
  private JobConf jc;
  private boolean abort = false;
  private Reporter rp;
  public static final Log l4j = LogFactory.getLog("ExecMapper");
  private static boolean done;

  // used to log memory usage periodically
  private MemoryMXBean memoryMXBean;
  private long numRows = 0;
  private long nextCntr = 1;
  private String lastInputFile = null;
  private MapredLocalWork localWork = null;
  
  private ExecMapperContext execContext = new ExecMapperContext();
  
  public static class ExecMapperContext {
    boolean inputFileChanged = false;
    String currentInputFile;
    JobConf jc;
    public boolean isInputFileChanged() {
      return inputFileChanged;
    }
    public void setInputFileChanged(boolean inputFileChanged) {
      this.inputFileChanged = inputFileChanged;
    }
    public String getCurrentInputFile() {
      return currentInputFile;
    }
    public void setCurrentInputFile(String currentInputFile) {
      this.currentInputFile = currentInputFile;
    }
    public JobConf getJc() {
      return jc;
    }
    public void setJc(JobConf jc) {
      this.jc = jc;
    }
  }

  @Override
  public void configure(JobConf job) {
    // Allocate the bean at the beginning -
    memoryMXBean = ManagementFactory.getMemoryMXBean();
    l4j.info("maximum memory = " + memoryMXBean.getHeapMemoryUsage().getMax());

    try {
      l4j.info("conf classpath = "
          + Arrays.asList(((URLClassLoader) job.getClassLoader()).getURLs()));
      l4j.info("thread classpath = "
          + Arrays.asList(((URLClassLoader) Thread.currentThread()
          .getContextClassLoader()).getURLs()));
    } catch (Exception e) {
      l4j.info("cannot get classpath: " + e.getMessage());
    }
    try {
      jc = job;
      execContext.jc = jc;
      // create map and fetch operators
      MapredWork mrwork = Utilities.getMapRedWork(job);
      mo = new MapOperator();
      mo.setConf(mrwork);
      // initialize map operator
      mo.setChildren(job);
      l4j.info(mo.dump(0));
      mo.setExecContext(execContext);
      mo.initializeLocalWork(jc);
      mo.initialize(jc, null);
      
      // initialize map local work
      localWork = mrwork.getMapLocalWork();
      if (localWork == null) {
        return;
      }
      fetchOperators = new HashMap<String, FetchOperator>();
      // create map local operators
      for (Map.Entry<String, FetchWork> entry : localWork.getAliasToFetchWork()
          .entrySet()) {
        fetchOperators.put(entry.getKey(), new FetchOperator(entry.getValue(),
            job));
        l4j.info("fetchoperator for " + entry.getKey() + " created");
      }
      // initialize map local operators
      for (Map.Entry<String, FetchOperator> entry : fetchOperators.entrySet()) {
        Operator<? extends Serializable> forwardOp = localWork.getAliasToWork()
            .get(entry.getKey());
        forwardOp.setExecContext(execContext);
        // All the operators need to be initialized before process
        forwardOp.initialize(jc, new ObjectInspector[] {entry.getValue()
            .getOutputObjectInspector()});
        l4j.info("fetchoperator for " + entry.getKey() + " initialized");
      }
      // defer processing of map local operators to first row if in case there
      // is no input (??)
    } catch (Throwable e) {
      abort = true;
      if (e instanceof OutOfMemoryError) {
        // will this be true here?
        // Don't create a new object if we are already out of memory
        throw (OutOfMemoryError) e;
      } else {
        throw new RuntimeException("Map operator initialization failed", e);
      }
    }

  }

  public void map(Object key, Object value, OutputCollector output,
      Reporter reporter) throws IOException {
    if (oc == null) {
      oc = output;
      rp = reporter;
      mo.setOutputCollector(oc);
      mo.setReporter(rp);
    }
    
    if(inputFileChanged()) {
      if (this.localWork != null
          && (localWork.getInputFileChangeSensitive() || this.lastInputFile == null)) {
        processMapLocalWork(localWork.getInputFileChangeSensitive());
      }
      this.lastInputFile = HiveConf.getVar(jc, HiveConf.ConfVars.HADOOPMAPFILENAME);
    }
    
    try {
      if (mo.getDone()) {
        done = true;
      } else {
        // Since there is no concept of a group, we don't invoke
        // startGroup/endGroup for a mapper
        mo.process((Writable) value);
        if (l4j.isInfoEnabled()) {
          numRows++;
          if (numRows == nextCntr) {
            long used_memory = memoryMXBean.getHeapMemoryUsage().getUsed();
            l4j.info("ExecMapper: processing " + numRows
                + " rows: used memory = " + used_memory);
            nextCntr = getNextCntr(numRows);
          }
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

  /**
   * For CompbineFileInputFormat, the mapper's input file will be changed on the
   * fly. If the map local work has any mapping depending on the current
   * mapper's input file, the work need to clear context and re-initialization
   * after the input file changed. This is first introduced to process bucket
   * map join.
   * 
   * @return
   */
  private boolean inputFileChanged() {
    String currentInputFile = HiveConf.getVar(jc, HiveConf.ConfVars.HADOOPMAPFILENAME);
    execContext.currentInputFile = currentInputFile;
    if (this.lastInputFile == null
        || !this.lastInputFile.equals(currentInputFile)) {
      execContext.inputFileChanged = true;
      return true;
    }
    execContext.inputFileChanged = false;
    return false;
  }

  private void processMapLocalWork(boolean inputFileChangeSenstive) {
    // process map local operators
    if (fetchOperators != null) {
      try {
        int fetchOpNum = 0;
        for (Map.Entry<String, FetchOperator> entry : fetchOperators
            .entrySet()) {
          int fetchOpRows = 0;
          String alias = entry.getKey();
          FetchOperator fetchOp = entry.getValue();
          
          if(inputFileChangeSenstive) {
            fetchOp.clearFetchContext();
            setUpFetchOpContext(fetchOp, alias);
          }
          
          Operator<? extends Serializable> forwardOp = localWork
              .getAliasToWork().get(alias);

          while (true) {
            InspectableObject row = fetchOp.getNextRow();
            if (row == null) {
              forwardOp.close(false);
              break;
            }
            fetchOpRows++;
            forwardOp.process(row.o, 0);
            // check if any operator had a fatal error or early exit during
            // execution
            if (forwardOp.getDone()) {
              done = true;
              break;
            }
          }

          if (l4j.isInfoEnabled()) {
            l4j
                .info("fetch " + fetchOpNum++ + " processed " + fetchOpRows
                    + " used mem: "
                    + memoryMXBean.getHeapMemoryUsage().getUsed());
          }
        }
      } catch (Throwable e) {
        abort = true;
        if (e instanceof OutOfMemoryError) {
          // Don't create a new object if we are already out of memory
          throw (OutOfMemoryError) e;
        } else {
          throw new RuntimeException("Hive Runtime Error: Map local work failed", e);
        }
      }
    }
  }
  
  private void setUpFetchOpContext(FetchOperator fetchOp, String alias)
      throws Exception {
    String currentInputFile = HiveConf.getVar(jc, HiveConf.ConfVars.HADOOPMAPFILENAME);
    BucketMapJoinContext bucketMatcherCxt = this.localWork.getBucketMapjoinContext();
    Class<? extends BucketMatcher> bucketMatcherCls = bucketMatcherCxt.getBucketMatcherClass();
    BucketMatcher bucketMatcher = (BucketMatcher) ReflectionUtils.newInstance(bucketMatcherCls, null);
    bucketMatcher.setAliasBucketFileNameMapping(bucketMatcherCxt.getAliasBucketFileNameMapping());
    List<Path> aliasFiles = bucketMatcher.getAliasBucketFiles(currentInputFile,
        bucketMatcherCxt.getMapJoinBigTableAlias(),
        alias);
    Iterator<Path> iter = aliasFiles.iterator();
    fetchOp.setupContext(iter, null);
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
      l4j.trace("Close called. no row processed by map.");
    }

    // detecting failed executions by exceptions thrown by the operator tree
    // ideally hadoop should let us know whether map execution failed or not
    try {
      mo.close(abort);
      if (fetchOperators != null) {
        MapredLocalWork localWork = mo.getConf().getMapLocalWork();
        for (Map.Entry<String, FetchOperator> entry : fetchOperators.entrySet()) {
          Operator<? extends Serializable> forwardOp = localWork
              .getAliasToWork().get(entry.getKey());
          forwardOp.close(abort);
        }
      }

      if (l4j.isInfoEnabled()) {
        long used_memory = memoryMXBean.getHeapMemoryUsage().getUsed();
        l4j.info("ExecMapper: processed " + numRows + " rows: used memory = "
            + used_memory);
      }

      reportStats rps = new reportStats(rp);
      mo.preorderMap(rps);
      return;
    } catch (Exception e) {
      if (!abort) {
        // signal new failure to map-reduce
        l4j.error("Hit error while closing operators - failing tree");
        throw new RuntimeException("Hive Runtime Error while closing operators", e);
      }
    }
  }

  public static boolean getDone() {
    return done;
  }

  /**
   * reportStats.
   *
   */
  public static class reportStats implements Operator.OperatorFunc {
    Reporter rp;

    public reportStats(Reporter rp) {
      this.rp = rp;
    }

    public void func(Operator op) {
      Map<Enum, Long> opStats = op.getStats();
      for (Map.Entry<Enum, Long> e : opStats.entrySet()) {
        if (rp != null) {
          rp.incrCounter(e.getKey(), e.getValue());
        }
      }
    }
  }
}
