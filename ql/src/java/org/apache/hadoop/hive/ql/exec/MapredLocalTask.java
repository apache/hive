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

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork.BucketMapJoinContext;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

public class MapredLocalTask  extends Task<MapredLocalWork> implements Serializable {

  private Map<String, FetchOperator> fetchOperators;
  private File jdbmFile;
  private JobConf job;
  public static final Log l4j = LogFactory.getLog("MapredLocalTask");
  private MapOperator mo;
  // not sure we need this exec context; but all the operators in the work
  // will pass this context throught
  private final ExecMapperContext execContext = new ExecMapperContext();

  public MapredLocalTask(){
    super();
  }

  @Override
  public void initialize(HiveConf conf, QueryPlan queryPlan,
      DriverContext driverContext) {
    super.initialize(conf, queryPlan, driverContext);
    job = new JobConf(conf, ExecDriver.class);
  }

  @Override
public int execute(DriverContext driverContext){
    // check the local work
    if(work == null){
      return -1;
    }
    fetchOperators = new HashMap<String, FetchOperator>();
    Map<FetchOperator, JobConf> fetchOpJobConfMap = new HashMap<FetchOperator, JobConf>();
    execContext.setJc(job);
    //set the local work, so all the operator can get this context
    execContext.setLocalWork(work);
    boolean inputFileChangeSenstive = work.getInputFileChangeSensitive();
    try{

      initializeOperators(fetchOpJobConfMap);
      //for each big table's bucket, call the start forward
      if(inputFileChangeSenstive){
        for( LinkedHashMap<String, ArrayList<String>> bigTableBucketFiles:
          work.getBucketMapjoinContext().getAliasBucketFileNameMapping().values()){
          for(String bigTableBucket: bigTableBucketFiles.keySet()){
            startForward(inputFileChangeSenstive,bigTableBucket);
          }
        }
      }else{
        startForward(inputFileChangeSenstive,null);
      }
    } catch (Throwable e) {
      if (e instanceof OutOfMemoryError) {
        // Don't create a new object if we are already out of memory
        l4j.error("Out of Merror Error");
      } else {
        l4j.error("Hive Runtime Error: Map local work failed");
        e.printStackTrace();
      }
    }
    return 0;
  }

  private void startForward(boolean inputFileChangeSenstive, String bigTableBucket)
    throws Exception{
    for (Map.Entry<String, FetchOperator> entry : fetchOperators.entrySet()) {
      int fetchOpRows = 0;
      String alias = entry.getKey();
      FetchOperator fetchOp = entry.getValue();

      if (inputFileChangeSenstive) {
        fetchOp.clearFetchContext();
        setUpFetchOpContext(fetchOp, alias,bigTableBucket);
      }

      //get the root operator
      Operator<? extends Serializable> forwardOp = work.getAliasToWork().get(alias);
      //walk through the operator tree
      while (true) {
        InspectableObject row = fetchOp.getNextRow();
        if (row == null) {
          if (inputFileChangeSenstive) {
            String fileName=this.getFileName(bigTableBucket);
            execContext.setCurrentBigBucketFile(fileName);
            forwardOp.reset();
          }
          forwardOp.close(false);
          break;
        }
        fetchOpRows++;
        forwardOp.process(row.o, 0);
        // check if any operator had a fatal error or early exit during
        // execution
        if (forwardOp.getDone()) {
          //ExecMapper.setDone(true);
          break;
        }
      }
    }
  }
  private void initializeOperators(Map<FetchOperator, JobConf> fetchOpJobConfMap)
    throws HiveException{
    // this mapper operator is used to initialize all the operators
    for (Map.Entry<String, FetchWork> entry : work.getAliasToFetchWork().entrySet()) {
      JobConf jobClone = new JobConf(job);

      Operator<? extends Serializable> tableScan = work.getAliasToWork().get(entry.getKey());
      boolean setColumnsNeeded = false;
      if(tableScan instanceof TableScanOperator) {
        ArrayList<Integer> list = ((TableScanOperator)tableScan).getNeededColumnIDs();
        if (list != null) {
          ColumnProjectionUtils.appendReadColumnIDs(jobClone, list);
          setColumnsNeeded = true;
        }
      }

      if (!setColumnsNeeded) {
        ColumnProjectionUtils.setFullyReadColumns(jobClone);
      }

      //create a fetch operator
      FetchOperator fetchOp = new FetchOperator(entry.getValue(),jobClone);
      fetchOpJobConfMap.put(fetchOp, jobClone);
      fetchOperators.put(entry.getKey(), fetchOp);
      l4j.info("fetchoperator for " + entry.getKey() + " created");
    }
    //initilize all forward operator
    for (Map.Entry<String, FetchOperator> entry : fetchOperators.entrySet()) {
      //get the forward op
      Operator<? extends Serializable> forwardOp = work.getAliasToWork().get(entry.getKey());

      //put the exe context into all the operators
      forwardOp.setExecContext(execContext);
      // All the operators need to be initialized before process
      FetchOperator fetchOp = entry.getValue();
      JobConf jobConf = fetchOpJobConfMap.get(fetchOp);

      if (jobConf == null) {
        jobConf = job;
      }
      //initialize the forward operator
      forwardOp.initialize(jobConf, new ObjectInspector[] {fetchOp.getOutputObjectInspector()});
      l4j.info("fetchoperator for " + entry.getKey() + " initialized");
    }
  }


  private void setUpFetchOpContext(FetchOperator fetchOp, String alias,String currentInputFile)
  throws Exception {

    BucketMapJoinContext bucketMatcherCxt = this.work
        .getBucketMapjoinContext();

    Class<? extends BucketMatcher> bucketMatcherCls = bucketMatcherCxt
        .getBucketMatcherClass();
    BucketMatcher bucketMatcher = (BucketMatcher) ReflectionUtils.newInstance(
        bucketMatcherCls, null);
    bucketMatcher.setAliasBucketFileNameMapping(bucketMatcherCxt
        .getAliasBucketFileNameMapping());

    List<Path> aliasFiles = bucketMatcher.getAliasBucketFiles(currentInputFile,
        bucketMatcherCxt.getMapJoinBigTableAlias(), alias);
    Iterator<Path> iter = aliasFiles.iterator();
    fetchOp.setupContext(iter, null);
  }

  private String getFileName(String path){
    if(path== null || path.length()==0) {
      return null;
    }

    int last_separator = path.lastIndexOf(Path.SEPARATOR)+1;
    String fileName = path.substring(last_separator);
    return fileName;

  }
  @Override
  public void localizeMRTmpFilesImpl(Context ctx){

  }

  @Override
  public String getName() {
    return "MAPREDLOCAL";
  }
  @Override
  public int getType() {
    //assert false;
    return StageType.MAPREDLOCAL;
  }

}
