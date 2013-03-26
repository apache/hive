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

package org.apache.hcatalog.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hcatalog.common.ErrorType;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;

public abstract class HCatBaseOutputFormat extends OutputFormat<WritableComparable<?>, HCatRecord> {

//  static final private Log LOG = LogFactory.getLog(HCatBaseOutputFormat.class);

  /**
   * Gets the table schema for the table specified in the HCatOutputFormat.setOutput call
   * on the specified job context.
   * @param context the context
   * @return the table schema
   * @throws IOException if HCatOutputFromat.setOutput has not been called for the passed context
   */
  public static HCatSchema getTableSchema(JobContext context) throws IOException {
      OutputJobInfo jobInfo = getJobInfo(context);
      return jobInfo.getTableInfo().getDataColumns();
  }

  /**
   * Check for validity of the output-specification for the job.
   * @param context information about the job
   * @throws IOException when output should not be attempted
   */
  @Override
  public void checkOutputSpecs(JobContext context
                                        ) throws IOException, InterruptedException {
      getOutputFormat(context).checkOutputSpecs(context);
  }

  /**
   * Gets the output format instance.
   * @param context the job context
   * @return the output format instance
   * @throws IOException
   */
  protected OutputFormat<WritableComparable<?>, HCatRecord> getOutputFormat(JobContext context) throws IOException {
      OutputJobInfo jobInfo = getJobInfo(context);
      HCatOutputStorageDriver  driver = getOutputDriverInstance(context, jobInfo);
      return driver.getOutputFormatContainer(driver.getOutputFormat());
  }

  /**
   * Gets the HCatOuputJobInfo object by reading the Configuration and deserializing
   * the string. If InputJobInfo is not present in the configuration, throws an
   * exception since that means HCatOutputFormat.setOutput has not been called.
   * @param jobContext the job context
   * @return the OutputJobInfo object
   * @throws IOException the IO exception
   */
  public static OutputJobInfo getJobInfo(JobContext jobContext) throws IOException {
      String jobString = jobContext.getConfiguration().get(HCatConstants.HCAT_KEY_OUTPUT_INFO);
      if( jobString == null ) {
          throw new HCatException(ErrorType.ERROR_NOT_INITIALIZED);
      }

      return (OutputJobInfo) HCatUtil.deserialize(jobString);
  }

  /**
   * Gets the output storage driver instance.
   * @param jobContext the job context
   * @param jobInfo the output job info
   * @return the output driver instance
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  static HCatOutputStorageDriver getOutputDriverInstance(
          JobContext jobContext, OutputJobInfo jobInfo) throws IOException {
    return getOutputDriverInstance(jobContext,jobInfo,(List<String>)null);
  }

  /**
   * Gets the output storage driver instance, with allowing specification of missing dynamic partvals
   * @param jobContext the job context
   * @param jobInfo the output job info
   * @return the output driver instance
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  static HCatOutputStorageDriver getOutputDriverInstance(
          JobContext jobContext, OutputJobInfo jobInfo, List<String> dynamicPartVals) throws IOException {
      try {
          Class<? extends HCatOutputStorageDriver> driverClass =
              (Class<? extends HCatOutputStorageDriver>)
              Class.forName(jobInfo.getTableInfo().getStorerInfo().getOutputSDClass());
          HCatOutputStorageDriver driver = driverClass.newInstance();

          Map<String, String> partitionValues = jobInfo.getPartitionValues();
          String location = jobInfo.getLocation();

          if (dynamicPartVals != null){
            // dynamic part vals specified
            List<String> dynamicPartKeys = jobInfo.getDynamicPartitioningKeys();
            if (dynamicPartVals.size() != dynamicPartKeys.size()){
              throw new HCatException(ErrorType.ERROR_INVALID_PARTITION_VALUES, 
                  "Unable to instantiate dynamic partitioning storage driver, mismatch between"
                  + " number of partition values obtained["+dynamicPartVals.size()
                  + "] and number of partition values required["+dynamicPartKeys.size()+"]");
            }
            for (int i = 0; i < dynamicPartKeys.size(); i++){
              partitionValues.put(dynamicPartKeys.get(i), dynamicPartVals.get(i));
            }

            // re-home location, now that we know the rest of the partvals
            Table table = jobInfo.getTableInfo().getTable();
            
            List<String> partitionCols = new ArrayList<String>();
            for(FieldSchema schema : table.getPartitionKeys()) {
              partitionCols.add(schema.getName());
            }

            location = driver.getOutputLocation(jobContext,
                table.getSd().getLocation() , partitionCols,
                partitionValues,jobContext.getConfiguration().get(HCatConstants.HCAT_DYNAMIC_PTN_JOBID));
          }

          //Initialize the storage driver
          driver.setSchema(jobContext, jobInfo.getOutputSchema());
          driver.setPartitionValues(jobContext, partitionValues);
          driver.setOutputPath(jobContext, location);
          
          driver.initialize(jobContext, jobInfo.getTableInfo().getStorerInfo().getProperties());
          
//          HCatUtil.logMap(LOG,"Setting outputPath ["+location+"] for ",partitionValues);
          return driver;
      } catch(Exception e) {
        if (e instanceof HCatException){
          throw (HCatException)e;
        }else{
          throw new HCatException(ErrorType.ERROR_INIT_STORAGE_DRIVER, e);
        }
      }
  }

  /**
   * Gets the output storage driver instance, with allowing specification 
   * of partvals from which it picks the dynamic partvals
   * @param context the job context
   * @param jobInfo the output job info
   * @return the output driver instance
   * @throws IOException
   */

  protected static HCatOutputStorageDriver getOutputDriverInstance(
      JobContext context, OutputJobInfo jobInfo,
      Map<String, String> fullPartSpec) throws IOException {
    List<String> dynamicPartKeys = jobInfo.getDynamicPartitioningKeys();
    if ((dynamicPartKeys == null)||(dynamicPartKeys.isEmpty())){
      return getOutputDriverInstance(context,jobInfo,(List<String>)null);
    }else{
      List<String> dynKeyVals = new ArrayList<String>();
      for (String dynamicPartKey : dynamicPartKeys){
        dynKeyVals.add(fullPartSpec.get(dynamicPartKey));
      }
      return getOutputDriverInstance(context,jobInfo,dynKeyVals);
    }
  }


  protected static void setPartDetails(OutputJobInfo jobInfo, final HCatSchema schema,
      Map<String, String> partMap) throws HCatException, IOException {
    List<Integer> posOfPartCols = new ArrayList<Integer>();
    List<Integer> posOfDynPartCols = new ArrayList<Integer>();

    // If partition columns occur in data, we want to remove them.
    // So, find out positions of partition columns in schema provided by user.
    // We also need to update the output Schema with these deletions.
    
    // Note that, output storage drivers never sees partition columns in data
    // or schema.

    HCatSchema schemaWithoutParts = new HCatSchema(schema.getFields());
    for(String partKey : partMap.keySet()){
      Integer idx;
      if((idx = schema.getPosition(partKey)) != null){
        posOfPartCols.add(idx);
        schemaWithoutParts.remove(schema.get(partKey));
      }
    }

    // Also, if dynamic partitioning is being used, we want to
    // set appropriate list of columns for the columns to be dynamically specified.
    // These would be partition keys too, so would also need to be removed from 
    // output schema and partcols

    if (jobInfo.isDynamicPartitioningUsed()){
      for (String partKey : jobInfo.getDynamicPartitioningKeys()){
        Integer idx;
        if((idx = schema.getPosition(partKey)) != null){
          posOfPartCols.add(idx);
          posOfDynPartCols.add(idx);
          schemaWithoutParts.remove(schema.get(partKey));
        }
      }
    }
    
    HCatUtil.validatePartitionSchema(jobInfo.getTableInfo().getTable(), schemaWithoutParts);
    jobInfo.setPosOfPartCols(posOfPartCols);
    jobInfo.setPosOfDynPartCols(posOfDynPartCols);
    jobInfo.setOutputSchema(schemaWithoutParts);
  }

  static void cancelDelegationTokens(JobContext context, OutputJobInfo outputJobInfo) throws Exception {
    HiveMetaStoreClient client = HCatOutputFormat.createHiveClient(outputJobInfo.getServerUri(), context.getConfiguration());
    // cancel the deleg. tokens that were acquired for this job now that
    // we are done - we should cancel if the tokens were acquired by
    // HCatOutputFormat and not if they were supplied by Oozie. In the latter
    // case the HCAT_KEY_TOKEN_SIGNATURE property in the conf will not be set
    String tokenStrForm = client.getTokenStrForm();
    if(tokenStrForm != null && context.getConfiguration().get(HCatConstants.HCAT_KEY_TOKEN_SIGNATURE) != null) {
      client.cancelDelegationToken(tokenStrForm);
    }

    String jcTokenStrForm =
      context.getConfiguration().get(HCatConstants.HCAT_KEY_JOBCLIENT_TOKEN_STRFORM);
    String jcTokenSignature =
      context.getConfiguration().get(HCatConstants.HCAT_KEY_JOBCLIENT_TOKEN_SIGNATURE);
    if(jcTokenStrForm != null && jcTokenSignature != null) {
      HCatUtil.cancelJobTrackerDelegationToken(tokenStrForm,jcTokenSignature);
    }
  }
}
