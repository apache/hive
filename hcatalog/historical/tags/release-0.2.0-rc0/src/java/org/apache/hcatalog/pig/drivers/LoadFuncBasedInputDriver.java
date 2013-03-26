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
package org.apache.hcatalog.pig.drivers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.HCatInputStorageDriver;
import org.apache.hcatalog.pig.PigHCatUtil;
import org.apache.pig.LoadFunc;
import org.apache.pig.data.Tuple;


/**
 * This is a base class which wraps a Load func in HCatInputStorageDriver.
 * If you already have a LoadFunc, then this class along with LoadFuncBasedInputFormat
 * is doing all the heavy lifting. For a new HCat Input Storage Driver just extend it
 * and override the initialize(). {@link PigStorageInputDriver} illustrates
 * that well.
 */
public abstract class LoadFuncBasedInputDriver extends HCatInputStorageDriver{

  private LoadFuncBasedInputFormat inputFormat;
  private HCatSchema dataSchema;
  private Map<String,String> partVals;
  private List<String> desiredColNames;
  protected LoadFunc lf;

  @Override
  public HCatRecord convertToHCatRecord(WritableComparable baseKey, Writable baseValue)
      throws IOException {

    List<Object> data = ((Tuple)baseValue).getAll();
    List<Object> hcatRecord = new ArrayList<Object>(desiredColNames.size());

    /* Iterate through columns asked for in output schema, look them up in
     * original data schema. If found, put it. Else look up in partition columns
     * if found, put it. Else, its a new column, so need to put null. Map lookup
     * on partition map will return null, if column is not found.
     */
    for(String colName : desiredColNames){
      Integer idx = dataSchema.getPosition(colName);
      hcatRecord.add( idx != null ? data.get(idx) : partVals.get(colName));
    }
    return new DefaultHCatRecord(hcatRecord);
  }

  @Override
  public InputFormat<? extends WritableComparable, ? extends Writable> getInputFormat(
      Properties hcatProperties) {

    return inputFormat;
  }

  @Override
  public void setOriginalSchema(JobContext jobContext, HCatSchema hcatSchema) throws IOException {

    dataSchema = hcatSchema;
  }

  @Override
  public void setOutputSchema(JobContext jobContext, HCatSchema hcatSchema) throws IOException {

    desiredColNames = hcatSchema.getFieldNames();
  }

  @Override
  public void setPartitionValues(JobContext jobContext, Map<String, String> partitionValues)
      throws IOException {

    partVals = partitionValues;
  }

  @Override
  public void initialize(JobContext context, Properties storageDriverArgs) throws IOException {

    lf.setLocation(location, new Job(context.getConfiguration()));
    inputFormat = new LoadFuncBasedInputFormat(lf, PigHCatUtil.getResourceSchema(dataSchema));
  }

  private String location;

  @Override
  public void setInputPath(JobContext jobContext, String location) throws IOException {

    this.location = location;
    super.setInputPath(jobContext, location);
  }
}
