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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.SerDe;

import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.LazyHCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.data.schema.HCatFieldSchema;

/** The HCat wrapper for the underlying RecordReader, 
 * this ensures that the initialize on
 * the underlying record reader is done with the underlying split, 
 * not with HCatSplit.
 */
class HCatRecordReader extends RecordReader<WritableComparable, HCatRecord> {
  
    Log LOG = LogFactory.getLog(HCatRecordReader.class);
    WritableComparable currentKey;
    Writable currentValue;

    /** The underlying record reader to delegate to. */
    //org.apache.hadoop.mapred.
    private final org.apache.hadoop.mapred.RecordReader
      <WritableComparable, Writable> baseRecordReader;

    /** The storage handler used */
    private final HCatStorageHandler storageHandler;

    private SerDe serde;

    private Map<String,String> valuesNotInDataCols;

    private HCatSchema outputSchema = null;
    private HCatSchema dataSchema = null;

    /**
     * Instantiates a new hcat record reader.
     * @param baseRecordReader the base record reader
     */
    public HCatRecordReader(HCatStorageHandler storageHandler, 
        org.apache.hadoop.mapred.RecordReader<WritableComparable, 
                     Writable> baseRecordReader, 
                     SerDe serde, 
                     Map<String,String> valuesNotInDataCols) {
      this.baseRecordReader = baseRecordReader;
      this.storageHandler = storageHandler;
      this.serde = serde;
      this.valuesNotInDataCols = valuesNotInDataCols;
    }
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#initialize(
     * org.apache.hadoop.mapreduce.InputSplit, 
     * org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public void initialize(org.apache.hadoop.mapreduce.InputSplit split, 
                           TaskAttemptContext taskContext)
    throws IOException, InterruptedException {
        org.apache.hadoop.mapred.InputSplit baseSplit;
        
        // Pull the output schema out of the TaskAttemptContext
        outputSchema = (HCatSchema)HCatUtil.deserialize(
          taskContext.getConfiguration().get(HCatConstants.HCAT_KEY_OUTPUT_SCHEMA));

        if( split instanceof HCatSplit ) {
            baseSplit = ((HCatSplit) split).getBaseSplit();
        } else {
          throw new IOException("Not a HCatSplit");
        }

        if (outputSchema == null){
          outputSchema = ((HCatSplit) split).getTableSchema();
        }

        // Pull the table schema out of the Split info
        // TODO This should be passed in the TaskAttemptContext instead
        dataSchema = ((HCatSplit)split).getDataSchema();
        
        Properties properties = new Properties();
        for (Map.Entry<String, String>param : 
            ((HCatSplit)split).getPartitionInfo()
                              .getJobProperties().entrySet()) {
          properties.setProperty(param.getKey(), param.getValue());
        }
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
     */
    @Override
    public WritableComparable getCurrentKey() 
    throws IOException, InterruptedException {
      return currentKey;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
     */
    @Override
    public HCatRecord getCurrentValue() 
    throws IOException, InterruptedException {
      HCatRecord r;

      try {

        r = new LazyHCatRecord(serde.deserialize(currentValue),serde.getObjectInspector());
        DefaultHCatRecord dr = new DefaultHCatRecord(outputSchema.size());
        int i = 0;
        for (String fieldName : outputSchema.getFieldNames()){
          Integer dataPosn = null;
          if ((dataPosn = dataSchema.getPosition(fieldName)) != null){
            dr.set(i, r.get(fieldName,dataSchema));
          } else {
            dr.set(i, valuesNotInDataCols.get(fieldName));
          }
          i++;
        }
        
        return dr;
          
      } catch (Exception e) { 
        throw new IOException("Failed to create HCatRecord ",e);
      }
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
     */
    @Override
    public float getProgress()  {
        try {
          return baseRecordReader.getProgress();
        } catch (IOException e) {
          LOG.warn(e.getMessage());
          LOG.warn(e.getStackTrace());
        }
        return 0.0f; // errored
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (currentKey == null) {
        currentKey = baseRecordReader.createKey();
        currentValue = baseRecordReader.createValue();
      }

        return baseRecordReader.next(currentKey, 
                                     currentValue);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#close()
     */
    @Override
    public void close() throws IOException {
        baseRecordReader.close();
    }

}
