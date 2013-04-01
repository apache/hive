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

import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.LazyHCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The HCat wrapper for the underlying RecordReader,
 * this ensures that the initialize on
 * the underlying record reader is done with the underlying split,
 * not with HCatSplit.
 */
class HCatRecordReader extends RecordReader<WritableComparable, HCatRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(HCatRecordReader.class);
    WritableComparable currentKey;
    Writable currentValue;

    /** The underlying record reader to delegate to. */
    private org.apache.hadoop.mapred.RecordReader<WritableComparable, Writable> baseRecordReader;

    /** The storage handler used */
    private final HCatStorageHandler storageHandler;

    private SerDe serde;

    private Map<String,String> valuesNotInDataCols;

    private HCatSchema outputSchema = null;
    private HCatSchema dataSchema = null;

    /**
     * Instantiates a new hcat record reader.
     */
    public HCatRecordReader(HCatStorageHandler storageHandler,
                     Map<String,String> valuesNotInDataCols) {
      this.storageHandler = storageHandler;
      this.valuesNotInDataCols = valuesNotInDataCols;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#initialize(
     * org.apache.hadoop.mapreduce.InputSplit,
     * org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public void initialize(org.apache.hadoop.mapreduce.InputSplit split,
        TaskAttemptContext taskContext) throws IOException, InterruptedException {

      HCatSplit hcatSplit = InternalUtil.castToHCatSplit(split);

      baseRecordReader = createBaseRecordReader(hcatSplit, storageHandler, taskContext);
      serde = createSerDe(hcatSplit, storageHandler, taskContext);

      // Pull the output schema out of the TaskAttemptContext
      outputSchema = (HCatSchema) HCatUtil.deserialize(
          taskContext.getConfiguration().get(HCatConstants.HCAT_KEY_OUTPUT_SCHEMA));

      if (outputSchema == null) {
        outputSchema = hcatSplit.getTableSchema();
      }

      // Pull the table schema out of the Split info
      // TODO This should be passed in the TaskAttemptContext instead
      dataSchema = hcatSplit.getDataSchema();
    }

    private org.apache.hadoop.mapred.RecordReader createBaseRecordReader(HCatSplit hcatSplit,
        HCatStorageHandler storageHandler, TaskAttemptContext taskContext) throws IOException {

      JobConf jobConf = HCatUtil.getJobConfFromContext(taskContext);
      HCatUtil.copyJobPropertiesToJobConf(hcatSplit.getPartitionInfo().getJobProperties(), jobConf);
      org.apache.hadoop.mapred.InputFormat inputFormat =
          HCatInputFormat.getMapRedInputFormat(jobConf, storageHandler.getInputFormatClass());
      return inputFormat.getRecordReader(hcatSplit.getBaseSplit(), jobConf,
          InternalUtil.createReporter(taskContext));
    }

    private SerDe createSerDe(HCatSplit hcatSplit, HCatStorageHandler storageHandler,
        TaskAttemptContext taskContext) throws IOException {

      SerDe serde = ReflectionUtils.newInstance(storageHandler.getSerDeClass(),
          taskContext.getConfiguration());

      try {
        InternalUtil.initializeInputSerDe(serde, storageHandler.getConf(),
            hcatSplit.getPartitionInfo().getTableInfo(),
            hcatSplit.getPartitionInfo().getPartitionSchema());
      } catch (SerDeException e) {
        throw new IOException("Failed initializing SerDe "
            + storageHandler.getSerDeClass().getName(), e);
      }

      return serde;
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
            LOG.warn("Exception in HCatRecord reader",e);
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
