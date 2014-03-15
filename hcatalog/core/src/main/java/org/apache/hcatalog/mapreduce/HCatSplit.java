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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hcatalog.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * The HCatSplit wrapper around the InputSplit returned by the underlying InputFormat 
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.mapreduce.HCatSplit} instead
 */
public class HCatSplit extends InputSplit
  implements Writable, org.apache.hadoop.mapred.InputSplit {

  private static final Logger LOG = LoggerFactory.getLogger(HCatSplit.class);
  /** The partition info for the split. */
  private PartInfo partitionInfo;

  /** The split returned by the underlying InputFormat split. */
  private org.apache.hadoop.mapred.InputSplit baseMapRedSplit;

  /** The schema for the HCatTable */
  private HCatSchema tableSchema;

  private HiveConf hiveConf;

  /**
   * Instantiates a new hcat split.
   */
  public HCatSplit() {
  }

  /**
   * Instantiates a new hcat split.
   *
   * @param partitionInfo the partition info
   * @param baseMapRedSplit the base mapred split
   * @param tableSchema the table level schema
   */
  public HCatSplit(PartInfo partitionInfo,
           org.apache.hadoop.mapred.InputSplit baseMapRedSplit,
           HCatSchema tableSchema) {

    this.partitionInfo = partitionInfo;
    // dataSchema can be obtained from partitionInfo.getPartitionSchema()
    this.baseMapRedSplit = baseMapRedSplit;
    this.tableSchema = tableSchema;
  }

  /**
   * Gets the partition info.
   * @return the partitionInfo
   */
  public PartInfo getPartitionInfo() {
    return partitionInfo;
  }

  /**
   * Gets the underlying InputSplit.
   * @return the baseMapRedSplit
   */
  public org.apache.hadoop.mapred.InputSplit getBaseSplit() {
    return baseMapRedSplit;
  }

  /**
   * Gets the data schema.
   * @return the table schema
   */
  public HCatSchema getDataSchema() {
    return this.partitionInfo.getPartitionSchema();
  }

  /**
   * Gets the table schema.
   * @return the table schema
   */
  public HCatSchema getTableSchema() {
    return this.tableSchema;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.InputSplit#getLength()
   */
  @Override
  public long getLength() {
    try {
      return baseMapRedSplit.getLength();
    } catch (IOException e) {
      LOG.warn("Exception in HCatSplit", e);
    }
    return 0; // we errored
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.InputSplit#getLocations()
   */
  @Override
  public String[] getLocations() {
    try {
      return baseMapRedSplit.getLocations();
    } catch (IOException e) {
      LOG.warn("Exception in HCatSplit", e);
    }
    return new String[0]; // we errored
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  @SuppressWarnings("unchecked")
  @Override
  public void readFields(DataInput input) throws IOException {
    String partitionInfoString = WritableUtils.readString(input);
    partitionInfo = (PartInfo) HCatUtil.deserialize(partitionInfoString);

    String baseSplitClassName = WritableUtils.readString(input);
    org.apache.hadoop.mapred.InputSplit split;
    try {
      Class<? extends org.apache.hadoop.mapred.InputSplit> splitClass =
        (Class<? extends org.apache.hadoop.mapred.InputSplit>) Class.forName(baseSplitClassName);

      //Class.forName().newInstance() does not work if the underlying
      //InputSplit has package visibility
      Constructor<? extends org.apache.hadoop.mapred.InputSplit>
        constructor =
        splitClass.getDeclaredConstructor(new Class[]{});
      constructor.setAccessible(true);

      split = constructor.newInstance();
      // read baseSplit from input
      ((Writable) split).readFields(input);
      this.baseMapRedSplit = split;
    } catch (Exception e) {
      throw new IOException("Exception from " + baseSplitClassName, e);
    }

    String tableSchemaString = WritableUtils.readString(input);
    tableSchema = (HCatSchema) HCatUtil.deserialize(tableSchemaString);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput output) throws IOException {
    String partitionInfoString = HCatUtil.serialize(partitionInfo);

    // write partitionInfo into output
    WritableUtils.writeString(output, partitionInfoString);

    WritableUtils.writeString(output, baseMapRedSplit.getClass().getName());
    Writable baseSplitWritable = (Writable) baseMapRedSplit;
    //write  baseSplit into output
    baseSplitWritable.write(output);

    //write the table schema into output
    String tableSchemaString = HCatUtil.serialize(tableSchema);
    WritableUtils.writeString(output, tableSchemaString);
  }

}
