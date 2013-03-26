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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.schema.HCatSchema;

/** The HCatSplit wrapper around the InputSplit returned by the underlying InputFormat */
class HCatSplit extends InputSplit implements Writable {

    /** The partition info for the split. */
    private PartInfo partitionInfo;

    /** The split returned by the underlying InputFormat split. */
    private InputSplit baseSplit;

    /** The schema for the HCatTable */
    private HCatSchema tableSchema;
    /**
     * Instantiates a new hcat split.
     */
    public HCatSplit() {
    }

    /**
     * Instantiates a new hcat split.
     *
     * @param partitionInfo the partition info
     * @param baseSplit the base split
     * @param tableSchema the table level schema
     */
    public HCatSplit(PartInfo partitionInfo, InputSplit baseSplit, HCatSchema tableSchema) {
        this.partitionInfo = partitionInfo;
        this.baseSplit = baseSplit;
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
     * @return the baseSplit
     */
    public InputSplit getBaseSplit() {
        return baseSplit;
    }

    /**
     * Sets the table schema.
     * @param tableSchema the new table schema
     */
    public void setTableSchema(HCatSchema tableSchema) {
        this.tableSchema = tableSchema;
    }

    /**
     * Gets the table schema.
     * @return the table schema
     */
    public HCatSchema getTableSchema() {
        return tableSchema;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.InputSplit#getLength()
     */
    @Override
    public long getLength() throws IOException, InterruptedException {
        return baseSplit.getLength();
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.InputSplit#getLocations()
     */
    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return baseSplit.getLocations();
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
        InputSplit split;
        try{
            Class<? extends InputSplit> splitClass =
                (Class<? extends InputSplit>) Class.forName(baseSplitClassName);

            //Class.forName().newInstance() does not work if the underlying
            //InputSplit has package visibility
            Constructor<? extends InputSplit> constructor =
                splitClass.getDeclaredConstructor(new Class[]{});
            constructor.setAccessible(true);

            split = constructor.newInstance();
            // read baseSplit from input
            ((Writable)split).readFields(input);
            this.baseSplit = split;
        }catch(Exception e){
            throw new IOException ("Exception from " +baseSplitClassName + " : " + e.getMessage());
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

        WritableUtils.writeString(output, baseSplit.getClass().getName());
        Writable baseSplitWritable = (Writable)baseSplit;
        //write  baseSplit into output
        baseSplitWritable.write(output);

        //write the table schema into output
        String tableSchemaString = HCatUtil.serialize(tableSchema);
        WritableUtils.writeString(output, tableSchemaString);
    }
}
