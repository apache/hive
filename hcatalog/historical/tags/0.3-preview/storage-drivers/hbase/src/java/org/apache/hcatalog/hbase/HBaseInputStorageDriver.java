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

package org.apache.hcatalog.hbase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.HCatInputStorageDriver;
import org.apache.hcatalog.mapreduce.HCatTableInfo;
import org.apache.hcatalog.mapreduce.InputJobInfo;

/**
 * The Class HBaseInputStorageDriver enables reading of HBase tables through
 * HCatalog.
 */
public class HBaseInputStorageDriver extends HCatInputStorageDriver {
    private HCatTableInfo   tableInfo;
    private ResultConverter converter;
    private HCatSchema      outputColSchema;
    private HCatSchema      dataSchema;
    private Configuration   jobConf;
    private String          scanColumns;

    /*
     * @param JobContext
     *
     * @param hcatProperties
     *
     * @see org.apache.hcatalog.mapreduce.HCatInputStorageDriver
     * #initialize(org.apache.hadoop.mapreduce.JobContext, java.util.Properties)
     */
    @Override
    public void initialize(JobContext context, Properties hcatProperties) throws IOException {
        jobConf = context.getConfiguration();
        String jobString = jobConf.get(HCatConstants.HCAT_KEY_JOB_INFO);
        if (jobString == null) {
            throw new IOException(
                    "InputJobInfo information not found in JobContext. "
                            + "HCatInputFormat.setInput() not called?");
        }
        InputJobInfo jobInfo = (InputJobInfo) HCatUtil.deserialize(jobString);
        tableInfo = jobInfo.getTableInfo();
        dataSchema = tableInfo.getDataColumns();
        List<FieldSchema> fields = HCatUtil.getFieldSchemaList(dataSchema
                .getFields());
        hcatProperties.setProperty(Constants.LIST_COLUMNS,
                MetaStoreUtils.getColumnNamesFromFieldSchema(fields));
        hcatProperties.setProperty(Constants.LIST_COLUMN_TYPES,
                MetaStoreUtils.getColumnTypesFromFieldSchema(fields));
        converter = new HBaseSerDeResultConverter(dataSchema, outputColSchema,
                hcatProperties);
        scanColumns = converter.getHBaseScanColumns();

    }

    /*
     * @param hcatProperties
     *
     * @return InputFormat
     *
     * @see org.apache.hcatalog.mapreduce.HCatInputStorageDriver
     * #getInputFormat(java.util.Properties)
     */
    @Override
    public InputFormat<ImmutableBytesWritable, Result> getInputFormat(
            Properties hcatProperties) {
        HBaseInputFormat tableInputFormat = new HBaseInputFormat();
        String hbaseTableName = HBaseHCatStorageHandler.getFullyQualifiedName(tableInfo);
        jobConf.set(TableInputFormat.INPUT_TABLE, hbaseTableName);
        jobConf.set(TableInputFormat.SCAN_COLUMNS, scanColumns);
        tableInputFormat.setConf(jobConf);
        // TODO: Make the caching configurable by the user
        tableInputFormat.getScan().setCaching(200);
        tableInputFormat.getScan().setCacheBlocks(false);
        return tableInputFormat;
    }

    /*
     * @param baseKey
     *
     * @param baseValue
     *
     * @return HCatRecord
     *
     * @throws IOException
     *
     * @see
     * org.apache.hcatalog.mapreduce.HCatInputStorageDriver#convertToHCatRecord
     * (org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.Writable)
     */
    @Override
    public HCatRecord convertToHCatRecord(WritableComparable baseKey,
            Writable baseValue) throws IOException {
        return this.converter.convert((Result) baseValue);
    }

    /*
     * @param jobContext
     *
     * @param howlSchema
     *
     * @throws IOException
     *
     * @see org.apache.hcatalog.mapreduce.HCatInputStorageDriver#
     * setOutputSchema(org.apache.hadoop.mapreduce.JobContext,
     * org.apache.hcatalog.data.schema.HCatSchema)
     */
    @Override
    public void setOutputSchema(JobContext jobContext, HCatSchema howlSchema)
            throws IOException {
        this.outputColSchema = howlSchema;
    }

    /*
     * @param jobContext
     *
     * @param partitionValues
     *
     * @throws IOException
     *
     * @see org.apache.hcatalog.mapreduce.HCatInputStorageDriver
     * #setPartitionValues(org.apache.hadoop.mapreduce.JobContext,
     * java.util.Map)
     */
    @Override
    public void setPartitionValues(JobContext jobContext,
            Map<String, String> partitionValues) throws IOException {
    }

    /*
     * @param jobContext
     *
     * @param hcatSchema
     *
     * @throws IOException
     *
     * @see org.apache.hcatalog.mapreduce.HCatInputStorageDriver
     * #setOriginalSchema(org.apache.hadoop.mapreduce.JobContext,
     * org.apache.hcatalog.data.schema.HCatSchema)
     */
    @Override
    public void setOriginalSchema(JobContext jobContext, HCatSchema hcatSchema)
            throws IOException {
        this.dataSchema = hcatSchema;
    }
}
