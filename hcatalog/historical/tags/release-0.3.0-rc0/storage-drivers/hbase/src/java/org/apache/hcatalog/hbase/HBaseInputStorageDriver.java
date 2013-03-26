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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hive.hbase.HBaseSerDe;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.hbase.snapshot.TableSnapshot;
import org.apache.hcatalog.mapreduce.HCatInputStorageDriver;
import org.apache.hcatalog.mapreduce.HCatTableInfo;
import org.apache.hcatalog.mapreduce.InputJobInfo;
import org.apache.hcatalog.mapreduce.StorerInfo;


/**
 * The Class HBaseInputStorageDriver enables reading of HBase tables through
 * HCatalog.
 */
public class HBaseInputStorageDriver extends HCatInputStorageDriver {

    private InputJobInfo inpJobInfo;
    private ResultConverter converter;
    private HCatSchema outputColSchema;
    private HCatSchema dataSchema;
    private Configuration jobConf;
    private String scanColumns;
    private HCatTableSnapshot snapshot;

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
        inpJobInfo = (InputJobInfo) HCatUtil.deserialize(jobString);
        dataSchema = inpJobInfo.getTableInfo().getDataColumns();
        List<FieldSchema> fields = HCatUtil.getFieldSchemaList(dataSchema
                .getFields());
        hcatProperties.setProperty(Constants.LIST_COLUMNS,
                MetaStoreUtils.getColumnNamesFromFieldSchema(fields));
        hcatProperties.setProperty(Constants.LIST_COLUMN_TYPES,
                MetaStoreUtils.getColumnTypesFromFieldSchema(fields));
        converter = new HBaseSerDeResultConverter(dataSchema, outputColSchema,
                hcatProperties);
        scanColumns = converter.getHBaseScanColumns();
        String hbaseTableName = HBaseHCatStorageHandler
                .getFullyQualifiedName(inpJobInfo.getTableInfo());
        String serSnapshot = (String) inpJobInfo.getProperties().get(
                HBaseConstants.PROPERTY_TABLE_SNAPSHOT_KEY);
        if(serSnapshot == null){
        snapshot = HBaseHCatStorageHandler.createSnapshot(jobConf,
                hbaseTableName);
        inpJobInfo.getProperties().setProperty(
                HBaseConstants.PROPERTY_TABLE_SNAPSHOT_KEY,
                HCatUtil.serialize(snapshot));
        }

        context.getConfiguration().set(HCatConstants.HCAT_KEY_JOB_INFO, HCatUtil.serialize(inpJobInfo));

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

        String hbaseTableName = HBaseHCatStorageHandler
                .getFullyQualifiedName(inpJobInfo.getTableInfo());
        HBaseInputFormat tableInputFormat = new HBaseInputFormat(inpJobInfo);
        jobConf.set(TableInputFormat.INPUT_TABLE, hbaseTableName);
        jobConf.set(TableInputFormat.SCAN_COLUMNS, scanColumns);
        jobConf.setInt(TableInputFormat.SCAN_MAXVERSIONS, 1);
        tableInputFormat.setConf(jobConf);
        // TODO: Make the caching configurable by the user
        tableInputFormat.getScan().setCaching(200);
        tableInputFormat.getScan().setCacheBlocks(false);
        return tableInputFormat;
    }

    /*
     * @param baseKey The key produced by the MR job.
     *
     * @param baseValue The value produced by the MR job.
     *
     * @return HCatRecord An instance of HCatRecord produced by the key, value.
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
     * @param jobContext The jobcontext of MR job
     *
     * @param howlSchema The output schema of the hcat record.
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
     * @param jobContext The jobcontext of MR job.
     *
     * @param hcatSchema The schema of the hcat record.
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

    static HCatTableSnapshot convertSnapshot(TableSnapshot hbaseSnapshot,
            HCatTableInfo hcatTableInfo) throws IOException {

        HCatSchema hcatTableSchema = hcatTableInfo.getDataColumns();
        Map<String, String> hcatHbaseColMap = getHCatHBaseColumnMapping(hcatTableInfo);
        HashMap<String, Long> revisionMap = new HashMap<String, Long>();

        for (HCatFieldSchema fSchema : hcatTableSchema.getFields()) {
            if(hcatHbaseColMap.containsKey(fSchema.getName())){
                String colFamily = hcatHbaseColMap.get(fSchema.getName());
                long revisionID = hbaseSnapshot.getRevision(colFamily);
                revisionMap.put(fSchema.getName(), revisionID);
            }
        }

        HCatTableSnapshot hcatSnapshot = new HCatTableSnapshot(
                 hcatTableInfo.getDatabaseName(), hcatTableInfo.getTableName(),revisionMap);
        return hcatSnapshot;
    }

    static TableSnapshot convertSnapshot(HCatTableSnapshot hcatSnapshot,
            HCatTableInfo hcatTableInfo) throws IOException {

        HCatSchema hcatTableSchema = hcatTableInfo.getDataColumns();
        Map<String, Long> revisionMap = new HashMap<String, Long>();
        Map<String, String> hcatHbaseColMap = getHCatHBaseColumnMapping(hcatTableInfo);
        for (HCatFieldSchema fSchema : hcatTableSchema.getFields()) {
            String colFamily = hcatHbaseColMap.get(fSchema.getName());
            if (hcatSnapshot.containsColumn(fSchema.getName())) {
                long revision = hcatSnapshot.getRevision(fSchema.getName());
                revisionMap.put(colFamily, revision);
            }
        }

        String fullyQualifiedName = hcatSnapshot.getDatabaseName() + "."
                + hcatSnapshot.getTableName();
        return new TableSnapshot(fullyQualifiedName, revisionMap);

    }

    private static Map<String, String> getHCatHBaseColumnMapping( HCatTableInfo hcatTableInfo)
            throws IOException {

        HCatSchema hcatTableSchema = hcatTableInfo.getDataColumns();
        StorerInfo storeInfo = hcatTableInfo.getStorerInfo();
        String hbaseColumnMapping = storeInfo.getProperties().getProperty(
                HBaseConstants.PROPERTY_COLUMN_MAPPING_KEY);

        Map<String, String> hcatHbaseColMap = new HashMap<String, String>();
        List<String> columnFamilies = new ArrayList<String>();
        List<String> columnQualifiers = new ArrayList<String>();
        try {
            HBaseSerDe.parseColumnMapping(hbaseColumnMapping, columnFamilies,
                    null, columnQualifiers, null);
        } catch (SerDeException e) {
            throw new IOException("Exception while converting snapshots.", e);
        }

        for (HCatFieldSchema column : hcatTableSchema.getFields()) {
            int fieldPos = hcatTableSchema.getPosition(column.getName());
            String colFamily = columnFamilies.get(fieldPos);
            if (colFamily.equals(HBaseSerDe.HBASE_KEY_COL) == false) {
                hcatHbaseColMap.put(column.getName(), colFamily);
            }
        }

        return hcatHbaseColMap;
    }

}
