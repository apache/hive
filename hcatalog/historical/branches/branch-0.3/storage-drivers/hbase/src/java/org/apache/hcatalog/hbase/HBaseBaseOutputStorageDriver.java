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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.hbase.snapshot.RevisionManager;
import org.apache.hcatalog.hbase.snapshot.Transaction;
import org.apache.hcatalog.mapreduce.HCatOutputStorageDriver;
import org.apache.hcatalog.mapreduce.HCatTableInfo;
import org.apache.hcatalog.mapreduce.OutputJobInfo;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Base class share by both {@link HBaseBulkOutputStorageDriver} and {@link HBaseDirectOutputStorageDriver}
 */
abstract  class HBaseBaseOutputStorageDriver extends HCatOutputStorageDriver {
    protected HCatTableInfo tableInfo;
    protected ResultConverter converter;
    protected OutputJobInfo outputJobInfo;
    protected HCatSchema schema;
    protected HCatSchema outputSchema;

    /**
     *  Subclasses are required to serialize OutputJobInfo back into jobContext.
     *  Since initialize() sets some properties in OutputJobInfo, requiring
     *  an update of the instance stored in jobContext.
     * @param context the job context object
     * @param hcatProperties the properties for the storage driver
     * @throws IOException
     */
    @Override
    public void initialize(JobContext context, Properties hcatProperties) throws IOException {
        hcatProperties = (Properties)hcatProperties.clone();
        super.initialize(context, hcatProperties);

        String jobString = context.getConfiguration().get(HCatConstants.HCAT_KEY_OUTPUT_INFO);
        if( jobString == null ) {
            throw new IOException("OutputJobInfo information not found in JobContext. HCatInputFormat.setOutput() not called?");
        }

        outputJobInfo = (OutputJobInfo) HCatUtil.deserialize(jobString);
        //override table properties with user defined ones
        //TODO in the future we should be more selective on what to override
        hcatProperties.putAll(outputJobInfo.getProperties());
        outputJobInfo.getProperties().putAll(hcatProperties);
        hcatProperties = outputJobInfo.getProperties();

        tableInfo = outputJobInfo.getTableInfo();
        schema = tableInfo.getDataColumns();
        String qualifiedTableName = HBaseHCatStorageHandler.getFullyQualifiedName(tableInfo);

        List<FieldSchema> fields = HCatUtil.getFieldSchemaList(outputSchema.getFields());
        hcatProperties.setProperty(Constants.LIST_COLUMNS,
                MetaStoreUtils.getColumnNamesFromFieldSchema(fields));
        hcatProperties.setProperty(Constants.LIST_COLUMN_TYPES,
                MetaStoreUtils.getColumnTypesFromFieldSchema(fields));

        context.getConfiguration().set(HBaseConstants.PROPERTY_OUTPUT_TABLE_NAME_KEY, qualifiedTableName);

        String txnString = outputJobInfo.getProperties().getProperty(HBaseConstants.PROPERTY_WRITE_TXN_KEY);
        if(txnString == null) {
            HBaseConfiguration.addHbaseResources(context.getConfiguration());
            //outputSchema should be set by HCatOutputFormat calling setSchema, prior to initialize being called
            //TODO reconcile output_revision passing to HBaseSerDeResultConverter
            //on the first call to this method hcatProperties will not contain an OUTPUT_VERSION but that doesn't
            //matter since we won't use any facilities that require that property set during that run
            converter = new HBaseSerDeResultConverter(schema,
                                                                                outputSchema,
                                                                                hcatProperties);
            RevisionManager rm = HBaseHCatStorageHandler.getOpenedRevisionManager(context.getConfiguration());
            Transaction txn = null;
            try {
                txn = rm.beginWriteTransaction(qualifiedTableName,
                                               Arrays.asList(converter.getHBaseScanColumns().split(" ")));
            } finally {
                rm.close();
            }
            outputJobInfo.getProperties()
                         .setProperty(HBaseConstants.PROPERTY_WRITE_TXN_KEY,
                                      HCatUtil.serialize(txn));
        }
        else {
            Transaction txn = (Transaction)HCatUtil.deserialize(txnString);
            converter = new HBaseSerDeResultConverter(schema,
                                                                                outputSchema,
                                                                                hcatProperties,
                                                                                txn.getRevisionNumber());
        }
    }

    @Override
    public void setSchema(JobContext jobContext, HCatSchema schema) throws IOException {
        this.outputSchema = schema;
    }

    @Override
    public WritableComparable<?> generateKey(HCatRecord value) throws IOException {
        //HBase doesn't use KEY as part of output
        return null;
    }

    @Override
    public Writable convertValue(HCatRecord value) throws IOException {
        return converter.convert(value);
    }

    @Override
    public void setPartitionValues(JobContext jobContext, Map<String, String> partitionValues) throws IOException {
        //no partitions for this driver
    }

    @Override
    public Path getWorkFilePath(TaskAttemptContext context, String outputLoc) throws IOException {
        return null;
    }

    @Override
    public void setOutputPath(JobContext jobContext, String location) throws IOException {
        //no output path
    }

    @Override
    public String getOutputLocation(JobContext jobContext, String tableLocation, List<String> partitionCols, Map<String, String> partitionValues, String dynHash) throws IOException {
        return null;
    }
}
