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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hcatalog.common.ErrorType;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * File-based storage (ie RCFile, Text, etc) implementation of OutputFormatContainer.
 * This implementation supports the following HCatalog features: partitioning, dynamic partitioning, Hadoop Archiving, etc.
 */
class FileOutputFormatContainer extends OutputFormatContainer {
    private OutputFormat<? super WritableComparable<?>, ? super Writable> of;

    private static final PathFilter hiddenFileFilter = new PathFilter(){
      public boolean accept(Path p){
        String name = p.getName();
        return !name.startsWith("_") && !name.startsWith(".");
      }
    };

    /**
     * @param of base OutputFormat to contain
     */
    public FileOutputFormatContainer(OutputFormat<? super WritableComparable<?>, ? super Writable> of) {
        super(of);
        this.of = of;
    }

    @Override
    public RecordWriter<WritableComparable<?>, HCatRecord> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new FileRecordWriterContainer(of.getRecordWriter(context),context);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        OutputJobInfo jobInfo = HCatOutputFormat.getJobInfo(context);
        try {
            handleDuplicatePublish(context,
                    jobInfo,
                    HCatOutputFormat.createHiveClient(jobInfo.getServerUri(),context.getConfiguration()),
                    jobInfo.getTableInfo().getTable());
        } catch (MetaException e) {
            throw new IOException(e);
        } catch (TException e) {
            throw new IOException(e);
        }
        of.checkOutputSpecs(context);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new FileOutputCommitterContainer(context,of.getOutputCommitter(context));
    }

    /**
     * Handles duplicate publish of partition. Fails if partition already exists.
     * For non partitioned tables, fails if files are present in table directory.
     * For dynamic partitioned publish, does nothing - check would need to be done at recordwriter time
     * @param context the job
     * @param outputInfo the output info
     * @param client the metastore client
     * @param table the table being written to
     * @throws IOException
     * @throws org.apache.hadoop.hive.metastore.api.MetaException
     * @throws org.apache.thrift.TException
     */
    private static void handleDuplicatePublish(JobContext context, OutputJobInfo outputInfo,
                                               HiveMetaStoreClient client, Table table) throws IOException, MetaException, TException {

        /*
        * For fully specified ptn, follow strict checks for existence of partitions in metadata
        * For unpartitioned tables, follow filechecks
        * For partially specified tables:
        *    This would then need filechecks at the start of a ptn write,
        *    Doing metadata checks can get potentially very expensive (fat conf) if
        *    there are a large number of partitions that match the partial specifications
        */

        if( table.getPartitionKeys().size() > 0 ) {
            if (!outputInfo.isDynamicPartitioningUsed()){
                List<String> partitionValues = getPartitionValueList(
                        table, outputInfo.getPartitionValues());
                // fully-specified partition
                List<String> currentParts = client.listPartitionNames(outputInfo.getDatabaseName(),
                        outputInfo.getTableName(), partitionValues, (short) 1);

                if( currentParts.size() > 0 ) {
                    throw new HCatException(ErrorType.ERROR_DUPLICATE_PARTITION);
                }
            }
        } else {
            List<String> partitionValues = getPartitionValueList(
                    table, outputInfo.getPartitionValues());
            // non-partitioned table

            Path tablePath = new Path(table.getSd().getLocation());
            FileSystem fs = tablePath.getFileSystem(context.getConfiguration());

            if ( fs.exists(tablePath) ) {
                FileStatus[] status = fs.globStatus(new Path(tablePath, "*"), hiddenFileFilter);

                if( status.length > 0 ) {
                    throw new HCatException(ErrorType.ERROR_NON_EMPTY_TABLE,
                            table.getDbName() + "." + table.getTableName());
                }
            }
        }
    }

    /**
     * Convert the partition value map to a value list in the partition key order.
     * @param table the table being written to
     * @param valueMap the partition value map
     * @return the partition value list
     * @throws java.io.IOException
     */
    static List<String> getPartitionValueList(Table table, Map<String, String> valueMap) throws IOException {

        if( valueMap.size() != table.getPartitionKeys().size() ) {
            throw new HCatException(ErrorType.ERROR_INVALID_PARTITION_VALUES,
                    "Table "
                            + table.getTableName() + " has " +
                            table.getPartitionKeys().size() + " partition keys, got "+
                            valueMap.size());
        }

        List<String> values = new ArrayList<String>();

        for(FieldSchema schema : table.getPartitionKeys()) {
            String value = valueMap.get(schema.getName().toLowerCase());

            if( value == null ) {
                throw new HCatException(ErrorType.ERROR_MISSING_PARTITION_KEY,
                        "Key " + schema.getName() + " of table " + table.getTableName());
            }

            values.add(value);
        }

        return values;
    }
}
