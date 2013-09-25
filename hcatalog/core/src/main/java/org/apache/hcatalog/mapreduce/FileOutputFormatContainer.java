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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hcatalog.common.ErrorType;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * File-based storage (ie RCFile, Text, etc) implementation of OutputFormatContainer.
 * This implementation supports the following HCatalog features: partitioning, dynamic partitioning, Hadoop Archiving, etc.
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.mapreduce.FileOutputFormatContainer} instead
 */
class FileOutputFormatContainer extends OutputFormatContainer {

  private static final PathFilter hiddenFileFilter = new PathFilter() {
    public boolean accept(Path p) {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  /**
   * @param of base OutputFormat to contain
   */
  public FileOutputFormatContainer(org.apache.hadoop.mapred.OutputFormat<? super WritableComparable<?>, ? super Writable> of) {
    super(of);
  }

  @Override
  public RecordWriter<WritableComparable<?>, HCatRecord> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    //this needs to be manually set, under normal circumstances MR Task does this
    setWorkOutputPath(context);

    //Configure the output key and value classes.
    // This is required for writing null as key for file based tables.
    context.getConfiguration().set("mapred.output.key.class",
      NullWritable.class.getName());
    String jobInfoString = context.getConfiguration().get(
      HCatConstants.HCAT_KEY_OUTPUT_INFO);
    OutputJobInfo jobInfo = (OutputJobInfo) HCatUtil
      .deserialize(jobInfoString);
    StorerInfo storeInfo = jobInfo.getTableInfo().getStorerInfo();
    HCatStorageHandler storageHandler = HCatUtil.getStorageHandler(
      context.getConfiguration(), storeInfo);
    Class<? extends SerDe> serde = storageHandler.getSerDeClass();
    SerDe sd = (SerDe) ReflectionUtils.newInstance(serde,
      context.getConfiguration());
    context.getConfiguration().set("mapred.output.value.class",
      sd.getSerializedClass().getName());

    RecordWriter<WritableComparable<?>, HCatRecord> rw;
    if (HCatBaseOutputFormat.getJobInfo(context).isDynamicPartitioningUsed()){
      // When Dynamic partitioning is used, the RecordWriter instance initialized here isn't used. Can use null.
      // (That's because records can't be written until the values of the dynamic partitions are deduced.
      // By that time, a new local instance of RecordWriter, with the correct output-path, will be constructed.)
      rw = new FileRecordWriterContainer((org.apache.hadoop.mapred.RecordWriter)null,context);
    } else {
      Path parentDir = new Path(context.getConfiguration().get("mapred.work.output.dir"));
      Path childPath = new Path(parentDir,FileOutputFormat.getUniqueName(new JobConf(context.getConfiguration()), "part"));

      rw = new FileRecordWriterContainer(
            getBaseOutputFormat().getRecordWriter(
                parentDir.getFileSystem(context.getConfiguration()),
                new JobConf(context.getConfiguration()),
                childPath.toString(),
                InternalUtil.createReporter(context)),
            context);
    }
    return rw;
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    OutputJobInfo jobInfo = HCatOutputFormat.getJobInfo(context);
    HiveMetaStoreClient client = null;
    try {
      HiveConf hiveConf = HCatUtil.getHiveConf(context.getConfiguration());
      client = HCatUtil.getHiveClient(hiveConf);
      handleDuplicatePublish(context,
        jobInfo,
        client,
        new Table(jobInfo.getTableInfo().getTable()));
    } catch (MetaException e) {
      throw new IOException(e);
    } catch (TException e) {
      throw new IOException(e);
    } finally {
      HCatUtil.closeHiveClientQuietly(client);
    }

    if (!jobInfo.isDynamicPartitioningUsed()) {
      JobConf jobConf = new JobConf(context.getConfiguration());
      getBaseOutputFormat().checkOutputSpecs(null, jobConf);
      //checkoutputspecs might've set some properties we need to have context reflect that
      HCatUtil.copyConf(jobConf, context.getConfiguration());
    }
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
    //this needs to be manually set, under normal circumstances MR Task does this
    setWorkOutputPath(context);
    return new FileOutputCommitterContainer(context,
      HCatBaseOutputFormat.getJobInfo(context).isDynamicPartitioningUsed() ?
        null :
        new JobConf(context.getConfiguration()).getOutputCommitter());
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
                         HiveMetaStoreClient client, Table table) throws IOException, MetaException, TException, NoSuchObjectException {

    /*
    * For fully specified ptn, follow strict checks for existence of partitions in metadata
    * For unpartitioned tables, follow filechecks
    * For partially specified tables:
    *    This would then need filechecks at the start of a ptn write,
    *    Doing metadata checks can get potentially very expensive (fat conf) if
    *    there are a large number of partitions that match the partial specifications
    */

    if (table.getPartitionKeys().size() > 0) {
      if (!outputInfo.isDynamicPartitioningUsed()) {
        List<String> partitionValues = getPartitionValueList(
          table, outputInfo.getPartitionValues());
        // fully-specified partition
        List<String> currentParts = client.listPartitionNames(outputInfo.getDatabaseName(),
          outputInfo.getTableName(), partitionValues, (short) 1);

        if (currentParts.size() > 0) {
          throw new HCatException(ErrorType.ERROR_DUPLICATE_PARTITION);
        }
      }
    } else {
      List<String> partitionValues = getPartitionValueList(
        table, outputInfo.getPartitionValues());
      // non-partitioned table

      Path tablePath = new Path(table.getTTable().getSd().getLocation());
      FileSystem fs = tablePath.getFileSystem(context.getConfiguration());

      if (fs.exists(tablePath)) {
        FileStatus[] status = fs.globStatus(new Path(tablePath, "*"), hiddenFileFilter);

        if (status.length > 0) {
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

    if (valueMap.size() != table.getPartitionKeys().size()) {
      throw new HCatException(ErrorType.ERROR_INVALID_PARTITION_VALUES,
        "Table "
          + table.getTableName() + " has " +
          table.getPartitionKeys().size() + " partition keys, got " +
          valueMap.size());
    }

    List<String> values = new ArrayList<String>();

    for (FieldSchema schema : table.getPartitionKeys()) {
      String value = valueMap.get(schema.getName().toLowerCase());

      if (value == null) {
        throw new HCatException(ErrorType.ERROR_MISSING_PARTITION_KEY,
          "Key " + schema.getName() + " of table " + table.getTableName());
      }

      values.add(value);
    }

    return values;
  }

  static void setWorkOutputPath(TaskAttemptContext context) throws IOException {
    String outputPath = context.getConfiguration().get("mapred.output.dir");
    //we need to do this to get the task path and set it for mapred implementation
    //since it can't be done automatically because of mapreduce->mapred abstraction
    if (outputPath != null)
      context.getConfiguration().set("mapred.work.output.dir",
        new FileOutputCommitter(new Path(outputPath), context).getWorkPath().toString());
  }
}
