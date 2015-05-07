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

package org.apache.hive.hcatalog.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.security.Credentials;
import org.apache.hive.hcatalog.common.ErrorType;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The OutputFormat to use to write data to HCatalog. The key value is ignored and
 *  should be given as null. The value is the HCatRecord to write.*/
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HCatOutputFormat extends HCatBaseOutputFormat {

  static final private Logger LOG = LoggerFactory.getLogger(HCatOutputFormat.class);

  private static int maxDynamicPartitions;
  private static boolean harRequested;

  /**
   * @see org.apache.hive.hcatalog.mapreduce.HCatOutputFormat#setOutput(org.apache.hadoop.conf.Configuration, Credentials, OutputJobInfo)
   */
  public static void setOutput(Job job, OutputJobInfo outputJobInfo) throws IOException {
    setOutput(job.getConfiguration(), job.getCredentials(), outputJobInfo);
  }

  /**
   * Set the information about the output to write for the job. This queries the metadata server
   * to find the StorageHandler to use for the table.  It throws an error if the
   * partition is already published.
   * @param conf the Configuration object
   * @param credentials the Credentials object
   * @param outputJobInfo the table output information for the job
   * @throws IOException the exception in communicating with the metadata server
   */
  @SuppressWarnings("unchecked")
  public static void setOutput(Configuration conf, Credentials credentials,
                 OutputJobInfo outputJobInfo) throws IOException {
    IMetaStoreClient client = null;

    try {

      HiveConf hiveConf = HCatUtil.getHiveConf(conf);
      client = HCatUtil.getHiveMetastoreClient(hiveConf);
      Table table = HCatUtil.getTable(client, outputJobInfo.getDatabaseName(),
        outputJobInfo.getTableName());

      List<String> indexList = client.listIndexNames(outputJobInfo.getDatabaseName(), outputJobInfo.getTableName(), Short.MAX_VALUE);

      for (String indexName : indexList) {
        Index index = client.getIndex(outputJobInfo.getDatabaseName(), outputJobInfo.getTableName(), indexName);
        if (!index.isDeferredRebuild()) {
          throw new HCatException(ErrorType.ERROR_NOT_SUPPORTED, "Store into a table with an automatic index from Pig/Mapreduce is not supported");
        }
      }
      StorageDescriptor sd = table.getTTable().getSd();

      if (sd.isCompressed()) {
        throw new HCatException(ErrorType.ERROR_NOT_SUPPORTED, "Store into a compressed partition from Pig/Mapreduce is not supported");
      }

      if (sd.getBucketCols() != null && !sd.getBucketCols().isEmpty()) {
        throw new HCatException(ErrorType.ERROR_NOT_SUPPORTED, "Store into a partition with bucket definition from Pig/Mapreduce is not supported");
      }

      if (sd.getSortCols() != null && !sd.getSortCols().isEmpty()) {
        throw new HCatException(ErrorType.ERROR_NOT_SUPPORTED, "Store into a partition with sorted column definition from Pig/Mapreduce is not supported");
      }

      // Set up a common id hash for this job, so that when we create any temporary directory
      // later on, it is guaranteed to be unique.
      String idHash;
      if ((idHash = conf.get(HCatConstants.HCAT_OUTPUT_ID_HASH)) == null) {
        idHash = String.valueOf(Math.random());
      }
      conf.set(HCatConstants.HCAT_OUTPUT_ID_HASH,idHash);

      if (table.getTTable().getPartitionKeysSize() == 0) {
        if ((outputJobInfo.getPartitionValues() != null) && (!outputJobInfo.getPartitionValues().isEmpty())) {
          // attempt made to save partition values in non-partitioned table - throw error.
          throw new HCatException(ErrorType.ERROR_INVALID_PARTITION_VALUES,
            "Partition values specified for non-partitioned table");
        }
        // non-partitioned table
        outputJobInfo.setPartitionValues(new HashMap<String, String>());

      } else {
        // partitioned table, we expect partition values
        // convert user specified map to have lower case key names
        Map<String, String> valueMap = new HashMap<String, String>();
        if (outputJobInfo.getPartitionValues() != null) {
          for (Map.Entry<String, String> entry : outputJobInfo.getPartitionValues().entrySet()) {
            valueMap.put(entry.getKey().toLowerCase(), entry.getValue());
          }
        }

        if ((outputJobInfo.getPartitionValues() == null)
          || (outputJobInfo.getPartitionValues().size() < table.getTTable().getPartitionKeysSize())) {
          // dynamic partition usecase - partition values were null, or not all were specified
          // need to figure out which keys are not specified.
          List<String> dynamicPartitioningKeys = new ArrayList<String>();
          boolean firstItem = true;
          for (FieldSchema fs : table.getPartitionKeys()) {
            if (!valueMap.containsKey(fs.getName().toLowerCase())) {
              dynamicPartitioningKeys.add(fs.getName().toLowerCase());
            }
          }

          if (valueMap.size() + dynamicPartitioningKeys.size() != table.getTTable().getPartitionKeysSize()) {
            // If this isn't equal, then bogus key values have been inserted, error out.
            throw new HCatException(ErrorType.ERROR_INVALID_PARTITION_VALUES, "Invalid partition keys specified");
          }

          outputJobInfo.setDynamicPartitioningKeys(dynamicPartitioningKeys);
          String dynHash;
          if ((dynHash = conf.get(HCatConstants.HCAT_DYNAMIC_PTN_JOBID)) == null) {
            dynHash = String.valueOf(Math.random());
          }
          conf.set(HCatConstants.HCAT_DYNAMIC_PTN_JOBID, dynHash);

          // if custom pattern is set in case of dynamic partitioning, configure custom path
          String customPattern = conf.get(HCatConstants.HCAT_DYNAMIC_CUSTOM_PATTERN);
          if (customPattern != null) {
            HCatFileUtil.setCustomPath(customPattern, outputJobInfo);
          }
        }

        outputJobInfo.setPartitionValues(valueMap);
      }

      // To get around hbase failure on single node, see BUG-4383
      conf.set("dfs.client.read.shortcircuit", "false");
      HCatSchema tableSchema = HCatUtil.extractSchema(table);
      StorerInfo storerInfo =
        InternalUtil.extractStorerInfo(table.getTTable().getSd(), table.getParameters());

      List<String> partitionCols = new ArrayList<String>();
      for (FieldSchema schema : table.getPartitionKeys()) {
        partitionCols.add(schema.getName());
      }

      HiveStorageHandler storageHandler = HCatUtil.getStorageHandler(conf, storerInfo);

      //Serialize the output info into the configuration
      outputJobInfo.setTableInfo(HCatTableInfo.valueOf(table.getTTable()));
      outputJobInfo.setOutputSchema(tableSchema);
      harRequested = getHarRequested(hiveConf);
      outputJobInfo.setHarRequested(harRequested);
      maxDynamicPartitions = getMaxDynamicPartitions(hiveConf);
      outputJobInfo.setMaximumDynamicPartitions(maxDynamicPartitions);

      HCatUtil.configureOutputStorageHandler(storageHandler, conf, outputJobInfo);

      Path tblPath = new Path(table.getTTable().getSd().getLocation());

      /*  Set the umask in conf such that files/dirs get created with table-dir
      * permissions. Following three assumptions are made:
      * 1. Actual files/dirs creation is done by RecordWriter of underlying
      * output format. It is assumed that they use default permissions while creation.
      * 2. Default Permissions = FsPermission.getDefault() = 777.
      * 3. UMask is honored by underlying filesystem.
      */

      FsPermission.setUMask(conf, FsPermission.getDefault().applyUMask(
        tblPath.getFileSystem(conf).getFileStatus(tblPath).getPermission()));

      if (Security.getInstance().isSecurityEnabled()) {
        Security.getInstance().handleSecurity(credentials, outputJobInfo, client, conf, harRequested);
      }
    } catch (Exception e) {
      if (e instanceof HCatException) {
        throw (HCatException) e;
      } else {
        throw new HCatException(ErrorType.ERROR_SET_OUTPUT, e);
      }
    } finally {
      HCatUtil.closeHiveClientQuietly(client);
    }
  }

  /**
   * @see org.apache.hive.hcatalog.mapreduce.HCatOutputFormat#setSchema(org.apache.hadoop.conf.Configuration, org.apache.hive.hcatalog.data.schema.HCatSchema)
   */
  public static void setSchema(final Job job, final HCatSchema schema) throws IOException {
    setSchema(job.getConfiguration(), schema);
  }

  /**
   * Set the schema for the data being written out to the partition. The
   * table schema is used by default for the partition if this is not called.
   * @param conf the job Configuration object
   * @param schema the schema for the data
   * @throws IOException
   */
  public static void setSchema(final Configuration conf, final HCatSchema schema) throws IOException {
    OutputJobInfo jobInfo = getJobInfo(conf);
    Map<String, String> partMap = jobInfo.getPartitionValues();
    setPartDetails(jobInfo, schema, partMap);
    conf.set(HCatConstants.HCAT_KEY_OUTPUT_INFO, HCatUtil.serialize(jobInfo));
  }

  /**
   * Get the record writer for the job. This uses the StorageHandler's default 
   * OutputFormat to get the record writer.
   * @param context the information about the current task
   * @return a RecordWriter to write the output for the job
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public RecordWriter<WritableComparable<?>, HCatRecord>
  getRecordWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return getOutputFormat(context).getRecordWriter(context);
  }


  /**
   * Get the output committer for this output format. This is responsible
   * for ensuring the output is committed correctly.
   * @param context the task context
   * @return an output committer
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context
  ) throws IOException, InterruptedException {
    return getOutputFormat(context).getOutputCommitter(context);
  }

  private static int getMaxDynamicPartitions(HiveConf hConf) {
    // by default the bounds checking for maximum number of
    // dynamic partitions is disabled (-1)
    int maxDynamicPartitions = -1;

    if (HCatConstants.HCAT_IS_DYNAMIC_MAX_PTN_CHECK_ENABLED) {
      maxDynamicPartitions = hConf.getIntVar(
        HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS);
    }

    return maxDynamicPartitions;
  }

  private static boolean getHarRequested(HiveConf hConf) {
    return hConf.getBoolVar(HiveConf.ConfVars.HIVEARCHIVEENABLED);
  }

}
