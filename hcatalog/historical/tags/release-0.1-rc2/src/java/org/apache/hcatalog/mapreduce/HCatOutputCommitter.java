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
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hcatalog.common.ErrorType;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.data.schema.HCatSchemaUtils;
import org.apache.thrift.TException;

public class HCatOutputCommitter extends OutputCommitter {

    /** The underlying output committer */
    private final OutputCommitter baseCommitter;

    public HCatOutputCommitter(OutputCommitter baseCommitter) {
        this.baseCommitter = baseCommitter;
    }

    @Override
    public void abortTask(TaskAttemptContext context) throws IOException {
        baseCommitter.abortTask(context);
    }

    @Override
    public void commitTask(TaskAttemptContext context) throws IOException {
        baseCommitter.commitTask(context);
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
        return baseCommitter.needsTaskCommit(context);
    }

    @Override
    public void setupJob(JobContext context) throws IOException {
      if( baseCommitter != null ) {
        baseCommitter.setupJob(context);
      }
    }

    @Override
    public void setupTask(TaskAttemptContext context) throws IOException {
        baseCommitter.setupTask(context);
    }

    @Override
    public void abortJob(JobContext jobContext, State state) throws IOException {
      if(baseCommitter != null) {
        baseCommitter.abortJob(jobContext, state);
      }
      OutputJobInfo jobInfo = HCatOutputFormat.getJobInfo(jobContext);

      try {
        HiveMetaStoreClient client = HCatOutputFormat.createHiveClient(
            jobInfo.getTableInfo().getServerUri(), jobContext.getConfiguration());
        // cancel the deleg. tokens that were acquired for this job now that
        // we are done - we should cancel if the tokens were acquired by
        // HCatOutputFormat and not if they were supplied by Oozie. In the latter
        // case the HCAT_KEY_TOKEN_SIGNATURE property in the conf will not be set
        String tokenStrForm = client.getTokenStrForm();
        if(tokenStrForm != null && jobContext.getConfiguration().get
            (HCatConstants.HCAT_KEY_TOKEN_SIGNATURE) != null) {
          client.cancelDelegationToken(tokenStrForm);
        }
      } catch(Exception e) {
        if( e instanceof HCatException ) {
          throw (HCatException) e;
        } else {
          throw new HCatException(ErrorType.ERROR_PUBLISHING_PARTITION, e);
        }
      }

      Path src = new Path(jobInfo.getLocation());
      FileSystem fs = src.getFileSystem(jobContext.getConfiguration());
      fs.delete(src, true);
    }

    public static final String SUCCEEDED_FILE_NAME = "_SUCCESS";
    static final String SUCCESSFUL_JOB_OUTPUT_DIR_MARKER =
      "mapreduce.fileoutputcommitter.marksuccessfuljobs";

    private static boolean getOutputDirMarking(Configuration conf) {
      return conf.getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER,
                             false);
    }

    @Override
    public void commitJob(JobContext jobContext) throws IOException {
      if(baseCommitter != null) {
        baseCommitter.commitJob(jobContext);
      }
      // create _SUCCESS FILE if so requested.
      OutputJobInfo jobInfo = HCatOutputFormat.getJobInfo(jobContext);
      if(getOutputDirMarking(jobContext.getConfiguration())) {
        Path outputPath = new Path(jobInfo.getLocation());
        if (outputPath != null) {
          FileSystem fileSys = outputPath.getFileSystem(jobContext.getConfiguration());
          // create a file in the folder to mark it
          if (fileSys.exists(outputPath)) {
            Path filePath = new Path(outputPath, SUCCEEDED_FILE_NAME);
            if(!fileSys.exists(filePath)) { // may have been created by baseCommitter.commitJob()
              fileSys.create(filePath).close();
            }
          }
        }
      }
      cleanupJob(jobContext);
    }

    @Override
    public void cleanupJob(JobContext context) throws IOException {

      OutputJobInfo jobInfo = HCatOutputFormat.getJobInfo(context);
      Configuration conf = context.getConfiguration();
      Table table = jobInfo.getTable();
      StorageDescriptor tblSD = table.getSd();
      Path tblPath = new Path(tblSD.getLocation());
      FileSystem fs = tblPath.getFileSystem(conf);

      if( table.getPartitionKeys().size() == 0 ) {
        //non partitioned table

        if( baseCommitter != null ) {
          baseCommitter.cleanupJob(context);
        }

        //Move data from temp directory the actual table directory
        //No metastore operation required.
        Path src = new Path(jobInfo.getLocation());
        moveTaskOutputs(fs, src, src, tblPath);
        fs.delete(src, true);
        return;
      }

      HiveMetaStoreClient client = null;
      List<String> values = null;
      boolean partitionAdded = false;
      HCatTableInfo tableInfo = jobInfo.getTableInfo();

      try {
        client = HCatOutputFormat.createHiveClient(tableInfo.getServerUri(), conf);

        StorerInfo storer = InitializeInput.extractStorerInfo(table.getSd(),table.getParameters());

        Partition partition = new Partition();
        partition.setDbName(tableInfo.getDatabaseName());
        partition.setTableName(tableInfo.getTableName());
        partition.setSd(new StorageDescriptor(tblSD));
        partition.getSd().setLocation(jobInfo.getLocation());

        updateTableSchema(client, table, jobInfo.getOutputSchema());

        List<FieldSchema> fields = new ArrayList<FieldSchema>();
        for(HCatFieldSchema fieldSchema : jobInfo.getOutputSchema().getFields()) {
          fields.add(HCatSchemaUtils.getFieldSchema(fieldSchema));
        }

        partition.getSd().setCols(fields);

        Map<String,String> partKVs = tableInfo.getPartitionValues();
        //Get partition value list
        partition.setValues(getPartitionValueList(table,partKVs));

        Map<String, String> params = new HashMap<String, String>();
        params.put(HCatConstants.HCAT_ISD_CLASS, storer.getInputSDClass());
        params.put(HCatConstants.HCAT_OSD_CLASS, storer.getOutputSDClass());

        //Copy table level hcat.* keys to the partition
        for(Map.Entry<Object, Object> entry : storer.getProperties().entrySet()) {
          params.put(entry.getKey().toString(), entry.getValue().toString());
        }

        partition.setParameters(params);

        // Sets permissions and group name on partition dirs.
        FileStatus tblStat = fs.getFileStatus(tblPath);
        String grpName = tblStat.getGroup();
        FsPermission perms = tblStat.getPermission();
        Path partPath = tblPath;
        for(FieldSchema partKey : table.getPartitionKeys()){
          partPath = constructPartialPartPath(partPath, partKey.getName().toLowerCase(), partKVs);
          fs.setPermission(partPath, perms);
          try{
            fs.setOwner(partPath, null, grpName);
          } catch(AccessControlException ace){
            // log the messages before ignoring. Currently, logging is not built in Hcatalog.
          }
        }

        //Publish the new partition
        client.add_partition(partition);
        partitionAdded = true; //publish to metastore done

        if( baseCommitter != null ) {
          baseCommitter.cleanupJob(context);
        }
        // cancel the deleg. tokens that were acquired for this job now that
        // we are done - we should cancel if the tokens were acquired by
        // HCatOutputFormat and not if they were supplied by Oozie. In the latter
        // case the HCAT_KEY_TOKEN_SIGNATURE property in the conf will not be set
        String tokenStrForm = client.getTokenStrForm();
        if(tokenStrForm != null && context.getConfiguration().get
            (HCatConstants.HCAT_KEY_TOKEN_SIGNATURE) != null) {
          client.cancelDelegationToken(tokenStrForm);
        }
      } catch (Exception e) {

        if( partitionAdded ) {
          try {
            //baseCommitter.cleanupJob failed, try to clean up the metastore
            client.dropPartition(tableInfo.getDatabaseName(),
                    tableInfo.getTableName(), values);
          } catch(Exception te) {
            //Keep cause as the original exception
            throw new HCatException(ErrorType.ERROR_PUBLISHING_PARTITION, e);
          }
        }

        if( e instanceof HCatException ) {
          throw (HCatException) e;
        } else {
          throw new HCatException(ErrorType.ERROR_PUBLISHING_PARTITION, e);
        }
      } finally {
        if( client != null ) {
          client.close();
        }
      }
    }

    private Path constructPartialPartPath(Path partialPath, String partKey, Map<String,String> partKVs){

      StringBuilder sb = new StringBuilder(FileUtils.escapePathName(partKey));
      sb.append("=");
      sb.append(FileUtils.escapePathName(partKVs.get(partKey)));
      return new Path(partialPath, sb.toString());
    }

    /**
     * Update table schema, adding new columns as added for the partition.
     * @param client the client
     * @param table the table
     * @param partitionSchema the schema of the partition
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws InvalidOperationException the invalid operation exception
     * @throws MetaException the meta exception
     * @throws TException the t exception
     */
    private void updateTableSchema(HiveMetaStoreClient client, Table table,
        HCatSchema partitionSchema) throws IOException, InvalidOperationException, MetaException, TException {

      List<FieldSchema> newColumns = HCatUtil.validatePartitionSchema(table, partitionSchema);

      if( newColumns.size() != 0 ) {
        List<FieldSchema> tableColumns = new ArrayList<FieldSchema>(table.getSd().getCols());
        tableColumns.addAll(newColumns);

        //Update table schema to add the newly added columns
        table.getSd().setCols(tableColumns);
        client.alter_table(table.getDbName(), table.getTableName(), table);
      }
    }

    /**
     * Convert the partition value map to a value list in the partition key order.
     * @param table the table being written to
     * @param valueMap the partition value map
     * @return the partition value list
     * @throws IOException
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

    /**
     * Move all of the files from the temp directory to the final location
     * @param fs the output file system
     * @param file the file to move
     * @param src the source directory
     * @param dest the target directory
     * @throws IOException
     */
    private void moveTaskOutputs(FileSystem fs,
                                 Path file,
                                 Path src,
                                 Path dest) throws IOException {
      if (fs.isFile(file)) {
        Path finalOutputPath = getFinalPath(file, src, dest);

        if (!fs.rename(file, finalOutputPath)) {
          if (!fs.delete(finalOutputPath, true)) {
            throw new HCatException(ErrorType.ERROR_MOVE_FAILED, "Failed to delete existing path " + finalOutputPath);
          }
          if (!fs.rename(file, finalOutputPath)) {
            throw new HCatException(ErrorType.ERROR_MOVE_FAILED, "Failed to move output to " + dest);
          }
        }
      } else if(fs.getFileStatus(file).isDir()) {
        FileStatus[] paths = fs.listStatus(file);
        Path finalOutputPath = getFinalPath(file, src, dest);
        fs.mkdirs(finalOutputPath);

        if (paths != null) {
          for (FileStatus path : paths) {
            moveTaskOutputs(fs, path.getPath(), src, dest);
          }
        }
      }
    }

    /**
     * Find the final name of a given output file, given the output directory
     * and the work directory.
     * @param file the file to move
     * @param src the source directory
     * @param dest the target directory
     * @return the final path for the specific output file
     * @throws IOException
     */
    private Path getFinalPath(Path file, Path src,
                              Path dest) throws IOException {
      URI taskOutputUri = file.toUri();
      URI relativePath = src.toUri().relativize(taskOutputUri);
      if (taskOutputUri == relativePath) {
        throw new HCatException(ErrorType.ERROR_MOVE_FAILED, "Can not get the relative path: base = " +
            src + " child = " + file);
      }
      if (relativePath.getPath().length() > 0) {
        return new Path(dest, relativePath.getPath());
      } else {
        return dest;
      }
    }

}
