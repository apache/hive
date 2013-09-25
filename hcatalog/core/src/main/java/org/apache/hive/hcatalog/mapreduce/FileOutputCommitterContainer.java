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
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.hcatalog.common.ErrorType;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.apache.hive.hcatalog.har.HarOutputCommitterPostProcessor;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Part of the FileOutput*Container classes
 * See {@link FileOutputFormatContainer} for more information
 */
class FileOutputCommitterContainer extends OutputCommitterContainer {

  private static final String TEMP_DIR_NAME = "_temporary";
  private static final String LOGS_DIR_NAME = "_logs";

  private static final Logger LOG = LoggerFactory.getLogger(FileOutputCommitterContainer.class);
  private final boolean dynamicPartitioningUsed;
  private boolean partitionsDiscovered;

  private Map<String, Map<String, String>> partitionsDiscoveredByPath;
  private Map<String, JobContext> contextDiscoveredByPath;
  private final HiveStorageHandler cachedStorageHandler;

  HarOutputCommitterPostProcessor harProcessor = new HarOutputCommitterPostProcessor();

  private String ptnRootLocation = null;

  private OutputJobInfo jobInfo = null;

  /**
   * @param context current JobContext
   * @param baseCommitter OutputCommitter to contain
   * @throws IOException
   */
  public FileOutputCommitterContainer(JobContext context,
                    org.apache.hadoop.mapred.OutputCommitter baseCommitter) throws IOException {
    super(context, baseCommitter);
    jobInfo = HCatOutputFormat.getJobInfo(context);
    dynamicPartitioningUsed = jobInfo.isDynamicPartitioningUsed();

    this.partitionsDiscovered = !dynamicPartitioningUsed;
    cachedStorageHandler = HCatUtil.getStorageHandler(context.getConfiguration(), jobInfo.getTableInfo().getStorerInfo());
  }

  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    if (!dynamicPartitioningUsed) {
      getBaseOutputCommitter().abortTask(HCatMapRedUtil.createTaskAttemptContext(context));
    }
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    if (!dynamicPartitioningUsed) {
         //See HCATALOG-499
      FileOutputFormatContainer.setWorkOutputPath(context);
      getBaseOutputCommitter().commitTask(HCatMapRedUtil.createTaskAttemptContext(context));
    }
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
    if (!dynamicPartitioningUsed) {
      return getBaseOutputCommitter().needsTaskCommit(HCatMapRedUtil.createTaskAttemptContext(context));
    } else {
      // called explicitly through FileRecordWriterContainer.close() if dynamic - return false by default
      return false;
    }
  }

  @Override
  public void setupJob(JobContext context) throws IOException {
    if (getBaseOutputCommitter() != null && !dynamicPartitioningUsed) {
      getBaseOutputCommitter().setupJob(HCatMapRedUtil.createJobContext(context));
    }
    // in dynamic usecase, called through FileRecordWriterContainer
  }

  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    if (!dynamicPartitioningUsed) {
      getBaseOutputCommitter().setupTask(HCatMapRedUtil.createTaskAttemptContext(context));
    }
  }

  @Override
  public void abortJob(JobContext jobContext, State state) throws IOException {
    try {
      if (dynamicPartitioningUsed) {
        discoverPartitions(jobContext);
      }
      org.apache.hadoop.mapred.JobContext mapRedJobContext = HCatMapRedUtil
          .createJobContext(jobContext);
      if (getBaseOutputCommitter() != null && !dynamicPartitioningUsed) {
        getBaseOutputCommitter().abortJob(mapRedJobContext, state);
      } else if (dynamicPartitioningUsed) {
        for (JobContext currContext : contextDiscoveredByPath.values()) {
          try {
            new JobConf(currContext.getConfiguration())
                .getOutputCommitter().abortJob(currContext,
                    state);
          } catch (Exception e) {
            throw new IOException(e);
          }
        }
      }
      Path src;
      OutputJobInfo jobInfo = HCatOutputFormat.getJobInfo(jobContext);
      if (dynamicPartitioningUsed) {
        src = new Path(getPartitionRootLocation(jobInfo.getLocation(), jobInfo.getTableInfo().getTable()
            .getPartitionKeysSize()));
      } else {
        src = new Path(jobInfo.getLocation());
      }
      FileSystem fs = src.getFileSystem(jobContext.getConfiguration());
      // Note fs.delete will fail on Windows. The reason is in OutputCommitter,
      // Hadoop is still writing to _logs/history. On Linux, OS don't care file is still
      // open and remove the directory anyway, but on Windows, OS refuse to remove a
      // directory containing open files. So on Windows, we will leave output directory
      // behind when job fail. User needs to remove the output directory manually
      LOG.info("Job failed. Try cleaning up temporary directory [{}].", src);
      fs.delete(src, true);
    } finally {
      cancelDelegationTokens(jobContext);
    }
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
    try {
      if (dynamicPartitioningUsed) {
        discoverPartitions(jobContext);
        // Commit each partition so it gets moved out of the job work
        // dir
        for (JobContext context : contextDiscoveredByPath.values()) {
          new JobConf(context.getConfiguration())
              .getOutputCommitter().commitJob(context);
        }
      }
      if (getBaseOutputCommitter() != null && !dynamicPartitioningUsed) {
        getBaseOutputCommitter().commitJob(
            HCatMapRedUtil.createJobContext(jobContext));
      }
      registerPartitions(jobContext);
      // create _SUCCESS FILE if so requested.
      OutputJobInfo jobInfo = HCatOutputFormat.getJobInfo(jobContext);
      if (getOutputDirMarking(jobContext.getConfiguration())) {
        Path outputPath = new Path(jobInfo.getLocation());
        FileSystem fileSys = outputPath.getFileSystem(jobContext
            .getConfiguration());
        // create a file in the folder to mark it
        if (fileSys.exists(outputPath)) {
          Path filePath = new Path(outputPath,
              SUCCEEDED_FILE_NAME);
          if (!fileSys.exists(filePath)) { // may have been
                           // created by
                           // baseCommitter.commitJob()
            fileSys.create(filePath).close();
          }
        }
      }
    } finally {
      cancelDelegationTokens(jobContext);
    }
  }

  @Override
  public void cleanupJob(JobContext context) throws IOException {
    throw new IOException("The method cleanupJob is deprecated and should not be called.");
  }

  private String getPartitionRootLocation(String ptnLocn, int numPtnKeys) {
    if (ptnRootLocation == null) {
      // we only need to calculate it once, it'll be the same for other partitions in this job.
      Path ptnRoot = new Path(ptnLocn);
      for (int i = 0; i < numPtnKeys; i++) {
//          LOG.info("Getting parent of "+ptnRoot.getName());
        ptnRoot = ptnRoot.getParent();
      }
      ptnRootLocation = ptnRoot.toString();
    }
//      LOG.info("Returning final parent : "+ptnRootLocation);
    return ptnRootLocation;
  }

  /**
   * Generate partition metadata object to be used to add to metadata.
   * @param context The job context.
   * @param jobInfo The OutputJobInfo.
   * @param partLocnRoot The table-equivalent location root of the partition
   *                       (temporary dir if dynamic partition, table dir if static)
   * @param partKVs The keyvalue pairs that form the partition
   * @param outputSchema The output schema for the partition
   * @param params The parameters to store inside the partition
   * @param table The Table metadata object under which this Partition will reside
   * @param fs FileSystem object to operate on the underlying filesystem
   * @param grpName Group name that owns the table dir
   * @param perms FsPermission that's the default permission of the table dir.
   * @return Constructed Partition metadata object
   * @throws java.io.IOException
   */

  private Partition constructPartition(
    JobContext context, OutputJobInfo jobInfo,
    String partLocnRoot, Map<String, String> partKVs,
    HCatSchema outputSchema, Map<String, String> params,
    Table table, FileSystem fs,
    String grpName, FsPermission perms) throws IOException {

    Partition partition = new Partition();
    partition.setDbName(table.getDbName());
    partition.setTableName(table.getTableName());
    partition.setSd(new StorageDescriptor(table.getTTable().getSd()));

    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    for (HCatFieldSchema fieldSchema : outputSchema.getFields()) {
      fields.add(HCatSchemaUtils.getFieldSchema(fieldSchema));
    }

    partition.getSd().setCols(fields);

    partition.setValues(FileOutputFormatContainer.getPartitionValueList(table, partKVs));

    partition.setParameters(params);

    // Sets permissions and group name on partition dirs and files.

    Path partPath;
    if (Boolean.valueOf((String)table.getProperty("EXTERNAL"))
         && jobInfo.getLocation() != null && jobInfo.getLocation().length() > 0) {
      // honor external table that specifies the location
      partPath = new Path(jobInfo.getLocation());
    } else {
      partPath = new Path(partLocnRoot);
      int i = 0;
      for (FieldSchema partKey : table.getPartitionKeys()) {
        if (i++ != 0) {
          applyGroupAndPerms(fs, partPath, perms, grpName, false);
        }
        partPath = constructPartialPartPath(partPath, partKey.getName().toLowerCase(), partKVs);
      }
    }

    // Apply the group and permissions to the leaf partition and files.
    // Need not bother in case of HDFS as permission is taken care of by setting UMask
    if (!ShimLoader.getHadoopShims().getHCatShim().isFileInHDFS(fs, partPath)) {
      applyGroupAndPerms(fs, partPath, perms, grpName, true);
    }

    // Set the location in the StorageDescriptor
    if (dynamicPartitioningUsed) {
      String dynamicPartitionDestination = getFinalDynamicPartitionDestination(table, partKVs);
      if (harProcessor.isEnabled()) {
        harProcessor.exec(context, partition, partPath);
        partition.getSd().setLocation(
          harProcessor.getProcessedLocation(new Path(dynamicPartitionDestination)));
      } else {
        partition.getSd().setLocation(dynamicPartitionDestination);
      }
    } else {
      partition.getSd().setLocation(partPath.toString());
    }
    return partition;
  }

  private void applyGroupAndPerms(FileSystem fs, Path dir, FsPermission permission,
                  String group, boolean recursive)
    throws IOException {
    fs.setPermission(dir, permission);
    if (recursive) {
      for (FileStatus fileStatus : fs.listStatus(dir)) {
        if (fileStatus.isDir()) {
          applyGroupAndPerms(fs, fileStatus.getPath(), permission, group, true);
        } else {
          fs.setPermission(fileStatus.getPath(), permission);
        }
      }
    }
  }

  private String getFinalDynamicPartitionDestination(Table table, Map<String, String> partKVs) {
    // file:///tmp/hcat_junit_warehouse/employee/_DYN0.7770480401313761/emp_country=IN/emp_state=KA  ->
    // file:///tmp/hcat_junit_warehouse/employee/emp_country=IN/emp_state=KA
    Path partPath = new Path(table.getTTable().getSd().getLocation());
    for (FieldSchema partKey : table.getPartitionKeys()) {
      partPath = constructPartialPartPath(partPath, partKey.getName().toLowerCase(), partKVs);
    }
    return partPath.toString();
  }

  private Map<String, String> getStorerParameterMap(StorerInfo storer) {
    Map<String, String> params = new HashMap<String, String>();

    //Copy table level hcat.* keys to the partition
    for (Entry<Object, Object> entry : storer.getProperties().entrySet()) {
      params.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return params;
  }

  private Path constructPartialPartPath(Path partialPath, String partKey, Map<String, String> partKVs) {

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
   * @throws java.io.IOException Signals that an I/O exception has occurred.
   * @throws org.apache.hadoop.hive.metastore.api.InvalidOperationException the invalid operation exception
   * @throws org.apache.hadoop.hive.metastore.api.MetaException the meta exception
   * @throws org.apache.thrift.TException the t exception
   */
  private void updateTableSchema(HiveMetaStoreClient client, Table table,
                   HCatSchema partitionSchema) throws IOException, InvalidOperationException, MetaException, TException {


    List<FieldSchema> newColumns = HCatUtil.validatePartitionSchema(table, partitionSchema);

    if (newColumns.size() != 0) {
      List<FieldSchema> tableColumns = new ArrayList<FieldSchema>(table.getTTable().getSd().getCols());
      tableColumns.addAll(newColumns);

      //Update table schema to add the newly added columns
      table.getTTable().getSd().setCols(tableColumns);
      client.alter_table(table.getDbName(), table.getTableName(), table.getTTable());
    }
  }

  /**
   * Move all of the files from the temp directory to the final location
   * @param fs the output file system
   * @param file the file to move
   * @param srcDir the source directory
   * @param destDir the target directory
   * @param dryRun - a flag that simply tests if this move would succeed or not based
   *                 on whether other files exist where we're trying to copy
   * @throws java.io.IOException
   */
  private void moveTaskOutputs(FileSystem fs,
                 Path file,
                 Path srcDir,
                 Path destDir, final boolean dryRun) throws IOException {

    if (file.getName().equals(TEMP_DIR_NAME) || file.getName().equals(LOGS_DIR_NAME) || file.getName().equals(SUCCEEDED_FILE_NAME)) {
      return;
    }
    final Path finalOutputPath = getFinalPath(file, srcDir, destDir);
    if (fs.isFile(file)) {
      if (dryRun){
        if(LOG.isDebugEnabled()) {
          LOG.debug("Testing if moving file: [" + file + "] to ["
              + finalOutputPath + "] would cause a problem");
        }
        if (fs.exists(finalOutputPath)) {
          throw new HCatException(ErrorType.ERROR_MOVE_FAILED, "Data already exists in " + finalOutputPath
              + ", duplicate publish not possible.");
        }
      } else {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Moving file: [ " + file + "] to [" + finalOutputPath + "]");
        }
        // Make sure the parent directory exists.  It is not an error
        // to recreate an existing directory
        fs.mkdirs(finalOutputPath.getParent());
        if (!fs.rename(file, finalOutputPath)) {
          if (!fs.delete(finalOutputPath, true)) {
            throw new HCatException(ErrorType.ERROR_MOVE_FAILED, "Failed to delete existing path " + finalOutputPath);
          }
          if (!fs.rename(file, finalOutputPath)) {
            throw new HCatException(ErrorType.ERROR_MOVE_FAILED, "Failed to move output to " + finalOutputPath);
          }
        }
      }
    } else if(fs.getFileStatus(file).isDir()) {
      FileStatus[] children = fs.listStatus(file);
      FileStatus firstChild = null;
      if (children != null) {
        int index=0;
        while (index < children.length) {
          if (!children[index].getPath().getName().equals(TEMP_DIR_NAME) && !children[index].getPath().getName().equals(LOGS_DIR_NAME) && !children[index].getPath().getName().equals(SUCCEEDED_FILE_NAME)) {
            firstChild = children[index];
            break;
          }
          index++;
        }
      }
      if(firstChild!=null && firstChild.isDir()) {
        // If the first child is directory, then rest would be directory too according to HCatalog dir structure
        // recurse in that case
        for (FileStatus child : children) {
          moveTaskOutputs(fs, child.getPath(), srcDir, destDir, dryRun);
        }
      } else {

        if (!dryRun) {
          if (dynamicPartitioningUsed) {
            // Optimization: if the first child is file, we have reached the leaf directory, move the parent directory itself
            // instead of moving each file under the directory. See HCATALOG-538

            final Path parentDir = finalOutputPath.getParent();
            // Create the directory
            Path placeholder = new Path(parentDir, "_placeholder");
            if (fs.mkdirs(parentDir)) {
              // It is weired but we need a placeholder, 
              // otherwise rename cannot move file to the right place
              fs.create(placeholder).close();
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug("Moving directory: " + file + " to " + parentDir);
            }
            if (!fs.rename(file, parentDir)) {
              final String msg = "Failed to move file: " + file + " to " + parentDir;
              LOG.error(msg);
              throw new HCatException(ErrorType.ERROR_MOVE_FAILED, msg);
            }
            fs.delete(placeholder, false);
          } else {
            // In case of no partition we have to move each file
            for (FileStatus child : children) {
              moveTaskOutputs(fs, child.getPath(), srcDir, destDir, dryRun);
            }
          }
        } else {
          if(fs.exists(finalOutputPath)) {
            throw new HCatException(ErrorType.ERROR_MOVE_FAILED, "Data already exists in " + finalOutputPath
                + ", duplicate publish not possible.");
          }
        }
      }
    } else {
      // Should never happen
      final String msg = "Unknown file type being asked to be moved, erroring out";
      throw new HCatException(ErrorType.ERROR_MOVE_FAILED, msg);
    }
  }

  /**
   * Find the final name of a given output file, given the output directory
   * and the work directory.
   * @param file the file to move
   * @param src the source directory
   * @param dest the target directory
   * @return the final path for the specific output file
   * @throws java.io.IOException
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

  /**
   * Run to discover dynamic partitions available
   */
  private void discoverPartitions(JobContext context) throws IOException {
    if (!partitionsDiscovered) {
      //      LOG.info("discover ptns called");
      OutputJobInfo jobInfo = HCatOutputFormat.getJobInfo(context);

      harProcessor.setEnabled(jobInfo.getHarRequested());

      List<Integer> dynamicPartCols = jobInfo.getPosOfDynPartCols();
      int maxDynamicPartitions = jobInfo.getMaxDynamicPartitions();

      Path loadPath = new Path(jobInfo.getLocation());
      FileSystem fs = loadPath.getFileSystem(context.getConfiguration());

      // construct a path pattern (e.g., /*/*) to find all dynamically generated paths
      String dynPathSpec = loadPath.toUri().getPath();
      dynPathSpec = dynPathSpec.replaceAll("__HIVE_DEFAULT_PARTITION__", "*");

      //      LOG.info("Searching for "+dynPathSpec);
      Path pathPattern = new Path(dynPathSpec);
      FileStatus[] status = fs.globStatus(pathPattern);

      partitionsDiscoveredByPath = new LinkedHashMap<String, Map<String, String>>();
      contextDiscoveredByPath = new LinkedHashMap<String, JobContext>();


      if (status.length == 0) {
        //        LOG.warn("No partition found genereated by dynamic partitioning in ["
        //            +loadPath+"] with depth["+jobInfo.getTable().getPartitionKeysSize()
        //            +"], dynSpec["+dynPathSpec+"]");
      } else {
        if ((maxDynamicPartitions != -1) && (status.length > maxDynamicPartitions)) {
          this.partitionsDiscovered = true;
          throw new HCatException(ErrorType.ERROR_TOO_MANY_DYNAMIC_PTNS,
            "Number of dynamic partitions being created "
              + "exceeds configured max allowable partitions["
              + maxDynamicPartitions
              + "], increase parameter ["
              + HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS.varname
              + "] if needed.");
        }

        for (FileStatus st : status) {
          LinkedHashMap<String, String> fullPartSpec = new LinkedHashMap<String, String>();
          Warehouse.makeSpecFromName(fullPartSpec, st.getPath());
          partitionsDiscoveredByPath.put(st.getPath().toString(), fullPartSpec);
          JobConf jobConf = (JobConf)context.getConfiguration();
          JobContext currContext = HCatMapRedUtil.createJobContext(
            jobConf,
            context.getJobID(),
            InternalUtil.createReporter(HCatMapRedUtil.createTaskAttemptContext(jobConf,
              ShimLoader.getHadoopShims().getHCatShim().createTaskAttemptID())));
          HCatOutputFormat.configureOutputStorageHandler(currContext, jobInfo, fullPartSpec);
          contextDiscoveredByPath.put(st.getPath().toString(), currContext);
        }
      }

      //      for (Entry<String,Map<String,String>> spec : partitionsDiscoveredByPath.entrySet()){
      //        LOG.info("Partition "+ spec.getKey());
      //        for (Entry<String,String> e : spec.getValue().entrySet()){
      //          LOG.info(e.getKey() + "=>" +e.getValue());
      //        }
      //      }

      this.partitionsDiscovered = true;
    }
  }

  private void registerPartitions(JobContext context) throws IOException{
    if (dynamicPartitioningUsed){
      discoverPartitions(context);
    }
    OutputJobInfo jobInfo = HCatOutputFormat.getJobInfo(context);
    Configuration conf = context.getConfiguration();
    Table table = new Table(jobInfo.getTableInfo().getTable());
    Path tblPath = new Path(table.getTTable().getSd().getLocation());
    FileSystem fs = tblPath.getFileSystem(conf);

    if( table.getPartitionKeys().size() == 0 ) {
      //Move data from temp directory the actual table directory
      //No metastore operation required.
      Path src = new Path(jobInfo.getLocation());
      moveTaskOutputs(fs, src, src, tblPath, false);
      fs.delete(src, true);
      return;
    }

    HiveMetaStoreClient client = null;
    HCatTableInfo tableInfo = jobInfo.getTableInfo();
    List<Partition> partitionsAdded = new ArrayList<Partition>();
    try {
      HiveConf hiveConf = HCatUtil.getHiveConf(conf);
      client = HCatUtil.getHiveClient(hiveConf);
      StorerInfo storer = InternalUtil.extractStorerInfo(table.getTTable().getSd(),table.getParameters());

      FileStatus tblStat = fs.getFileStatus(tblPath);
      String grpName = tblStat.getGroup();
      FsPermission perms = tblStat.getPermission();

      List<Partition> partitionsToAdd = new ArrayList<Partition>();
      if (!dynamicPartitioningUsed){
        partitionsToAdd.add(
            constructPartition(
                context,jobInfo,
                tblPath.toString(), jobInfo.getPartitionValues()
                ,jobInfo.getOutputSchema(), getStorerParameterMap(storer)
                ,table, fs
                ,grpName,perms));
      }else{
        for (Entry<String,Map<String,String>> entry : partitionsDiscoveredByPath.entrySet()){
          partitionsToAdd.add(
              constructPartition(
                  context,jobInfo,
                  getPartitionRootLocation(entry.getKey(),entry.getValue().size()), entry.getValue()
                  ,jobInfo.getOutputSchema(), getStorerParameterMap(storer)
                  ,table, fs
                  ,grpName,perms));
        }
      }

      ArrayList<Map<String,String>> ptnInfos = new ArrayList<Map<String,String>>();
      for(Partition ptn : partitionsToAdd){
        ptnInfos.add(InternalUtil.createPtnKeyValueMap(new Table(tableInfo.getTable()), ptn));
      }

      //Publish the new partition(s)
      if (dynamicPartitioningUsed && harProcessor.isEnabled() && (!partitionsToAdd.isEmpty())){

        Path src = new Path(ptnRootLocation);
        // check here for each dir we're copying out, to see if it
        // already exists, error out if so
        moveTaskOutputs(fs, src, src, tblPath, true);
        moveTaskOutputs(fs, src, src, tblPath, false);
        fs.delete(src, true);
        try {
          updateTableSchema(client, table, jobInfo.getOutputSchema());
          LOG.info("HAR is being used. The table {} has new partitions {}.", table.getTableName(), ptnInfos);
          client.add_partitions(partitionsToAdd);
          partitionsAdded = partitionsToAdd;
        } catch (Exception e){
          // There was an error adding partitions : rollback fs copy and rethrow
          for (Partition p : partitionsToAdd){
            Path ptnPath = new Path(harProcessor.getParentFSPath(new Path(p.getSd().getLocation())));
            if (fs.exists(ptnPath)){
              fs.delete(ptnPath,true);
            }
          }
          throw e;
        }

      }else{
        // no harProcessor, regular operation
        updateTableSchema(client, table, jobInfo.getOutputSchema());
        LOG.info("HAR not is not being used. The table {} has new partitions {}.", table.getTableName(), ptnInfos);
        if (dynamicPartitioningUsed && (partitionsToAdd.size()>0)){
          Path src = new Path(ptnRootLocation);
          moveTaskOutputs(fs, src, src, tblPath, true);
          moveTaskOutputs(fs, src, src, tblPath, false);
          fs.delete(src, true);
        }
        client.add_partitions(partitionsToAdd);
        partitionsAdded = partitionsToAdd;
      }
    } catch (Exception e) {
      if (partitionsAdded.size() > 0) {
        try {
          // baseCommitter.cleanupJob failed, try to clean up the
          // metastore
          for (Partition p : partitionsAdded) {
            client.dropPartition(tableInfo.getDatabaseName(),
                tableInfo.getTableName(), p.getValues());
          }
        } catch (Exception te) {
          // Keep cause as the original exception
          throw new HCatException(
              ErrorType.ERROR_PUBLISHING_PARTITION, e);
        }
      }
      if (e instanceof HCatException) {
        throw (HCatException) e;
      } else {
        throw new HCatException(ErrorType.ERROR_PUBLISHING_PARTITION, e);
      }
    } finally {
      HCatUtil.closeHiveClientQuietly(client);
    }
  }

  private void cancelDelegationTokens(JobContext context) throws IOException{
    LOG.info("Cancelling deletgation token for the job.");
    HiveMetaStoreClient client = null;
    try {
      HiveConf hiveConf = HCatUtil
          .getHiveConf(context.getConfiguration());
      client = HCatUtil.getHiveClient(hiveConf);
      // cancel the deleg. tokens that were acquired for this job now that
      // we are done - we should cancel if the tokens were acquired by
      // HCatOutputFormat and not if they were supplied by Oozie.
      // In the latter case the HCAT_KEY_TOKEN_SIGNATURE property in
      // the conf will not be set
      String tokenStrForm = client.getTokenStrForm();
      if (tokenStrForm != null
          && context.getConfiguration().get(
              HCatConstants.HCAT_KEY_TOKEN_SIGNATURE) != null) {
        client.cancelDelegationToken(tokenStrForm);
      }
    } catch (MetaException e) {
      LOG.warn("MetaException while cancelling delegation token.", e);
    } catch (TException e) {
      LOG.warn("TException while cancelling delegation token.", e);
    } finally {
      HCatUtil.closeHiveClientQuietly(client);
    }
  }


}
