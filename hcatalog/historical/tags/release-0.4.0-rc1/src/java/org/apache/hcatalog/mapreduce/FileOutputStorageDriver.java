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
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.security.AccessControlException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Base class for File-based OutputStorageDrivers to extend. Provides subclasses
 * the convenience of not having to rewrite mechanisms such as, dynamic
 * partitioning, partition registration, success file, etc.
 */
public abstract class FileOutputStorageDriver extends HCatOutputStorageDriver {

    /** The directory under which data is initially written for a partitioned table */
    protected static final String DYNTEMP_DIR_NAME = "_DYN";

    /** The directory under which data is initially written for a non partitioned table */
    protected static final String TEMP_DIR_NAME = "_TEMP";
    private OutputFormat<WritableComparable<?>, ? super Writable> outputFormat;


    @Override
    public void initialize(JobContext jobContext, Properties hcatProperties) throws IOException {
        super.initialize(jobContext, hcatProperties);
    }

    @Override
    public final String getOutputLocation(JobContext jobContext,
                                    String tableLocation, List<String> partitionCols, Map<String, String> partitionValues, String dynHash) throws IOException {
        String parentPath = tableLocation;
        // For dynamic partitioned writes without all keyvalues specified,
        // we create a temp dir for the associated write job
        if (dynHash != null){
            parentPath = new Path(tableLocation, DYNTEMP_DIR_NAME+dynHash).toString();
        }

        // For non-partitioned tables, we send them to the temp dir
        if((dynHash == null) && ( partitionValues == null || partitionValues.size() == 0 )) {
            return new Path(tableLocation, TEMP_DIR_NAME).toString();
        }

        List<String> values = new ArrayList<String>();
        for(String partitionCol : partitionCols) {
            values.add(partitionValues.get(partitionCol));
        }

        String partitionLocation = FileUtils.makePartName(partitionCols, values);

        Path path = new Path(parentPath, partitionLocation);
        return path.toString();
    }

    @Override
    public final Path getWorkFilePath(TaskAttemptContext context, String outputLoc) throws IOException{
        return new Path(new FileOutputCommitter(new Path(outputLoc), context).getWorkPath(), org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.getUniqueFile(context, "part", ""));
    }

    /**
   * Any initialization of file paths, set permissions and group on freshly created files
   * This is called at RecordWriter instantiation time which can be at write-time for
   * a dynamic partitioning usecase
     * @param context
     * @throws IOException
     */
    static void prepareOutputLocation(HCatOutputStorageDriver osd, TaskAttemptContext context) throws IOException {
      OutputJobInfo info =  HCatBaseOutputFormat.getJobInfo(context);
//      Path workFile = osd.getWorkFilePath(context,info.getLocation());
      Path workFile = osd.getWorkFilePath(context,context.getConfiguration().get("mapred.output.dir"));
      Path tblPath = new Path(info.getTableInfo().getTable().getSd().getLocation());
      FileSystem fs = tblPath.getFileSystem(context.getConfiguration());
      FileStatus tblPathStat = fs.getFileStatus(tblPath);

//      LOG.info("Attempting to set permission ["+tblPathStat.getPermission()+"] on ["+
//          workFile+"], location=["+info.getLocation()+"] , mapred.locn =["+
//          context.getConfiguration().get("mapred.output.dir")+"]");
//
//      FileStatus wFileStatus = fs.getFileStatus(workFile);
//      LOG.info("Table : "+tblPathStat.getPath());
//      LOG.info("Working File : "+wFileStatus.getPath());

      fs.setPermission(workFile, tblPathStat.getPermission());
      try{
        fs.setOwner(workFile, null, tblPathStat.getGroup());
      } catch(AccessControlException ace){
        // log the messages before ignoring. Currently, logging is not built in HCat.
      }
    }

    @Override
    OutputFormatContainer getOutputFormatContainer(OutputFormat outputFormat) {
        //broken
        return new FileOutputFormatContainer(null);
    }
}

