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

package org.apache.hcatalog.hbase;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatMapRedUtil;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * MapReduce job which reads a series of Puts stored in a sequence file
 * and imports the data into HBase. It needs to create the necessary HBase
 * regions using HFileOutputFormat and then notify the correct region servers
 * to doBulkLoad(). This will be used After an MR job has written the SequenceFile
 * and data needs to be bulk loaded onto HBase.
 */
class ImportSequenceFile {
  private final static Logger LOG = LoggerFactory.getLogger(ImportSequenceFile.class);
  private final static String NAME = "HCatImportSequenceFile";
  private final static String IMPORTER_WORK_DIR = "_IMPORTER_MR_WORK_DIR";


  private static class SequenceFileImporter extends Mapper<ImmutableBytesWritable, Put, ImmutableBytesWritable, Put> {

    @Override
    public void map(ImmutableBytesWritable rowKey, Put value,
            Context context)
      throws IOException, InterruptedException {
      context.write(new ImmutableBytesWritable(value.getRow()), value);
    }
  }

  private static class ImporterOutputFormat extends HFileOutputFormat {
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
      final OutputCommitter baseOutputCommitter = super.getOutputCommitter(context);

      return new OutputCommitter() {
        @Override
        public void setupJob(JobContext jobContext) throws IOException {
          baseOutputCommitter.setupJob(jobContext);
        }

        @Override
        public void setupTask(TaskAttemptContext taskContext) throws IOException {
          baseOutputCommitter.setupTask(taskContext);
        }

        @Override
        public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
          return baseOutputCommitter.needsTaskCommit(taskContext);
        }

        @Override
        public void commitTask(TaskAttemptContext taskContext) throws IOException {
          baseOutputCommitter.commitTask(taskContext);
        }

        @Override
        public void abortTask(TaskAttemptContext taskContext) throws IOException {
          baseOutputCommitter.abortTask(taskContext);
        }

        @Override
        public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
          try {
            baseOutputCommitter.abortJob(jobContext, state);
          } finally {
            cleanupScratch(jobContext);
          }
        }

        @Override
        public void commitJob(JobContext jobContext) throws IOException {
          try {
            baseOutputCommitter.commitJob(jobContext);
            Configuration conf = jobContext.getConfiguration();
            try {
              //import hfiles
              new LoadIncrementalHFiles(conf)
                .doBulkLoad(HFileOutputFormat.getOutputPath(jobContext),
                  new HTable(conf,
                    conf.get(HBaseConstants.PROPERTY_OUTPUT_TABLE_NAME_KEY)));
            } catch (Exception e) {
              throw new IOException("BulkLoad failed.", e);
            }
          } finally {
            cleanupScratch(jobContext);
          }
        }

        @Override
        public void cleanupJob(JobContext context) throws IOException {
          try {
            baseOutputCommitter.cleanupJob(context);
          } finally {
            cleanupScratch(context);
          }
        }

        private void cleanupScratch(JobContext context) throws IOException {
          FileSystem fs = FileSystem.get(context.getConfiguration());
          fs.delete(HFileOutputFormat.getOutputPath(context), true);
        }
      };
    }
  }

  private static Job createSubmittableJob(Configuration conf, String tableName, Path inputDir, Path scratchDir, boolean localMode)
    throws IOException {
    HBaseHCatStorageHandler.setHBaseSerializers(conf);
    Job job = new Job(conf, NAME + "_" + tableName);
    job.setJarByClass(SequenceFileImporter.class);
    FileInputFormat.setInputPaths(job, inputDir);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapperClass(SequenceFileImporter.class);

    HTable table = new HTable(conf, tableName);
    job.setReducerClass(PutSortReducer.class);
    FileOutputFormat.setOutputPath(job, scratchDir);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(Put.class);
    HFileOutputFormat.configureIncrementalLoad(job, table);
    URI partitionURI;
    try {
      partitionURI = new URI(TotalOrderPartitioner.getPartitionFile(job.getConfiguration())
          + "#" + TotalOrderPartitioner.DEFAULT_PATH);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    DistributedCache.addCacheFile(partitionURI, job.getConfiguration());
    DistributedCache.createSymlink(job.getConfiguration());
    //override OutputFormatClass with our own so we can include cleanup in the committer
    job.setOutputFormatClass(ImporterOutputFormat.class);

    //local mode doesn't support symbolic links so we have to manually set the actual path
    if (localMode) {
      String partitionFile = null;
      URI[] uris = DistributedCache.getCacheFiles(job.getConfiguration());
      if(uris == null) {
        throw new IllegalStateException("No cache file existed in job configuration");
      }
      for (URI uri : uris) {
        if (TotalOrderPartitioner.DEFAULT_PATH.equals(uri.getFragment())) {
          partitionFile = uri.toString();
          break;
        }
      }
      if(partitionFile == null) {
        throw new IllegalStateException("Unable to find " +
            TotalOrderPartitioner.DEFAULT_PATH + " in cache");
      }
      partitionFile = partitionFile.substring(0, partitionFile.lastIndexOf("#"));
      job.getConfiguration().set(TotalOrderPartitioner.PARTITIONER_PATH, partitionFile.toString());
    }

    return job;
  }

  /**
   * Method to run the Importer MapReduce Job. Normally will be called by another MR job
   * during OutputCommitter.commitJob().
   * @param parentContext JobContext of the parent job
   * @param tableName name of table to bulk load data into
   * @param InputDir path of SequenceFile formatted data to read
   * @param scratchDir temporary path for the Importer MR job to build the HFiles which will be imported
   * @return
   */
  static boolean runJob(JobContext parentContext, String tableName, Path InputDir, Path scratchDir) {
    Configuration parentConf = parentContext.getConfiguration();
    Configuration conf = new Configuration();
    for (Map.Entry<String, String> el : parentConf) {
      if (el.getKey().startsWith("hbase."))
        conf.set(el.getKey(), el.getValue());
      if (el.getKey().startsWith("mapred.cache.archives"))
        conf.set(el.getKey(), el.getValue());
    }

    //Inherit jar dependencies added to distributed cache loaded by parent job
    conf.set("mapred.job.classpath.archives", parentConf.get("mapred.job.classpath.archives", ""));
    conf.set("mapreduce.job.cache.archives.visibilities", parentConf.get("mapreduce.job.cache.archives.visibilities", ""));

    //Temporary fix until hbase security is ready
    //We need the written HFile to be world readable so
    //hbase regionserver user has the privileges to perform a hdfs move
    if (parentConf.getBoolean("hadoop.security.authorization", false)) {
      FsPermission.setUMask(conf, FsPermission.valueOf("----------"));
    }

    conf.set(HBaseConstants.PROPERTY_OUTPUT_TABLE_NAME_KEY, tableName);
    conf.setBoolean(JobContext.JOB_CANCEL_DELEGATION_TOKEN, false);

    boolean localMode = "local".equals(conf.get("mapred.job.tracker"));

    boolean success = false;
    try {
      FileSystem fs = FileSystem.get(parentConf);
      Path workDir = new Path(new Job(parentConf).getWorkingDirectory(), IMPORTER_WORK_DIR);
      if (!fs.mkdirs(workDir))
        throw new IOException("Importer work directory already exists: " + workDir);
      Job job = createSubmittableJob(conf, tableName, InputDir, scratchDir, localMode);
      job.setWorkingDirectory(workDir);
      job.getCredentials().addAll(parentContext.getCredentials());
      success = job.waitForCompletion(true);
      fs.delete(workDir, true);
      //We only cleanup on success because failure might've been caused by existence of target directory
      if (localMode && success) {
        new ImporterOutputFormat().getOutputCommitter(HCatMapRedUtil.createTaskAttemptContext(conf, new TaskAttemptID())).commitJob(job);
      }
    } catch (InterruptedException e) {
      LOG.error("ImportSequenceFile Failed", e);
    } catch (ClassNotFoundException e) {
      LOG.error("ImportSequenceFile Failed", e);
    } catch (IOException e) {
      LOG.error("ImportSequenceFile Failed", e);
    }
    return success;
  }

}
