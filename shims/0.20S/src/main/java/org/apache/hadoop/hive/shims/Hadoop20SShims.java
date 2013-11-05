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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.shims;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Iterator;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.ProxyFileSystem;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskLogServlet;
import org.apache.hadoop.mapred.WebHCatJTShim20S;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.security.UserGroupInformation;


/**
 * Implemention of shims against Hadoop 0.20 with Security.
 */
public class Hadoop20SShims extends HadoopShimsSecure {

  @Override
  public String getTaskAttemptLogUrl(JobConf conf,
    String taskTrackerHttpAddress, String taskAttemptId)
    throws MalformedURLException {
    URL taskTrackerHttpURL = new URL(taskTrackerHttpAddress);
    return TaskLogServlet.getTaskLogUrl(
      taskTrackerHttpURL.getHost(),
      Integer.toString(taskTrackerHttpURL.getPort()),
      taskAttemptId);
  }

  @Override
  public JobTrackerState getJobTrackerState(ClusterStatus clusterStatus) throws Exception {
    JobTrackerState state;
    switch (clusterStatus.getJobTrackerState()) {
    case INITIALIZING:
      return JobTrackerState.INITIALIZING;
    case RUNNING:
      return JobTrackerState.RUNNING;
    default:
      String errorMsg = "Unrecognized JobTracker state: " + clusterStatus.getJobTrackerState();
      throw new Exception(errorMsg);
    }
  }

  @Override
  public org.apache.hadoop.mapreduce.TaskAttemptContext newTaskAttemptContext(Configuration conf, final Progressable progressable) {
    return new org.apache.hadoop.mapreduce.TaskAttemptContext(conf, new TaskAttemptID()) {
      @Override
      public void progress() {
        progressable.progress();
      }
    };
  }

  @Override
  public TaskAttemptID newTaskAttemptID(JobID jobId, boolean isMap, int taskId, int id) {
    return new TaskAttemptID(jobId.getJtIdentifier(), jobId.getId(), isMap, taskId, id);
  }

  @Override
  public org.apache.hadoop.mapreduce.JobContext newJobContext(Job job) {
    return new org.apache.hadoop.mapreduce.JobContext(job.getConfiguration(), job.getJobID());
  }

  @Override
  public boolean isLocalMode(Configuration conf) {
    return "local".equals(getJobLauncherRpcAddress(conf));
  }

  @Override
  public String getJobLauncherRpcAddress(Configuration conf) {
    return conf.get("mapred.job.tracker");
  }

  @Override
  public void setJobLauncherRpcAddress(Configuration conf, String val) {
    conf.set("mapred.job.tracker", val);
  }

  @Override
  public String getJobLauncherHttpAddress(Configuration conf) {
    return conf.get("mapred.job.tracker.http.address");
  }

  @Override
  public boolean moveToAppropriateTrash(FileSystem fs, Path path, Configuration conf)
          throws IOException {
    // older versions of Hadoop don't have a Trash constructor based on the
    // Path or FileSystem. So need to achieve this by creating a dummy conf.
    // this needs to be filtered out based on version

    Configuration dupConf = new Configuration(conf);
    FileSystem.setDefaultUri(dupConf, fs.getUri());
    Trash trash = new Trash(dupConf);
    return trash.moveToTrash(path);
  }
  @Override
  public long getDefaultBlockSize(FileSystem fs, Path path) {
    return fs.getDefaultBlockSize();
  }

  @Override
  public short getDefaultReplication(FileSystem fs, Path path) {
    return fs.getDefaultReplication();
  }

  @Override
  public void setTotalOrderPartitionFile(JobConf jobConf, Path partitionFile){
    TotalOrderPartitioner.setPartitionFile(jobConf, partitionFile);
  }

  /**
   * Returns a shim to wrap MiniMrCluster
   */
  public MiniMrShim getMiniMrCluster(Configuration conf, int numberOfTaskTrackers,
                                     String nameNode, int numDir) throws IOException {
    return new MiniMrShim(conf, numberOfTaskTrackers, nameNode, numDir);
  }

  /**
   * Shim for MiniMrCluster
   */
  public class MiniMrShim implements HadoopShims.MiniMrShim {

    private final MiniMRCluster mr;

    public MiniMrShim(Configuration conf, int numberOfTaskTrackers,
        String nameNode, int numDir) throws IOException {
      this.mr = new MiniMRCluster(numberOfTaskTrackers, nameNode, numDir);
    }

    @Override
    public int getJobTrackerPort() throws UnsupportedOperationException {
      return mr.getJobTrackerPort();
    }

    @Override
    public void shutdown() throws IOException {
      mr.shutdown();
    }

    @Override
    public void setupConfiguration(Configuration conf) {
      setJobLauncherRpcAddress(conf, "localhost:" + mr.getJobTrackerPort());
    }
  }

  // Don't move this code to the parent class. There's a binary
  // incompatibility between hadoop 1 and 2 wrt MiniDFSCluster and we
  // need to have two different shim classes even though they are
  // exactly the same.
  public HadoopShims.MiniDFSShim getMiniDfs(Configuration conf,
      int numDataNodes,
      boolean format,
      String[] racks) throws IOException {
    return new MiniDFSShim(new MiniDFSCluster(conf, numDataNodes, format, racks));
  }

  /**
   * MiniDFSShim.
   *
   */
  public class MiniDFSShim implements HadoopShims.MiniDFSShim {
    private final MiniDFSCluster cluster;

    public MiniDFSShim(MiniDFSCluster cluster) {
      this.cluster = cluster;
    }

    public FileSystem getFileSystem() throws IOException {
      return cluster.getFileSystem();
    }

    public void shutdown() {
      cluster.shutdown();
    }
  }
  private volatile HCatHadoopShims hcatShimInstance;
  @Override
  public HCatHadoopShims getHCatShim() {
    if(hcatShimInstance == null) {
      hcatShimInstance = new HCatHadoopShims20S();
    }
    return hcatShimInstance;
  }
  private final class HCatHadoopShims20S implements HCatHadoopShims {
    @Override
    public TaskID createTaskID() {
      return new TaskID();
    }

    @Override
    public TaskAttemptID createTaskAttemptID() {
      return new TaskAttemptID();
    }

    @Override
    public TaskAttemptContext createTaskAttemptContext(Configuration conf, TaskAttemptID taskId) {
      return new TaskAttemptContext(conf, taskId);
    }

    @Override
    public org.apache.hadoop.mapred.TaskAttemptContext createTaskAttemptContext(org.apache.hadoop.mapred.JobConf conf,
                 org.apache.hadoop.mapred.TaskAttemptID taskId, Progressable progressable) {
      org.apache.hadoop.mapred.TaskAttemptContext newContext = null;
      try {
        java.lang.reflect.Constructor construct = org.apache.hadoop.mapred.TaskAttemptContext.class.getDeclaredConstructor(
                org.apache.hadoop.mapred.JobConf.class, org.apache.hadoop.mapred.TaskAttemptID.class,
                Progressable.class);
        construct.setAccessible(true);
        newContext = (org.apache.hadoop.mapred.TaskAttemptContext)construct.newInstance(conf, taskId, progressable);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return newContext;
    }

    @Override
    public JobContext createJobContext(Configuration conf,
                                       JobID jobId) {
      return new JobContext(conf, jobId);
    }

    @Override
    public org.apache.hadoop.mapred.JobContext createJobContext(org.apache.hadoop.mapred.JobConf conf,
                                   org.apache.hadoop.mapreduce.JobID jobId, Progressable progressable) {
      org.apache.hadoop.mapred.JobContext newContext = null;
      try {
        java.lang.reflect.Constructor construct = org.apache.hadoop.mapred.JobContext.class.getDeclaredConstructor(
                org.apache.hadoop.mapred.JobConf.class, org.apache.hadoop.mapreduce.JobID.class,
                Progressable.class);
        construct.setAccessible(true);
        newContext = (org.apache.hadoop.mapred.JobContext)construct.newInstance(conf, jobId, progressable);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return newContext;
    }

    @Override
    public void commitJob(OutputFormat outputFormat, Job job) throws IOException {
      if( job.getConfiguration().get("mapred.job.tracker", "").equalsIgnoreCase("local") ) {
        try {
          //In local mode, mapreduce will not call OutputCommitter.cleanupJob.
          //Calling it from here so that the partition publish happens.
          //This call needs to be removed after MAPREDUCE-1447 is fixed.
          outputFormat.getOutputCommitter(createTaskAttemptContext(
                  job.getConfiguration(), createTaskAttemptID())).commitJob(job);
        } catch (IOException e) {
          throw new IOException("Failed to cleanup job",e);
        } catch (InterruptedException e) {
          throw new IOException("Failed to cleanup job",e);
        }
      }
    }

    @Override
    public void abortJob(OutputFormat outputFormat, Job job) throws IOException {
      if (job.getConfiguration().get("mapred.job.tracker", "")
              .equalsIgnoreCase("local")) {
        try {
          // This call needs to be removed after MAPREDUCE-1447 is fixed.
          outputFormat.getOutputCommitter(createTaskAttemptContext(
                  job.getConfiguration(), new TaskAttemptID())).abortJob(job, JobStatus.State.FAILED);
        } catch (IOException e) {
          throw new IOException("Failed to abort job", e);
        } catch (InterruptedException e) {
          throw new IOException("Failed to abort job", e);
        }
      }
    }

    @Override
    public InetSocketAddress getResourceManagerAddress(Configuration conf)
    {
      return JobTracker.getAddress(conf);
    }

    @Override
    public String getPropertyName(PropertyName name) {
      switch (name) {
        case CACHE_ARCHIVES:
          return DistributedCache.CACHE_ARCHIVES;
        case CACHE_FILES:
          return DistributedCache.CACHE_FILES;
        case CACHE_SYMLINK:
          return DistributedCache.CACHE_SYMLINK;
      }

      return "";
    }

    @Override
    public boolean isFileInHDFS(FileSystem fs, Path path) throws IOException {
      // In hadoop 1.x.x the file system URI is sufficient to determine the uri of the file
      return "hdfs".equals(fs.getUri().getScheme());
    }
  }
  @Override
  public WebHCatJTShim getWebHCatShim(Configuration conf, UserGroupInformation ugi) throws IOException {
    return new WebHCatJTShim20S(conf, ugi);//this has state, so can't be cached
  }

  @Override
  public Iterator<FileStatus> listLocatedStatus(final FileSystem fs,
                                                final Path path,
                                                final PathFilter filter
  ) throws IOException {
    return new Iterator<FileStatus>() {
      private final FileStatus[] result = fs.listStatus(path, filter);
      private int current = 0;

      @Override
      public boolean hasNext() {
        return current < result.length;
      }

      @Override
      public FileStatus next() {
        return result[current++];
      }

      @Override
      public void remove() {
        throw new IllegalArgumentException("Not supported");
      }
    };
  }

  @Override
  public BlockLocation[] getLocations(FileSystem fs,
                                      FileStatus status) throws IOException {
    return fs.getFileBlockLocations(status, 0, status.getLen());
  }

  @Override
  public FileSystem createProxyFileSystem(FileSystem fs, URI uri) {
    return new ProxyFileSystem(fs, uri);
  }
}
