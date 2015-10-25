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
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.ProxyFileSystem;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskLogServlet;
import org.apache.hadoop.mapred.WebHCatJTShim20S;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.KerberosName;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.tools.distcp2.DistCp;
import org.apache.hadoop.tools.distcp2.DistCpOptions;
import org.apache.hadoop.tools.distcp2.DistCpOptions.FileAttribute;

import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.VersionInfo;


/**
 * Implemention of shims against Hadoop 0.20 with Security.
 */
public class Hadoop20SShims extends HadoopShimsSecure {

  @Override
  public HadoopShims.CombineFileInputFormatShim getCombineFileInputFormat() {
    return new CombineFileInputFormatShim() {
      @Override
      public RecordReader getRecordReader(InputSplit split,
          JobConf job, Reporter reporter) throws IOException {
        throw new IOException("CombineFileInputFormat.getRecordReader not needed.");
      }

      @Override
      protected FileStatus[] listStatus(JobConf job) throws IOException {
        FileStatus[] result = super.listStatus(job);
        boolean foundDir = false;
        for (FileStatus stat: result) {
          if (stat.isDir()) {
            foundDir = true;
            break;
          }
        }
        if (!foundDir) {
          return result;
        }
        ArrayList<FileStatus> files = new ArrayList<FileStatus>();
        for (FileStatus stat: result) {
          if (!stat.isDir()) {
            files.add(stat);
          }
        }
        return files.toArray(new FileStatus[files.size()]);
      }
    };
  }

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
  public void refreshDefaultQueue(Configuration conf, String userName) {
    // MR1 does not expose API required to set MR queue mapping for user
  }

  @Override
  public void setTotalOrderPartitionFile(JobConf jobConf, Path partitionFile){
    TotalOrderPartitioner.setPartitionFile(jobConf, partitionFile);
  }

  @Override
  public Comparator<LongWritable> getLongComparator() {
    return new Comparator<LongWritable>() {
      @Override
      public int compare(LongWritable o1, LongWritable o2) {
        return o1.compareTo(o2);
      }
    };
  }

  /**
   * Returns a shim to wrap MiniMrCluster
   */
  @Override
  public MiniMrShim getMiniMrCluster(Configuration conf, int numberOfTaskTrackers,
                                     String nameNode, int numDir) throws IOException {
    return new MiniMrShim(conf, numberOfTaskTrackers, nameNode, numDir);
  }

  @Override
  public MiniMrShim getMiniTezCluster(Configuration conf, int numberOfTaskTrackers,
      String nameNode, boolean isLlap) throws IOException {
    throw new IOException("Cannot run tez on current hadoop, Version: " + VersionInfo.getVersion());
  }

  @Override
  public MiniMrShim getMiniSparkCluster(Configuration conf, int numberOfTaskTrackers,
    String nameNode, int numDir) throws IOException {
    throw new IOException("Cannot run Spark on YARN on current Hadoop, Version: " + VersionInfo.getVersion());
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
      MiniMRCluster.JobTrackerRunner runner = mr.getJobTrackerRunner();
      JobTracker tracker = runner.getJobTracker();
      if (tracker != null) {
        for (JobInProgress running : tracker.getRunningJobs()) {
          try {
            running.kill();
          } catch (Exception e) {
            // ignore
          }
        }
      }
      runner.shutdown();
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
  @Override
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

    @Override
    public FileSystem getFileSystem() throws IOException {
      return cluster.getFileSystem();
    }

    @Override
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
        case CLASSPATH_ARCHIVES:
          return "mapred.job.classpath.archives";
        case CLASSPATH_FILES:
          return "mapred.job.classpath.files";
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
  public List<FileStatus> listLocatedStatus(final FileSystem fs,
                                            final Path path,
                                            final PathFilter filter
                                            ) throws IOException {
    return Arrays.asList(fs.listStatus(path, filter));
  }

  @Override
  public BlockLocation[] getLocations(FileSystem fs,
                                      FileStatus status) throws IOException {
    return fs.getFileBlockLocations(status, 0, status.getLen());
  }

  @Override
  public TreeMap<Long, BlockLocation> getLocationsWithOffset(FileSystem fs,
                                                             FileStatus status) throws IOException {
    TreeMap<Long, BlockLocation> offsetBlockMap = new TreeMap<Long, BlockLocation>();
    BlockLocation[] locations = getLocations(fs, status);
    for (BlockLocation location : locations) {
      offsetBlockMap.put(location.getOffset(), location);
    }
    return offsetBlockMap;
  }

  @Override
  public void hflush(FSDataOutputStream stream) throws IOException {
    stream.sync();
  }

  @Override
  public HdfsFileStatus getFullFileStatus(Configuration conf, FileSystem fs, Path file)
      throws IOException {
    return new Hadoop20SFileStatus(fs.getFileStatus(file));
  }

  @Override
  public void setFullFileStatus(Configuration conf, HdfsFileStatus sourceStatus,
    FileSystem fs, Path target) throws IOException {
    String group = sourceStatus.getFileStatus().getGroup();
    String permission = Integer.toString(sourceStatus.getFileStatus().getPermission().toShort(), 8);
    //use FsShell to change group and permissions recursively
    try {
      FsShell fshell = new FsShell();
      fshell.setConf(conf);
      run(fshell, new String[]{"-chgrp", "-R", group, target.toString()});
      run(fshell, new String[]{"-chmod", "-R", permission, target.toString()});
    } catch (Exception e) {
      throw new IOException("Unable to set permissions of " + target, e);
    }
    try {
      if (LOG.isDebugEnabled()) {  //some trace logging
        getFullFileStatus(conf, fs, target).debugLog();
      }
    } catch (Exception e) {
      //ignore.
    }
  }

  public class Hadoop20SFileStatus implements HdfsFileStatus {
    private final FileStatus fileStatus;
    public Hadoop20SFileStatus(FileStatus fileStatus) {
      this.fileStatus = fileStatus;
    }
    @Override
    public FileStatus getFileStatus() {
      return fileStatus;
    }
    @Override
    public void debugLog() {
      if (fileStatus != null) {
        LOG.debug(fileStatus.toString());
      }
    }
  }

  @Override
  public FileSystem createProxyFileSystem(FileSystem fs, URI uri) {
    return new ProxyFileSystem(fs, uri);
  }
  @Override
  public Map<String, String> getHadoopConfNames() {
    Map<String, String> ret = new HashMap<String, String>();
    ret.put("HADOOPFS", "fs.default.name");
    ret.put("HADOOPMAPFILENAME", "map.input.file");
    ret.put("HADOOPMAPREDINPUTDIR", "mapred.input.dir");
    ret.put("HADOOPMAPREDINPUTDIRRECURSIVE", "mapred.input.dir.recursive");
    ret.put("MAPREDMAXSPLITSIZE", "mapred.max.split.size");
    ret.put("MAPREDMINSPLITSIZE", "mapred.min.split.size");
    ret.put("MAPREDMINSPLITSIZEPERNODE", "mapred.min.split.size.per.node");
    ret.put("MAPREDMINSPLITSIZEPERRACK", "mapred.min.split.size.per.rack");
    ret.put("HADOOPNUMREDUCERS", "mapred.reduce.tasks");
    ret.put("HADOOPJOBNAME", "mapred.job.name");
    ret.put("HADOOPSPECULATIVEEXECREDUCERS", "mapred.reduce.tasks.speculative.execution");
    ret.put("MAPREDSETUPCLEANUPNEEDED", "mapred.committer.job.setup.cleanup.needed");
    ret.put("MAPREDTASKCLEANUPNEEDED", "mapreduce.job.committer.task.cleanup.needed");
    return ret;
  }

  @Override
  public ZeroCopyReaderShim getZeroCopyReader(FSDataInputStream in, ByteBufferPoolShim pool) throws IOException {
    /* not supported */
    return null;
  }

  @Override
  public DirectDecompressorShim getDirectDecompressor(DirectCompressionType codec) {
    /* not supported */
    return null;
  }

  @Override
  public Configuration getConfiguration(org.apache.hadoop.mapreduce.JobContext context) {
    return context.getConfiguration();
  }

  @Override
  public JobConf getJobConf(org.apache.hadoop.mapred.JobContext context) {
    return context.getJobConf();
  }

  @Override
  public FileSystem getNonCachedFileSystem(URI uri, Configuration conf) throws IOException {
    boolean origDisableHDFSCache =
        conf.getBoolean("fs." + uri.getScheme() + ".impl.disable.cache", false);
    // hadoop-1 compatible flag.
    conf.setBoolean("fs." + uri.getScheme() + ".impl.disable.cache", true);
    FileSystem fs = FileSystem.get(uri, conf);
    conf.setBoolean("fs." + uri.getScheme() + ".impl.disable.cache", origDisableHDFSCache);
    return fs;
  }

  @Override
  public void getMergedCredentials(JobConf jobConf) throws IOException {
    throw new IOException("Merging of credentials not supported in this version of hadoop");
  }

  @Override
  public void mergeCredentials(JobConf dest, JobConf src) throws IOException {
    throw new IOException("Merging of credentials not supported in this version of hadoop");
  }

  @Override
  public String getPassword(Configuration conf, String name) {
    // No password API, just retrieve value from conf
    return conf.get(name);
  }

  @Override
  public boolean supportStickyBit() {
    return false;
  }

  @Override
  public boolean hasStickyBit(FsPermission permission) {
    return false;
  }

  @Override
  public boolean supportTrashFeature() {
    return false;
  }

  @Override
  public Path getCurrentTrashPath(Configuration conf, FileSystem fs) {
    return null;
  }

  @Override
  public boolean isDirectory(FileStatus fileStatus) {
    return fileStatus.isDir();
  }

  /**
   * Returns a shim to wrap KerberosName
   */
  @Override
  public KerberosNameShim getKerberosNameShim(String name) throws IOException {
    return new KerberosNameShim(name);
  }

  /**
   * Shim for KerberosName
   */
  public class KerberosNameShim implements HadoopShimsSecure.KerberosNameShim {

    private final KerberosName kerberosName;

    public KerberosNameShim(String name) {
      kerberosName = new KerberosName(name);
    }

    @Override
    public String getDefaultRealm() {
      return kerberosName.getDefaultRealm();
    }

    @Override
    public String getServiceName() {
      return kerberosName.getServiceName();
    }

    @Override
    public String getHostName() {
      return kerberosName.getHostName();
    }

    @Override
    public String getRealm() {
      return kerberosName.getRealm();
    }

    @Override
    public String getShortName() throws IOException {
      return kerberosName.getShortName();
    }
  }

  @Override
  public StoragePolicyShim getStoragePolicyShim(FileSystem fs) {
    return null;
  }

  @Override
  public boolean runDistCp(Path src, Path dst, Configuration conf) throws IOException {

    DistCpOptions options = new DistCpOptions(Collections.singletonList(src), dst);
    options.setSyncFolder(true);
    options.setSkipCRC(true);
    options.preserve(FileAttribute.BLOCKSIZE);
    try {
      DistCp distcp = new DistCp(conf, options);
      distcp.execute();
      return true;
    } catch (Exception e) {
      throw new IOException("Cannot execute DistCp process: " + e, e);
    }
  }

  @Override
  public HdfsEncryptionShim createHdfsEncryptionShim(FileSystem fs, Configuration conf) throws IOException {
    return new HadoopShims.NoopHdfsEncryptionShim();
  }

  @Override
  public Path getPathWithoutSchemeAndAuthority(Path path) {
    return path;
  }

  @Override
  public List<HdfsFileStatusWithId> listLocatedHdfsStatus(
      FileSystem fs, Path path, PathFilter filter) throws IOException {
    throw new UnsupportedOperationException("Not supported on old version");
  }

  @Override
  public int readByteBuffer(FSDataInputStream file, ByteBuffer dest) throws IOException {
    // Inefficient for direct buffers; only here for compat.
    int pos = dest.position();
    if (dest.hasArray()) {
      int result = file.read(dest.array(), dest.arrayOffset(), dest.remaining());
      if (result > 0) {
        dest.position(pos + result);
      }
      return result;
    } else {
      byte[] arr = new byte[dest.remaining()];
      int result = file.read(arr, 0, arr.length);
      if (result > 0) {
        dest.put(arr, 0, result);
        dest.position(pos + result);
      }
      return result;
    }
  }

  @Override
  public void addDelegationTokens(FileSystem fs, Credentials cred, String uname) throws IOException {
    Token<?> fsToken = fs.getDelegationToken(uname);
    cred.addToken(fsToken.getService(), fsToken);
  }

  @Override
  public long getFileId(FileSystem fs, String path) throws IOException {
    throw new UnsupportedOperationException("Not supported on old version");
  }
}
