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
package org.apache.hadoop.hive.shims;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.security.AccessControlException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.security.auth.Subject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProvider.Options;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.DefaultFileAccess;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.ProxyFileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.TrashPolicy;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.WebHCatJTShim23;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.SuppressFBWarnings;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.test.MiniTezCluster;

import static org.apache.hadoop.hive.shims.Utils.RAW_RESERVED_VIRTUAL_PATH;
import static org.apache.hadoop.tools.DistCpConstants.CONF_LABEL_DISTCP_JOB_ID;

/**
 * Implemention of shims against Hadoop 0.23.0.
 */
public class Hadoop23Shims extends HadoopShimsSecure {

  HadoopShims.MiniDFSShim cluster = null;
  final boolean storagePolicy;

  public Hadoop23Shims() {
    // in-memory HDFS
    boolean storage = false;
    try {
      Class.forName("org.apache.hadoop.hdfs.protocol.BlockStoragePolicy",
            false, ShimLoader.class.getClassLoader());
      storage = true;
    } catch (ClassNotFoundException ce) {
    }
    this.storagePolicy = storage;
  }

  @Override
  public HadoopShims.CombineFileInputFormatShim getCombineFileInputFormat() {
    return new CombineFileInputFormatShim() {
      @Override
      public RecordReader getRecordReader(InputSplit split,
          JobConf job, Reporter reporter) throws IOException {
        throw new IOException("CombineFileInputFormat.getRecordReader not needed.");
      }

      @Override
      protected List<FileStatus> listStatus(JobContext job) throws IOException {
        List<FileStatus> result = super.listStatus(job);
        Iterator<FileStatus> it = result.iterator();
        while (it.hasNext()) {
          FileStatus stat = it.next();
          if (!stat.isFile() || (stat.getLen() == 0 && !stat.getPath().toUri().getScheme().equals("nullscan"))) {
            it.remove();
          }
        }
        return result;
      }
    };
  }

  @Override
  public String getTaskAttemptLogUrl(JobConf conf,
    String taskTrackerHttpAddress, String taskAttemptId)
    throws MalformedURLException {
    if (conf.get("mapreduce.framework.name") != null
      && conf.get("mapreduce.framework.name").equals("yarn")) {
      // if the cluster is running in MR2 mode, return null
      LOG.warn("Can't fetch tasklog: TaskLogServlet is not supported in MR2 mode.");
      return null;
    } else {
      // Was using Hadoop-internal API to get tasklogs, disable until  MAPREDUCE-5857 is fixed.
      LOG.warn("Can't fetch tasklog: TaskLogServlet is not supported in MR1 mode.");
      return null;
    }
  }

  @Override
  public JobTrackerState getJobTrackerState(ClusterStatus clusterStatus) throws Exception {
    switch (clusterStatus.getJobTrackerStatus()) {
    case INITIALIZING:
      return JobTrackerState.INITIALIZING;
    case RUNNING:
      return JobTrackerState.RUNNING;
    default:
      String errorMsg = "Unrecognized JobTracker state: " + clusterStatus.getJobTrackerStatus();
      throw new Exception(errorMsg);
    }
  }

  @Override
  public org.apache.hadoop.mapreduce.TaskAttemptContext newTaskAttemptContext(Configuration conf, final Progressable progressable) {
    TaskAttemptID taskAttemptId = TaskAttemptID.forName(conf.get(MRJobConfig.TASK_ATTEMPT_ID));
    if (taskAttemptId == null) {
      // If the caller is not within a mapper/reducer (if reading from the table via CliDriver),
      // then TaskAttemptID.forname() may return NULL. Fall back to using default constructor.
      taskAttemptId = new TaskAttemptID();
    }
    return new TaskAttemptContextImpl(conf, taskAttemptId) {
      @Override
      public void progress() {
        progressable.progress();
      }
    };
  }

  @Override
  public TaskAttemptID newTaskAttemptID(JobID jobId, boolean isMap, int taskId, int id) {
    return new TaskAttemptID(jobId.getJtIdentifier(), jobId.getId(), isMap ?  TaskType.MAP : TaskType.REDUCE, taskId, id);
  }

  @Override
  public org.apache.hadoop.mapreduce.JobContext newJobContext(Job job) {
    return new JobContextImpl(job.getConfiguration(), job.getJobID());
  }

  @Override
  public boolean isLocalMode(Configuration conf) {
    return "local".equals(conf.get("mapreduce.framework.name"));
  }

  @Override
  public String getJobLauncherRpcAddress(Configuration conf) {
    return conf.get("yarn.resourcemanager.address");
  }

  @Override
  public void setJobLauncherRpcAddress(Configuration conf, String val) {
    if (val.equals("local")) {
      // LocalClientProtocolProvider expects both parameters to be 'local'.
      conf.set("mapreduce.framework.name", val);
      conf.set("mapreduce.jobtracker.address", val);
    }
    else {
      conf.set("mapreduce.framework.name", "yarn");
      conf.set("yarn.resourcemanager.address", val);
    }
  }

  @Override
  public String getJobLauncherHttpAddress(Configuration conf) {
    return conf.get("yarn.resourcemanager.webapp.address");
  }

  @Override
  public long getDefaultBlockSize(FileSystem fs, Path path) {
    return fs.getDefaultBlockSize(path);
  }

  @Override
  public short getDefaultReplication(FileSystem fs, Path path) {
    return fs.getDefaultReplication(path);
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
   * Load the fair scheduler queue for given user if available.
   */
  @Override
  public void refreshDefaultQueue(Configuration conf, String userName) throws IOException {
    //no op
  }

  private boolean isFairScheduler (Configuration conf) {
    return "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler".
        equalsIgnoreCase(conf.get(YarnConfiguration.RM_SCHEDULER));
  }

  /**
   * Returns a shim to wrap MiniMrCluster
   */
  @Override
  public MiniMrShim getMiniMrCluster(Configuration conf, int numberOfTaskTrackers,
                                     String nameNode, int numDir) throws IOException {
    return new MiniMrShim(conf, numberOfTaskTrackers, nameNode, numDir);
  }

  /**
   * Shim for MiniMrCluster
   */
  public class MiniMrShim implements HadoopShims.MiniMrShim {

    private final MiniMRCluster mr;
    private final Configuration conf;

    public MiniMrShim() {
      mr = null;
      conf = null;
    }

    public MiniMrShim(Configuration conf, int numberOfTaskTrackers,
                      String nameNode, int numDir) throws IOException {
      this.conf = conf;

      JobConf jConf = new JobConf(conf);
      jConf.set("yarn.scheduler.capacity.root.queues", "default");
      jConf.set("yarn.scheduler.capacity.root.default.capacity", "100");
      jConf.setInt(MRJobConfig.MAP_MEMORY_MB, 512);
      jConf.setInt(MRJobConfig.REDUCE_MEMORY_MB, 512);
      jConf.setInt(MRJobConfig.MR_AM_VMEM_MB, 128);
      jConf.setInt(YarnConfiguration.YARN_MINICLUSTER_NM_PMEM_MB, 512);
      jConf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
      jConf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 512);

      mr = new MiniMRCluster(numberOfTaskTrackers, nameNode, numDir, null, null, jConf);
    }

    @Override
    public int getJobTrackerPort() throws UnsupportedOperationException {
      String address = conf.get("yarn.resourcemanager.address");
      address = StringUtils.substringAfterLast(address, ":");

      if (StringUtils.isBlank(address)) {
        throw new IllegalArgumentException("Invalid YARN resource manager port.");
      }

      return Integer.parseInt(address);
    }

    @Override
    public void shutdown() throws IOException {
      mr.shutdown();
    }

    @Override
    public void setupConfiguration(Configuration conf) {
      JobConf jConf = mr.createJobConf();
      for (Map.Entry<String, String> pair: jConf) {
        conf.set(pair.getKey(), pair.getValue());
      }
      conf.setInt(MRJobConfig.MAP_MEMORY_MB, 512);
      conf.setInt(MRJobConfig.REDUCE_MEMORY_MB, 512);
      conf.setInt(MRJobConfig.MR_AM_VMEM_MB, 128);
    }
  }

  @Override
  public HadoopShims.MiniMrShim getLocalMiniTezCluster(Configuration conf, boolean usingLlap) {
    return new MiniTezLocalShim(conf, usingLlap);
  }

  public class MiniTezLocalShim extends Hadoop23Shims.MiniMrShim {
    private final Configuration conf;
    private final boolean isLlap;

    public MiniTezLocalShim(Configuration conf, boolean usingLlap) {
      this.conf = conf;
      this.isLlap = usingLlap;
      setupConfiguration(conf);
    }

    @Override
    public int getJobTrackerPort() throws UnsupportedOperationException {
      throw new UnsupportedOperationException("No JobTracker port for local mode");
    }

    @Override
    public void setupConfiguration(Configuration conf) {
      conf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
      // TODO: enable below option once HIVE-26445 is investigated
      // hiveConf.setBoolean("tez.local.mode.without.network", true);
      conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);

      conf.setBoolean(TezConfiguration.TEZ_IGNORE_LIB_URIS, true);

      // TODO Force fs to file://, setup staging dir?
      //      conf.set("fs.defaultFS", "file:///");
      //      conf.set(TezConfiguration.TEZ_AM_STAGING_DIR, "/tmp");

      if (!isLlap) { // Conf for non-llap
        conf.setBoolean("hive.llap.io.enabled", false);
      } else { // Conf for llap
        conf.set("hive.llap.execution.mode", "only");
      }
    }

    @Override
    public void shutdown() throws IOException {
      // Nothing to do
    }
  }

  /**
   * Returns a shim to wrap MiniMrTez
   */
  @Override
  public MiniMrShim getMiniTezCluster(Configuration conf, int numberOfTaskTrackers,
      String nameNode, boolean usingLlap) throws IOException {
    return new MiniTezShim(conf, numberOfTaskTrackers, nameNode, usingLlap);
  }

  /**
   * Shim for MiniTezCluster
   */
  public class MiniTezShim extends Hadoop23Shims.MiniMrShim {

    private final MiniTezCluster mr;
    private final Configuration conf;
    private final boolean isLlap;

    public MiniTezShim(Configuration conf, int numberOfTaskTrackers, String nameNode,
                       boolean usingLlap) throws IOException {
      mr = new MiniTezCluster("hive", numberOfTaskTrackers);
      conf.setInt(YarnConfiguration.YARN_MINICLUSTER_NM_PMEM_MB, 4096);
      conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
      conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 4096);

      conf.set("fs.defaultFS", nameNode);
      conf.set("tez.am.log.level", "DEBUG");
      conf.set(MRJobConfig.MR_AM_STAGING_DIR, "/apps_staging_dir");
      mr.init(conf);
      mr.start();
      this.conf = mr.getConfig();
      this.isLlap = usingLlap;
    }

    @Override
    public int getJobTrackerPort() throws UnsupportedOperationException {
      String address = conf.get("yarn.resourcemanager.address");
      address = StringUtils.substringAfterLast(address, ":");

      if (StringUtils.isBlank(address)) {
        throw new IllegalArgumentException("Invalid YARN resource manager port.");
      }

      return Integer.parseInt(address);
    }

    @Override
    public void shutdown() throws IOException {
      mr.stop();
    }

    @Override
    public void setupConfiguration(Configuration conf) {
      Configuration config = mr.getConfig();
      for (Map.Entry<String, String> pair : config) {
        conf.set(pair.getKey(), pair.getValue());
      }
      if (isLlap) {
        conf.set("hive.llap.execution.mode", "all");
      }
    }
  }

  private void configureImpersonation(Configuration conf) {
    String user;
    try {
      user = Utils.getUGI().getShortUserName();
    } catch (Exception e) {
      String msg = "Cannot obtain username: " + e;
      throw new IllegalStateException(msg, e);
    }
    conf.set("hadoop.proxyuser." + user + ".groups", "*");
    conf.set("hadoop.proxyuser." + user + ".hosts", "*");
  }

  @Override
  public void setHadoopCallerContext(String callerContext) {
    CallerContext.setCurrent(new CallerContext.Builder(callerContext).build());
  }

  @Override
  public void setHadoopQueryContext(String queryId) {
    setHadoopCallerContext("hive_queryId_" + queryId);
  }

  @Override
  public void setHadoopSessionContext(String sessionId) {
    setHadoopCallerContext("hive_sessionId_" + sessionId);
  }

  @Override
  public HadoopShims.MiniDFSShim getMiniDfs(Configuration conf,
      int numDataNodes,
      boolean format,
      String[] racks) throws IOException{
    return getMiniDfs(conf, numDataNodes, format, racks, false);
  }
  // Don't move this code to the parent class. There's a binary
  // incompatibility between hadoop 1 and 2 wrt MiniDFSCluster and we
  // need to have two different shim classes even though they are
  // exactly the same.
  @Override
  public HadoopShims.MiniDFSShim getMiniDfs(Configuration conf,
      int numDataNodes,
      boolean format,
      String[] racks,
      boolean isHA) throws IOException {
    configureImpersonation(conf);
    MiniDFSCluster miniDFSCluster;
    if (isHA) {
      MiniDFSNNTopology topo = new MiniDFSNNTopology()
        .addNameservice(new MiniDFSNNTopology.NSConf("minidfs").addNN(
          new MiniDFSNNTopology.NNConf("nn1")).addNN(
          new MiniDFSNNTopology.NNConf("nn2")));
      miniDFSCluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numDataNodes).format(format)
        .racks(racks).nnTopology(topo).build();
      miniDFSCluster.waitActive();
      miniDFSCluster.transitionToActive(0);
    } else {
      miniDFSCluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numDataNodes).format(format)
        .racks(racks).build();
    }

    // Need to set the client's KeyProvider to the NN's for JKS,
    // else the updates do not get flushed properly
    KeyProviderCryptoExtension keyProvider =  miniDFSCluster.getNameNode(0).getNamesystem().getProvider();
    if (keyProvider != null) {
      try {
        setKeyProvider(miniDFSCluster.getFileSystem(0).getClient(), keyProvider);
      } catch (Exception err) {
        throw new IOException(err);
      }
    }

    cluster = new MiniDFSShim(miniDFSCluster);
    return cluster;
  }

  private static void setKeyProvider(DFSClient dfsClient, KeyProviderCryptoExtension provider)
      throws Exception {
    Method setKeyProviderHadoop27Method = null;
    try {
      setKeyProviderHadoop27Method = DFSClient.class.getMethod("setKeyProvider", KeyProvider.class);
    } catch (NoSuchMethodException err) {
      // We can just use setKeyProvider() as it is
    }

    if (setKeyProviderHadoop27Method != null) {
      // Method signature changed in Hadoop 2.7. Cast provider to KeyProvider
      setKeyProviderHadoop27Method.invoke(dfsClient, (KeyProvider) provider);
    } else {
      dfsClient.setKeyProvider(provider);
    }
  }

  /**
   * MiniDFSShim.
   *
   */
  public static class MiniDFSShim implements HadoopShims.MiniDFSShim {
    private final MiniDFSCluster cluster;

    public MiniDFSShim(MiniDFSCluster cluster) {
      this.cluster = cluster;
    }

    @Override
    public FileSystem getFileSystem() throws IOException {
      return cluster.getFileSystem(0);
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
      hcatShimInstance = new HCatHadoopShims23();
    }
    return hcatShimInstance;
  }

  private static final class HCatHadoopShims23 implements HCatHadoopShims {
    @Override
    public TaskID createTaskID() {
      return new TaskID("", 0, TaskType.MAP, 0);
    }

    @Override
    public TaskAttemptID createTaskAttemptID() {
      return new TaskAttemptID("", 0, TaskType.MAP, 0, 0);
    }

    @Override
    public org.apache.hadoop.mapreduce.TaskAttemptContext createTaskAttemptContext(Configuration conf,
                                                                                   org.apache.hadoop.mapreduce.TaskAttemptID taskId) {
      return new org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl(
              conf instanceof JobConf? new JobConf(conf) : conf,
              taskId);
    }

    @Override
    public org.apache.hadoop.mapred.TaskAttemptContext createTaskAttemptContext(org.apache.hadoop.mapred.JobConf conf,
                                                                                org.apache.hadoop.mapred.TaskAttemptID taskId, Progressable progressable) {
      org.apache.hadoop.mapred.TaskAttemptContext newContext = null;
      try {
        java.lang.reflect.Constructor<org.apache.hadoop.mapred.TaskAttemptContextImpl> construct = org.apache.hadoop.mapred.TaskAttemptContextImpl.class.getDeclaredConstructor(
                org.apache.hadoop.mapred.JobConf.class, org.apache.hadoop.mapred.TaskAttemptID.class,
                Reporter.class);
        construct.setAccessible(true);
        newContext = construct.newInstance(
                new JobConf(conf), taskId, progressable);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return newContext;
    }

    @Override
    public JobContext createJobContext(Configuration conf,
                                       JobID jobId) {
      return new JobContextImpl(conf instanceof JobConf? new JobConf(conf) : conf,
              jobId);
    }

    @Override
    public org.apache.hadoop.mapred.JobContext createJobContext(org.apache.hadoop.mapred.JobConf conf,
                                                                org.apache.hadoop.mapreduce.JobID jobId, Progressable progressable) {
      return new org.apache.hadoop.mapred.JobContextImpl(
              new JobConf(conf), jobId, progressable);
    }

    @Override
    public void commitJob(OutputFormat outputFormat, Job job) throws IOException {
      // Do nothing as this was fixed by MAPREDUCE-1447.
    }

    @Override
    public void abortJob(OutputFormat outputFormat, Job job) throws IOException {
      // Do nothing as this was fixed by MAPREDUCE-1447.
    }

    @Override
    public InetSocketAddress getResourceManagerAddress(Configuration conf) {
      String addr = conf.get("yarn.resourcemanager.address", "localhost:8032");

      return NetUtils.createSocketAddr(addr);
    }

    @Override
    public String getPropertyName(PropertyName name) {
      switch (name) {
        case CACHE_ARCHIVES:
          return MRJobConfig.CACHE_ARCHIVES;
        case CACHE_FILES:
          return MRJobConfig.CACHE_FILES;
        case CACHE_SYMLINK:
          return MRJobConfig.CACHE_SYMLINK;
        case CLASSPATH_ARCHIVES:
          return MRJobConfig.CLASSPATH_ARCHIVES;
        case CLASSPATH_FILES:
          return MRJobConfig.CLASSPATH_FILES;
      }

      return "";
    }

    @Override
    public boolean isFileInHDFS(FileSystem fs, Path path) throws IOException {
      // In case of viewfs we need to lookup where the actual file is to know the filesystem in use.
      // resolvePath is a sure shot way of knowing which file system the file is.
      return "hdfs".equals(fs.resolvePath(path).toUri().getScheme());
    }
  }
  @Override
  public WebHCatJTShim getWebHCatShim(Configuration conf, UserGroupInformation ugi) throws IOException {
    return new WebHCatJTShim23(conf, ugi);//this has state, so can't be cached
  }

  private static final class HdfsFileStatusWithIdImpl implements HdfsFileStatusWithId {
    private final LocatedFileStatus lfs;
    private final long fileId;

    public HdfsFileStatusWithIdImpl(LocatedFileStatus lfs, long fileId) {
      this.lfs = lfs;
      this.fileId = fileId;
    }

    @Override
    public FileStatus getFileStatus() {
      return lfs;
    }

    @Override
    public Long getFileId() {
      if (fileId == HdfsConstants.GRANDFATHER_INODE_ID) {
        return null;
      }
      return fileId;
    }
  }

  @Override
  public List<HdfsFileStatusWithId> listLocatedHdfsStatus(
      FileSystem fs, Path p, PathFilter filter) throws IOException {
    DistributedFileSystem dfs = ensureDfs(fs);
    DFSClient dfsc = dfs.getClient();
    final String src = p.toUri().getPath();
    DirectoryListing current = dfsc.listPaths(src,
        org.apache.hadoop.hdfs.protocol.HdfsFileStatus.EMPTY_NAME, true);
    if (current == null) { // the directory does not exist
      throw new FileNotFoundException("File " + p + " does not exist.");
    }
    final URI fsUri = fs.getUri();
    List<HdfsFileStatusWithId> result = new ArrayList<HdfsFileStatusWithId>(
        current.getPartialListing().length);
    while (current != null) {
      org.apache.hadoop.hdfs.protocol.HdfsFileStatus[] hfss = current.getPartialListing();
      for (int i = 0; i < hfss.length; ++i) {
        HdfsLocatedFileStatus next = (HdfsLocatedFileStatus)(hfss[i]);
        if (filter != null) {
          Path filterPath = next.getFullPath(p).makeQualified(fsUri, null);
          if (!filter.accept(filterPath)) continue;
        }
        LocatedFileStatus lfs = next.makeQualifiedLocated(fsUri, p);
        result.add(new HdfsFileStatusWithIdImpl(lfs, next.getFileId()));
      }
      current = current.hasMore() ? dfsc.listPaths(src, current.getLastName(), true) : null;
    }
    return result;
  }

  private DistributedFileSystem ensureDfs(FileSystem fs) {
    if (!(fs instanceof DistributedFileSystem)) {
      throw new UnsupportedOperationException("Only supported for DFS; got " + fs.getClass());
    }
    DistributedFileSystem dfs = (DistributedFileSystem)fs;
    return dfs;
  }

  @Override
  public BlockLocation[] getLocations(FileSystem fs,
                                      FileStatus status) throws IOException {
    if (status instanceof LocatedFileStatus) {
      return ((LocatedFileStatus) status).getBlockLocations();
    } else {
      return fs.getFileBlockLocations(status, 0, status.getLen());
    }
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
    stream.hflush();
  }

  static class ProxyFileSystem23 extends ProxyFileSystem {
    public ProxyFileSystem23(FileSystem fs) {
      super(fs);
    }
    public ProxyFileSystem23(FileSystem fs, URI uri) {
      super(fs, uri);
    }

    @Override
    public FutureDataInputStreamBuilder openFile(Path path) throws IOException, UnsupportedOperationException {
      return super.openFile(ProxyFileSystem23.super.swizzleParamPath(path));
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f)
      throws FileNotFoundException, IOException {
      return new RemoteIterator<LocatedFileStatus>() {
        private final RemoteIterator<LocatedFileStatus> stats =
            ProxyFileSystem23.super.listLocatedStatus(
                ProxyFileSystem23.super.swizzleParamPath(f));

        @Override
        public boolean hasNext() throws IOException {
          return stats.hasNext();
        }

        @Override
        public LocatedFileStatus next() throws IOException {
          LocatedFileStatus result = stats.next();
          return new LocatedFileStatus(
              ProxyFileSystem23.super.swizzleFileStatus(result, false),
              result.getBlockLocations());
        }
      };
    }

    /**
     * Proxy file system also needs to override the access() method behavior.
     * Cannot add Override annotation since FileSystem.access() may not exist in
     * the version of hadoop used to build Hive.
     */
    @Override
    public void access(Path path, FsAction action) throws AccessControlException,
        FileNotFoundException, IOException {
      Path underlyingFsPath = swizzleParamPath(path);
      FileStatus underlyingFsStatus = fs.getFileStatus(underlyingFsPath);
      try {
        if (accessMethod != null) {
            accessMethod.invoke(fs, underlyingFsPath, action);
        } else {
          // If the FS has no access() method, we can try DefaultFileAccess ..
          DefaultFileAccess.checkFileAccess(fs, underlyingFsStatus, action);
        }
      } catch (AccessControlException err) {
        throw err;
      } catch (FileNotFoundException err) {
        throw err;
      } catch (IOException err) {
        throw err;
      } catch (Exception err) {
        throw new RuntimeException(err.getMessage(), err);
      }
    }
  }

  @Override
  public FileSystem createProxyFileSystem(FileSystem fs, URI uri) {
    return new ProxyFileSystem23(fs, uri);
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
    return FileSystem.newInstance(uri, conf);
  }

  @Override
  public void getMergedCredentials(JobConf jobConf) throws IOException {
    jobConf.getCredentials().mergeAll(UserGroupInformation.getCurrentUser().getCredentials());
  }

  @Override
  public void mergeCredentials(JobConf dest, JobConf src) throws IOException {
    dest.getCredentials().mergeAll(src.getCredentials());
  }

  protected static final Method accessMethod;
  protected static final Method getPasswordMethod;

  static {
    Method m = null;
    try {
      m = FileSystem.class.getMethod("access", Path.class, FsAction.class);
    } catch (NoSuchMethodException err) {
      // This version of Hadoop does not support FileSystem.access().
    }
    accessMethod = m;

    try {
      m = Configuration.class.getMethod("getPassword", String.class);
    } catch (NoSuchMethodException err) {
      // This version of Hadoop does not support getPassword(), just retrieve password from conf.
      m = null;
    }
    getPasswordMethod = m;
  }

  @Override
  public void checkFileAccess(FileSystem fs, FileStatus stat, FsAction action)
      throws IOException, AccessControlException, Exception {
    try {
      if (accessMethod == null) {
        // Have to rely on Hive implementation of filesystem permission checks.
        DefaultFileAccess.checkFileAccess(fs, stat, action);
      } else {
        accessMethod.invoke(fs, stat.getPath(), action);
      }
    } catch (Exception err) {
      throw wrapAccessException(err);
    }
  }

  /**
   * If there is an AccessException buried somewhere in the chain of failures, wrap the original
   * exception in an AccessException. Othewise just return the original exception.
   */
  private static Exception wrapAccessException(Exception err) {
    final int maxDepth = 20;
    Throwable curErr = err;
    for (int idx = 0; curErr != null && idx < maxDepth; ++idx) {
      // fs.permission.AccessControlException removed by HADOOP-11356, but Hive users on older
      // Hadoop versions may still see this exception .. have to reference by name.
      if (curErr instanceof org.apache.hadoop.security.AccessControlException
          || curErr.getClass().getName().equals("org.apache.hadoop.fs.permission.AccessControlException")) {
        Exception newErr = new AccessControlException(curErr.getMessage());
        newErr.initCause(err);
        return newErr;
      }
      curErr = curErr.getCause();
    }
    return err;
  }

  @Override
  public String getPassword(Configuration conf, String name) throws IOException {
    if (getPasswordMethod == null) {
      // Just retrieve value from conf
      return conf.get(name);
    } else {
      try {
        char[] pw = (char[]) getPasswordMethod.invoke(conf, name);
        if (pw == null) {
          return null;
        }
        return new String(pw);
      } catch (Exception err) {
        throw new IOException(err.getMessage(), err);
      }
    }
  }

  @Override
  public boolean supportStickyBit() {
    return true;
  }

  @Override
  public boolean hasStickyBit(FsPermission permission) {
    return permission.getStickyBit();
  }

  @Override
  public boolean supportTrashFeature() {
    return true;
  }

  @Override
  public Path getCurrentTrashPath(Configuration conf, FileSystem fs) {
    TrashPolicy tp = TrashPolicy.getInstance(conf, fs, fs.getHomeDirectory());
    return tp.getCurrentTrashDir();
  }

  /**
   * Returns a shim to wrap KerberosName
   */
  @Override
  public KerberosNameShim getKerberosNameShim(String name) throws IOException {
    return new KerberosNameShim(name);
  }

  @Override
  public boolean isDirectory(FileStatus fileStatus) {
    return fileStatus.isDirectory();
  }

  /**
   * Shim for KerberosName
   */
  public static class KerberosNameShim implements HadoopShimsSecure.KerberosNameShim {

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

  public static class StoragePolicyShim implements HadoopShims.StoragePolicyShim {

    private final DistributedFileSystem dfs;

    public StoragePolicyShim(DistributedFileSystem fs) {
      this.dfs = fs;
    }

    @Override
    public void setStoragePolicy(Path path, StoragePolicyValue policy)
        throws IOException {
      switch (policy) {
      case MEMORY: {
        dfs.setStoragePolicy(path, HdfsConstants.MEMORY_STORAGE_POLICY_NAME);
        break;
      }
      case SSD: {
        dfs.setStoragePolicy(path, HdfsConstants.ALLSSD_STORAGE_POLICY_NAME);
        break;
      }
      case DEFAULT: {
        /* do nothing */
        break;
      }
      default:
        throw new IllegalArgumentException("Unknown storage policy " + policy);
      }
    }
  }


  @Override
  public HadoopShims.StoragePolicyShim getStoragePolicyShim(FileSystem fs) {
    if (!storagePolicy) {
      return null;
    }
    try {
      return new StoragePolicyShim((DistributedFileSystem) fs);
    } catch (ClassCastException ce) {
      return null;
    }
  }

  List<String> constructDistCpParams(List<Path> srcPaths, Path dst, Configuration conf) throws IOException {
    // -update and -delete are mandatory options for directory copy to work.
    List<String> params = constructDistCpDefaultParams(conf, dst.getFileSystem(conf),
            srcPaths.get(0).getFileSystem(conf), srcPaths);
    if (!params.contains("-delete")) {
      params.add("-delete");
    }
    for (Path src : srcPaths) {
      params.add(src.toString());
    }
    params.add(dst.toString());
    return params;
  }

  private List<String> constructDistCpDefaultParams(Configuration conf, FileSystem dstFs,
                                                    FileSystem sourceFs, List<Path> srcPaths) throws IOException {
    List<String> params = new ArrayList<String>();
    boolean needToAddPreserveOption = true;
    for (Map.Entry<String,String> entry : conf.getPropsWithPrefix(Utils.DISTCP_OPTIONS_PREFIX).entrySet()){
      String distCpOption = entry.getKey();
      String distCpVal = entry.getValue();
      if (distCpOption.startsWith("p")) {
        needToAddPreserveOption = false;
      }
      params.add("-" + distCpOption);
      if ((distCpVal != null) && (!distCpVal.isEmpty())){
        params.add(distCpVal);
      }
    }
    if (needToAddPreserveOption) {
      if (conf.getBoolean("dfs.xattr.supported.only.on.reserved.namespace", false)) {
        boolean shouldCopyXAttrs =  srcPaths.get(0).toUri().getPath().startsWith(RAW_RESERVED_VIRTUAL_PATH)
          && Utils.checkFileSystemXAttrSupport(sourceFs, new Path(RAW_RESERVED_VIRTUAL_PATH))
          && Utils.checkFileSystemXAttrSupport(dstFs, new Path(RAW_RESERVED_VIRTUAL_PATH));
        params.add(shouldCopyXAttrs ? "-pbx" : "-pb");
      } else {
        params.add((Utils.checkFileSystemXAttrSupport(dstFs)
          && Utils.checkFileSystemXAttrSupport(sourceFs)) ? "-pbx" : "-pb");
      }
    }
    if (!params.contains("-update")) {
      params.add("-update");
    }
    return params;
  }

  /**
   * Constructs the params to be passed for using snapshot based replication
   * @param srcPaths source paths
   * @param dst destination
   * @param sourceSnap starting snapshot
   * @param destSnap end snapshot
   * @param conf hive configuration
   * @param diff -diff or -rdiff
   * @return
   */
  List<String> constructDistCpWithSnapshotParams(List<Path> srcPaths, Path dst, String sourceSnap, String destSnap,
      Configuration conf, String diff) throws IOException {
    // Get the default distcp params
    List<String> params = constructDistCpDefaultParams(conf, dst.getFileSystem(conf),
            srcPaths.get(0).getFileSystem(conf), srcPaths);
    if (params.contains("-delete")) {
      params.remove("-delete");
    }

    // Add distCp snapshot diff parameters.

    // Add -diff or -rdiff
    params.add(diff);
    // Add the starting snapshot.
    params.add(sourceSnap);
    // Add the second snapshot
    params.add(destSnap);

    // Add the source paths.
    for (Path src : srcPaths) {
      params.add(src.toString());
    }
    // Add the target path.
    params.add(dst.toString());
    return params;
  }

  @Override
  public boolean runDistCpAs(List<Path> srcPaths, Path dst, Configuration conf,
                             UserGroupInformation proxyUser) throws IOException {
    try {
      return proxyUser.doAs(new PrivilegedExceptionAction<Boolean>() {
        @Override
        public Boolean run() throws Exception {
          return runDistCp(srcPaths, dst, conf);
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean runDistCp(List<Path> srcPaths, Path dst, Configuration conf) throws IOException {
    // Creates the command-line parameters for distcp
    List<String> params = constructDistCpParams(srcPaths, dst, conf);
    DistCp distcp = null;
    try {
      distcp = new DistCp(conf, null);
      distcp.getConf().setBoolean("mapred.mapper.new-api", true);

      // HIVE-13704 states that we should use run() instead of execute() due to a hadoop known issue
      // added by HADOOP-10459
      return runDistCpInternal(distcp, params) ==  0;
    } catch (Exception e) {
      throw new IOException("Cannot execute DistCp process: ", e);
    } finally {
      // Set the job id from distCp conf to the callers configuration.
      if (distcp != null) {
        String jobId = distcp.getConf().get(CONF_LABEL_DISTCP_JOB_ID);
        if (jobId != null) {
          conf.set(CONF_LABEL_DISTCP_JOB_ID, jobId);
        }
      }
    }
  }

  @Override
  public boolean runDistCpWithSnapshots(String oldSnapshot, String newSnapshot, List<Path> srcPaths, Path dst,
      boolean overwriteTarget, Configuration conf)
      throws IOException {
    List<String> params = constructDistCpWithSnapshotParams(srcPaths, dst, oldSnapshot, newSnapshot, conf, "-diff");
    try {
      DistCp distcp = new DistCp(conf, null);
      distcp.getConf().setBoolean("mapred.mapper.new-api", true);
      int returnCode = runDistCpInternal(distcp, params);
      if (returnCode == 0) {
        return true;
      } else if (returnCode == DistCpConstants.INVALID_ARGUMENT) {
        // Handling FileNotFoundException, if source got deleted, in that case we don't want to copy either, So it is
        // like a success case, we didn't had anything to copy and we copied nothing, so, we need not to fail.
        LOG.warn("Copy failed with INVALID_ARGUMENT for source: {} to target: {} snapshot1: {} snapshot2: {} "
            + "params: {}", srcPaths, dst, oldSnapshot, newSnapshot, params);
        return true;
      } else if (returnCode == DistCpConstants.UNKNOWN_ERROR && overwriteTarget) {
        // Check if this error is due to target modified.
        if (shouldRdiff(dst, conf, oldSnapshot, overwriteTarget)) {
          LOG.warn("Copy failed due to target modified. Attempting to restore back the target. source: {} target: {} "
              + "snapshot: {}", srcPaths, dst, oldSnapshot);
          List<String> rParams = constructDistCpWithSnapshotParams(srcPaths, dst, ".", oldSnapshot, conf, "-rdiff");
          DistCp rDistcp = new DistCp(conf, null);
          returnCode = runDistCpInternal(rDistcp, rParams);
          if (returnCode == 0) {
            LOG.info("Target restored to previous state.  source: {} target: {} snapshot: {}. Reattempting to copy.",
                srcPaths, dst, oldSnapshot);
            dst.getFileSystem(conf).deleteSnapshot(dst, oldSnapshot);
            dst.getFileSystem(conf).createSnapshot(dst, oldSnapshot);
            returnCode = runDistCpInternal(distcp, params);
            if (returnCode == 0) {
              return true;
            } else {
              LOG.error("Copy failed with after target restore for source: {} to target: {} snapshot1: {} snapshot2: "
                  + "{} params: {}. Return code: {}", srcPaths, dst, oldSnapshot, newSnapshot, params, returnCode);
              return false;
            }
          }
        }
      }
    } catch (Exception e) {
      throw new IOException("Cannot execute DistCp process: ", e);
    }
    return false;
  }

  protected int runDistCpInternal(DistCp distcp, List<String> params) {
    ensureMapReduceQueue(distcp.getConf());
    return distcp.run(params.toArray(new String[0]));
  }

  /**
   * This method ensures if there is an explicit tez.queue.name set, the hadoop shim will submit jobs
   * to the same yarn queue. This solves a security issue where e.g settings have the following values:
   * tez.queue.name=sample
   * hive.server2.tez.queue.access.check=true
   * In this case, when a query submits Tez DAGs, the tez client layer checks whether the end user has access to
   * the yarn queue 'sample' via YarnQueueHelper, but this is not respected in case of MR jobs that run
   * even if the query execution engine is Tez. E.g. an EXPORT TABLE can submit DistCp MR jobs at some stages when
   * certain criteria are met. We tend to restrict the setting of mapreduce.job.queuename in order to bypass this
   * security flaw, and even the default queue is unexpected if we explicitly set tez.queue.name.
   * Under the hood the desired behavior is to have DistCp jobs in the same yarn queue as other parts
   * of the query. Most of the time, the user isn't aware that a query involves DistCp jobs, hence isn't aware
   * of these details.
   */
  protected void ensureMapReduceQueue(Configuration conf) {
    String queueName = conf.get(TezConfiguration.TEZ_QUEUE_NAME);
    boolean isTez = "tez".equalsIgnoreCase(conf.get("hive.execution.engine"));
    boolean shouldMapredJobsFollowTezQueue = conf.getBoolean("hive.mapred.job.follow.tez.queue", false);

    LOG.debug("Checking tez.queue.name {}, isTez: {}, shouldMapredJobsFollowTezQueue: {}", queueName, isTez,
        shouldMapredJobsFollowTezQueue);
    if (isTez && shouldMapredJobsFollowTezQueue && queueName != null && queueName.length() > 0) {
      LOG.info("Setting mapreduce.job.queuename (current: '{}') to become tez.queue.name: '{}'",
          conf.get(MRJobConfig.QUEUE_NAME), queueName);
      conf.set(MRJobConfig.QUEUE_NAME, queueName);
    }
  }

  /**
   * Checks wether reverse diff on the snapshot should be performed or not.
   * @param p path where snapshot exists.
   * @param conf the hive configuration.
   * @param snapshot the name of snapshot.
   * @param overwriteTarget whether overwriting target is enabled.
   * @return true, if we need to do rdiff.
   */
  private static boolean shouldRdiff(Path p, Configuration conf, String snapshot, boolean overwriteTarget) throws Exception {
    // Using the configuration in string form since hive-shims doesn't have a dependency on hive-common.
    boolean targetModified = false;
    try {
      DistributedFileSystem dfs = (DistributedFileSystem) p.getFileSystem(conf);
      // Check if the target got modified
      SnapshotDiffReportListing snapshotDiff = dfs.getClient()
          .getSnapshotDiffReportListing(p.toUri().getPath(), snapshot, "", DFSUtilClient.EMPTY_BYTES, -1);

      // If there is even a single entry in these list, we can conclude that the target is modified, and we need to
      // do a reverse diff.
      if (snapshotDiff.getCreateList() != null && !snapshotDiff.getCreateList().isEmpty()) {
        targetModified = true;
      }
      if (snapshotDiff.getModifyList() != null && !snapshotDiff.getModifyList().isEmpty()) {
        targetModified = true;
      }
      if (snapshotDiff.getDeleteList() != null && !snapshotDiff.getDeleteList().isEmpty()) {
        targetModified = true;
      }
    } catch (Exception e) {
      LOG.error("Failed to compute snapshot diff for path: {} and snapshot: {}", p, snapshot);
    }
    if (targetModified && !overwriteTarget) {
      throw new Exception(
          "The target modified during snapshot based data copy for path: " + p + " and snapshot: " + snapshot);
    }
    return targetModified;
  }

  public boolean runDistCpWithSnapshotsAs(String oldSnapshot, String newSnapshot, List<Path> srcPaths, Path dst,
      boolean overwriteTarget, UserGroupInformation proxyUser, Configuration conf) throws IOException {
    try {
      return proxyUser.doAs(new PrivilegedExceptionAction<Boolean>() {
        @Override
        public Boolean run() throws Exception {
          return runDistCpWithSnapshots(oldSnapshot, newSnapshot, srcPaths, dst, overwriteTarget, conf);
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private static Boolean hdfsEncryptionSupport;

  @SuppressFBWarnings(value = "LI_LAZY_INIT_STATIC", justification = "All threads set the same value despite data race")
  public static boolean isHdfsEncryptionSupported() {
    if (hdfsEncryptionSupport == null) {
      Method m = null;

      try {
        m = HdfsAdmin.class.getMethod("getEncryptionZoneForPath", Path.class);
      } catch (NoSuchMethodException e) {
        // This version of Hadoop does not support HdfsAdmin.getEncryptionZoneForPath().
        // Hadoop 2.6.0 introduces this new method.
      }

      hdfsEncryptionSupport = (m != null);
    }

    return hdfsEncryptionSupport;
  }

  public static class HdfsEncryptionShim implements HadoopShims.HdfsEncryptionShim {
    private static final String HDFS_SECURITY_DEFAULT_CIPHER = "AES/CTR/NoPadding";

    /**
     * Gets information about HDFS encryption zones
     */
    private HdfsAdmin hdfsAdmin = null;

    /**
     * Used to compare encryption key strengths.
     */
    private KeyProvider keyProvider = null;

    private final Configuration conf;

    public HdfsEncryptionShim(URI uri, Configuration conf) throws IOException {
      this.conf = conf;
      this.hdfsAdmin = new HdfsAdmin(uri, conf);
      this.keyProvider = this.hdfsAdmin.getKeyProvider();
    }

    @Override
    public boolean isPathEncrypted(Path path) throws IOException {
      Path fullPath;
      if (path.isAbsolute()) {
        fullPath = path;
      } else {
        fullPath = path.getFileSystem(conf).makeQualified(path);
      }
      if (!isFileInHdfs(path.getFileSystem(conf), path)) {
        return false;
      }

      return (getEncryptionZoneForPath(fullPath) != null);
    }

    /**
     * Returns true if the given fs supports mount functionality. In general we
     * can have child file systems only in the case of mount fs like
     * ViewFsOverloadScheme or ViewDistributedFileSystem. Returns false if the
     * getChildFileSystems API returns null.
     */
    private boolean isMountedFs(FileSystem fs) {
      return fs.getChildFileSystems() != null;
    }

    private boolean isFileInHdfs(FileSystem fs, Path path) throws IOException {
      String hdfsScheme = "hdfs";
      boolean isHdfs = hdfsScheme.equalsIgnoreCase(path.toUri().getScheme());
      // The ViewHDFS supports that, any non-hdfs paths can be mounted as hdfs
      // paths. Here HDFSEncryptionShim actually works only for hdfs paths. But
      // in the case of ViewHDFS, paths can be with hdfs scheme, but they might
      // actually resolve to other fs.
      // ex: hdfs://ns1/test ---> o3fs://b.v.ozone1/test
      // So, we need to lookup where the actual file is to know the filesystem
      // in use. The resolvePath is a sure shot way of knowing which file system
      // the file is.
      if (isHdfs && isMountedFs(fs)) {
        isHdfs = hdfsScheme.equals(fs.resolvePath(path).toUri().getScheme());
      }
      return isHdfs;
    }

    public EncryptionZone getEncryptionZoneForPath(Path path) throws IOException {
      if (path.getFileSystem(conf).exists(path)) {
        return hdfsAdmin.getEncryptionZoneForPath(path);
      } else if (!path.getParent().equals(path)) {
        return getEncryptionZoneForPath(path.getParent());
      } else {
        return null;
       }
    }

    @Override
    public boolean arePathsOnSameEncryptionZone(Path path1, Path path2) throws IOException {
      return equivalentEncryptionZones(getEncryptionZoneForPath(path1),
                                       getEncryptionZoneForPath(path2));
    }

    private boolean equivalentEncryptionZones(EncryptionZone zone1, EncryptionZone zone2) {
      if (zone1 == null && zone2 == null) {
        return true;
      } else if (zone1 == null || zone2 == null) {
        return false;
      }

      return zone1.equals(zone2);
    }

    @Override
    public boolean arePathsOnSameEncryptionZone(Path path1, Path path2,
                                                HadoopShims.HdfsEncryptionShim encryptionShim2) throws IOException {
      if (!(encryptionShim2 instanceof Hadoop23Shims.HdfsEncryptionShim)) {
        LOG.warn("EncryptionShim for path2 (" + path2 + ") is of unexpected type: " + encryptionShim2.getClass()
            + ". Assuming path2 is on the same EncryptionZone as path1(" + path1 + ").");
        return true;
      }

      return equivalentEncryptionZones(hdfsAdmin.getEncryptionZoneForPath(path1),
          ((HdfsEncryptionShim)encryptionShim2).hdfsAdmin.getEncryptionZoneForPath(path2));
    }

    /**
     * Compares two encryption key strengths.
     *
     * @param path1 First  path to compare
     * @param path2 Second path to compare
     * @param encryptionShim2 The encryption-shim corresponding to path2.
     * @return 1 if path1 is stronger; 0 if paths are equals; -1 if path1 is weaker.
     * @throws IOException If an error occurred attempting to get key metadata
     */
    @Override
    public int comparePathKeyStrength(Path path1, Path path2, HadoopShims.HdfsEncryptionShim encryptionShim2) throws IOException {
      EncryptionZone zone1, zone2;

      zone1 = getEncryptionZoneForPath(path1);
      zone2 = ((HdfsEncryptionShim)encryptionShim2).hdfsAdmin.getEncryptionZoneForPath(path2);

      if (zone1 == null && zone2 == null) {
        return 0;
      } else if (zone1 == null) {
        return -1;
      } else if (zone2 == null) {
        return 1;
      }

      return compareKeyStrength(zone1, zone2);
    }

    @Override
    public void createEncryptionZone(Path path, String keyName) throws IOException {
      hdfsAdmin.createEncryptionZone(path, keyName);
    }

    @Override
    public void createKey(String keyName, int bitLength)
      throws IOException, NoSuchAlgorithmException {

      checkKeyProvider();

      if (keyProvider.getMetadata(keyName) == null) {
        final KeyProvider.Options options = new Options(this.conf);
        options.setCipher(HDFS_SECURITY_DEFAULT_CIPHER);
        options.setBitLength(bitLength);
        keyProvider.createKey(keyName, options);
        keyProvider.flush();
      } else {
        throw new IOException("key '" + keyName + "' already exists");
      }
    }

    @Override
    public void deleteKey(String keyName) throws IOException {
      checkKeyProvider();

      if (keyProvider.getMetadata(keyName) != null) {
        keyProvider.deleteKey(keyName);
        keyProvider.flush();
      } else {
        throw new IOException("key '" + keyName + "' does not exist.");
      }
    }

    @Override
    public List<String> getKeys() throws IOException {
      checkKeyProvider();
      return keyProvider.getKeys();
    }

    private void checkKeyProvider() throws IOException {
      if (keyProvider == null) {
        throw new IOException("HDFS security key provider is not configured on your server.");
      }
    }

    /**
     * Compares two encryption key strengths.
     *
     * @param zone1 First  EncryptionZone to compare
     * @param zone2 Second EncryptionZone to compare
     * @return 1 if zone1 is stronger; 0 if zones are equal; -1 if zone1 is weaker.
     * @throws IOException If an error occurred attempting to get key metadata
     */
    private int compareKeyStrength(EncryptionZone zone1, EncryptionZone zone2) throws IOException {

      // zone1, zone2 should already have been checked for nulls.
      assert zone1 != null && zone2 != null : "Neither EncryptionZone under comparison can be null.";

      CipherSuite suite1 = zone1.getSuite();
      CipherSuite suite2 = zone2.getSuite();

      if (suite1 == null && suite2 == null) {
        return 0;
      } else if (suite1 == null) {
        return -1;
      } else if (suite2 == null) {
        return 1;
      }

      return Integer.compare(suite1.getAlgorithmBlockSize(), suite2.getAlgorithmBlockSize());
    }
  }

  @Override
  public HadoopShims.HdfsEncryptionShim createHdfsEncryptionShim(FileSystem fs, Configuration conf) throws IOException {
    if (isHdfsEncryptionSupported()) {
      URI uri = fs.getUri();
      if ("hdfs".equals(uri.getScheme()) && fs instanceof DistributedFileSystem) {
        return new HdfsEncryptionShim(uri, conf);
      }
    }

    return new HadoopShims.NoopHdfsEncryptionShim();
  }

  @Override
  public Path getPathWithoutSchemeAndAuthority(Path path) {
    return Path.getPathWithoutSchemeAndAuthority(path);
  }

  @Override
  public int readByteBuffer(FSDataInputStream file, ByteBuffer dest) throws IOException {
    int pos = dest.position();
    int result = file.read(dest);
    if (result > 0) {
      // Ensure this explicitly since versions before 2.7 read doesn't do it.
      dest.position(pos + result);
    }
    return result;
  }
  @Override
  public void addDelegationTokens(FileSystem fs, Credentials cred, String uname) throws IOException {
    // Use method addDelegationTokens instead of getDelegationToken to get all the tokens including KMS.
    fs.addDelegationTokens(uname, cred);
  }

  @Override
  public long getFileId(FileSystem fs, String path) throws IOException {
    HdfsFileStatus fileInfo = ensureDfs(fs).getClient().getFileInfo(path);
    if (fileInfo == null) {
      throw new FileNotFoundException(path + " does not exist.");
    }
    return fileInfo.getFileId();
  }


  private final static java.lang.reflect.Method getSubjectMethod;
  private final static java.lang.reflect.Constructor<UserGroupInformation> ugiCtor;
  private final static String ugiCloneError;
  static {
    Class<UserGroupInformation> clazz = UserGroupInformation.class;
    java.lang.reflect.Method method = null;
    java.lang.reflect.Constructor<UserGroupInformation> ctor;
    String error = null;
    try {
      method = clazz.getDeclaredMethod("getSubject");
      method.setAccessible(true);
      ctor = clazz.getDeclaredConstructor(Subject.class);
      ctor.setAccessible(true);
    } catch (Throwable t) {
      error = t.getMessage();
      method = null;
      ctor = null;
      LOG.error("Cannot create UGI reflection methods", t);
    }
    getSubjectMethod = method;
    ugiCtor = ctor;
    ugiCloneError = error;
  }

  @Override
  public UserGroupInformation cloneUgi(UserGroupInformation baseUgi) throws IOException {
    // Based on UserGroupInformation::createProxyUser.
    // TODO: use a proper method after we can depend on HADOOP-13081.
    if (getSubjectMethod == null) {
      throw new IOException("The UGI method was not found: " + ugiCloneError);
    }
    try {
      Subject origSubject = (Subject) getSubjectMethod.invoke(baseUgi);
      
      Subject subject = new Subject(false, origSubject.getPrincipals(),
          cloneCredentials(origSubject.getPublicCredentials()),
          cloneCredentials(origSubject.getPrivateCredentials()));
      return ugiCtor.newInstance(subject);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new IOException(e);
    }
  }

  private static Set<Object> cloneCredentials(Set<Object> old) {
    Set<Object> set = new HashSet<>();
    // Make sure Hadoop credentials objects do not reuse the maps.
    for (Object o : old) {
      set.add(o instanceof Credentials ? new Credentials((Credentials)o) : o);
    }
    return set;
  }
  
  private static Boolean hdfsErasureCodingSupport;

  /**
   * @return true if the runtime version of hdfs supports erasure coding
   */
  private static synchronized boolean isHdfsErasureCodingSupported() {
    if (hdfsErasureCodingSupport == null) {
      Method m = null;

      try {
        m = HdfsAdmin.class.getMethod("getErasureCodingPolicies");
      } catch (NoSuchMethodException e) {
        // This version of Hadoop does not support HdfsAdmin.getErasureCodingPolicies().
        // Hadoop 3.0.0 introduces this new method.
      }
      hdfsErasureCodingSupport = (m != null);
    }

    return hdfsErasureCodingSupport;
  }

  /**
   * Returns a new instance of the HdfsErasureCoding shim.
   *
   * @param fs a FileSystem object
   * @param conf a Configuration object
   * @return a new instance of the HdfsErasureCoding shim.
   * @throws IOException If an error occurred while creating the instance.
   */
  @Override
  public HadoopShims.HdfsErasureCodingShim createHdfsErasureCodingShim(FileSystem fs,
      Configuration conf) throws IOException {
    if (isHdfsErasureCodingSupported()) {
      URI uri = fs.getUri();
      if ("hdfs".equals(uri.getScheme())) {
        return new HdfsErasureCodingShim(uri, conf);
      }
    }
    return new HadoopShims.NoopHdfsErasureCodingShim();
  }

  /**
   * Information about an Erasure Coding Policy.
   */
  private static class HdfsFileErasureCodingPolicyImpl implements HdfsFileErasureCodingPolicy {
    private final String name;
    private final String status;

    HdfsFileErasureCodingPolicyImpl(String name, String status) {
      this.name = name;
      this.status = status;
    }

    HdfsFileErasureCodingPolicyImpl(String name) {
     this(name, null);
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public String getStatus() {
      return status;
    }
  }

  /**
   * This class encapsulates methods used to get Erasure Coding information from
   * HDFS paths in order to to provide commands similar to those provided by the hdfs ec command.
   * https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HDFSErasureCoding.html
   */
  public static class HdfsErasureCodingShim implements HadoopShims.HdfsErasureCodingShim {
    /**
     * Gets information about HDFS encryption zones.
     */
    private HdfsAdmin hdfsAdmin = null;

    private final Configuration conf;

    HdfsErasureCodingShim(URI uri, Configuration conf) throws IOException {
      this.conf = conf;
      this.hdfsAdmin = new HdfsAdmin(uri, conf);
    }

    /**
     * Lists all (enabled, disabled and removed) erasure coding policies registered in HDFS.
     * @return a list of erasure coding policies
     */
    @Override
    public List<HdfsFileErasureCodingPolicy> getAllErasureCodingPolicies() throws IOException {
      ErasureCodingPolicyInfo[] erasureCodingPolicies = hdfsAdmin.getErasureCodingPolicies();
      List<HdfsFileErasureCodingPolicy> policies = new ArrayList<>(erasureCodingPolicies.length);
      for (ErasureCodingPolicyInfo erasureCodingPolicy : erasureCodingPolicies) {
        policies.add(new HdfsFileErasureCodingPolicyImpl(erasureCodingPolicy.getPolicy().getName(),
            erasureCodingPolicy.getState().toString()));
      }
      return policies;
    }


    /**
     * Enable an erasure coding policy.
     * @param ecPolicyName the name of the erasure coding policy
     */
    @Override
    public void enableErasureCodingPolicy(String ecPolicyName)  throws IOException {
      hdfsAdmin.enableErasureCodingPolicy(ecPolicyName);
    }

    /**
     * Sets an erasure coding policy on a directory at the specified path.
     * @param path a directory in HDFS
     * @param ecPolicyName the name of the erasure coding policy
     */
    @Override
    public void setErasureCodingPolicy(Path path, String ecPolicyName) throws IOException {
      hdfsAdmin.setErasureCodingPolicy(path, ecPolicyName);
    }

    /**
     * Get details of the erasure coding policy of a file or directory at the specified path.
     * @param path an hdfs file or directory
     * @return an erasure coding policy
     */
    @Override
    public HdfsFileErasureCodingPolicy getErasureCodingPolicy(Path path) throws IOException {
      ErasureCodingPolicy erasureCodingPolicy = hdfsAdmin.getErasureCodingPolicy(path);
      if (erasureCodingPolicy == null) {
        return null;
      }
      return new HdfsFileErasureCodingPolicyImpl(erasureCodingPolicy.getName());
    }

    /**
     * Unset an erasure coding policy set by a previous call to setPolicy on a directory.
     * @param path a directory in HDFS
     */
    @Override
    public void unsetErasureCodingPolicy(Path path) throws IOException {
      hdfsAdmin.unsetErasureCodingPolicy(path);
    }

    /**
     * Remove an erasure coding policy.
     * @param ecPolicyName the name of the erasure coding policy
     */
    @Override
    public void removeErasureCodingPolicy(String ecPolicyName) throws IOException {
      hdfsAdmin.removeErasureCodingPolicy(ecPolicyName);
    }

    /**
     * Disable an erasure coding policy.
     * @param ecPolicyName the name of the erasure coding policy
     */
    @Override
    public void disableErasureCodingPolicy(String ecPolicyName) throws IOException {
      hdfsAdmin.disableErasureCodingPolicy(ecPolicyName);
    }

    /**
     * @return true if if the runtime MR stat for Erasure Coding is available.
     */
    @Override
    public boolean isMapReduceStatAvailable() {
      // Look for FileSystemCounter.BYTES_READ_EC, this is present in hadoop 3.2
      Field field = null;
      try {
        field = FileSystemCounter.class.getField("BYTES_READ_EC");
      } catch (NoSuchFieldException e) {
        // This version of Hadoop does not support EC stats for MR
      }
      return (field != null);
    }
  }
}
