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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.security.AccessControlException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProvider.Options;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.DefaultFileAccess;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.ProxyFileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.TrashPolicy;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.WebHCatJTShim23;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
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
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.test.MiniTezCluster;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

/**
 * Implemention of shims against Hadoop 0.23.0.
 */
public class Hadoop23Shims extends HadoopShimsSecure {

  HadoopShims.MiniDFSShim cluster = null;
  final boolean zeroCopy;
  final boolean storagePolicy;

  public Hadoop23Shims() {
    boolean zcr = false;
    boolean storage = false;
    try {
      Class.forName("org.apache.hadoop.fs.CacheFlag", false,
          ShimLoader.class.getClassLoader());
      zcr = true;
    } catch (ClassNotFoundException ce) {
    }
    
    if (zcr) {
      // in-memory HDFS is only available after zcr
      try {
        Class.forName("org.apache.hadoop.hdfs.protocol.BlockStoragePolicy",
            false, ShimLoader.class.getClassLoader());
        storage = true;
      } catch (ClassNotFoundException ce) {
      }
    }
    this.storagePolicy = storage;
    this.zeroCopy = zcr;
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
          if (!stat.isFile()) {
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
  public void startPauseMonitor(Configuration conf) {
    try {
      Class.forName("org.apache.hadoop.util.JvmPauseMonitor");
      org.apache.hadoop.util.JvmPauseMonitor pauseMonitor = new org.apache.hadoop.util
          .JvmPauseMonitor(conf);
      pauseMonitor.start();
    } catch (Throwable t) {
      LOG.warn("Could not initiate the JvmPauseMonitor thread." + " GCs and Pauses may not be " +
          "warned upon.", t);
    }
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

  protected boolean isExtendedAclEnabled(Configuration conf) {
    return Objects.equal(conf.get("dfs.namenode.acls.enabled"), "true");
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
  public boolean moveToAppropriateTrash(FileSystem fs, Path path, Configuration conf)
          throws IOException {
    return Trash.moveToAppropriateTrash(fs, path, conf);
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
    if (StringUtils.isNotBlank(userName) && isFairScheduler(conf)) {
      ShimLoader.getSchedulerShims().refreshDefaultQueue(conf, userName);
    }
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
    }
  }

  /**
   * Returns a shim to wrap MiniMrTez
   */
  @Override
  public MiniMrShim getMiniTezCluster(Configuration conf, int numberOfTaskTrackers,
                                     String nameNode, int numDir) throws IOException {
    return new MiniTezShim(conf, numberOfTaskTrackers, nameNode, numDir);
  }

  /**
   * Shim for MiniTezCluster
   */
  public class MiniTezShim extends Hadoop23Shims.MiniMrShim {

    private final MiniTezCluster mr;
    private final Configuration conf;

    public MiniTezShim(Configuration conf, int numberOfTaskTrackers,
                      String nameNode, int numDir) throws IOException {

      mr = new MiniTezCluster("hive", numberOfTaskTrackers);
      conf.set("fs.defaultFS", nameNode);
      conf.set("tez.am.log.level", "DEBUG");
      conf.set(MRJobConfig.MR_AM_STAGING_DIR, "/apps_staging_dir");
      mr.init(conf);
      mr.start();
      this.conf = mr.getConfig();
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
      for (Map.Entry<String, String> pair: config) {
        conf.set(pair.getKey(), pair.getValue());
      }

      Path jarPath = new Path("hdfs:///user/hive");
      Path hdfsPath = new Path("hdfs:///user/");
      try {
        FileSystem fs = cluster.getFileSystem();
        jarPath = fs.makeQualified(jarPath);
        conf.set("hive.jar.directory", jarPath.toString());
        fs.mkdirs(jarPath);
        hdfsPath = fs.makeQualified(hdfsPath);
        conf.set("hive.user.install.directory", hdfsPath.toString());
        fs.mkdirs(hdfsPath);
      } catch (Exception e) {
        e.printStackTrace();
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

  /**
   * Returns a shim to wrap MiniSparkOnYARNCluster
   */
  @Override
  public MiniMrShim getMiniSparkCluster(Configuration conf, int numberOfTaskTrackers,
    String nameNode, int numDir) throws IOException {
    return new MiniSparkShim(conf, numberOfTaskTrackers, nameNode, numDir);
  }

  /**
   * Shim for MiniSparkOnYARNCluster
   */
  public class MiniSparkShim extends Hadoop23Shims.MiniMrShim {

    private final MiniSparkOnYARNCluster mr;
    private final Configuration conf;

    public MiniSparkShim(Configuration conf, int numberOfTaskTrackers,
      String nameNode, int numDir) throws IOException {
      mr = new MiniSparkOnYARNCluster("sparkOnYarn");
      conf.set("fs.defaultFS", nameNode);
      conf.set("yarn.resourcemanager.scheduler.class", "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler");
      configureImpersonation(conf);
      mr.init(conf);
      mr.start();
      this.conf = mr.getConfig();
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

      Path jarPath = new Path("hdfs:///user/hive");
      Path hdfsPath = new Path("hdfs:///user/");
      try {
        FileSystem fs = cluster.getFileSystem();
        jarPath = fs.makeQualified(jarPath);
        conf.set("hive.jar.directory", jarPath.toString());
        fs.mkdirs(jarPath);
        hdfsPath = fs.makeQualified(hdfsPath);
        conf.set("hive.user.install.directory", hdfsPath.toString());
        fs.mkdirs(hdfsPath);
      } catch (Exception e) {
        e.printStackTrace();
      }
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
    configureImpersonation(conf);
    MiniDFSCluster miniDFSCluster = new MiniDFSCluster(conf, numDataNodes, format, racks);

    // Need to set the client's KeyProvider to the NN's for JKS,
    // else the updates do not get flushed properly
    KeyProviderCryptoExtension keyProvider =  miniDFSCluster.getNameNode().getNamesystem().getProvider();
    if (keyProvider != null) {
      miniDFSCluster.getFileSystem().getClient().setKeyProvider(keyProvider);
    }

    cluster = new MiniDFSShim(miniDFSCluster);
    return cluster;
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
      hcatShimInstance = new HCatHadoopShims23();
    }
    return hcatShimInstance;
  }
  private final class HCatHadoopShims23 implements HCatHadoopShims {
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
        java.lang.reflect.Constructor construct = org.apache.hadoop.mapred.TaskAttemptContextImpl.class.getDeclaredConstructor(
                org.apache.hadoop.mapred.JobConf.class, org.apache.hadoop.mapred.TaskAttemptID.class,
                Reporter.class);
        construct.setAccessible(true);
        newContext = (org.apache.hadoop.mapred.TaskAttemptContext) construct.newInstance(
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

  @Override
  public List<FileStatus> listLocatedStatus(final FileSystem fs,
                                            final Path path,
                                            final PathFilter filter
                                           ) throws IOException {
    RemoteIterator<LocatedFileStatus> itr = fs.listLocatedStatus(path);
    List<FileStatus> result = new ArrayList<FileStatus>();
    while(itr.hasNext()) {
      FileStatus stat = itr.next();
      if (filter == null || filter.accept(stat.getPath())) {
        result.add(stat);
      }
    }
    return result;
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

  @Override
  public HdfsFileStatus getFullFileStatus(Configuration conf, FileSystem fs,
      Path file) throws IOException {
    FileStatus fileStatus = fs.getFileStatus(file);
    AclStatus aclStatus = null;
    if (isExtendedAclEnabled(conf)) {
      //Attempt extended Acl operations only if its enabled, but don't fail the operation regardless.
      try {
        aclStatus = fs.getAclStatus(file);
      } catch (Exception e) {
        LOG.info("Skipping ACL inheritance: File system for path " + file + " " +
                "does not support ACLs but dfs.namenode.acls.enabled is set to true: " + e, e);
      }
    }
    return new Hadoop23FileStatus(fileStatus, aclStatus);
  }

  @Override
  public void setFullFileStatus(Configuration conf, HdfsFileStatus sourceStatus,
    FileSystem fs, Path target) throws IOException {
    String group = sourceStatus.getFileStatus().getGroup();
    //use FsShell to change group, permissions, and extended ACL's recursively
    try {
      FsShell fsShell = new FsShell();
      fsShell.setConf(conf);
      run(fsShell, new String[]{"-chgrp", "-R", group, target.toString()});

      if (isExtendedAclEnabled(conf)) {
        //Attempt extended Acl operations only if its enabled, 8791but don't fail the operation regardless.
        try {
          AclStatus aclStatus = ((Hadoop23FileStatus) sourceStatus).getAclStatus();
          if (aclStatus != null) {
            List<AclEntry> aclEntries = aclStatus.getEntries();
            removeBaseAclEntries(aclEntries);

            //the ACL api's also expect the tradition user/group/other permission in the form of ACL
            FsPermission sourcePerm = sourceStatus.getFileStatus().getPermission();
            aclEntries.add(newAclEntry(AclEntryScope.ACCESS, AclEntryType.USER, sourcePerm.getUserAction()));
            aclEntries.add(newAclEntry(AclEntryScope.ACCESS, AclEntryType.GROUP, sourcePerm.getGroupAction()));
            aclEntries.add(newAclEntry(AclEntryScope.ACCESS, AclEntryType.OTHER, sourcePerm.getOtherAction()));

            //construct the -setfacl command
            String aclEntry = Joiner.on(",").join(aclStatus.getEntries());
            run(fsShell, new String[]{"-setfacl", "-R", "--set", aclEntry, target.toString()});
          }
        } catch (Exception e) {
          LOG.info("Skipping ACL inheritance: File system for path " + target + " " +
                  "does not support ACLs but dfs.namenode.acls.enabled is set to true: " + e, e);
        }
      } else {
        String permission = Integer.toString(sourceStatus.getFileStatus().getPermission().toShort(), 8);
        run(fsShell, new String[]{"-chmod", "-R", permission, target.toString()});
      }
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

  public class Hadoop23FileStatus implements HdfsFileStatus {
    private final FileStatus fileStatus;
    private final AclStatus aclStatus;
    public Hadoop23FileStatus(FileStatus fileStatus, AclStatus aclStatus) {
      this.fileStatus = fileStatus;
      this.aclStatus = aclStatus;
    }
    @Override
    public FileStatus getFileStatus() {
      return fileStatus;
    }
    public AclStatus getAclStatus() {
      return aclStatus;
    }
    @Override
    public void debugLog() {
      if (fileStatus != null) {
        LOG.debug(fileStatus.toString());
      }
      if (aclStatus != null) {
        LOG.debug(aclStatus.toString());
      }
    }
  }

  /**
   * Create a new AclEntry with scope, type and permission (no name).
   *
   * @param scope
   *          AclEntryScope scope of the ACL entry
   * @param type
   *          AclEntryType ACL entry type
   * @param permission
   *          FsAction set of permissions in the ACL entry
   * @return AclEntry new AclEntry
   */
  private AclEntry newAclEntry(AclEntryScope scope, AclEntryType type,
      FsAction permission) {
    return new AclEntry.Builder().setScope(scope).setType(type)
        .setPermission(permission).build();
  }

  /**
   * Removes basic permission acls (unamed acls) from the list of acl entries
   * @param entries acl entries to remove from.
   */
  private void removeBaseAclEntries(List<AclEntry> entries) {
    Iterables.removeIf(entries, new Predicate<AclEntry>() {
      @Override
      public boolean apply(AclEntry input) {
          if (input.getName() == null) {
            return true;
          }
          return false;
      }
  });
  }

  class ProxyFileSystem23 extends ProxyFileSystem {
    public ProxyFileSystem23(FileSystem fs) {
      super(fs);
    }
    public ProxyFileSystem23(FileSystem fs, URI uri) {
      super(fs, uri);
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
  public Map<String, String> getHadoopConfNames() {
    Map<String, String> ret = new HashMap<String, String>();
    ret.put("HADOOPFS", "fs.defaultFS");
    ret.put("HADOOPMAPFILENAME", "mapreduce.map.input.file");
    ret.put("HADOOPMAPREDINPUTDIR", "mapreduce.input.fileinputformat.inputdir");
    ret.put("HADOOPMAPREDINPUTDIRRECURSIVE", "mapreduce.input.fileinputformat.input.dir.recursive");
    ret.put("MAPREDMAXSPLITSIZE", "mapreduce.input.fileinputformat.split.maxsize");
    ret.put("MAPREDMINSPLITSIZE", "mapreduce.input.fileinputformat.split.minsize");
    ret.put("MAPREDMINSPLITSIZEPERNODE", "mapreduce.input.fileinputformat.split.minsize.per.node");
    ret.put("MAPREDMINSPLITSIZEPERRACK", "mapreduce.input.fileinputformat.split.minsize.per.rack");
    ret.put("HADOOPNUMREDUCERS", "mapreduce.job.reduces");
    ret.put("HADOOPJOBNAME", "mapreduce.job.name");
    ret.put("HADOOPSPECULATIVEEXECREDUCERS", "mapreduce.reduce.speculative");
    ret.put("MAPREDSETUPCLEANUPNEEDED", "mapreduce.job.committer.setup.cleanup.needed");
    ret.put("MAPREDTASKCLEANUPNEEDED", "mapreduce.job.committer.task.cleanup.needed");
    return ret;
 }

  @Override
  public ZeroCopyReaderShim getZeroCopyReader(FSDataInputStream in, ByteBufferPoolShim pool) throws IOException {
    if(zeroCopy) {
      return ZeroCopyShims.getZeroCopyReader(in, pool);
    }
    /* not supported */
    return null;
  }

  @Override
  public DirectDecompressorShim getDirectDecompressor(DirectCompressionType codec) {
    if(zeroCopy) {
      return ZeroCopyShims.getDirectDecompressor(codec);
    }
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

  @Override
  public boolean runDistCp(Path src, Path dst, Configuration conf) throws IOException {
    int rc;

    // Creates the command-line parameters for distcp
    String[] params = {"-update", "-skipcrccheck", src.toString(), dst.toString()};

    try {
      Class clazzDistCp = Class.forName("org.apache.hadoop.tools.DistCp");
      Constructor c = clazzDistCp.getConstructor();
      c.setAccessible(true);
      Tool distcp = (Tool)c.newInstance();
      distcp.setConf(conf);
      rc = distcp.run(params);
    } catch (ClassNotFoundException e) {
      throw new IOException("Cannot find DistCp class package: " + e.getMessage());
    } catch (NoSuchMethodException e) {
      throw new IOException("Cannot get DistCp constructor: " + e.getMessage());
    } catch (Exception e) {
      throw new IOException("Cannot execute DistCp process: " + e, e);
    }

    return (0 == rc);
  }

  private static Boolean hdfsEncryptionSupport;

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

  public class HdfsEncryptionShim implements HadoopShims.HdfsEncryptionShim {
    private final String HDFS_SECURITY_DEFAULT_CIPHER = "AES/CTR/NoPadding";

    /**
     * Gets information about HDFS encryption zones
     */
    private HdfsAdmin hdfsAdmin = null;

    /**
     * Used to compare encryption key strengths.
     */
    private KeyProvider keyProvider = null;

    private Configuration conf;

    public HdfsEncryptionShim(URI uri, Configuration conf) throws IOException {
      DistributedFileSystem dfs = (DistributedFileSystem)FileSystem.get(uri, conf);

      this.conf = conf;
      this.keyProvider = dfs.getClient().getKeyProvider();
      this.hdfsAdmin = new HdfsAdmin(uri, conf);
    }

    @Override
    public boolean isPathEncrypted(Path path) throws IOException {
      Path fullPath;
      if (path.isAbsolute()) {
        fullPath = path;
      } else {
        fullPath = path.getFileSystem(conf).makeQualified(path);
      }
      if(!"hdfs".equalsIgnoreCase(path.toUri().getScheme())) {
        return false;
      }
      return (hdfsAdmin.getEncryptionZoneForPath(fullPath) != null);
    }

    @Override
    public boolean arePathsOnSameEncryptionZone(Path path1, Path path2) throws IOException {
      EncryptionZone zone1, zone2;

      zone1 = hdfsAdmin.getEncryptionZoneForPath(path1);
      zone2 = hdfsAdmin.getEncryptionZoneForPath(path2);

      if (zone1 == null && zone2 == null) {
        return true;
      } else if (zone1 == null || zone2 == null) {
        return false;
      }

      return zone1.equals(zone2);
    }

    @Override
    public int comparePathKeyStrength(Path path1, Path path2) throws IOException {
      EncryptionZone zone1, zone2;

      zone1 = hdfsAdmin.getEncryptionZoneForPath(path1);
      zone2 = hdfsAdmin.getEncryptionZoneForPath(path2);

      if (zone1 == null && zone2 == null) {
        return 0;
      } else if (zone1 == null) {
        return -1;
      } else if (zone2 == null) {
        return 1;
      }

      return compareKeyStrength(zone1.getKeyName(), zone2.getKeyName());
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
     * @param keyname1 Keyname to compare
     * @param keyname2 Keyname to compare
     * @return 1 if path1 is stronger; 0 if paths are equals; -1 if path1 is weaker.
     * @throws IOException If an error occurred attempting to get key metadata
     */
    private int compareKeyStrength(String keyname1, String keyname2) throws IOException {
      KeyProvider.Metadata meta1, meta2;

      if (keyProvider == null) {
        throw new IOException("HDFS security key provider is not configured on your server.");
      }

      meta1 = keyProvider.getMetadata(keyname1);
      meta2 = keyProvider.getMetadata(keyname2);

      if (meta1.getBitLength() < meta2.getBitLength()) {
        return -1;
      } else if (meta1.getBitLength() == meta2.getBitLength()) {
        return 0;
      } else {
        return 1;
      }
    }
  }

  @Override
  public HadoopShims.HdfsEncryptionShim createHdfsEncryptionShim(FileSystem fs, Configuration conf) throws IOException {
    if (isHdfsEncryptionSupported()) {
      URI uri = fs.getUri();
      if ("hdfs".equals(uri.getScheme())) {
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
  public void addDelegationTokens(FileSystem fs, Credentials cred, String uname) throws IOException {
    // Use method addDelegationTokens instead of getDelegationToken to get all the tokens including KMS.
    fs.addDelegationTokens(uname, cred);
  }
}
