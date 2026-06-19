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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.security.AccessControlException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobProfile;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;

/**
 * In order to be compatible with multiple versions of Hadoop, all parts
 * of the Hadoop interface that are not cross-version compatible are
 * encapsulated in an implementation of this class. Users should use
 * the ShimLoader class as a factory to obtain an implementation of
 * HadoopShims corresponding to the version of Hadoop currently on the
 * classpath.
 */
public interface HadoopShims {

  String USER_ID = "%s_User:%s";

  /**
   * Constructs and Returns TaskAttempt Logger Url
   * or null if the TaskLogServlet is not available
   *
   *  @return TaskAttempt Logger Url
   */
  String getTaskAttemptLogUrl(JobConf conf,
      String taskTrackerHttpAddress,
      String taskAttemptId)
          throws MalformedURLException;

  /**
   * Returns a shim to wrap MiniMrCluster
   */
  public MiniMrShim getMiniMrCluster(Configuration conf, int numberOfTaskTrackers,
      String nameNode, int numDir) throws IOException;

  public MiniMrShim getMiniTezCluster(Configuration conf, int numberOfTaskTrackers,
      String nameNode, boolean usingLlap) throws IOException;

  public MiniMrShim getLocalMiniTezCluster(Configuration conf, boolean usingLlap);


  /**
   * Set up the caller context for HDFS and Yarn.
   */
  void setHadoopCallerContext(String callerContext);

  /**
   * Set up context specific caller context with query prefix.
   */
  void setHadoopQueryContext(String queryId);

  /**
   * Set up context specific caller context with session prefix.
   */
  void setHadoopSessionContext(String sessionId);

  /**
   * Shim for MiniMrCluster
   */
  public interface MiniMrShim {
    public int getJobTrackerPort() throws UnsupportedOperationException;
    public void shutdown() throws IOException;
    public void setupConfiguration(Configuration conf);
  }

  /**
   * Returns a shim to wrap MiniDFSCluster. This is necessary since this class
   * was moved from org.apache.hadoop.dfs to org.apache.hadoop.hdfs
   */
  MiniDFSShim getMiniDfs(Configuration conf,
      int numDataNodes,
      boolean format,
      String[] racks) throws IOException;

  MiniDFSShim getMiniDfs(Configuration conf,
      int numDataNodes,
      boolean format,
      String[] racks,
      boolean isHA) throws IOException;

  /**
   * Shim around the functions in MiniDFSCluster that Hive uses.
   */
  public interface MiniDFSShim {
    FileSystem getFileSystem() throws IOException;

    void shutdown() throws IOException;
  }

  CombineFileInputFormatShim getCombineFileInputFormat();

  enum JobTrackerState { INITIALIZING, RUNNING };

  /**
   * Convert the ClusterStatus to its Thrift equivalent: JobTrackerState.
   * See MAPREDUCE-2455 for why this is a part of the shim.
   * @param clusterStatus
   * @return the matching JobTrackerState
   * @throws Exception if no equivalent JobTrackerState exists
   */
  public JobTrackerState getJobTrackerState(ClusterStatus clusterStatus) throws Exception;

  public TaskAttemptContext newTaskAttemptContext(Configuration conf, final Progressable progressable);

  public TaskAttemptID newTaskAttemptID(JobID jobId, boolean isMap, int taskId, int id);

  public JobContext newJobContext(Job job);

  /**
   * Check wether MR is configured to run in local-mode
   * @param conf
   * @return
   */
  public boolean isLocalMode(Configuration conf);

  /**
   * All retrieval of jobtracker/resource manager rpc address
   * in the configuration should be done through this shim
   * @param conf
   * @return
   */
  public String getJobLauncherRpcAddress(Configuration conf);

  /**
   * All updates to jobtracker/resource manager rpc address
   * in the configuration should be done through this shim
   * @param conf
   */
  public void setJobLauncherRpcAddress(Configuration conf, String val);

  /**
   * All references to jobtracker/resource manager http address
   * in the configuration should be done through this shim
   * @param conf
   * @return
   */
  public String getJobLauncherHttpAddress(Configuration conf);

  /**
   * Get the default block size for the path. FileSystem alone is not sufficient to
   * determine the same, as in case of CSMT the underlying file system determines that.
   * @param fs
   * @param path
   * @return
   */
  public long getDefaultBlockSize(FileSystem fs, Path path);

  /**
   * Get the default replication for a path. In case of CSMT the given path will be used to
   * locate the actual filesystem.
   * @param fs
   * @param path
   * @return
   */
  public short getDefaultReplication(FileSystem fs, Path path);

  /**
   * Reset the default fair scheduler queue mapping to end user.
   *
   * @param conf
   * @param userName end user name
   */
  public void refreshDefaultQueue(Configuration conf, String userName)
      throws IOException;

  /**
   * The method sets to set the partition file has a different signature between
   * hadoop versions.
   * @param jobConf
   * @param partition
   */
  void setTotalOrderPartitionFile(JobConf jobConf, Path partition);

  Comparator<LongWritable> getLongComparator();

  /**
   * CombineFileInputFormatShim.
   *
   * @param <K>
   * @param <V>
   */
  interface CombineFileInputFormatShim<K, V> {
    Path[] getInputPathsShim(JobConf conf);

    void createPool(JobConf conf, PathFilter... filters);

    CombineFileSplit[] getSplits(JobConf job, int numSplits) throws IOException;

    CombineFileSplit getInputSplitShim() throws IOException;

    RecordReader getRecordReader(JobConf job, CombineFileSplit split, Reporter reporter,
        Class<RecordReader<K, V>> rrClass) throws IOException;
  }

  /**
   * List a directory for file status with ID.
   *
   * @param fs The {@code FileSystem} to load the path
   * @param path The directory to list
   * @param filter A filter to use on the files in the directory
   * @return A list of file status with IDs
   * @throws IOException An I/O exception of some sort has occurred
   * @throws UnsupportedOperationException the {@code FileSystem} is not a
   *           {@code DistributedFileSystem}
   */
  List<HdfsFileStatusWithId> listLocatedHdfsStatus(
      FileSystem fs, Path path, PathFilter filter) throws IOException;

  /**
   * For file status returned by listLocatedStatus, convert them into a list
   * of block locations.
   * @param fs the file system
   * @param status the file information
   * @return the block locations of the file
   * @throws IOException
   */
  BlockLocation[] getLocations(FileSystem fs,
      FileStatus status) throws IOException;

  /**
   * For the block locations returned by getLocations() convert them into a Treemap
   * &lt;Offset,blockLocation&gt; by iterating over the list of blockLocation.
   * Using TreeMap from offset to blockLocation, makes it O(logn) to get a particular
   * block based upon offset.
   * @param fs the file system
   * @param status the file information
   * @return TreeMap&lt;Long, BlockLocation&gt;
   * @throws IOException
   */
  TreeMap<Long, BlockLocation> getLocationsWithOffset(FileSystem fs,
      FileStatus status) throws IOException;

  /**
   * Flush and make visible to other users the changes to the given stream.
   * @param stream the stream to hflush.
   * @throws IOException
   */
  public void hflush(FSDataOutputStream stream) throws IOException;

  public interface HdfsFileStatusWithId {
    public FileStatus getFileStatus();
    public Long getFileId();
  }

  public HCatHadoopShims getHCatShim();
  public interface HCatHadoopShims {

    enum PropertyName {CACHE_ARCHIVES, CACHE_FILES, CACHE_SYMLINK, CLASSPATH_ARCHIVES, CLASSPATH_FILES}

    public TaskID createTaskID();

    public TaskAttemptID createTaskAttemptID();

    public org.apache.hadoop.mapreduce.TaskAttemptContext createTaskAttemptContext(Configuration conf,
        TaskAttemptID taskId);

    public org.apache.hadoop.mapred.TaskAttemptContext createTaskAttemptContext(JobConf conf,
        org.apache.hadoop.mapred.TaskAttemptID taskId, Progressable progressable);

    public JobContext createJobContext(Configuration conf, JobID jobId);

    public org.apache.hadoop.mapred.JobContext createJobContext(JobConf conf, JobID jobId, Progressable progressable);

    public void commitJob(OutputFormat outputFormat, Job job) throws IOException;

    public void abortJob(OutputFormat outputFormat, Job job) throws IOException;

    /* Referring to job tracker in 0.20 and resource manager in 0.23 */
    public InetSocketAddress getResourceManagerAddress(Configuration conf);

    public String getPropertyName(PropertyName name);

    /**
     * Checks if file is in HDFS filesystem.
     *
     * @param fs
     * @param path
     * @return true if the file is in HDFS, false if the file is in other file systems.
     */
    public boolean isFileInHDFS(FileSystem fs, Path path) throws IOException;
  }
  /**
   * Provides a Hadoop JobTracker shim.
   * @param conf not {@code null}
   */
  public WebHCatJTShim getWebHCatShim(Configuration conf, UserGroupInformation ugi) throws IOException;
  public interface WebHCatJTShim {
    /**
     * Grab a handle to a job that is already known to the JobTracker.
     *
     * @return Profile of the job, or null if not found.
     */
    public JobProfile getJobProfile(org.apache.hadoop.mapred.JobID jobid) throws IOException;
    /**
     * Grab a handle to a job that is already known to the JobTracker.
     *
     * @return Status of the job, or null if not found.
     */
    public JobStatus getJobStatus(org.apache.hadoop.mapred.JobID jobid) throws IOException;
    /**
     * Kill a job.
     */
    public void killJob(org.apache.hadoop.mapred.JobID jobid) throws IOException;
    /**
     * Get all the jobs submitted.
     */
    public JobStatus[] getAllJobs() throws IOException;
    /**
     * Close the connection to the Job Tracker.
     */
    public void close();
    /**
     * Does exactly what org.apache.hadoop.mapreduce.Job#addCacheFile(URI) in Hadoop 2.
     * Assumes that both parameters are not {@code null}.
     */
    public void addCacheFile(URI uri, Job job);
    /**
     * Kills all jobs tagged with the given tag that have been started after the
     * given timestamp.
     */
    public void killJobs(String tag, long timestamp);
    /**
     * Returns all jobs tagged with the given tag that have been started after the
     * given timestamp. Returned jobIds are MapReduce JobIds.
     */
    public Set<String> getJobs(String tag, long timestamp);
  }

  /**
   * Create a proxy file system that can serve a given scheme/authority using some
   * other file system.
   */
  public FileSystem createProxyFileSystem(FileSystem fs, URI uri);

  /**
   * Create a shim for DFS storage policy.
   */

  public enum StoragePolicyValue {
    MEMORY, /* 1-replica memory */
    SSD, /* 3-replica ssd */
    DEFAULT /* system defaults (usually 3-replica disk) */;

    public static StoragePolicyValue lookup(String name) {
      if (name == null) {
        return DEFAULT;
      }
      return StoragePolicyValue.valueOf(name.toUpperCase().trim());
    }
  };

  public interface StoragePolicyShim {
    void setStoragePolicy(Path path, StoragePolicyValue policy) throws IOException;
  }

  /**
   *  obtain a storage policy shim associated with the filesystem.
   *  Returns null when the filesystem has no storage policies.
   */
  public StoragePolicyShim getStoragePolicyShim(FileSystem fs);

  /**
   * Get configuration from JobContext
   */
  public Configuration getConfiguration(JobContext context);

  /**
   * Get job conf from the old style JobContext.
   * @param context job context
   * @return job conf
   */
  public JobConf getJobConf(org.apache.hadoop.mapred.JobContext context);

  public FileSystem getNonCachedFileSystem(URI uri, Configuration conf) throws IOException;

  public void getMergedCredentials(JobConf jobConf) throws IOException;

  public void mergeCredentials(JobConf dest, JobConf src) throws IOException;

  /**
   * Check if the configured UGI has access to the path for the given file system action.
   * Method will return successfully if action is permitted. AccessControlExceptoin will
   * be thrown if user does not have access to perform the action. Other exceptions may
   * be thrown for non-access related errors.
   * @param fs
   * @param status
   * @param action
   * @throws IOException
   * @throws AccessControlException
   * @throws Exception
   */
  public void checkFileAccess(FileSystem fs, FileStatus status, FsAction action)
      throws IOException, AccessControlException, Exception;

  /**
   * Use password API (if available) to fetch credentials/password
   * @param conf
   * @param name
   * @return
   */
  public String getPassword(Configuration conf, String name) throws IOException;

  /**
   * check whether current hadoop supports sticky bit
   * @return
   */
  boolean supportStickyBit();

  /**
   * Check stick bit in the permission
   * @param permission
   * @return sticky bit
   */
  boolean hasStickyBit(FsPermission permission);

  /**
   * @return True if the current hadoop supports trash feature.
   */
  boolean supportTrashFeature();

  /**
   * @return Path to HDFS trash, if current hadoop supports trash feature.  Null otherwise.
   */
  Path getCurrentTrashPath(Configuration conf, FileSystem fs);

  /**
   * Check whether file is directory.
   */
  boolean isDirectory(FileStatus fileStatus);

  /**
   * Returns a shim to wrap KerberosName
   */
  public KerberosNameShim getKerberosNameShim(String name) throws IOException;

  /**
   * Shim for KerberosName
   */
  public interface KerberosNameShim {
    public String getDefaultRealm();
    public String getServiceName();
    public String getHostName();
    public String getRealm();
    public String getShortName() throws IOException;
  }

  /**
   * Copies a source dir/file to a destination by orchestrating the copy between hdfs nodes.
   * This distributed process is meant to copy huge files that could take some time if a single
   * copy is done. This is a variation which allows proxying as a different user to perform
   * the distcp, and requires that the caller have requisite proxy user privileges.
   *
   * @param srcPaths List of Path to the source files or directories to copy
   * @param dst Path to the destination file or directory
   * @param conf The hadoop configuration object
   * @param proxyUser The user to perform the distcp as
   * @return true if it is successful; false otherwise.
   */
  boolean runDistCpAs(List<Path> srcPaths, Path dst, Configuration conf, UserGroupInformation proxyUser)
          throws IOException;

  /**
   * Copies a source dir/file to a destination by orchestrating the copy between hdfs nodes.
   * This distributed process is meant to copy huge files that could take some time if a single
   * copy is done.
   *
   * @param srcPaths List of Path to the source files or directories to copy
   * @param dst Path to the destination file or directory
   * @param conf The hadoop configuration object
   * @return true if it is successful; false otherwise.
   */
  public boolean runDistCp(List<Path> srcPaths, Path dst, Configuration conf)
      throws IOException;

  /**
   * Copies a source dir/file to a destination by orchestrating the copy between hdfs nodes.
   * This distributed process is meant to copy huge files that could take some time if a single
   * copy is done. This method allows to specify usage of -diff feature of
   * distcp
   * @param oldSnapshot    initial snapshot
   * @param newSnapshot    final snapshot
   * @param srcPaths List of Path to the source files or directories to copy
   * @param dst      Path to the destination file or directory
   * @param overwriteTarget if true, in case the target is modified, restores back the target to the original state
   *                        and reattempt copying using snapshots.
   * @param conf     The hadoop configuration object
   * @return true if it is successful; false otherwise.
   */
  boolean runDistCpWithSnapshots(String oldSnapshot, String newSnapshot, List<Path> srcPaths, Path dst,
      boolean overwriteTarget, Configuration conf) throws IOException;

  /**
   * Copies a source dir/file to a destination by orchestrating the copy between hdfs nodes.
   * This distributed process is meant to copy huge files that could take some time if a single
   * copy is done. This method allows to specify usage of -diff feature of
   * distcp. This is a variation which allows proxying as a different
   * user to perform
   * the distcp, and requires that the caller have requisite proxy user privileges.
   * @param oldSnapshot     initial snapshot
   * @param newSnapshot     final snapshot
   * @param srcPaths  List of Path to the source files or directories to copy
   * @param dst       Path to the destination file or directory
   * @param overwriteTarget if true, in case the target is modified, restores back the target to the original state
   *                        and reattempt copying using snapshots.
   * @param proxyUser The user to perform the distcp as
   * @param conf      The hadoop configuration object
   * @return true if it is successful; false otherwise.
   */
  boolean runDistCpWithSnapshotsAs(String oldSnapshot, String newSnapshot, List<Path> srcPaths, Path dst,
      boolean overwriteTarget, UserGroupInformation proxyUser, Configuration conf) throws IOException;


  /**
   * This interface encapsulates methods used to get encryption information from
   * HDFS paths.
   */
  public interface HdfsEncryptionShim {
    /**
     * Checks if a given HDFS path is encrypted.
     *
     * @param path Path to HDFS file system
     * @return True if it is encrypted; False otherwise.
     * @throws IOException If an error occurred attempting to get encryption information
     */
    public boolean isPathEncrypted(Path path) throws IOException;

    /**
     * Checks if two HDFS paths are on the same encrypted or unencrypted zone.
     *
     * @param path1 Path to HDFS file system
     * @param path2 Path to HDFS file system
     * @return True if both paths are in the same zone; False otherwise.
     * @throws IOException If an error occurred attempting to get encryption information
     */
    public boolean arePathsOnSameEncryptionZone(Path path1, Path path2) throws IOException;

    /**
     * Checks if two HDFS paths are on the same encrypted or unencrypted zone.
     *
     * @param path1 Path to HDFS file system
     * @param path2 Path to HDFS file system
     * @param encryptionShim2 The encryption-shim corresponding to path2.
     * @return True if both paths are in the same zone; False otherwise.
     * @throws IOException If an error occurred attempting to get encryption information
     */
    public boolean arePathsOnSameEncryptionZone(Path path1, Path path2, HdfsEncryptionShim encryptionShim2) throws IOException;

    /**
     * Compares two encrypted path strengths.
     *
     * @param path1 HDFS path to compare.
     * @param path2 HDFS path to compare.
     * @param encryptionShim2 The encryption-shim corresponding to path2.
     * @return 1 if path1 is stronger; 0 if paths are equals; -1 if path1 is weaker.
     * @throws IOException If an error occurred attempting to get encryption/key metadata
     */
    public int comparePathKeyStrength(Path path1, Path path2, HdfsEncryptionShim encryptionShim2) throws IOException;

    /**
     * Create encryption zone by path and keyname.
     * @param path HDFS path to create encryption zone
     * @param keyName keyname
     * @throws IOException
     */
    @VisibleForTesting
    public void createEncryptionZone(Path path, String keyName) throws IOException;

    /**
     * Get encryption zone by path.
     * @param path HDFS path to create encryption zone.
     * @throws IOException
     */
    EncryptionZone getEncryptionZoneForPath(Path path) throws IOException;

    /**
     * Creates an encryption key.
     *
     * @param keyName Name of the key
     * @param bitLength Key encryption length in bits (128 or 256).
     * @throws IOException If an error occurs while creating the encryption key
     * @throws NoSuchAlgorithmException If cipher algorithm is invalid.
     */
    @VisibleForTesting
    public void createKey(String keyName, int bitLength)
      throws IOException, NoSuchAlgorithmException;

    @VisibleForTesting
    public void deleteKey(String keyName) throws IOException;

    @VisibleForTesting
    public List<String> getKeys() throws IOException;
  }

  /**
   * This is a dummy class used when the hadoop version does not support hdfs encryption.
   */
  public static class NoopHdfsEncryptionShim implements HdfsEncryptionShim {
    @Override
    public boolean isPathEncrypted(Path path) throws IOException {
    /* not supported */
      return false;
    }

    @Override
    public boolean arePathsOnSameEncryptionZone(Path path1, Path path2) throws IOException {
    /* not supported */
      return true;
    }

    @Override
    public boolean arePathsOnSameEncryptionZone(Path path1, Path path2, HdfsEncryptionShim encryptionShim2) throws IOException {
      // Not supported.
      return true;
    }

    @Override
    public int comparePathKeyStrength(Path path1, Path path2, HdfsEncryptionShim encryptionShim2) throws IOException {
    /* not supported */
      return 0;
    }

    @Override
    public void createEncryptionZone(Path path, String keyName) {
    /* not supported */
    }

    @Override
    public EncryptionZone getEncryptionZoneForPath(Path path) throws IOException {
      return null;
    }

    @Override
    public void createKey(String keyName, int bitLength) {
    /* not supported */
    }

    @Override
    public void deleteKey(String keyName) throws IOException {
    /* not supported */
    }

    @Override
    public List<String> getKeys() throws IOException{
    /* not supported */
      return null;
    }
  }

  /**
   * Returns a new instance of the HdfsEncryption shim.
   *
   * @param fs A FileSystem object to HDFS
   * @param conf A Configuration object
   * @return A new instance of the HdfsEncryption shim.
   * @throws IOException If an error occurred while creating the instance.
   */
  public HdfsEncryptionShim createHdfsEncryptionShim(FileSystem fs, Configuration conf) throws IOException;

  /**
   * Information about an Erasure Coding Policy.
   */
  interface HdfsFileErasureCodingPolicy {
    String getName();
    String getStatus();
  }

  /**
   * This interface encapsulates methods used to get Erasure Coding information from
   * HDFS paths in order to to provide commands similar to those provided by the hdfs ec command.
   * https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HDFSErasureCoding.html
   */
  interface HdfsErasureCodingShim {
    /**
     * Lists all (enabled, disabled and removed) erasure coding policies registered in HDFS.
     * @return a list of erasure coding policies
     */
    List<HdfsFileErasureCodingPolicy> getAllErasureCodingPolicies() throws IOException;

    /**
     * Enable an erasure coding policy.
     * @param ecPolicyName the name of the erasure coding policy
     */
    void enableErasureCodingPolicy(String ecPolicyName)  throws IOException;

    /**
     * Sets an erasure coding policy on a directory at the specified path.
     * @param path a directory in HDFS
     * @param ecPolicyName the name of the erasure coding policy
     */
    void setErasureCodingPolicy(Path path, String ecPolicyName) throws IOException;

    /**
     * Get details of the erasure coding policy of a file or directory at the specified path.
     * @param path an hdfs file or directory
     * @return an erasure coding policy
     */
    HdfsFileErasureCodingPolicy getErasureCodingPolicy(Path path) throws IOException;

    /**
     * Unset an erasure coding policy set by a previous call to setPolicy on a directory.
     * @param path a directory in HDFS
     */
    void unsetErasureCodingPolicy(Path path) throws IOException;

    /**
     * Remove an erasure coding policy.
     * @param ecPolicyName the name of the erasure coding policy
     */
    void removeErasureCodingPolicy(String ecPolicyName) throws IOException;

    /**
     * Disable an erasure coding policy.
     * @param ecPolicyName the name of the erasure coding policy
     */
    void disableErasureCodingPolicy(String ecPolicyName) throws IOException;

    /**
     * @return true if if the runtime MR stat for Erasure Coding is available.
     */
    boolean isMapReduceStatAvailable();
  }

  /**
   * This is a dummy class used when the hadoop version does not support hdfs Erasure Coding.
   */
  class NoopHdfsErasureCodingShim implements HadoopShims.HdfsErasureCodingShim {

    @Override
    public List<HadoopShims.HdfsFileErasureCodingPolicy> getAllErasureCodingPolicies() {
      return Collections.emptyList();
    }

    @Override
    public void enableErasureCodingPolicy(String ecPolicyName) throws IOException {
    }

    @Override
    public void setErasureCodingPolicy(Path path, String ecPolicyName) throws IOException {
    }

    @Override
    public HdfsFileErasureCodingPolicy getErasureCodingPolicy(Path path) throws IOException {
      return null;
    }

    @Override
    public void unsetErasureCodingPolicy(Path path) throws IOException {
    }

    @Override
    public void removeErasureCodingPolicy(String ecPolicyName) throws IOException {
    }

    @Override
    public void disableErasureCodingPolicy(String ecPolicyName) throws IOException {
    }

    @Override
    public boolean isMapReduceStatAvailable() {
      return false;
    }

  }

  /**
   * Returns a new instance of the HdfsErasureCoding shim.
   *
   * @param fs a FileSystem object
   * @param conf a Configuration object
   * @return a new instance of the HdfsErasureCoding shim.
   * @throws IOException If an error occurred while creating the instance.
   */
  HadoopShims.HdfsErasureCodingShim createHdfsErasureCodingShim(FileSystem fs, Configuration conf) throws IOException;

  public Path getPathWithoutSchemeAndAuthority(Path path);

  /**
   * Reads data into ByteBuffer.
   * @param file File.
   * @param dest Buffer.
   * @return Number of bytes read, just like file.read. If any bytes were read, dest position
   *         will be set to old position + number of bytes read.
   */
  int readByteBuffer(FSDataInputStream file, ByteBuffer dest) throws IOException;

  /**
   * Get Delegation token and add it to Credential.
   * @param fs FileSystem object to HDFS
   * @param cred Credentials object to add the token to.
   * @param uname user name.
   * @throws IOException If an error occurred on adding the token.
   */
  public void addDelegationTokens(FileSystem fs, Credentials cred, String uname) throws IOException;

  /**
   * Gets file ID. Only supported on hadoop-2.
   * @return inode ID of the file.
   */
  long getFileId(FileSystem fs, String path) throws IOException;

  /** Clones the UGI and the Subject. */
  UserGroupInformation cloneUgi(UserGroupInformation baseUgi) throws IOException;
}
