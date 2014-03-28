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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.ProxyFileSystem;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.io.HiveIOExceptionHandlerUtil;
import org.apache.hadoop.hive.shims.HadoopShims.DirectDecompressorShim;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.TaskLogServlet;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.HadoopArchives;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.VersionInfo;

/**
 * Implemention of shims against Hadoop 0.20.0.
 */
public class Hadoop20Shims implements HadoopShims {

  /**
   * Returns a shim to wrap MiniMrCluster
   */
  public MiniMrShim getMiniMrCluster(Configuration conf, int numberOfTaskTrackers,
                                     String nameNode, int numDir) throws IOException {
    return new MiniMrShim(conf, numberOfTaskTrackers, nameNode, numDir);
  }

  @Override
  public MiniMrShim getMiniTezCluster(Configuration conf, int numberOfTaskTrackers,
                                     String nameNode, int numDir) throws IOException {
    throw new IOException("Cannot run tez on current hadoop, Version: " + VersionInfo.getVersion());
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

  public HadoopShims.CombineFileInputFormatShim getCombineFileInputFormat() {
    return new CombineFileInputFormatShim() {
      @Override
      public RecordReader getRecordReader(InputSplit split,
          JobConf job, Reporter reporter) throws IOException {
        throw new IOException("CombineFileInputFormat.getRecordReader not needed.");
      }
    };
  }

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

  public static class InputSplitShim extends CombineFileSplit implements HadoopShims.InputSplitShim {
    long shrinkedLength;
    boolean _isShrinked;
    public InputSplitShim() {
      super();
      _isShrinked = false;
    }

    public InputSplitShim(CombineFileSplit old) throws IOException {
      super(old.getJob(), old.getPaths(), old.getStartOffsets(),
          old.getLengths(), dedup(old.getLocations()));
      _isShrinked = false;
    }

    private static String[] dedup(String[] locations) {
      Set<String> dedup = new HashSet<String>();
      Collections.addAll(dedup, locations);
      return dedup.toArray(new String[dedup.size()]);
    }

    @Override
    public void shrinkSplit(long length) {
      _isShrinked = true;
      shrinkedLength = length;
    }

    public boolean isShrinked() {
      return _isShrinked;
    }

    public long getShrinkedLength() {
      return shrinkedLength;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      _isShrinked = in.readBoolean();
      if (_isShrinked) {
        shrinkedLength = in.readLong();
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      out.writeBoolean(_isShrinked);
      if (_isShrinked) {
        out.writeLong(shrinkedLength);
      }
    }
  }

  /* This class should be replaced with org.apache.hadoop.mapred.lib.CombineFileRecordReader class, once
   * https://issues.apache.org/jira/browse/MAPREDUCE-955 is fixed. This code should be removed - it is a copy
   * of org.apache.hadoop.mapred.lib.CombineFileRecordReader
   */
  public static class CombineFileRecordReader<K, V> implements RecordReader<K, V> {

    static final Class[] constructorSignature = new Class[] {
        InputSplit.class,
        Configuration.class,
        Reporter.class,
        Integer.class
        };

    protected CombineFileSplit split;
    protected JobConf jc;
    protected Reporter reporter;
    protected Class<RecordReader<K, V>> rrClass;
    protected Constructor<RecordReader<K, V>> rrConstructor;
    protected FileSystem fs;

    protected int idx;
    protected long progress;
    protected RecordReader<K, V> curReader;
    protected boolean isShrinked;
    protected long shrinkedLength;

    public boolean next(K key, V value) throws IOException {

      while ((curReader == null)
          || !doNextWithExceptionHandler((K) ((CombineHiveKey) key).getKey(),
              value)) {
        if (!initNextRecordReader(key)) {
          return false;
        }
      }
      return true;
    }

    public K createKey() {
      K newKey = curReader.createKey();
      return (K)(new CombineHiveKey(newKey));
    }

    public V createValue() {
      return curReader.createValue();
    }

    /**
     * Return the amount of data processed.
     */
    public long getPos() throws IOException {
      return progress;
    }

    public void close() throws IOException {
      if (curReader != null) {
        curReader.close();
        curReader = null;
      }
    }

    /**
     * Return progress based on the amount of data processed so far.
     */
    public float getProgress() throws IOException {
      long subprogress = 0;    // bytes processed in current split
      if (null != curReader) {
        // idx is always one past the current subsplit's true index.
        subprogress = (long)(curReader.getProgress() * split.getLength(idx - 1));
      }
      return Math.min(1.0f, (progress + subprogress) / (float) (split.getLength()));
    }

    /**
     * A generic RecordReader that can hand out different recordReaders
     * for each chunk in the CombineFileSplit.
     */
    public CombineFileRecordReader(JobConf job, CombineFileSplit split,
        Reporter reporter,
        Class<RecordReader<K, V>> rrClass)
        throws IOException {
      this.split = split;
      this.jc = job;
      this.rrClass = rrClass;
      this.reporter = reporter;
      this.idx = 0;
      this.curReader = null;
      this.progress = 0;

      isShrinked = false;

      assert (split instanceof InputSplitShim);
      if (((InputSplitShim) split).isShrinked()) {
        isShrinked = true;
        shrinkedLength = ((InputSplitShim) split).getShrinkedLength();
      }

      try {
        rrConstructor = rrClass.getDeclaredConstructor(constructorSignature);
        rrConstructor.setAccessible(true);
      } catch (Exception e) {
        throw new RuntimeException(rrClass.getName() +
            " does not have valid constructor", e);
      }
      initNextRecordReader(null);
    }

    /**
     * do next and handle exception inside it.
     * @param key
     * @param value
     * @return
     * @throws IOException
     */
    private boolean doNextWithExceptionHandler(K key, V value) throws IOException {
      try {
        return curReader.next(key, value);
      } catch (Exception e) {
        return HiveIOExceptionHandlerUtil.handleRecordReaderNextException(e, jc);
      }
    }

    /**
     * Get the record reader for the next chunk in this CombineFileSplit.
     */
    protected boolean initNextRecordReader(K key) throws IOException {

      if (curReader != null) {
        curReader.close();
        curReader = null;
        if (idx > 0) {
          progress += split.getLength(idx - 1); // done processing so far
        }
      }

      // if all chunks have been processed or reached the length, nothing more to do.
      if (idx == split.getNumPaths() || (isShrinked && progress > shrinkedLength)) {
        return false;
      }

      // get a record reader for the idx-th chunk
      try {
        curReader = rrConstructor.newInstance(new Object[]
            {split, jc, reporter, Integer.valueOf(idx)});

        // change the key if need be
        if (key != null) {
          K newKey = curReader.createKey();
          ((CombineHiveKey)key).setKey(newKey);
        }

        // setup some helper config variables.
        jc.set("map.input.file", split.getPath(idx).toString());
        jc.setLong("map.input.start", split.getOffset(idx));
        jc.setLong("map.input.length", split.getLength(idx));
      } catch (Exception e) {
        curReader=HiveIOExceptionHandlerUtil.handleRecordReaderCreationException(e, jc);
      }
      idx++;
      return true;
    }
  }

  public abstract static class CombineFileInputFormatShim<K, V> extends
      CombineFileInputFormat<K, V>
      implements HadoopShims.CombineFileInputFormatShim<K, V> {

    public Path[] getInputPathsShim(JobConf conf) {
      try {
        return FileInputFormat.getInputPaths(conf);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void createPool(JobConf conf, PathFilter... filters) {
      super.createPool(conf, filters);
    }

    @Override
    public InputSplitShim[] getSplits(JobConf job, int numSplits) throws IOException {
      long minSize = job.getLong("mapred.min.split.size", 0);

      // For backward compatibility, let the above parameter be used
      if (job.getLong("mapred.min.split.size.per.node", 0) == 0) {
        super.setMinSplitSizeNode(minSize);
      }

      if (job.getLong("mapred.min.split.size.per.rack", 0) == 0) {
        super.setMinSplitSizeRack(minSize);
      }

      if (job.getLong("mapred.max.split.size", 0) == 0) {
        super.setMaxSplitSize(minSize);
      }

      CombineFileSplit[] splits = (CombineFileSplit[]) super.getSplits(job, numSplits);

      InputSplitShim[] isplits = new InputSplitShim[splits.length];
      for (int pos = 0; pos < splits.length; pos++) {
        isplits[pos] = new InputSplitShim(splits[pos]);
      }

      return isplits;
    }

    public InputSplitShim getInputSplitShim() throws IOException {
      return new InputSplitShim();
    }

    public RecordReader getRecordReader(JobConf job, HadoopShims.InputSplitShim split,
        Reporter reporter,
        Class<RecordReader<K, V>> rrClass)
        throws IOException {
      CombineFileSplit cfSplit = (CombineFileSplit) split;
      return new CombineFileRecordReader(job, cfSplit, reporter, rrClass);
    }

  }

  public String getInputFormatClassName() {
    return "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat";
  }

  String[] ret = new String[2];

  @Override
  public int createHadoopArchive(Configuration conf, Path sourceDir, Path destDir,
      String archiveName) throws Exception {

    HadoopArchives har = new HadoopArchives(conf);
    List<String> args = new ArrayList<String>();

    args.add("-archiveName");
    args.add(archiveName);
    args.add(sourceDir.toString());
    args.add(destDir.toString());

    return ToolRunner.run(har, args.toArray(new String[0]));
  }

  /*
   *(non-Javadoc)
   * @see org.apache.hadoop.hive.shims.HadoopShims#getHarUri(java.net.URI, java.net.URI, java.net.URI)
   * This particular instance is for Hadoop 20 which creates an archive
   * with the entire directory path from which one created the archive as
   * compared against the one used by Hadoop 1.0 (within HadoopShimsSecure)
   * where a relative path is stored within the archive.
   */
  public URI getHarUri (URI original, URI base, URI originalBase)
    throws URISyntaxException {
    URI relative = null;

    String dirInArchive = original.getPath();
    if (dirInArchive.length() > 1 && dirInArchive.charAt(0) == '/') {
      dirInArchive = dirInArchive.substring(1);
    }

    relative = new URI(null, null, dirInArchive, null);

    return base.resolve(relative);
  }

  public static class NullOutputCommitter extends OutputCommitter {
    @Override
    public void setupJob(JobContext jobContext) { }
    @Override
    public void cleanupJob(JobContext jobContext) { }

    @Override
    public void setupTask(TaskAttemptContext taskContext) { }
    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext) {
      return false;
    }
    @Override
    public void commitTask(TaskAttemptContext taskContext) { }
    @Override
    public void abortTask(TaskAttemptContext taskContext) { }
  }

  public void prepareJobOutput(JobConf conf) {
    conf.setOutputCommitter(Hadoop20Shims.NullOutputCommitter.class);

    // option to bypass job setup and cleanup was introduced in hadoop-21 (MAPREDUCE-463)
    // but can be backported. So we disable setup/cleanup in all versions >= 0.19
    conf.setBoolean("mapred.committer.job.setup.cleanup.needed", false);

    // option to bypass task cleanup task was introduced in hadoop-23 (MAPREDUCE-2206)
    // but can be backported. So we disable setup/cleanup in all versions >= 0.19
    conf.setBoolean("mapreduce.job.committer.task.cleanup.needed", false);
  }

  @Override
  public UserGroupInformation getUGIForConf(Configuration conf) throws LoginException {
    UserGroupInformation ugi =
      UnixUserGroupInformation.readFromConf(conf, UnixUserGroupInformation.UGI_PROPERTY_NAME);
    if(ugi == null) {
      ugi = UserGroupInformation.login(conf);
    }
    return ugi;
  }

  @Override
  public boolean isSecureShimImpl() {
    return false;
  }

  @Override
  public String getShortUserName(UserGroupInformation ugi) {
    return ugi.getUserName();
  }

  @Override
  public String getTokenStrForm(String tokenSignature) throws IOException {
    throw new UnsupportedOperationException("Tokens are not supported in current hadoop version");
  }

  @Override
  public void setTokenStr(UserGroupInformation ugi, String tokenStr, String tokenService)
    throws IOException {
    throw new UnsupportedOperationException("Tokens are not supported in current hadoop version");
  }

  @Override
  public String addServiceToToken(String tokenStr, String tokenService) throws IOException {
    throw new UnsupportedOperationException("Tokens are not supported in current hadoop version");
  }


  @Override
  public <T> T doAs(UserGroupInformation ugi, PrivilegedExceptionAction<T> pvea) throws
    IOException, InterruptedException {
    try {
      return Subject.doAs(SecurityUtil.getSubject(ugi),pvea);
    } catch (PrivilegedActionException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Path createDelegationTokenFile(Configuration conf) throws IOException {
    throw new UnsupportedOperationException("Tokens are not supported in current hadoop version");
  }

  @Override
  public UserGroupInformation createRemoteUser(String userName, List<String> groupNames) {
    if (groupNames.isEmpty()) {
      groupNames = new ArrayList<String>();
      groupNames.add(userName);
    }
    return new UnixUserGroupInformation(userName, groupNames.toArray(new String[0]));
  }

  @Override
  public void loginUserFromKeytab(String principal, String keytabFile) throws IOException {
    throwKerberosUnsupportedError();
  }

  @Override
  public UserGroupInformation loginUserFromKeytabAndReturnUGI(
      String principal, String keytabFile) throws IOException {
    throwKerberosUnsupportedError();
    return null;
  }

  @Override
  public void reLoginUserFromKeytab() throws IOException{
    throwKerberosUnsupportedError();
  }

  @Override
  public boolean isLoginKeytabBased() throws IOException {
    return false;
  }

  private void throwKerberosUnsupportedError() throws UnsupportedOperationException{
    throw new UnsupportedOperationException("Kerberos login is not supported" +
        " in this hadoop version (" + VersionInfo.getVersion() + ")");
  }

  @Override
  public UserGroupInformation createProxyUser(String userName) throws IOException {
    return createRemoteUser(userName, null);
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
  public void hflush(FSDataOutputStream stream) throws IOException {
    stream.sync();
  }

  @Override
  public void authorizeProxyAccess(String proxyUser, UserGroupInformation realUserUgi,
      String ipAddress, Configuration conf) throws IOException {
    // This hadoop version doesn't have proxy verification
  }

  public boolean isSecurityEnabled() {
    return false;
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
  public String unquoteHtmlChars(String item) {
    return item;
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
  public void closeAllForUGI(UserGroupInformation ugi) {
    // No such functionality in ancient hadoop
    return;
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
  public String getTokenFileLocEnvName() {
    throw new UnsupportedOperationException(
        "Kerberos not supported in current hadoop version");
  }
  @Override
  public HCatHadoopShims getHCatShim() {
      throw new UnsupportedOperationException("HCatalog does not support Hadoop 0.20.x");
  }
  @Override
  public WebHCatJTShim getWebHCatShim(Configuration conf, UserGroupInformation ugi) throws IOException {
      throw new UnsupportedOperationException("WebHCat does not support Hadoop 0.20.x");
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
    ret.put("MAPREDMINSPLITSIZEPERRACK", "mapred.min.split.size.per.rack");
    ret.put("MAPREDMINSPLITSIZEPERNODE", "mapred.min.split.size.per.node");
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
}
