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
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.io.HiveIOExceptionHandlerUtil;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.hive.thrift.DelegationTokenSelector;
import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.hadoop.tools.HadoopArchives;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ToolRunner;

/**
 * Base implemention for shims against secure Hadoop 0.20.3/0.23.
 */
public abstract class HadoopShimsSecure implements HadoopShims {

  static final Log LOG = LogFactory.getLog(HadoopShimsSecure.class);

  @Override
  public String unquoteHtmlChars(String item) {
    return HtmlQuoting.unquoteHtmlChars(item);
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
      return Math.min(1.0f, progress / (float) (split.getLength()));
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
        return HiveIOExceptionHandlerUtil
            .handleRecordReaderNextException(e, jc);
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

      // if all chunks have been processed, nothing more to do.
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
        curReader = HiveIOExceptionHandlerUtil.handleRecordReaderCreationException(
            e, jc);
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
      long minSize = job.getLong(ShimLoader.getHadoopShims().getHadoopConfNames().get("MAPREDMINSPLITSIZE"), 0);

      // For backward compatibility, let the above parameter be used
      if (job.getLong(ShimLoader.getHadoopShims().getHadoopConfNames().get("MAPREDMINSPLITSIZEPERNODE"), 0) == 0) {
        super.setMinSplitSizeNode(minSize);
      }

      if (job.getLong(ShimLoader.getHadoopShims().getHadoopConfNames().get("MAPREDMINSPLITSIZEPERRACK"), 0) == 0) {
        super.setMinSplitSizeRack(minSize);
      }

      if (job.getLong(ShimLoader.getHadoopShims().getHadoopConfNames().get("MAPREDMAXSPLITSIZE"), 0) == 0) {
        super.setMaxSplitSize(minSize);
      }

      InputSplit[] splits = (InputSplit[]) super.getSplits(job, numSplits);

      InputSplitShim[] isplits = new InputSplitShim[splits.length];
      for (int pos = 0; pos < splits.length; pos++) {
        isplits[pos] = new InputSplitShim((CombineFileSplit)splits[pos]);
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
    args.add("-p");
    args.add(sourceDir.toString());
    args.add(destDir.toString());

    return ToolRunner.run(har, args.toArray(new String[0]));
  }

  /*
   * This particular instance is for Hadoop 1.0 which creates an archive
   * with only the relative path of the archived directory stored within
   * the archive as compared to the full path in case of earlier versions.
   * See this api in Hadoop20Shims for comparison.
   */
  public URI getHarUri(URI original, URI base, URI originalBase)
    throws URISyntaxException {
    URI relative = originalBase.relativize(original);
    if (relative.isAbsolute()) {
      throw new URISyntaxException("Couldn't create URI for location.",
                                   "Relative: " + relative + " Base: "
                                   + base + " OriginalBase: " + originalBase);
    }

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
    conf.setOutputCommitter(NullOutputCommitter.class);

    // option to bypass job setup and cleanup was introduced in hadoop-21 (MAPREDUCE-463)
    // but can be backported. So we disable setup/cleanup in all versions >= 0.19
    conf.setBoolean(ShimLoader.getHadoopShims().getHadoopConfNames().get("MAPREDSETUPCLEANUPNEEDED"), false);

    // option to bypass task cleanup task was introduced in hadoop-23 (MAPREDUCE-2206)
    // but can be backported. So we disable setup/cleanup in all versions >= 0.19
    conf.setBoolean(ShimLoader.getHadoopShims().getHadoopConfNames().get("MAPREDTASKCLEANUPNEEDED"), false);
  }

  @Override
  public UserGroupInformation getUGIForConf(Configuration conf) throws IOException {
    return UserGroupInformation.getCurrentUser();
  }

  @Override
  public boolean isSecureShimImpl() {
    return true;
  }

  @Override
  public String getShortUserName(UserGroupInformation ugi) {
    return ugi.getShortUserName();
  }

  @Override
  public String getTokenStrForm(String tokenSignature) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    TokenSelector<? extends TokenIdentifier> tokenSelector = new DelegationTokenSelector();

    Token<? extends TokenIdentifier> token = tokenSelector.selectToken(
        tokenSignature == null ? new Text() : new Text(tokenSignature), ugi.getTokens());
    return token != null ? token.encodeToUrlString() : null;
  }

  /**
   * Create a delegation token object for the given token string and service.
   * Add the token to given UGI
   */
  @Override
  public void setTokenStr(UserGroupInformation ugi, String tokenStr, String tokenService) throws IOException {
    Token<DelegationTokenIdentifier> delegationToken = createToken(tokenStr, tokenService);
    ugi.addToken(delegationToken);
  }

  /**
   * Add a given service to delegation token string.
   */
  @Override
  public String addServiceToToken(String tokenStr, String tokenService)
  throws IOException {
    Token<DelegationTokenIdentifier> delegationToken = createToken(tokenStr, tokenService);
    return delegationToken.encodeToUrlString();
  }

  /**
   * Create a new token using the given string and service
   * @param tokenStr
   * @param tokenService
   * @return
   * @throws IOException
   */
  private Token<DelegationTokenIdentifier> createToken(String tokenStr, String tokenService)
      throws IOException {
    Token<DelegationTokenIdentifier> delegationToken = new Token<DelegationTokenIdentifier>();
    delegationToken.decodeFromUrlString(tokenStr);
    delegationToken.setService(new Text(tokenService));
    return delegationToken;
  }

  @Override
  public <T> T doAs(UserGroupInformation ugi, PrivilegedExceptionAction<T> pvea) throws IOException, InterruptedException {
    return ugi.doAs(pvea);
  }

  @Override
  public Path createDelegationTokenFile(Configuration conf) throws IOException {

    //get delegation token for user
    String uname = UserGroupInformation.getLoginUser().getShortUserName();
    FileSystem fs = FileSystem.get(conf);
    Token<?> fsToken = fs.getDelegationToken(uname);

    File t = File.createTempFile("hive_hadoop_delegation_token", null);
    Path tokenPath = new Path(t.toURI());

    //write credential with token to file
    Credentials cred = new Credentials();
    cred.addToken(fsToken.getService(), fsToken);
    cred.writeTokenStorageFile(tokenPath, conf);

    return tokenPath;
  }

  @Override
  public UserGroupInformation createProxyUser(String userName) throws IOException {
    return UserGroupInformation.createProxyUser(
        userName, UserGroupInformation.getLoginUser());
  }

  @Override
  public void authorizeProxyAccess(String proxyUser, UserGroupInformation realUserUgi,
      String ipAddress,  Configuration conf) throws IOException {
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
    ProxyUsers.authorize(UserGroupInformation.createProxyUser(proxyUser, realUserUgi),
        ipAddress, conf);
  }

  @Override
  public boolean isSecurityEnabled() {
    return UserGroupInformation.isSecurityEnabled();
  }

  @Override
  public UserGroupInformation createRemoteUser(String userName, List<String> groupNames) {
    return UserGroupInformation.createRemoteUser(userName);
  }

  @Override
  public void closeAllForUGI(UserGroupInformation ugi) {
    try {
      FileSystem.closeAllForUGI(ugi);
    } catch (IOException e) {
      LOG.error("Could not clean up file-system handles for UGI: " + ugi, e);
    }
  }

  @Override
  public void loginUserFromKeytab(String principal, String keytabFile) throws IOException {
    String hostPrincipal = SecurityUtil.getServerPrincipal(principal, "0.0.0.0");
    UserGroupInformation.loginUserFromKeytab(hostPrincipal, keytabFile);
  }

  @Override
  public UserGroupInformation loginUserFromKeytabAndReturnUGI(
      String principal, String keytabFile) throws IOException {
    String hostPrincipal = SecurityUtil.getServerPrincipal(principal, "0.0.0.0");
    return UserGroupInformation.loginUserFromKeytabAndReturnUGI(hostPrincipal, keytabFile);
  }

  @Override
  public String getTokenFileLocEnvName() {
    return UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;
  }

  @Override
  public void reLoginUserFromKeytab() throws IOException{
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    //checkTGT calls ugi.relogin only after checking if it is close to tgt expiry
    //hadoop relogin is actually done only every x minutes (x=10 in hadoop 1.x)
    if(ugi.isFromKeytab()){
      ugi.checkTGTAndReloginFromKeytab();
    }
  }

  @Override
  public boolean isLoginKeytabBased() throws IOException {
    return UserGroupInformation.isLoginKeytabBased();
  }

  @Override
  abstract public JobTrackerState getJobTrackerState(ClusterStatus clusterStatus) throws Exception;

  @Override
  abstract public org.apache.hadoop.mapreduce.TaskAttemptContext newTaskAttemptContext(Configuration conf, final Progressable progressable);

  @Override
  abstract public org.apache.hadoop.mapreduce.JobContext newJobContext(Job job);

  @Override
  abstract public boolean isLocalMode(Configuration conf);

  @Override
  abstract public void setJobLauncherRpcAddress(Configuration conf, String val);

  @Override
  abstract public String getJobLauncherHttpAddress(Configuration conf);

  @Override
  abstract public String getJobLauncherRpcAddress(Configuration conf);

  @Override
  abstract public short getDefaultReplication(FileSystem fs, Path path);

  @Override
  abstract public long getDefaultBlockSize(FileSystem fs, Path path);

  @Override
  abstract public boolean moveToAppropriateTrash(FileSystem fs, Path path, Configuration conf)
          throws IOException;

  @Override
  abstract public FileSystem createProxyFileSystem(FileSystem fs, URI uri);
}
