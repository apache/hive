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
import java.net.MalformedURLException;
import java.security.PrivilegedExceptionAction;
import java.util.List;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
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

  static final Log LOG = LogFactory.getLog(HadoopShims.class);

  /**
   * Return true if the current version of Hadoop uses the JobShell for
   * command line interpretation.
   */
  boolean usesJobShell();

  /**
   * Constructs and Returns TaskAttempt Log Url
   * or null if the TaskLogServlet is not available
   *
   *  @return TaskAttempt Log Url
   */
  String getTaskAttemptLogUrl(JobConf conf,
    String taskTrackerHttpAddress,
    String taskAttemptId)
    throws MalformedURLException;

  /**
   * Return true if the job has not switched to RUNNING state yet
   * and is still in PREP state
   */
  boolean isJobPreparing(RunningJob job) throws IOException;

  /**
   * Calls fs.deleteOnExit(path) if such a function exists.
   *
   * @return true if the call was successful
   */
  boolean fileSystemDeleteOnExit(FileSystem fs, Path path) throws IOException;

  /**
   * Calls fmt.validateInput(conf) if such a function exists.
   */
  void inputFormatValidateInput(InputFormat fmt, JobConf conf) throws IOException;

  /**
   * If JobClient.getCommandLineConfig exists, sets the given
   * property/value pair in that Configuration object.
   *
   * This applies for Hadoop 0.17 through 0.19
   */
  void setTmpFiles(String prop, String files);

  /**
   * return the last access time of the given file.
   * @param file
   * @return last access time. -1 if not supported.
   */
  long getAccessTime(FileStatus file);

  /**
   * Returns a shim to wrap MiniDFSCluster. This is necessary since this class
   * was moved from org.apache.hadoop.dfs to org.apache.hadoop.hdfs
   */
  MiniDFSShim getMiniDfs(Configuration conf,
      int numDataNodes,
      boolean format,
      String[] racks) throws IOException;

  /**
   * Shim around the functions in MiniDFSCluster that Hive uses.
   */
  public interface MiniDFSShim {
    FileSystem getFileSystem() throws IOException;

    void shutdown() throws IOException;
  }

  /**
   * We define this function here to make the code compatible between
   * hadoop 0.17 and hadoop 0.20.
   *
   * Hive binary that compiled Text.compareTo(Text) with hadoop 0.20 won't
   * work with hadoop 0.17 because in hadoop 0.20, Text.compareTo(Text) is
   * implemented in org.apache.hadoop.io.BinaryComparable, and Java compiler
   * references that class, which is not available in hadoop 0.17.
   */
  int compareText(Text a, Text b);

  CombineFileInputFormatShim getCombineFileInputFormat();

  String getInputFormatClassName();

  /**
   * Wrapper for Configuration.setFloat, which was not introduced
   * until 0.20.
   */
  void setFloatConf(Configuration conf, String varName, float val);

  /**
   * getTaskJobIDs returns an array of String with two elements. The first
   * element is a string representing the task id and the second is a string
   * representing the job id. This is necessary as TaskID and TaskAttemptID
   * are not supported in Haddop 0.17
   */
  String[] getTaskJobIDs(TaskCompletionEvent t);

  int createHadoopArchive(Configuration conf, Path parentDir, Path destDir,
      String archiveName) throws Exception;
  /**
   * Hive uses side effect files exclusively for it's output. It also manages
   * the setup/cleanup/commit of output from the hive client. As a result it does
   * not need support for the same inside the MR framework
   *
   * This routine sets the appropriate options related to bypass setup/cleanup/commit
   * support in the MR framework, but does not set the OutputFormat class.
   */
  void prepareJobOutput(JobConf conf);

  /**
   * Used by TaskLogProcessor to Remove HTML quoting from a string
   * @param item the string to unquote
   * @return the unquoted string
   *
   */
  public String unquoteHtmlChars(String item);

  /**
   * Get the UGI that the given job configuration will run as.
   *
   * In secure versions of Hadoop, this simply returns the current
   * access control context's user, ignoring the configuration.
   */
  public UserGroupInformation getUGIForConf(Configuration conf) throws LoginException, IOException;

  /**
   * Used by metastore server to perform requested rpc in client context.
   * @param ugi
   * @param pvea
   * @throws IOException
   * @throws InterruptedException
   */
  public void doAs(UserGroupInformation ugi, PrivilegedExceptionAction<Void> pvea) throws
    IOException, InterruptedException;

  /**
   * Used by metastore server to creates UGI object for a remote user.
   * @param userName remote User Name
   * @param groupNames group names associated with remote user name
   * @return UGI created for the remote user.
   */

  public UserGroupInformation createRemoteUser(String userName, List<String> groupNames);
  /**
   * Get the short name corresponding to the subject in the passed UGI
   *
   * In secure versions of Hadoop, this returns the short name (after
   * undergoing the translation in the kerberos name rule mapping).
   * In unsecure versions of Hadoop, this returns the name of the subject
   */
  public String getShortUserName(UserGroupInformation ugi);

  /**
   * Return true if the Shim is based on Hadoop Security APIs.
   */
  public boolean isSecureShimImpl();

  /**
   * Get the string form of the token given a token signature.
   * The signature is used as the value of the "service" field in the token for lookup.
   * Ref: AbstractDelegationTokenSelector in Hadoop. If there exists such a token
   * in the token cache (credential store) of the job, the lookup returns that.
   * This is relevant only when running against a "secure" hadoop release
   * The method gets hold of the tokens if they are set up by hadoop - this should
   * happen on the map/reduce tasks if the client added the tokens into hadoop's
   * credential store in the front end during job submission. The method will
   * select the hive delegation token among the set of tokens and return the string
   * form of it
   * @param tokenSignature
   * @return the string form of the token found
   * @throws IOException
   */
  String getTokenStrForm(String tokenSignature) throws IOException;


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

  public JobContext newJobContext(Job job);

  /**
   * InputSplitShim.
   *
   */
  public interface InputSplitShim extends InputSplit {
    JobConf getJob();

    long getLength();

    /** Returns an array containing the startoffsets of the files in the split. */
    long[] getStartOffsets();

    /** Returns an array containing the lengths of the files in the split. */
    long[] getLengths();

    /** Returns the start offset of the i<sup>th</sup> Path. */
    long getOffset(int i);

    /** Returns the length of the i<sup>th</sup> Path. */
    long getLength(int i);

    /** Returns the number of Paths in the split. */
    int getNumPaths();

    /** Returns the i<sup>th</sup> Path. */
    Path getPath(int i);

    /** Returns all the Paths in the split. */
    Path[] getPaths();

    /** Returns all the Paths where this input-split resides. */
    String[] getLocations() throws IOException;

    void shrinkSplit(long length);

    String toString();

    void readFields(DataInput in) throws IOException;

    void write(DataOutput out) throws IOException;
  }

  /**
   * CombineFileInputFormatShim.
   *
   * @param <K>
   * @param <V>
   */
  interface CombineFileInputFormatShim<K, V> {
    Path[] getInputPathsShim(JobConf conf);

    void createPool(JobConf conf, PathFilter... filters);

    InputSplitShim[] getSplits(JobConf job, int numSplits) throws IOException;

    InputSplitShim getInputSplitShim() throws IOException;

    RecordReader getRecordReader(JobConf job, InputSplitShim split, Reporter reporter,
        Class<RecordReader<K, V>> rrClass) throws IOException;
  }
}
