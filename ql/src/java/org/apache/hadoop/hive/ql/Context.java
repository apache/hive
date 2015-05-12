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

package org.apache.hadoop.hive.ql;

import java.io.DataInput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.antlr.runtime.TokenRewriteStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lockmgr.HiveLock;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockManager;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObj;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.util.StringUtils;

/**
 * Context for Semantic Analyzers. Usage: not reusable - construct a new one for
 * each query should call clear() at end of use to remove temporary folders
 */
public class Context {
  private boolean isHDFSCleanup;
  private Path resFile;
  private Path resDir;
  private FileSystem resFs;
  private static final Log LOG = LogFactory.getLog("hive.ql.Context");
  private Path[] resDirPaths;
  private int resDirFilesNum;
  boolean initialized;
  String originalTracker = null;
  private final Map<String, ContentSummary> pathToCS = new ConcurrentHashMap<String, ContentSummary>();

  // scratch path to use for all non-local (ie. hdfs) file system tmp folders
  private final Path nonLocalScratchPath;

  // scratch directory to use for local file system tmp folders
  private final String localScratchDir;

  // the permission to scratch directory (local and hdfs)
  private final String scratchDirPermission;

  // Keeps track of scratch directories created for different scheme/authority
  private final Map<String, Path> fsScratchDirs = new HashMap<String, Path>();

  private final Configuration conf;
  protected int pathid = 10000;
  protected boolean explain = false;
  protected String cboInfo;
  protected boolean cboSucceeded;
  protected boolean explainLogical = false;
  protected String cmd = "";
  // number of previous attempts
  protected int tryCount = 0;
  private TokenRewriteStream tokenRewriteStream;

  private String executionId;

  // List of Locks for this query
  protected List<HiveLock> hiveLocks;
  protected HiveLockManager hiveLockMgr;

  // Transaction manager for this query
  protected HiveTxnManager hiveTxnManager;

  // Used to track what type of acid operation (insert, update, or delete) we are doing.  Useful
  // since we want to change where bucket columns are accessed in some operators and
  // optimizations when doing updates and deletes.
  private AcidUtils.Operation acidOperation = AcidUtils.Operation.NOT_ACID;

  private boolean needLockMgr;

  // Keep track of the mapping from load table desc to the output and the lock
  private final Map<LoadTableDesc, WriteEntity> loadTableOutputMap =
      new HashMap<LoadTableDesc, WriteEntity>();
  private final Map<WriteEntity, List<HiveLockObj>> outputLockObjects =
      new HashMap<WriteEntity, List<HiveLockObj>>();

  private final String stagingDir;

  public Context(Configuration conf) throws IOException {
    this(conf, generateExecutionId());
  }

  /**
   * Create a Context with a given executionId.  ExecutionId, together with
   * user name and conf, will determine the temporary directory locations.
   */
  public Context(Configuration conf, String executionId)  {
    this.conf = conf;
    this.executionId = executionId;

    // local & non-local tmp location is configurable. however it is the same across
    // all external file systems
    nonLocalScratchPath = new Path(SessionState.getHDFSSessionPath(conf), executionId);
    localScratchDir = new Path(SessionState.getLocalSessionPath(conf), executionId).toUri().getPath();
    scratchDirPermission = HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCHDIRPERMISSION);
    stagingDir = HiveConf.getVar(conf, HiveConf.ConfVars.STAGINGDIR);
  }


  public Map<LoadTableDesc, WriteEntity> getLoadTableOutputMap() {
    return loadTableOutputMap;
  }

  public Map<WriteEntity, List<HiveLockObj>> getOutputLockObjects() {
    return outputLockObjects;
  }

  /**
   * Set the context on whether the current query is an explain query.
   * @param value true if the query is an explain query, false if not
   */
  public void setExplain(boolean value) {
    explain = value;
  }

  /**
   * Find whether the current query is an explain query
   * @return true if the query is an explain query, false if not
   */
  public boolean getExplain() {
    return explain;
  }

  /**
   * Find whether the current query is a logical explain query
   */
  public boolean getExplainLogical() {
    return explainLogical;
  }

  /**
   * Set the context on whether the current query is a logical
   * explain query.
   */
  public void setExplainLogical(boolean explainLogical) {
    this.explainLogical = explainLogical;
  }

  /**
   * Set the original query command.
   * @param cmd the original query command string
   */
  public void setCmd(String cmd) {
    this.cmd = cmd;
  }

  /**
   * Find the original query command.
   * @return the original query command string
   */
  public String getCmd () {
    return cmd;
  }

  /**
   * Gets a temporary staging directory related to a path.
   * If a path already contains a staging directory, then returns the current directory; otherwise
   * create the directory if needed.
   *
   * @param inputPath URI of the temporary directory
   * @param mkdir Create the directory if True.
   * @return A temporary path.
   */
  private Path getStagingDir(Path inputPath, boolean mkdir) {
    final URI inputPathUri = inputPath.toUri();
    final String inputPathName = inputPathUri.getPath();
    final String fileSystem = inputPathUri.getScheme() + ":" + inputPathUri.getAuthority();
    final FileSystem fs;

    try {
      fs = inputPath.getFileSystem(conf);
    } catch (IOException e) {
      throw new IllegalStateException("Error getting FileSystem for " + inputPath + ": "+ e, e);
    }

    String stagingPathName;
    if (inputPathName.indexOf(stagingDir) == -1) {
      stagingPathName = new Path(inputPathName, stagingDir).toString();
    } else {
      stagingPathName = inputPathName.substring(0, inputPathName.indexOf(stagingDir) + stagingDir.length());
    }

    final String key = fileSystem + "-" + stagingPathName + "-" + TaskRunner.getTaskRunnerID();

    Path dir = fsScratchDirs.get(key);
    if (dir == null) {
      // Append task specific info to stagingPathName, instead of creating a sub-directory.
      // This way we don't have to worry about deleting the stagingPathName separately at
      // end of query execution.
      dir = fs.makeQualified(new Path(stagingPathName + "_" + this.executionId + "-" + TaskRunner.getTaskRunnerID()));

      LOG.debug("Created staging dir = " + dir + " for path = " + inputPath);

      if (mkdir) {
        try {
          if (!FileUtils.mkdir(fs, dir, true, conf)) {
            throw new IllegalStateException("Cannot create staging directory  '" + dir.toString() + "'");
          }

          if (isHDFSCleanup) {
            fs.deleteOnExit(dir);
          }
        } catch (IOException e) {
          throw new RuntimeException("Cannot create staging directory '" + dir.toString() + "': " + e.getMessage(), e);
        }
      }

      fsScratchDirs.put(key, dir);
    }

    return dir;
  }

  /**
   * Get a tmp directory on specified URI
   *
   * @param scheme Scheme of the target FS
   * @param authority Authority of the target FS
   * @param mkdir create the directory if true
   * @param scratchDir path of tmp directory
   */
  private Path getScratchDir(String scheme, String authority,
      boolean mkdir, String scratchDir) {

    String fileSystem =  scheme + ":" + authority;
    Path dir = fsScratchDirs.get(fileSystem + "-" + TaskRunner.getTaskRunnerID());

    if (dir == null) {
      Path dirPath = new Path(scheme, authority,
          scratchDir + "-" + TaskRunner.getTaskRunnerID());
      if (mkdir) {
        try {
          FileSystem fs = dirPath.getFileSystem(conf);
          dirPath = new Path(fs.makeQualified(dirPath).toString());
          FsPermission fsPermission = new FsPermission(scratchDirPermission);

          if (!fs.mkdirs(dirPath, fsPermission)) {
            throw new RuntimeException("Cannot make directory: "
                + dirPath.toString());
          }
          if (isHDFSCleanup) {
            fs.deleteOnExit(dirPath);
          }
        } catch (IOException e) {
          throw new RuntimeException (e);
        }
      }
      dir = dirPath;
      fsScratchDirs.put(fileSystem + "-" + TaskRunner.getTaskRunnerID(), dir);

    }

    return dir;
  }


  /**
   * Create a local scratch directory on demand and return it.
   */
  public Path getLocalScratchDir(boolean mkdir) {
    try {
      FileSystem fs = FileSystem.getLocal(conf);
      URI uri = fs.getUri();
      return getScratchDir(uri.getScheme(), uri.getAuthority(),
          mkdir, localScratchDir);
    } catch (IOException e) {
      throw new RuntimeException (e);
    }
  }


  /**
   * Create a map-reduce scratch directory on demand and return it.
   *
   */
  public Path getMRScratchDir() {

    // if we are executing entirely on the client side - then
    // just (re)use the local scratch directory
    if(isLocalOnlyExecutionMode()) {
      return getLocalScratchDir(!explain);
    }

    try {
      Path dir = FileUtils.makeQualified(nonLocalScratchPath, conf);
      URI uri = dir.toUri();

      Path newScratchDir = getScratchDir(uri.getScheme(), uri.getAuthority(),
          !explain, uri.getPath());
      LOG.info("New scratch dir is " + newScratchDir);
      return newScratchDir;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("Error while making MR scratch "
          + "directory - check filesystem config (" + e.getCause() + ")", e);
    }
  }

  private Path getExternalScratchDir(URI extURI) {
    return getStagingDir(new Path(extURI.getScheme(), extURI.getAuthority(), extURI.getPath()), !explain);
  }

  /**
   * Remove any created scratch directories.
   */
  public void removeScratchDir() {
    for (Map.Entry<String, Path> entry : fsScratchDirs.entrySet()) {
      try {
        Path p = entry.getValue();
        p.getFileSystem(conf).delete(p, true);
      } catch (Exception e) {
        LOG.warn("Error Removing Scratch: "
            + StringUtils.stringifyException(e));
      }
    }
    fsScratchDirs.clear();
  }

  private String nextPathId() {
    return Integer.toString(pathid++);
  }


  private static final String MR_PREFIX = "-mr-";
  private static final String EXT_PREFIX = "-ext-";
  private static final String LOCAL_PREFIX = "-local-";

  /**
   * Check if path is for intermediate data
   * @return true if a uri is a temporary uri for map-reduce intermediate data,
   *         false otherwise
   */
  public boolean isMRTmpFileURI(String uriStr) {
    return (uriStr.indexOf(executionId) != -1) &&
        (uriStr.indexOf(MR_PREFIX) != -1);
  }

  public Path getMRTmpPath(URI uri) {
    return new Path(getStagingDir(new Path(uri), !explain), MR_PREFIX + nextPathId());
  }

  /**
   * Get a path to store map-reduce intermediate data in.
   *
   * @return next available path for map-red intermediate data
   */
  public Path getMRTmpPath() {
    return new Path(getMRScratchDir(), MR_PREFIX +
        nextPathId());
  }

  /**
   * Get a tmp path on local host to store intermediate data.
   *
   * @return next available tmp path on local fs
   */
  public Path getLocalTmpPath() {
    return new Path(getLocalScratchDir(true), LOCAL_PREFIX + nextPathId());
  }

  /**
   * Get a path to store tmp data destined for external Path.
   *
   * @param path external Path to which the tmp data has to be eventually moved
   * @return next available tmp path on the file system corresponding extURI
   */
  public Path getExternalTmpPath(Path path) {
    URI extURI = path.toUri();
    if (extURI.getScheme().equals("viewfs")) {
      // if we are on viewfs we don't want to use /tmp as tmp dir since rename from /tmp/..
      // to final /user/hive/warehouse/ will fail later, so instead pick tmp dir
      // on same namespace as tbl dir.
      return getExtTmpPathRelTo(path.getParent());
    }
    return new Path(getExternalScratchDir(extURI), EXT_PREFIX +
        nextPathId());
  }

  /**
   * This is similar to getExternalTmpPath() with difference being this method returns temp path
   * within passed in uri, whereas getExternalTmpPath() ignores passed in path and returns temp
   * path within /tmp
   */
  public Path getExtTmpPathRelTo(Path path) {
    return new Path(getStagingDir(path, !explain), EXT_PREFIX + nextPathId());
  }

  /**
   * @return the resFile
   */
  public Path getResFile() {
    return resFile;
  }

  /**
   * @param resFile
   *          the resFile to set
   */
  public void setResFile(Path resFile) {
    this.resFile = resFile;
    resDir = null;
    resDirPaths = null;
    resDirFilesNum = 0;
  }

  /**
   * @return the resDir
   */
  public Path getResDir() {
    return resDir;
  }

  /**
   * @param resDir
   *          the resDir to set
   */
  public void setResDir(Path resDir) {
    this.resDir = resDir;
    resFile = null;

    resDirFilesNum = 0;
    resDirPaths = null;
  }

  public void clear() throws IOException {
    if (resDir != null) {
      try {
        FileSystem fs = resDir.getFileSystem(conf);
        fs.delete(resDir, true);
      } catch (IOException e) {
        LOG.info("Context clear error: " + StringUtils.stringifyException(e));
      }
    }

    if (resFile != null) {
      try {
        FileSystem fs = resFile.getFileSystem(conf);
        fs.delete(resFile, false);
      } catch (IOException e) {
        LOG.info("Context clear error: " + StringUtils.stringifyException(e));
      }
    }
    removeScratchDir();
    originalTracker = null;
    setNeedLockMgr(false);
  }

  public DataInput getStream() {
    try {
      if (!initialized) {
        initialized = true;
        if ((resFile == null) && (resDir == null)) {
          return null;
        }

        if (resFile != null) {
          return resFile.getFileSystem(conf).open(resFile);
        }

        resFs = resDir.getFileSystem(conf);
        FileStatus status = resFs.getFileStatus(resDir);
        assert status.isDir();
        FileStatus[] resDirFS = resFs.globStatus(new Path(resDir + "/*"), FileUtils.HIDDEN_FILES_PATH_FILTER);
        resDirPaths = new Path[resDirFS.length];
        int pos = 0;
        for (FileStatus resFS : resDirFS) {
          if (!resFS.isDir()) {
            resDirPaths[pos++] = resFS.getPath();
          }
        }
        if (pos == 0) {
          return null;
        }

        return resFs.open(resDirPaths[resDirFilesNum++]);
      } else {
        return getNextStream();
      }
    } catch (FileNotFoundException e) {
      LOG.info("getStream error: " + StringUtils.stringifyException(e));
      return null;
    } catch (IOException e) {
      LOG.info("getStream error: " + StringUtils.stringifyException(e));
      return null;
    }
  }

  private DataInput getNextStream() {
    try {
      if (resDir != null && resDirFilesNum < resDirPaths.length
          && (resDirPaths[resDirFilesNum] != null)) {
        return resFs.open(resDirPaths[resDirFilesNum++]);
      }
    } catch (FileNotFoundException e) {
      LOG.info("getNextStream error: " + StringUtils.stringifyException(e));
      return null;
    } catch (IOException e) {
      LOG.info("getNextStream error: " + StringUtils.stringifyException(e));
      return null;
    }

    return null;
  }

  public void resetStream() {
    if (initialized) {
      resDirFilesNum = 0;
      initialized = false;
    }
  }

  /**
   * Little abbreviation for StringUtils.
   */
  private static boolean strEquals(String str1, String str2) {
    return org.apache.commons.lang.StringUtils.equals(str1, str2);
  }

  /**
   * Set the token rewrite stream being used to parse the current top-level SQL
   * statement. Note that this should <b>not</b> be used for other parsing
   * activities; for example, when we encounter a reference to a view, we switch
   * to a new stream for parsing the stored view definition from the catalog,
   * but we don't clobber the top-level stream in the context.
   *
   * @param tokenRewriteStream
   *          the stream being used
   */
  public void setTokenRewriteStream(TokenRewriteStream tokenRewriteStream) {
    assert (this.tokenRewriteStream == null);
    this.tokenRewriteStream = tokenRewriteStream;
  }

  /**
   * @return the token rewrite stream being used to parse the current top-level
   *         SQL statement, or null if it isn't available (e.g. for parser
   *         tests)
   */
  public TokenRewriteStream getTokenRewriteStream() {
    return tokenRewriteStream;
  }

  /**
   * Generate a unique executionId.  An executionId, together with user name and
   * the configuration, will determine the temporary locations of all intermediate
   * files.
   *
   * In the future, users can use the executionId to resume a query.
   */
  public static String generateExecutionId() {
    Random rand = new Random();
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss_SSS");
    String executionId = "hive_" + format.format(new Date()) + "_"
        + Math.abs(rand.nextLong());
    return executionId;
  }

  /**
   * Does Hive wants to run tasks entirely on the local machine
   * (where the query is being compiled)?
   *
   * Today this translates into running hadoop jobs locally
   */
  public boolean isLocalOnlyExecutionMode() {
    // Always allow spark to run in a cluster mode. Without this, depending on
    // user's local hadoop settings, true may be returned, which causes plan to be
    // stored in local path.
    if (HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("spark")) {
      return false;
    }

    return ShimLoader.getHadoopShims().isLocalMode(conf);
  }

  public List<HiveLock> getHiveLocks() {
    return hiveLocks;
  }

  public void setHiveLocks(List<HiveLock> hiveLocks) {
    this.hiveLocks = hiveLocks;
  }

  public HiveTxnManager getHiveTxnManager() {
    return hiveTxnManager;
  }

  public void setHiveTxnManager(HiveTxnManager txnMgr) {
    hiveTxnManager = txnMgr;
  }

  public void setOriginalTracker(String originalTracker) {
    this.originalTracker = originalTracker;
  }

  public void restoreOriginalTracker() {
    if (originalTracker != null) {
      ShimLoader.getHadoopShims().setJobLauncherRpcAddress(conf, originalTracker);
      originalTracker = null;
    }
  }

  public void addCS(String path, ContentSummary cs) {
    pathToCS.put(path, cs);
  }

  public ContentSummary getCS(Path path) {
    return getCS(path.toString());
  }

  public ContentSummary getCS(String path) {
    return pathToCS.get(path);
  }

  public Map<String, ContentSummary> getPathToCS() {
    return pathToCS;
  }

  public Configuration getConf() {
    return conf;
  }

  /**
   * @return the isHDFSCleanup
   */
  public boolean isHDFSCleanup() {
    return isHDFSCleanup;
  }

  /**
   * @param isHDFSCleanup the isHDFSCleanup to set
   */
  public void setHDFSCleanup(boolean isHDFSCleanup) {
    this.isHDFSCleanup = isHDFSCleanup;
  }

  public boolean isNeedLockMgr() {
    return needLockMgr;
  }

  public void setNeedLockMgr(boolean needLockMgr) {
    this.needLockMgr = needLockMgr;
  }

  public int getTryCount() {
    return tryCount;
  }

  public void setTryCount(int tryCount) {
    this.tryCount = tryCount;
  }

  public void setAcidOperation(AcidUtils.Operation op) {
    acidOperation = op;
  }

  public AcidUtils.Operation getAcidOperation() {
    return acidOperation;
  }

  public String getCboInfo() {
    return cboInfo;
  }

  public void setCboInfo(String cboInfo) {
    this.cboInfo = cboInfo;
  }

  public boolean isCboSucceeded() {
    return cboSucceeded;
  }

  public void setCboSucceeded(boolean cboSucceeded) {
    this.cboSucceeded = cboSucceeded;
  }

}
