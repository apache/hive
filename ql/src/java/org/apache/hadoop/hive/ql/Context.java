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

package org.apache.hadoop.hive.ql;

import java.io.DataInput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.antlr.runtime.TokenRewriteStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.BlobStorageUtils;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager.Heartbeater;
import org.apache.hadoop.hive.ql.lockmgr.HiveLock;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObj;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.AnalyzeState;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.wm.WmContext;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Context for Semantic Analyzers. Usage: not reusable - construct a new one for
 * each query should call clear() at end of use to remove temporary folders
 */
public class Context {
  private boolean isHDFSCleanup;
  private Path resFile;
  private Path resDir;
  private FileSystem resFs;
  private static final Logger LOG = LoggerFactory.getLogger("hive.ql.Context");
  private Path[] resDirPaths;
  private int resDirFilesNum;
  boolean initialized;
  String originalTracker = null;
  private CompilationOpContext opContext;
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
  protected ExplainConfiguration explainConfig = null;
  protected String cboInfo;
  protected boolean cboSucceeded;
  protected String cmd = "";
  private TokenRewriteStream tokenRewriteStream;
  // Holds the qualified name to tokenRewriteStream for the views
  // referenced by the query. This is used to rewrite the view AST
  // with column masking and row filtering policies.
  private final Map<String, TokenRewriteStream> viewsTokenRewriteStreams;

  private final String executionId;
  // Some statements, e.g., UPDATE, DELETE, or MERGE, get rewritten into different
  // subqueries that create new contexts. We keep them here so we can clean them
  // up when we are done.
  private final Set<Context> rewrittenStatementContexts;

  // List of Locks for this query
  protected List<HiveLock> hiveLocks;

  // Transaction manager for this query
  protected HiveTxnManager hiveTxnManager;

  private boolean needLockMgr;

  private AtomicInteger sequencer = new AtomicInteger();

  private final Map<String, Table> cteTables = new HashMap<String, Table>();

  // Keep track of the mapping from load table desc to the output and the lock
  private final Map<LoadTableDesc, WriteEntity> loadTableOutputMap =
      new HashMap<LoadTableDesc, WriteEntity>();
  private final Map<WriteEntity, List<HiveLockObj>> outputLockObjects =
      new HashMap<WriteEntity, List<HiveLockObj>>();

  private final String stagingDir;

  private Heartbeater heartbeater;

  private boolean skipTableMasking;

  // Identify whether the query involves an UPDATE, DELETE or MERGE
  private boolean isUpdateDeleteMerge;

  // Whether the analyzer has been instantiated to read and load materialized view plans
  private boolean isLoadingMaterializedView;

  /**
   * This determines the prefix of the
   * {@link org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.Phase1Ctx#dest}
   * name for a given subtree of the AST.  Most of the times there is only 1 destination in a
   * given tree but multi-insert has several and multi-insert representing MERGE must use
   * different prefixes to encode the purpose of different Insert branches
   */
  private Map<Integer, DestClausePrefix> insertBranchToNamePrefix = new HashMap<>();
  private Operation operation = Operation.OTHER;
  private WmContext wmContext;

  public void setOperation(Operation operation) {
    this.operation = operation;
  }

  public WmContext getWmContext() {
    return wmContext;
  }

  public void setWmContext(final WmContext wmContext) {
    this.wmContext = wmContext;
  }

  /**
   * These ops require special handling in various places
   * (note that Insert into Acid table is in OTHER category)
   */
  public enum Operation {UPDATE, DELETE, MERGE, OTHER};
  public enum DestClausePrefix {
    INSERT("insclause-"), UPDATE("updclause-"), DELETE("delclause-");
    private final String prefix;
    DestClausePrefix(String prefix) {
      this.prefix = prefix;
    }
    @Override
    public String toString() {
      return prefix;
    }
  }
  private String getMatchedText(ASTNode n) {
    return getTokenRewriteStream().toString(n.getTokenStartIndex(), n.getTokenStopIndex() + 1).trim();
  }
  /**
   * The suffix is always relative to a given ASTNode
   */
  public DestClausePrefix getDestNamePrefix(ASTNode curNode, QB queryBlock) {
    assert curNode != null : "must supply curNode";
    if(queryBlock.isInsideView() || queryBlock.getParseInfo().getIsSubQ()) {
      /**
       * Views get inlined in the logical plan but not in the AST
       * {@link org.apache.hadoop.hive.ql.parse.SemanticAnalyzer#replaceViewReferenceWithDefinition(QB, Table, String, String)}
       * Since here we only care to identify clauses representing Update/Delete which are not
       * possible inside a view/subquery, we can immediately return the default {@link DestClausePrefix.INSERT}
       */
      return DestClausePrefix.INSERT;
    }
    if(curNode.getType() != HiveParser.TOK_INSERT_INTO) {
      //select statement
      assert curNode.getType() == HiveParser.TOK_DESTINATION;
      if(operation == Operation.OTHER) {
        //not an 'interesting' op
        return DestClausePrefix.INSERT;
      }
      //if it is an 'interesting' op but it's a select it must be a sub-query or a derived table
      //it doesn't require a special Acid code path - the reset of the code here is to ensure
      //the tree structure is what we expect
      boolean thisIsInASubquery = false;
      parentLoop: while(curNode.getParent() != null) {
        curNode = (ASTNode) curNode.getParent();
        switch (curNode.getType()) {
          case HiveParser.TOK_SUBQUERY_EXPR:
            //this is a real subquery (foo IN (select ...))
          case HiveParser.TOK_SUBQUERY:
            //this is a Derived Table Select * from (select a from ...))
            //strictly speaking SetOps should have a TOK_SUBQUERY parent so next 6 items are redundant
          case HiveParser.TOK_UNIONALL:
          case HiveParser.TOK_UNIONDISTINCT:
          case HiveParser.TOK_EXCEPTALL:
          case HiveParser.TOK_EXCEPTDISTINCT:
          case HiveParser.TOK_INTERSECTALL:
          case HiveParser.TOK_INTERSECTDISTINCT:
            thisIsInASubquery = true;
            break parentLoop;
        }
      }
      if(!thisIsInASubquery) {
        throw new IllegalStateException("Expected '" + getMatchedText(curNode) + "' to be in sub-query or set operation.");
      } 
      return DestClausePrefix.INSERT;
    }
    switch (operation) {
      case OTHER:
        return DestClausePrefix.INSERT;
      case UPDATE:
        return DestClausePrefix.UPDATE;
      case DELETE:
        return DestClausePrefix.DELETE;
      case MERGE:
      /* This is the structrue expected here
        HiveParser.TOK_QUERY;
          HiveParser.TOK_FROM
          HiveParser.TOK_INSERT;
            HiveParser.TOK_INSERT_INTO;
          HiveParser.TOK_INSERT;
            HiveParser.TOK_INSERT_INTO;
          .....*/
        ASTNode insert = (ASTNode) curNode.getParent();
        assert insert != null && insert.getType() == HiveParser.TOK_INSERT;
        ASTNode query = (ASTNode) insert.getParent();
        assert query != null && query.getType() == HiveParser.TOK_QUERY;
        
        for(int childIdx = 1; childIdx < query.getChildCount(); childIdx++) {//1st child is TOK_FROM
          assert query.getChild(childIdx).getType() == HiveParser.TOK_INSERT;
          if(insert == query.getChild(childIdx)) {
            DestClausePrefix prefix = insertBranchToNamePrefix.get(childIdx);
            if(prefix == null) {
              throw new IllegalStateException("Found a node w/o branch mapping: '" +
                getMatchedText(insert) + "'");
            }
            return prefix;
          }
        }
        throw new IllegalStateException("Could not locate '" + getMatchedText(insert) + "'");
      default:
        throw new IllegalStateException("Unexpected operation: " + operation);
    }
  }
  /**
   * Will make SemanticAnalyzer.Phase1Ctx#dest in subtree rooted at 'tree' use 'prefix'.  This to
   * handle multi-insert stmt that represents Merge stmt and has insert branches representing
   * update/delete/insert.
   * @param pos ordinal index of specific TOK_INSERT as child of TOK_QUERY
   * @return previous prefix for 'tree' or null
   */
  public DestClausePrefix addDestNamePrefix(int pos, DestClausePrefix prefix) {
    return insertBranchToNamePrefix.put(pos, prefix);
  }
  public Context(Configuration conf) throws IOException {
    this(conf, generateExecutionId());
  }

  /**
   * Create a Context with a given executionId.  ExecutionId, together with
   * user name and conf, will determine the temporary directory locations.
   */
  private Context(Configuration conf, String executionId)  {
    this.conf = conf;
    this.executionId = executionId;
    this.rewrittenStatementContexts = new HashSet<>();

    // local & non-local tmp location is configurable. however it is the same across
    // all external file systems
    nonLocalScratchPath = new Path(SessionState.getHDFSSessionPath(conf), executionId);
    localScratchDir = new Path(SessionState.getLocalSessionPath(conf), executionId).toUri().getPath();
    scratchDirPermission = HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCHDIRPERMISSION);
    stagingDir = HiveConf.getVar(conf, HiveConf.ConfVars.STAGINGDIR);
    opContext = new CompilationOpContext();

    viewsTokenRewriteStreams = new HashMap<>();
  }

  public Map<String, Path> getFsScratchDirs() {
    return fsScratchDirs;
  }

  public Map<LoadTableDesc, WriteEntity> getLoadTableOutputMap() {
    return loadTableOutputMap;
  }

  public Map<WriteEntity, List<HiveLockObj>> getOutputLockObjects() {
    return outputLockObjects;
  }

  /**
   * Find whether we should execute the current query due to explain
   * @return true if the query needs to be executed, false if not
   */
  public boolean isExplainSkipExecution() {
    return (explainConfig != null && explainConfig.getAnalyze() != AnalyzeState.RUNNING);
  }

  /**
   * Find whether the current query is a logical explain query
   */
  public boolean getExplainLogical() {
    return explainConfig != null && explainConfig.isLogical();
  }

  public AnalyzeState getExplainAnalyze() {
    if (explainConfig != null) {
      return explainConfig.getAnalyze();
    }
    return null;
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
      dir = fs.makeQualified(new Path(
          stagingPathName + "_" + this.executionId + "-" + TaskRunner.getTaskRunnerID()));

      LOG.debug("Created staging dir = " + dir + " for path = " + inputPath);

      if (mkdir) {
        try {
          if (!FileUtils.mkdir(fs, dir, conf)) {
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
      return getLocalScratchDir(!isExplainSkipExecution());
    }

    try {
      Path dir = FileUtils.makeQualified(nonLocalScratchPath, conf);
      URI uri = dir.toUri();

      Path newScratchDir = getScratchDir(uri.getScheme(), uri.getAuthority(),
          !isExplainSkipExecution(), uri.getPath());
      LOG.info("New scratch dir is " + newScratchDir);
      return newScratchDir;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("Error while making MR scratch "
          + "directory - check filesystem config (" + e.getCause() + ")", e);
    }
  }

  /**
   * Create a temporary directory depending of the path specified.
   * - If path is an Object store filesystem, then use the default MR scratch directory (HDFS), unless isFinalJob and
   * {@link BlobStorageUtils#areOptimizationsEnabled(Configuration)} are both true, then return a path on
   * the blobstore.
   * - If path is on HDFS, then create a staging directory inside the path
   *
   * @param path Path used to verify the Filesystem to use for temporary directory
   * @param isFinalJob true if the required {@link Path} will be used for the final job (e.g. the final FSOP)
   *
   * @return A path to the new temporary directory
   */
  public Path getTempDirForInterimJobPath(Path path) {
    // For better write performance, we use HDFS for temporary data when object store is used.
    // Note that the scratch directory configuration variable must use HDFS or any other
    // non-blobstorage system to take advantage of this performance.
    boolean isBlobstorageOptimized = BlobStorageUtils.isBlobStoragePath(conf, path)
        && !BlobStorageUtils.isBlobStorageAsScratchDir(conf) && BlobStorageUtils.areOptimizationsEnabled(conf);

    if (isPathLocal(path) || isBlobstorageOptimized) {
      return getMRTmpPath();
    }
    return getExtTmpPathRelTo(path);
  }

  /**
   * Create a temporary directory depending of the path specified.
   * - If path is an Object store filesystem, then use the default MR scratch directory (HDFS)
   * - If path is on HDFS, then create a staging directory inside the path
   *
   * @param path Path used to verify the Filesystem to use for temporary directory
   * @return A path to the new temporary directory
   */
  public Path getTempDirForFinalJobPath(Path path) {
    return getExtTmpPathRelTo(path);
  }

  /*
   * Checks if the path is for the local filesystem or not
   */
  private boolean isPathLocal(Path path) {
    boolean isLocal = false;
    if (path != null) {
      String scheme = path.toUri().getScheme();
      if (scheme != null) {
        isLocal = scheme.equals(Utilities.HADOOP_LOCAL_FS_SCHEME);
      }
    }
    return isLocal;
  }

  private Path getExternalScratchDir(URI extURI) {
    return getStagingDir(new Path(extURI.getScheme(), extURI.getAuthority(), extURI.getPath()), !isExplainSkipExecution());
  }

  /**
   * Remove any created scratch directories.
   */
  public void removeScratchDir() {
    for (Map.Entry<String, Path> entry : fsScratchDirs.entrySet()) {
      try {
        Path p = entry.getValue();
        FileSystem fs = p.getFileSystem(conf);
        LOG.debug("Deleting scratch dir: {}",  p);
        fs.delete(p, true);
        fs.cancelDeleteOnExit(p);
      } catch (Exception e) {
        LOG.warn("Error Removing Scratch: "
            + StringUtils.stringifyException(e));
      }
    }
    fsScratchDirs.clear();
  }

  /**
   * Remove any created directories for CTEs.
   */
  public void removeMaterializedCTEs() {
    // clean CTE tables
    for (Table materializedTable : cteTables.values()) {
      Path location = materializedTable.getDataLocation();
      try {
        FileSystem fs = location.getFileSystem(conf);
        boolean status = fs.delete(location, true);
        LOG.info("Removed " + location + " for materialized "
            + materializedTable.getTableName() + ", status=" + status);
      } catch (IOException e) {
        // ignore
        LOG.warn("Error removing " + location + " for materialized " + materializedTable.getTableName() +
                ": " + StringUtils.stringifyException(e));
      }
    }
    cteTables.clear();
  }

  private String nextPathId() {
    return Integer.toString(pathid++);
  }


  private static final String MR_PREFIX = "-mr-";
  public static final String EXT_PREFIX = "-ext-";
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
    return new Path(getStagingDir(new Path(uri), !isExplainSkipExecution()), MR_PREFIX + nextPathId());
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
    return new Path(getStagingDir(path, !isExplainSkipExecution()), EXT_PREFIX + nextPathId());
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
    // First clear the other contexts created by this query
    for (Context subContext : rewrittenStatementContexts) {
      subContext.clear();
    }
    // Then clear this context
    if (resDir != null) {
      try {
        FileSystem fs = resDir.getFileSystem(conf);
        LOG.debug("Deleting result dir: {}",  resDir);
        fs.delete(resDir, true);
      } catch (IOException e) {
        LOG.info("Context clear error: " + StringUtils.stringifyException(e));
      }
    }

    if (resFile != null) {
      try {
        FileSystem fs = resFile.getFileSystem(conf);
        LOG.debug("Deleting result file: {}",  resFile);
        fs.delete(resFile, false);
      } catch (IOException e) {
        LOG.info("Context clear error: " + StringUtils.stringifyException(e));
      }
    }
    removeMaterializedCTEs();
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
    assert (this.tokenRewriteStream == null || this.getExplainAnalyze() == AnalyzeState.RUNNING);
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

  public void addViewTokenRewriteStream(String viewFullyQualifiedName,
      TokenRewriteStream tokenRewriteStream) {
    viewsTokenRewriteStreams.put(viewFullyQualifiedName, tokenRewriteStream);
  }

  public TokenRewriteStream getViewTokenRewriteStream(String viewFullyQualifiedName) {
    return viewsTokenRewriteStreams.get(viewFullyQualifiedName);
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

  public void addRewrittenStatementContext(Context context) {
    rewrittenStatementContexts.add(context);
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

  public Table getMaterializedTable(String cteName) {
    return cteTables.get(cteName);
  }

  public void addMaterializedTable(String cteName, Table table) {
    cteTables.put(cteName, table);
  }

  public AtomicInteger getSequencer() {
    return sequencer;
  }

  public CompilationOpContext getOpContext() {
    return opContext;
  }

  public Heartbeater getHeartbeater() {
    return heartbeater;
  }

  public void setHeartbeater(Heartbeater heartbeater) {
    this.heartbeater = heartbeater;
  }

  public void checkHeartbeaterLockException() throws LockException {
    if (getHeartbeater() != null && getHeartbeater().getLockException() != null) {
      throw getHeartbeater().getLockException();
    }
  }

  public boolean isSkipTableMasking() {
    return skipTableMasking;
  }

  public void setSkipTableMasking(boolean skipTableMasking) {
    this.skipTableMasking = skipTableMasking;
  }

  public ExplainConfiguration getExplainConfig() {
    return explainConfig;
  }
  private boolean isExplainPlan = false;
  public boolean isExplainPlan() {
    return isExplainPlan;
  }
  public void setExplainPlan(boolean t) {
    this.isExplainPlan = t;
  }

  public void setExplainConfig(ExplainConfiguration explainConfig) {
    this.explainConfig = explainConfig;
  }

  public void resetOpContext() {
    opContext = new CompilationOpContext();
    sequencer = new AtomicInteger();
  }

  public boolean getIsUpdateDeleteMerge() {
    return isUpdateDeleteMerge;
  }

  public void setIsUpdateDeleteMerge(boolean isUpdate) {
    this.isUpdateDeleteMerge = isUpdate;
  }

  public boolean isLoadingMaterializedView() {
    return isLoadingMaterializedView;
  }

  public void setIsLoadingMaterializedView(boolean isLoadingMaterializedView) {
    this.isLoadingMaterializedView = isLoadingMaterializedView;
  }

  public String getExecutionId() {
    return executionId;
  }
}
