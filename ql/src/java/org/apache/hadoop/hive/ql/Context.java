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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.antlr.runtime.TokenRewriteStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hive.ql.lockmgr.HiveLock;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockManager;
import org.apache.hadoop.conf.Configuration;

/**
 * Context for Semantic Analyzers. Usage: not reusable - construct a new one for
 * each query should call clear() at end of use to remove temporary folders
 */
public class Context {
  private Path resFile;
  private Path resDir;
  private FileSystem resFs;
  private static final Log LOG = LogFactory.getLog("hive.ql.Context");
  private Path[] resDirPaths;
  private int resDirFilesNum;
  boolean initialized;
  String originalTracker = null;
  private Map<String, ContentSummary> pathToCS = new ConcurrentHashMap<String, ContentSummary>();

  // scratch path to use for all non-local (ie. hdfs) file system tmp folders
  private final Path nonLocalScratchPath;

  // scratch directory to use for local file system tmp folders
  private final String localScratchDir;

  // Keeps track of scratch directories created for different scheme/authority
  private final Map<String, String> fsScratchDirs = new HashMap<String, String>();

  private Configuration conf;
  protected int pathid = 10000;
  protected boolean explain = false;
  private TokenRewriteStream tokenRewriteStream;

  String executionId;

  // List of Locks for this query
  protected List<HiveLock> hiveLocks;
  protected HiveLockManager hiveLockMgr;

  private boolean needLockMgr;

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

    // non-local tmp location is configurable. however it is the same across
    // all external file systems
    nonLocalScratchPath =
      new Path(HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCHDIR),
               executionId);

    // local tmp location is not configurable for now
    localScratchDir = System.getProperty("java.io.tmpdir")
      + Path.SEPARATOR + System.getProperty("user.name") + Path.SEPARATOR
      + executionId;
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
  public boolean getExplain () {
    return explain;
  }


  /**
   * Get a tmp directory on specified URI
   *
   * @param scheme Scheme of the target FS
   * @param authority Authority of the target FS
   * @param mkdir create the directory if true
   * @param scratchdir path of tmp directory
   */
  private String getScratchDir(String scheme, String authority,
                               boolean mkdir, String scratchDir) {

    String fileSystem =  scheme + ":" + authority;
    String dir = fsScratchDirs.get(fileSystem);

    if (dir == null) {
      Path dirPath = new Path(scheme, authority, scratchDir);
      if (mkdir) {
        try {
          FileSystem fs = dirPath.getFileSystem(conf);
          if (!fs.mkdirs(dirPath))
            throw new RuntimeException("Cannot make directory: "
                                       + dirPath.toString());
        } catch (IOException e) {
          throw new RuntimeException (e);
        }
      }
      dir = dirPath.toString();
      fsScratchDirs.put(fileSystem, dir);
    }
    return dir;
  }


  /**
   * Create a local scratch directory on demand and return it.
   */
  public String getLocalScratchDir(boolean mkdir) {
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
  public String getMRScratchDir() {

    // if we are executing entirely on the client side - then
    // just (re)use the local scratch directory
    if(isLocalOnlyExecutionMode())
      return getLocalScratchDir(!explain);

    try {
      Path dir = FileUtils.makeQualified(nonLocalScratchPath, conf);
      URI uri = dir.toUri();
      return getScratchDir(uri.getScheme(), uri.getAuthority(),
                           !explain, uri.getPath());

    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("Error while making MR scratch "
          + "directory - check filesystem config (" + e.getCause() + ")", e);
    }
  }

  private String getExternalScratchDir(URI extURI) {
    return getScratchDir(extURI.getScheme(), extURI.getAuthority(),
                         !explain, nonLocalScratchPath.toUri().getPath());
  }

  /**
   * Remove any created scratch directories.
   */
  private void removeScratchDir() {
    for (Map.Entry<String, String> entry : fsScratchDirs.entrySet()) {
      try {
        Path p = new Path(entry.getValue());
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

  /**
   * Get a path to store map-reduce intermediate data in.
   *
   * @return next available path for map-red intermediate data
   */
  public String getMRTmpFileURI() {
    return getMRScratchDir() + Path.SEPARATOR + MR_PREFIX +
      nextPathId();
  }


  /**
   * Given a URI for mapreduce intermediate output, swizzle the
   * it to point to the local file system. This can be called in
   * case the caller decides to run in local mode (in which case
   * all intermediate data can be stored locally)
   *
   * @param originalURI uri to localize
   * @return localized path for map-red intermediate data
   */
  public String localizeMRTmpFileURI(String originalURI) {
    Path o = new Path(originalURI);
    Path mrbase = new Path(getMRScratchDir());

    URI relURI = mrbase.toUri().relativize(o.toUri());
    if (relURI.equals(o.toUri()))
      throw new RuntimeException
        ("Invalid URI: " + originalURI + ", cannot relativize against" +
         mrbase.toString());

    return getLocalScratchDir(!explain) + Path.SEPARATOR +
      relURI.getPath();
  }


  /**
   * Get a tmp path on local host to store intermediate data.
   *
   * @return next available tmp path on local fs
   */
  public String getLocalTmpFileURI() {
    return getLocalScratchDir(true) + Path.SEPARATOR + LOCAL_PREFIX +
      nextPathId();
  }

  /**
   * Get a path to store tmp data destined for external URI.
   *
   * @param extURI
   *          external URI to which the tmp data has to be eventually moved
   * @return next available tmp path on the file system corresponding extURI
   */
  public String getExternalTmpFileURI(URI extURI) {
    return getExternalScratchDir(extURI) +  Path.SEPARATOR + EXT_PREFIX +
      nextPathId();
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
        FileStatus[] resDirFS = resFs.globStatus(new Path(resDir + "/*"));
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
    return HiveConf.getVar(conf, HiveConf.ConfVars.HADOOPJT).equals("local");
  }

  public List<HiveLock> getHiveLocks() {
    return hiveLocks;
  }

  public void setHiveLocks(List<HiveLock> hiveLocks) {
    this.hiveLocks = hiveLocks;
  }

  public HiveLockManager getHiveLockMgr() {
    return hiveLockMgr;
  }

  public void setHiveLockMgr(HiveLockManager hiveLockMgr) {
    this.hiveLockMgr = hiveLockMgr;
  }

  public void setOriginalTracker(String originalTracker) {
    this.originalTracker = originalTracker;
  }

  public void restoreOriginalTracker() {
    if (originalTracker != null) {
      HiveConf.setVar(conf, HiveConf.ConfVars.HADOOPJT, originalTracker);
      originalTracker = null;
    }
  }

  public void addCS(String path, ContentSummary cs) {
    pathToCS.put(path, cs);
  }

  public ContentSummary getCS(String path) {
    return pathToCS.get(path);
  }

  public Configuration getConf() {
    return conf;
  }


  /**
   * Given a mapping from paths to objects, localize any MR tmp paths
   * @param map mapping from paths to objects
   */
  public void localizeKeys(Map<String, Object> map) {
    for (Map.Entry<String, Object> entry: map.entrySet()) {
      String path = entry.getKey();
      if (isMRTmpFileURI(path)) {
        Object val = entry.getValue();
        map.remove(path);
        map.put(localizeMRTmpFileURI(path), val);
      }
    }
  }

  /**
   * Given a list of paths, localize any MR tmp paths contained therein
   * @param paths list of paths to be localized
   */
  public void localizePaths(List<String> paths) {
    Iterator<String> iter = paths.iterator();
    List<String> toAdd = new ArrayList<String> ();
    while(iter.hasNext()) {
      String path = iter.next();
      if (isMRTmpFileURI(path)) {
        iter.remove();
        toAdd.add(localizeMRTmpFileURI(path));
      }
    }
    paths.addAll(toAdd);
  }

  public boolean isNeedLockMgr() {
    return needLockMgr;
  }

  public void setNeedLockMgr(boolean needLockMgr) {
    this.needLockMgr = needLockMgr;
  }
}
