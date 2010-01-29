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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.antlr.runtime.TokenRewriteStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.util.StringUtils;

/**
 * Context for Semantic Analyzers. Usage: not reusable - construct a new one for
 * each query should call clear() at end of use to remove temporary folders
 */
public class Context {
  private Path resFile;
  private Path resDir;
  private FileSystem resFs;
  static final private Log LOG = LogFactory.getLog("hive.ql.Context");
  private Path[] resDirPaths;
  private int resDirFilesNum;
  boolean initialized;
  
  // Path without a file system
  // hive.exec.scratchdir: default: "/tmp/"+System.getProperty("user.name")+"/hive"
  // Used for creating temporary path on external file systems
  private String scratchPath;
  // Path on the local file system
  // System.getProperty("java.io.tmpdir") + Path.SEPARATOR
  // + System.getProperty("user.name") + Path.SEPARATOR + executionId
  private Path localScratchDir;
  // On the default FileSystem (usually HDFS):
  // also based on hive.exec.scratchdir which by default is
  // "/tmp/"+System.getProperty("user.name")+"/hive"
  private Path MRScratchDir;
  
  // allScratchDirs contains all scratch directories including
  // localScratchDir and MRScratchDir.
  // The external scratch dirs will be also based on hive.exec.scratchdir.
  private final Map<String,Path> externalScratchDirs = new HashMap<String,Path>();
  
  private HiveConf conf;
  protected int pathid = 10000;
  protected boolean explain = false;
  private TokenRewriteStream tokenRewriteStream;

  String executionId;

  public Context(HiveConf conf) throws IOException {
    this(conf, generateExecutionId());
  }

  /**
   * Create a Context with a given executionId.  ExecutionId, together with
   * user name and conf, will determine the temporary directory locations. 
   */
  public Context(HiveConf conf, String executionId) throws IOException {
    this.conf = conf;
    this.executionId = executionId;
    Path tmpPath = new Path(conf.getVar(HiveConf.ConfVars.SCRATCHDIR));
    scratchPath = tmpPath.toUri().getPath();
  }

  /**
   * Set the context on whether the current query is an explain query
   * 
   * @param value
   *          true if the query is an explain query, false if not
   */
  public void setExplain(boolean value) {
    explain = value;
  }

  /**
   * Find out whether the current query is an explain query
   * 
   * @return true if the query is an explain query, false if not
   */
  public boolean getExplain() {
    return explain;
  }

  /**
   * Make a tmp directory for MR intermediate data If URI/Scheme are not
   * supplied - those implied by the default filesystem will be used (which will
   * typically correspond to hdfs instance on hadoop cluster)
   * 
   * @param mkdir  if true, will make the directory. Will throw IOException if that fails.
   */
  private static Path makeMRScratchDir(HiveConf conf, String executionId, boolean mkdir)
      throws IOException {
    
    Path dir = FileUtils.makeQualified(
        new Path(conf.getVar(HiveConf.ConfVars.SCRATCHDIR), executionId), conf);
    
    if (mkdir) {
      FileSystem fs = dir.getFileSystem(conf);
      if (!fs.mkdirs(dir)) {
        throw new IOException("Cannot make directory: " + dir);
      }
    }
    return dir;
  }

  /**
   * Make a tmp directory on specified URI Currently will use the same path as
   * implied by SCRATCHDIR config variable
   */
  private static Path makeExternalScratchDir(HiveConf conf, String executionId,
      boolean mkdir, URI extURI) throws IOException {
    
    Path dir = new Path(extURI.getScheme(), extURI.getAuthority(),
        conf.getVar(HiveConf.ConfVars.SCRATCHDIR) + Path.SEPARATOR + executionId);
    
    if (mkdir) {
      FileSystem fs = dir.getFileSystem(conf);
      if (!fs.mkdirs(dir)) {
        throw new IOException("Cannot make directory: " + dir);
      }
    }
    return dir;
  }
  
  /**
   * Make a tmp directory for local file system.
   * 
   * @param mkdir  if true, will make the directory. Will throw IOException if that fails.
   */
  private static Path makeLocalScratchDir(HiveConf conf, String executionId, boolean mkdir)
      throws IOException {
    
    FileSystem fs = FileSystem.getLocal(conf);
    Path dir = fs.makeQualified(new Path(System.getProperty("java.io.tmpdir")
        + Path.SEPARATOR + System.getProperty("user.name") + Path.SEPARATOR
        + executionId));
    
    if (mkdir) {
      if (!fs.mkdirs(dir)) {
        throw new IOException("Cannot make directory: " + dir);
      }
    }
    return dir;
  }

  /**
   * Get a tmp directory on specified URI Will check if this has already been
   * made (either via MR or Local FileSystem or some other external URI
   */
  private String getExternalScratchDir(URI extURI) {
    try {
      String fileSystem = extURI.getScheme() + ":" + extURI.getAuthority();
      Path dir = externalScratchDirs.get(fileSystem);
      if (dir == null) {
        dir = makeExternalScratchDir(conf, executionId, !explain, extURI);
        externalScratchDirs.put(fileSystem, dir);
      }
      return dir.toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create a map-reduce scratch directory on demand and return it
   */
  private String getMRScratchDir() {
    try {
      if (MRScratchDir == null) {
        MRScratchDir = makeMRScratchDir(conf, executionId, !explain);
      }
      return MRScratchDir.toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("Error while making MR scratch "
          + "directory - check filesystem config (" + e.getCause() + ")", e);
    }
  }

  /**
   * Create a local scratch directory on demand and return it
   */
  private String getLocalScratchDir() {
    try {
      if (localScratchDir == null) {
        localScratchDir = makeLocalScratchDir(conf, executionId, true);
      }
      return localScratchDir.toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("Error while making local scratch "
          + "directory - check filesystem config (" + e.getCause() + ")", e);
    }
  }

  private void removeDir(Path p) {
    try {
      p.getFileSystem(conf).delete(p, true);
    } catch (Exception e) {
      LOG.warn("Error Removing Scratch: "
          + StringUtils.stringifyException(e));
    }
  }
  
  /**
   * Remove any created scratch directories
   */
  private void removeScratchDir() {
    
    for (Map.Entry<String,Path> p : externalScratchDirs.entrySet()) {
      removeDir(p.getValue());
    }
    externalScratchDirs.clear();
    
    if (MRScratchDir != null) {
      removeDir(MRScratchDir);
      MRScratchDir = null;
    }
    
    if (localScratchDir != null) {
      removeDir(localScratchDir);
      localScratchDir = null;
    }
  }

  /**
   * Return the next available path in the current scratch dir
   */
  private String nextPath(String base) {
    return base + Path.SEPARATOR + Integer.toString(pathid++);
  }

  /**
   * check if path is tmp path. the assumption is that all uri's relative to
   * scratchdir are temporary
   * 
   * @return true if a uri is a temporary uri for map-reduce intermediate data,
   *         false otherwise
   */
  public boolean isMRTmpFileURI(String uriStr) {
    return (uriStr.indexOf(scratchPath) != -1);
  }

  /**
   * Get a path to store map-reduce intermediate data in
   * 
   * @return next available path for map-red intermediate data
   */
  public String getMRTmpFileURI() {
    return nextPath(getMRScratchDir());
  }

  /**
   * Get a tmp path on local host to store intermediate data
   * 
   * @return next available tmp path on local fs
   */
  public String getLocalTmpFileURI() {
    return nextPath(getLocalScratchDir());
  }

  /**
   * Get a path to store tmp data destined for external URI
   * 
   * @param extURI
   *          external URI to which the tmp data has to be eventually moved
   * @return next available tmp path on the file system corresponding extURI
   */
  public String getExternalTmpFileURI(URI extURI) {
    return nextPath(getExternalScratchDir(extURI));
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
   * Little abbreviation for StringUtils
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
  
}
