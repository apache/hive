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

import java.io.File;
import java.io.DataInput;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.hive.common.FileUtils;

/**
 * Context for Semantic Analyzers.
 * Usage:
 * not reusable - construct a new one for each query
 * should call clear() at end of use to remove temporary folders
 */
public class Context {
  private Path resFile;
  private Path resDir;
  private FileSystem resFs;
  static final private Log LOG = LogFactory.getLog("hive.ql.Context");
  private Path[] resDirPaths;
  private int    resDirFilesNum;
  boolean initialized;
  private String scratchPath;
  private Path MRScratchDir;
  private Path localScratchDir;
  private ArrayList<Path> allScratchDirs = new ArrayList<Path> ();
  private HiveConf conf;
  Random rand = new Random ();
  protected int randomid = Math.abs(rand.nextInt());
  protected int pathid = 10000;
  protected boolean explain = false;

  public Context(HiveConf conf) {
    this.conf = conf;
    Path tmpPath = new Path(conf.getVar(HiveConf.ConfVars.SCRATCHDIR));
    scratchPath = tmpPath.toUri().getPath();
  }

  /**
   * Set the context on whether the current query is an explain query
   * @param value true if the query is an explain query, false if not
   */
  public void setExplain(boolean value) {
    explain = value;
  }

  /**
   * Find out whether the current query is an explain query
   * @return true if the query is an explain query, false if not
   */
  public boolean getExplain() {
    return explain;
  }

  /**
   * Make a tmp directory on the local filesystem
   */
  private void makeLocalScratchDir() throws IOException {
    while (true) {
      localScratchDir = new Path(System.getProperty("java.io.tmpdir")
                                 + File.separator + Math.abs(rand.nextInt()));
      FileSystem fs = FileSystem.getLocal(conf);
      if (fs.mkdirs(localScratchDir)) {
        localScratchDir = fs.makeQualified(localScratchDir);
        allScratchDirs.add(localScratchDir);
        break;
      }
    }
  }

  /**
   * Make a tmp directory for MR intermediate data
   * If URI/Scheme are not supplied - those implied by the default filesystem
   * will be used (which will typically correspond to hdfs instance on hadoop cluster)
   */
  private void makeMRScratchDir() throws IOException {
    while(true) {
      MRScratchDir = FileUtils.makeQualified
        (new Path(conf.getVar(HiveConf.ConfVars.SCRATCHDIR),
                  Integer.toString(Math.abs(rand.nextInt()))), conf);

      if (explain) {
        allScratchDirs.add(MRScratchDir);
        return;
      }

      FileSystem fs = MRScratchDir.getFileSystem(conf);
      if (fs.mkdirs(MRScratchDir)) {
        allScratchDirs.add(MRScratchDir);
        return;
      }
    }
  }
  
  /**
   * Make a tmp directory on specified URI
   * Currently will use the same path as implied by SCRATCHDIR config variable
   */
  private Path makeExternalScratchDir(URI extURI) throws IOException {
    while(true) {
      String extPath = scratchPath + File.separator + 
        Integer.toString(Math.abs(rand.nextInt()));
      Path extScratchDir = new Path(extURI.getScheme(), extURI.getAuthority(),
                                    extPath);

      if (explain) {
        allScratchDirs.add(extScratchDir);        
        return extScratchDir;
      }

      FileSystem fs = extScratchDir.getFileSystem(conf);
      if (fs.mkdirs(extScratchDir)) {
        allScratchDirs.add(extScratchDir);
        return extScratchDir;
      }
    }
  }

  /**
   * Get a tmp directory on specified URI
   * Will check if this has already been made
   * (either via MR or Local FileSystem or some other external URI
   */
  private String getExternalScratchDir(URI extURI) {
    try {
      // first check if we already made a scratch dir on this URI
      for (Path p: allScratchDirs) {
        URI pURI = p.toUri();
        if (strEquals(pURI.getScheme(), extURI.getScheme()) &&
            strEquals(pURI.getAuthority(), extURI.getAuthority())) {
          return p.toString();
        }
      }
      return makeExternalScratchDir(extURI).toString();
    } catch (IOException e) {
      throw new RuntimeException (e);
    }
  }
  
  /**
   * Create a map-reduce scratch directory on demand and return it
   */
  private String getMRScratchDir() {
    if (MRScratchDir == null) {
      try {
        makeMRScratchDir();
      } catch (IOException e) {
        throw new RuntimeException (e);
      }
    }
    return MRScratchDir.toString();
  }

  /**
   * Create a local scratch directory on demand and return it
   */
  private String getLocalScratchDir() {
    if (localScratchDir == null) {
      try {
        makeLocalScratchDir();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return localScratchDir.toString();
  }

  /**
   * Remove any created scratch directories
   */
  private void removeScratchDir() {
    if (explain) {
      try {
        if (localScratchDir != null)
          FileSystem.getLocal(conf).delete(localScratchDir);
      } catch (Exception e) {
        LOG.warn("Error Removing Scratch: " + StringUtils.stringifyException(e));
      }
    } else {
      for (Path p: allScratchDirs) {
        try {
          p.getFileSystem(conf).delete(p);
        } catch (Exception e) {
          LOG.warn("Error Removing Scratch: " + StringUtils.stringifyException(e));
        }
      }
    }
    MRScratchDir = null;
    localScratchDir = null;
  }

  /**
   * Return the next available path in the current scratch dir
   */
  private String nextPath(String base) {
    return base + File.separator + Integer.toString(pathid++);
  }
  
  /**
   * check if path is tmp path. the assumption is that all uri's relative
   * to scratchdir are temporary
   * @return true if a uri is a temporary uri for map-reduce intermediate
   *         data, false otherwise
   */
  public boolean isMRTmpFileURI(String uriStr) {
    return (uriStr.indexOf(scratchPath) != -1);
  }

  /**
   * Get a path to store map-reduce intermediate data in
   * @return next available path for map-red intermediate data
   */
  public String getMRTmpFileURI() {
    return nextPath(getMRScratchDir());
  }


  /**
   * Get a tmp path on local host to store intermediate data
   * @return next available tmp path on local fs
   */
  public String getLocalTmpFileURI() {
    return nextPath(getLocalScratchDir());
  }
  

  /**
   * Get a path to store tmp data destined for external URI
   * @param extURI external URI to which the tmp data has to be 
   *               eventually moved
   * @return next available tmp path on the file system corresponding
   *              extURI
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
   * @param resFile the resFile to set
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
   * @param resDir the resDir to set
   */
  public void setResDir(Path resDir) {
    this.resDir = resDir;
    resFile = null;

    resDirFilesNum = 0;
    resDirPaths = null;
  }  
  
  public void clear() throws IOException {
    if (resDir != null)
    {
      try
      {
        FileSystem fs = resDir.getFileSystem(conf);
        fs.delete(resDir, true);
      } catch (IOException e) {
        LOG.info("Context clear error: " + StringUtils.stringifyException(e));
      }
    }

    if (resFile != null)
    {
      try
      {
        FileSystem fs = resFile.getFileSystem(conf);
      	fs.delete(resFile, false);
      } catch (IOException e) {
        LOG.info("Context clear error: " + StringUtils.stringifyException(e));
      }
    }
    removeScratchDir();
  }

  public DataInput getStream() {
    try
    {
      if (!initialized) {
        initialized = true;
        if ((resFile == null) && (resDir == null)) return null;
      
        if (resFile != null) {
          return (DataInput)resFile.getFileSystem(conf).open(resFile);
        }
        
        resFs = resDir.getFileSystem(conf);
        FileStatus status = resFs.getFileStatus(resDir);
        assert status.isDir();
        FileStatus[] resDirFS = resFs.globStatus(new Path(resDir + "/*"));
        resDirPaths = new Path[resDirFS.length];
        int pos = 0;
        for (FileStatus resFS: resDirFS)
          if (!resFS.isDir())
            resDirPaths[pos++] = resFS.getPath();
        if (pos == 0) return null;
        
        return (DataInput)resFs.open(resDirPaths[resDirFilesNum++]);
      }
      else {
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
    try
    {
      if (resDir != null && resDirFilesNum < resDirPaths.length && 
          (resDirPaths[resDirFilesNum] != null))
        return (DataInput)resFs.open(resDirPaths[resDirFilesNum++]);
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
}

