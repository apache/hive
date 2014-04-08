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
package org.apache.hadoop.hive.ql.exec.tez;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.security.auth.login.LoginException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.AMConfiguration;
import org.apache.tez.client.PreWarmContext;
import org.apache.tez.client.TezSession;
import org.apache.tez.client.TezSessionConfiguration;
import org.apache.tez.dag.api.SessionNotRunning;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.mapreduce.hadoop.MRHelpers;

/**
 * Holds session state related to Tez
 */
public class TezSessionState {

  private static final Log LOG = LogFactory.getLog(TezSessionState.class.getName());
  private static final String TEZ_DIR = "_tez_session_dir";

  private HiveConf conf;
  private Path tezScratchDir;
  private LocalResource appJarLr;
  private TezSession session;
  private String sessionId;
  private DagUtils utils;
  private String queueName;
  private boolean defaultQueue = false;
  private String user;

  private HashSet<String> additionalAmFiles = null;

  private static List<TezSessionState> openSessions
    = Collections.synchronizedList(new LinkedList<TezSessionState>());

  /**
   * Constructor. We do not automatically connect, because we only want to
   * load tez classes when the user has tez installed.
   */
  public TezSessionState(DagUtils utils) {
    this.utils = utils;
  }

  /**
   * Constructor. We do not automatically connect, because we only want to
   * load tez classes when the user has tez installed.
   */
  public TezSessionState(String sessionId) {
    this(DagUtils.getInstance());
    this.sessionId = sessionId;
  }

  /**
   * Returns whether a session has been established
   */
  public boolean isOpen() {
    return session != null;
  }

  /**
   * Get all open sessions. Only used to clean up at shutdown.
   * @return List<TezSessionState>
   */
  public static List<TezSessionState> getOpenSessions() {
    return openSessions;
  }

  public static String makeSessionId() {
    return UUID.randomUUID().toString();
  }

  public void open(HiveConf conf)
      throws IOException, LoginException, URISyntaxException, TezException {
    open(conf, null);
  }

  /**
   * Creates a tez session. A session is tied to either a cli/hs2 session. You can
   * submit multiple DAGs against a session (as long as they are executed serially).
   * @throws IOException
   * @throws URISyntaxException
   * @throws LoginException
   * @throws TezException
   */
  public void open(HiveConf conf, List<LocalResource> additionalLr)
    throws IOException, LoginException, IllegalArgumentException, URISyntaxException, TezException {
    this.conf = conf;

    UserGroupInformation ugi;
    ugi = ShimLoader.getHadoopShims().getUGIForConf(conf);
    user = ShimLoader.getHadoopShims().getShortUserName(ugi);
    LOG.info("User of session id " + sessionId + " is " + user);

    // create the tez tmp dir
    tezScratchDir = createTezDir(sessionId);

    // generate basic tez config
    TezConfiguration tezConfig = new TezConfiguration(conf);

    tezConfig.set(TezConfiguration.TEZ_AM_STAGING_DIR, tezScratchDir.toUri().toString());

    // unless already installed on all the cluster nodes, we'll have to
    // localize hive-exec.jar as well.
    appJarLr = createJarLocalResource(utils.getExecJarPathLocal());

    // configuration for the application master
    Map<String, LocalResource> commonLocalResources = new HashMap<String, LocalResource>();
    commonLocalResources.put(utils.getBaseName(appJarLr), appJarLr);
    if (additionalLr != null) {
      additionalAmFiles = new HashSet<String>();
      for (LocalResource lr : additionalLr) {
        String baseName = utils.getBaseName(lr);
        additionalAmFiles.add(baseName);
        commonLocalResources.put(baseName, lr);
      }
    }

    // Create environment for AM.
    Map<String, String> amEnv = new HashMap<String, String>();
    MRHelpers.updateEnvironmentForMRAM(conf, amEnv);

    AMConfiguration amConfig = new AMConfiguration(amEnv, commonLocalResources, tezConfig, null);

    // configuration for the session
    TezSessionConfiguration sessionConfig = new TezSessionConfiguration(amConfig, tezConfig);

    // and finally we're ready to create and start the session
    session = new TezSession("HIVE-"+sessionId, sessionConfig);

    LOG.info("Opening new Tez Session (id: "+sessionId+", scratch dir: "+tezScratchDir+")");

    session.start();

    if (HiveConf.getBoolVar(conf, ConfVars.HIVE_PREWARM_ENABLED)) {
      int n = HiveConf.getIntVar(conf, ConfVars.HIVE_PREWARM_NUM_CONTAINERS);
      LOG.info("Prewarming " + n + " containers  (id: " + sessionId
          + ", scratch dir: " + tezScratchDir + ")");
      PreWarmContext context = utils.createPreWarmContext(sessionConfig, n, commonLocalResources);
      try {
        session.preWarm(context);
      } catch (InterruptedException ie) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Hive Prewarm threw an exception ", ie);
        }
      }
    }

    // In case we need to run some MR jobs, we'll run them under tez MR emulation. The session
    // id is used for tez to reuse the current session rather than start a new one.
    conf.set("mapreduce.framework.name", "yarn-tez");
    conf.set("mapreduce.tez.session.tokill-application-id", session.getApplicationId().toString());

    openSessions.add(this);
  }

  public boolean hasResources(List<LocalResource> lrs) {
    if (lrs == null || lrs.isEmpty()) return true;
    if (additionalAmFiles == null || additionalAmFiles.isEmpty()) return false;
    for (LocalResource lr : lrs) {
      if (!additionalAmFiles.contains(utils.getBaseName(lr))) return false;
    }
    return true;
  }

  /**
   * Close a tez session. Will cleanup any tez/am related resources. After closing a session
   * no further DAGs can be executed against it.
   * @param keepTmpDir whether or not to remove the scratch dir at the same time.
   * @throws IOException
   * @throws TezException
   */
  public void close(boolean keepTmpDir) throws TezException, IOException {
    if (!isOpen()) {
      return;
    }

    LOG.info("Closing Tez Session");
    try {
      session.stop();
      openSessions.remove(this);
    } catch (SessionNotRunning nr) {
      // ignore
    }

    if (!keepTmpDir) {
      cleanupScratchDir();
    }
    session = null;
    tezScratchDir = null;
    conf = null;
    appJarLr = null;
    additionalAmFiles = null;
  }

  public void cleanupScratchDir () throws IOException {
    FileSystem fs = tezScratchDir.getFileSystem(conf);
    fs.delete(tezScratchDir, true);
    tezScratchDir = null;
  }

  public String getSessionId() {
    return sessionId;
  }

  public TezSession getSession() {
    return session;
  }

  public Path getTezScratchDir() {
    return tezScratchDir;
  }

  public LocalResource getAppJarLr() {
    return appJarLr;
  }

  /**
   * createTezDir creates a temporary directory in the scratchDir folder to
   * be used with Tez. Assumes scratchDir exists.
   */
  private Path createTezDir(String sessionId)
    throws IOException {

    // tez needs its own scratch dir (per session)
    Path tezDir = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCHDIR),
        TEZ_DIR);
    tezDir = new Path(tezDir, sessionId);
    FileSystem fs = tezDir.getFileSystem(conf);
    FsPermission fsPermission = new FsPermission((short)00777);
    Utilities.createDirsWithPermission(conf, tezDir, fsPermission, true);

    // don't keep the directory around on non-clean exit
    fs.deleteOnExit(tezDir);

    return tezDir;
  }

  /**
   * Returns a local resource representing a jar.
   * This resource will be used to execute the plan on the cluster.
   * @param localJarPath Local path to the jar to be localized.
   * @return LocalResource corresponding to the localized hive exec resource.
   * @throws IOException when any file system related call fails.
   * @throws LoginException when we are unable to determine the user.
   * @throws URISyntaxException when current jar location cannot be determined.
   */
  private LocalResource createJarLocalResource(String localJarPath)
      throws IOException, LoginException, IllegalArgumentException,
      FileNotFoundException {
    Path destDirPath = null;
    FileSystem destFs = null;
    FileStatus destDirStatus = null;

    {
      String hiveJarDir = utils.getHiveJarDirectory(conf);
      if (hiveJarDir != null) {
        LOG.info("Hive jar directory is " + hiveJarDir);
        // check if it is a valid directory in HDFS
        destDirPath = new Path(hiveJarDir);
        destFs = destDirPath.getFileSystem(conf);
        destDirStatus = validateTargetDir(destDirPath, destFs);
      }
    }

    /*
     * Specified location does not exist or is not a directory.
     * Try to push the jar to the hdfs location pointed by config variable HIVE_INSTALL_DIR.
     * Path will be HIVE_INSTALL_DIR/{username}/.hiveJars/
     * This will probably never ever happen.
     */
    if (destDirStatus == null || !destDirStatus.isDir()) {
      destDirPath = utils.getDefaultDestDir(conf);
      LOG.info("Jar dir is null/directory doesn't exist. Choosing HIVE_INSTALL_DIR - "
          + destDirPath);
      destFs = destDirPath.getFileSystem(conf);
      destDirStatus = validateTargetDir(destDirPath, destFs);
    }

    // we couldn't find any valid locations. Throw exception
    if (destDirStatus == null || !destDirStatus.isDir()) {
      throw new IOException(ErrorMsg.NO_VALID_LOCATIONS.getMsg());
    }

    Path localFile = new Path(localJarPath);
    String sha = getSha(localFile);

    String destFileName = localFile.getName();

    // Now, try to find the file based on SHA and name. Currently we require exact name match.
    // We could also allow cutting off versions and other stuff provided that SHA matches...
    destFileName = FilenameUtils.removeExtension(destFileName) + "-" + sha
        + FilenameUtils.EXTENSION_SEPARATOR + FilenameUtils.getExtension(destFileName);

    if (LOG.isDebugEnabled()) {
      LOG.debug("The destination file name for [" + localJarPath + "] is " + destFileName);
    }

    // TODO: if this method is ever called on more than one jar, getting the dir and the
    //       list need to be refactored out to be done only once.
    Path destFile = new Path(destDirPath.toString() + "/" + destFileName);
    return utils.localizeResource(localFile, destFile, conf);
  }

  private FileStatus validateTargetDir(Path hiveJarDirPath, FileSystem fs) throws IOException {
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IOException(ErrorMsg.INVALID_HDFS_URI.format(hiveJarDirPath.toString()));
    }
    try {
      return fs.getFileStatus(hiveJarDirPath);
    } catch (FileNotFoundException fe) {
      // do nothing
    }
    return null;
  }

  private String getSha(Path localFile) throws IOException, IllegalArgumentException {
    InputStream is = null;
    try {
      FileSystem localFs = FileSystem.getLocal(conf);
      is = localFs.open(localFile);
      return DigestUtils.sha256Hex(is);
    } finally {
      if (is != null) {
        is.close();
      }
    }
  }
  public void setQueueName(String queueName) {
    this.queueName = queueName;
  }

  public String getQueueName() {
    return queueName;
  }

  public void setDefault() {
    defaultQueue  = true;
  }

  public boolean isDefault() {
    return defaultQueue;
  }

  public HiveConf getConf() {
    return conf;
  }

  public String getUser() {
    return user;
  }
}
