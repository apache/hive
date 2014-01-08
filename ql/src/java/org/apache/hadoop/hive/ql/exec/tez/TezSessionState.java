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
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.AMConfiguration;
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
  public TezSessionState() {
    this(DagUtils.getInstance());
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

  /**
   * Creates a tez session. A session is tied to either a cli/hs2 session. You can
   * submit multiple DAGs against a session (as long as they are executed serially).
   * @throws IOException
   * @throws URISyntaxException
   * @throws LoginException
   * @throws TezException
   */
  public void open(String sessionId, HiveConf conf)
      throws IOException, LoginException, URISyntaxException, TezException {

    this.sessionId = sessionId;
    this.conf = conf;

    // create the tez tmp dir
    tezScratchDir = createTezDir(sessionId);

    // generate basic tez config
    TezConfiguration tezConfig = new TezConfiguration(conf);

    tezConfig.set(TezConfiguration.TEZ_AM_STAGING_DIR, tezScratchDir.toUri().toString());

    // unless already installed on all the cluster nodes, we'll have to
    // localize hive-exec.jar as well.
    appJarLr = createHiveExecLocalResource();

    // configuration for the application master
    Map<String, LocalResource> commonLocalResources = new HashMap<String, LocalResource>();
    commonLocalResources.put(utils.getBaseName(appJarLr), appJarLr);

    AMConfiguration amConfig = new AMConfiguration(null, commonLocalResources,
         tezConfig, null);

    // configuration for the session
    TezSessionConfiguration sessionConfig = new TezSessionConfiguration(amConfig, tezConfig);

    // and finally we're ready to create and start the session
    session = new TezSession("HIVE-"+sessionId, sessionConfig);

    LOG.info("Opening new Tez Session (id: "+sessionId+", scratch dir: "+tezScratchDir+")");
    session.start();

    // In case we need to run some MR jobs, we'll run them under tez MR emulation. The session
    // id is used for tez to reuse the current session rather than start a new one.
    conf.set("mapreduce.framework.name", "yarn-tez");
    conf.set("mapreduce.tez.session.tokill-application-id", session.getApplicationId().toString());

    openSessions.add(this);
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
      FileSystem fs = tezScratchDir.getFileSystem(conf);
      fs.delete(tezScratchDir, true);
    }
    session = null;
    tezScratchDir = null;
    conf = null;
    appJarLr = null;
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
    fs.mkdirs(tezDir);

    // don't keep the directory around on non-clean exit
    fs.deleteOnExit(tezDir);

    return tezDir;
  }

  /**
   * Returns a local resource representing the hive-exec jar. This resource will
   * be used to execute the plan on the cluster.
   * @return LocalResource corresponding to the localized hive exec resource.
   * @throws IOException when any file system related call fails.
   * @throws LoginException when we are unable to determine the user.
   * @throws URISyntaxException when current jar location cannot be determined.
   */
  private LocalResource createHiveExecLocalResource()
      throws IOException, LoginException, URISyntaxException {
    String hiveJarDir = conf.getVar(HiveConf.ConfVars.HIVE_JAR_DIRECTORY);
    String currentVersionPathStr = utils.getExecJarPathLocal();
    String currentJarName = utils.getResourceBaseName(currentVersionPathStr);
    FileSystem fs = null;
    Path jarPath = null;
    FileStatus dirStatus = null;

    if (hiveJarDir != null) {
      // check if it is a valid directory in HDFS
      Path hiveJarDirPath = new Path(hiveJarDir);
      fs = hiveJarDirPath.getFileSystem(conf);

      if (!(fs instanceof DistributedFileSystem)) {
        throw new IOException(ErrorMsg.INVALID_HDFS_URI.format(hiveJarDir));
      }

      try {
        dirStatus = fs.getFileStatus(hiveJarDirPath);
      } catch (FileNotFoundException fe) {
        // do nothing
      }
      if ((dirStatus != null) && (dirStatus.isDir())) {
        FileStatus[] listFileStatus = fs.listStatus(hiveJarDirPath);
        for (FileStatus fstatus : listFileStatus) {
          String jarName = utils.getResourceBaseName(fstatus.getPath().toString());
          if (jarName.equals(currentJarName)) {
            // we have found the jar we need.
            jarPath = fstatus.getPath();
            return utils.localizeResource(null, jarPath, conf);
          }
        }

        // jar wasn't in the directory, copy the one in current use
        if (jarPath == null) {
          Path dest = new Path(hiveJarDir + "/" + currentJarName);
          return utils.localizeResource(new Path(currentVersionPathStr), dest, conf);
        }
      }
    }

    /*
     * specified location does not exist or is not a directory
     * try to push the jar to the hdfs location pointed by
     * config variable HIVE_INSTALL_DIR. Path will be
     * HIVE_INSTALL_DIR/{username}/.hiveJars/
     */
    if ((hiveJarDir == null) || (dirStatus == null) ||
        ((dirStatus != null) && (!dirStatus.isDir()))) {
      Path dest = utils.getDefaultDestDir(conf);
      String destPathStr = dest.toString();
      String jarPathStr = destPathStr + "/" + currentJarName;
      dirStatus = fs.getFileStatus(dest);
      if (dirStatus.isDir()) {
        return utils.localizeResource(new Path(currentVersionPathStr), new Path(jarPathStr), conf);
      } else {
        throw new IOException(ErrorMsg.INVALID_DIR.format(dest.toString()));
      }
    }

    // we couldn't find any valid locations. Throw exception
    throw new IOException(ErrorMsg.NO_VALID_LOCATIONS.getMsg());
  }
}
