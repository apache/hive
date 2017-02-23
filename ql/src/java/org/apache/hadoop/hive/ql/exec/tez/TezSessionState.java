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

import java.util.Collection;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import javax.security.auth.login.LoginException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.coordinator.LlapCoordinator;
import org.apache.hadoop.hive.llap.impl.LlapProtocolClientImpl;
import org.apache.hadoop.hive.llap.security.LlapTokenClient;
import org.apache.hadoop.hive.llap.security.LlapTokenIdentifier;
import org.apache.hadoop.hive.llap.tez.LlapProtocolClientProxy;
import org.apache.hadoop.hive.llap.tezplugins.LlapContainerLauncher;
import org.apache.hadoop.hive.llap.tezplugins.LlapTaskCommunicator;
import org.apache.hadoop.hive.llap.tezplugins.LlapTaskSchedulerService;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.PreWarmVertex;
import org.apache.tez.dag.api.SessionNotRunning;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.serviceplugins.api.ContainerLauncherDescriptor;
import org.apache.tez.serviceplugins.api.ServicePluginsDescriptor;
import org.apache.tez.serviceplugins.api.TaskCommunicatorDescriptor;
import org.apache.tez.serviceplugins.api.TaskSchedulerDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.tez.monitoring.TezJobMonitor;

/**
 * Holds session state related to Tez
 */
public class TezSessionState {

  private static final Logger LOG = LoggerFactory.getLogger(TezSessionState.class.getName());
  private static final String TEZ_DIR = "_tez_session_dir";
  public static final String LLAP_SERVICE = "LLAP";
  private static final String LLAP_SCHEDULER = LlapTaskSchedulerService.class.getName();
  private static final String LLAP_LAUNCHER = LlapContainerLauncher.class.getName();
  private static final String LLAP_TASK_COMMUNICATOR = LlapTaskCommunicator.class.getName();

  private HiveConf conf;
  private Path tezScratchDir;
  private LocalResource appJarLr;
  private TezClient session;
  private Future<TezClient> sessionFuture;
  /** Console used for user feedback during async session opening. */
  private LogHelper console;
  private String sessionId;
  private final DagUtils utils;
  private String queueName;
  private boolean defaultQueue = false;
  private String user;

  private AtomicReference<String> ownerThread = new AtomicReference<>(null);

  private final Set<String> additionalFilesNotFromConf = new HashSet<String>();
  private final Set<LocalResource> localizedResources = new HashSet<LocalResource>();
  private boolean doAsEnabled;

  /**
   * Constructor. We do not automatically connect, because we only want to
   * load tez classes when the user has tez installed.
   */
  public TezSessionState(DagUtils utils) {
    this.utils = utils;
  }

  public String toString() {
    return "sessionId=" + sessionId + ", queueName=" + queueName + ", user=" + user
        + ", doAs=" + doAsEnabled + ", isOpen=" + isOpen() + ", isDefault=" + defaultQueue;
  }

  /**
   * Constructor. We do not automatically connect, because we only want to
   * load tez classes when the user has tez installed.
   */
  public TezSessionState(String sessionId) {
    this(DagUtils.getInstance());
    this.sessionId = sessionId;
  }

  public boolean isOpening() {
    if (session != null || sessionFuture == null) return false;
    try {
      session = sessionFuture.get(0, TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    } catch (CancellationException e) {
      return false;
    } catch (TimeoutException e) {
      return true;
    }
    return false;
  }

  public boolean isOpen() {
    if (session != null) return true;
    if (sessionFuture == null) return false;
    try {
      session = sessionFuture.get(0, TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    } catch (TimeoutException | CancellationException e) {
      return false;
    }
    return true;
  }


  /**
   * Get all open sessions. Only used to clean up at shutdown.
   * @return List<TezSessionState>
   */
  public static String makeSessionId() {
    return UUID.randomUUID().toString();
  }

  public void open(HiveConf conf)
      throws IOException, LoginException, URISyntaxException, TezException {
    Set<String> noFiles = null;
    open(conf, noFiles, null);
  }

  /**
   * Creates a tez session. A session is tied to either a cli/hs2 session. You can
   * submit multiple DAGs against a session (as long as they are executed serially).
   * @throws IOException
   * @throws URISyntaxException
   * @throws LoginException
   * @throws TezException
   * @throws InterruptedException
   */
  public void open(HiveConf conf, String[] additionalFiles)
    throws IOException, LoginException, IllegalArgumentException, URISyntaxException, TezException {
    openInternal(conf, setFromArray(additionalFiles), false, null, null);
  }

  private static Set<String> setFromArray(String[] additionalFiles) {
    if (additionalFiles == null) return null;
    Set<String> files = new HashSet<>();
    for (String originalFile : additionalFiles) {
      files.add(originalFile);
    }
    return files;
  }

  public void beginOpen(HiveConf conf, String[] additionalFiles, LogHelper console)
    throws IOException, LoginException, IllegalArgumentException, URISyntaxException, TezException {
    openInternal(conf, setFromArray(additionalFiles), true, console, null);
  }

  public void open(HiveConf conf, Collection<String> additionalFiles, Path scratchDir)
      throws LoginException, IOException, URISyntaxException, TezException {
    openInternal(conf, additionalFiles, false, null, scratchDir);
  }

  protected void openInternal(final HiveConf conf, Collection<String> additionalFiles,
      boolean isAsync, LogHelper console, Path scratchDir) throws IOException, LoginException,
        IllegalArgumentException, URISyntaxException, TezException {
    this.conf = conf;
    // TODO Why is the queue name set again. It has already been setup via setQueueName. Do only one of the two.
    String confQueueName = conf.get(TezConfiguration.TEZ_QUEUE_NAME);
    if (queueName != null && !queueName.equals(confQueueName)) {
      LOG.warn("Resetting a queue name that was already set: was "
          + queueName + ", now " + confQueueName);
    }
    this.queueName = confQueueName;
    this.doAsEnabled = conf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS);

    final boolean llapMode = "llap".equalsIgnoreCase(HiveConf.getVar(
        conf, HiveConf.ConfVars.HIVE_EXECUTION_MODE));

    // TODO This - at least for the session pool - will always be the hive user. How does doAs above this affect things ?
    UserGroupInformation ugi = Utils.getUGI();
    user = ugi.getShortUserName();
    LOG.info("User of session id " + sessionId + " is " + user);

    // create the tez tmp dir
    tezScratchDir = scratchDir == null ? createTezDir(sessionId) : scratchDir;

    additionalFilesNotFromConf.clear();
    if (additionalFiles != null) {
      additionalFilesNotFromConf.addAll(additionalFiles);
    }

    refreshLocalResourcesFromConf(conf);

    // unless already installed on all the cluster nodes, we'll have to
    // localize hive-exec.jar as well.
    appJarLr = createJarLocalResource(utils.getExecJarPathLocal());

    // configuration for the application master
    final Map<String, LocalResource> commonLocalResources = new HashMap<String, LocalResource>();
    commonLocalResources.put(utils.getBaseName(appJarLr), appJarLr);
    for (LocalResource lr : localizedResources) {
      commonLocalResources.put(utils.getBaseName(lr), lr);
    }

    if (llapMode) {
      // localize llap client jars
      addJarLRByClass(LlapTaskSchedulerService.class, commonLocalResources);
      addJarLRByClass(LlapProtocolClientImpl.class, commonLocalResources);
      addJarLRByClass(LlapProtocolClientProxy.class, commonLocalResources);
      addJarLRByClassName("org.apache.hadoop.registry.client.api.RegistryOperations", commonLocalResources);
    }

    // Create environment for AM.
    Map<String, String> amEnv = new HashMap<String, String>();
    MRHelpers.updateEnvBasedOnMRAMEnv(conf, amEnv);

    // and finally we're ready to create and start the session
    // generate basic tez config
    final TezConfiguration tezConfig = new TezConfiguration(conf);

    // set up the staging directory to use
    tezConfig.set(TezConfiguration.TEZ_AM_STAGING_DIR, tezScratchDir.toUri().toString());
    conf.stripHiddenConfigurations(tezConfig);

    ServicePluginsDescriptor servicePluginsDescriptor;

    Credentials llapCredentials = null;
    if (llapMode) {
      if (UserGroupInformation.isSecurityEnabled()) {
        llapCredentials = new Credentials();
        llapCredentials.addToken(LlapTokenIdentifier.KIND_NAME, getLlapToken(user, tezConfig));
      }
      // TODO Change this to not serialize the entire Configuration - minor.
      UserPayload servicePluginPayload = TezUtils.createUserPayloadFromConf(tezConfig);
      // we need plugins to handle llap and uber mode
      servicePluginsDescriptor = ServicePluginsDescriptor.create(true,
          new TaskSchedulerDescriptor[] { TaskSchedulerDescriptor.create(
              LLAP_SERVICE, LLAP_SCHEDULER).setUserPayload(servicePluginPayload) },
          new ContainerLauncherDescriptor[] { ContainerLauncherDescriptor.create(
              LLAP_SERVICE, LLAP_LAUNCHER) },
          new TaskCommunicatorDescriptor[] { TaskCommunicatorDescriptor.create(
              LLAP_SERVICE, LLAP_TASK_COMMUNICATOR).setUserPayload(servicePluginPayload) });
    } else {
      servicePluginsDescriptor = ServicePluginsDescriptor.create(true);
    }

    // container prewarming. tell the am how many containers we need
    if (HiveConf.getBoolVar(conf, ConfVars.HIVE_PREWARM_ENABLED)) {
      int n = HiveConf.getIntVar(conf, ConfVars.HIVE_PREWARM_NUM_CONTAINERS);
      n = Math.max(tezConfig.getInt(
          TezConfiguration.TEZ_AM_SESSION_MIN_HELD_CONTAINERS,
          TezConfiguration.TEZ_AM_SESSION_MIN_HELD_CONTAINERS_DEFAULT), n);
      tezConfig.setInt(TezConfiguration.TEZ_AM_SESSION_MIN_HELD_CONTAINERS, n);
    }

    setupSessionAcls(tezConfig, conf);

    final TezClient session = TezClient.newBuilder("HIVE-" + sessionId, tezConfig)
        .setIsSession(true).setLocalResources(commonLocalResources)
        .setCredentials(llapCredentials).setServicePluginDescriptor(servicePluginsDescriptor)
        .build();

    LOG.info("Opening new Tez Session (id: " + sessionId
        + ", scratch dir: " + tezScratchDir + ")");

    TezJobMonitor.initShutdownHook();
    if (!isAsync) {
      startSessionAndContainers(session, conf, commonLocalResources, tezConfig, false);
      this.session = session;
    } else {
      FutureTask<TezClient> sessionFuture = new FutureTask<>(new Callable<TezClient>() {
        @Override
        public TezClient call() throws Exception {
          try {
            return startSessionAndContainers(session, conf, commonLocalResources, tezConfig, true);
          } catch (Throwable t) {
            LOG.error("Failed to start Tez session", t);
            throw (t instanceof Exception) ? (Exception)t : new Exception(t);
          }
        }
      });
      new Thread(sessionFuture, "Tez session start thread").start();
      // We assume here nobody will try to get session before open() returns.
      this.console = console;
      this.sessionFuture = sessionFuture;
    }
  }

  private static Token<LlapTokenIdentifier> getLlapToken(
      String user, final Configuration conf) throws IOException {
    // TODO: parts of this should be moved out of TezSession to reuse the clients, but there's
    //       no good place for that right now (HIVE-13698).
    SessionState session = SessionState.get();
    boolean isInHs2 = session != null && session.isHiveServerQuery();
    Token<LlapTokenIdentifier> token = null;
    // For Tez, we don't use appId to distinguish the tokens.

    LlapCoordinator coordinator = null;
    if (isInHs2) {
      // We are in HS2, get the token locally.
      // TODO: coordinator should be passed in; HIVE-13698. Must be initialized for now.
      coordinator = LlapCoordinator.getInstance();
      if (coordinator == null) {
        throw new IOException("LLAP coordinator not initialized; cannot get LLAP tokens");
      }
      // Signing is not required for Tez.
      token = coordinator.getLocalTokenClient(conf, user).createToken(null, null, false);
    } else {
      // We are not in HS2; always create a new client for now.
      token = new LlapTokenClient(conf).getDelegationToken(null);
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("Obtained a LLAP token: " + token);
    }
    return token;
  }

  private TezClient startSessionAndContainers(TezClient session, HiveConf conf,
      Map<String, LocalResource> commonLocalResources, TezConfiguration tezConfig,
      boolean isOnThread) throws TezException, IOException {
    session.start();
    boolean isSuccessful = false;
    try {
      if (HiveConf.getBoolVar(conf, ConfVars.HIVE_PREWARM_ENABLED)) {
        int n = HiveConf.getIntVar(conf, ConfVars.HIVE_PREWARM_NUM_CONTAINERS);
        LOG.info("Prewarming " + n + " containers  (id: " + sessionId
            + ", scratch dir: " + tezScratchDir + ")");
        PreWarmVertex prewarmVertex = utils.createPreWarmVertex(
            tezConfig, n, commonLocalResources);
        try {
          session.preWarm(prewarmVertex);
        } catch (IOException ie) {
          if (!isOnThread && ie.getMessage().contains("Interrupted while waiting")) {
            LOG.warn("Hive Prewarm threw an exception ", ie);
          } else {
            throw ie;
          }
        }
      }
      try {
        session.waitTillReady();
      } catch (InterruptedException ie) {
        if (isOnThread) throw new IOException(ie);
        //ignore
      }
      isSuccessful = true;
      // sessionState.getQueueName() comes from cluster wide configured queue names.
      // sessionState.getConf().get("tez.queue.name") is explicitly set by user in a session.
      // TezSessionPoolManager sets tez.queue.name if user has specified one or use the one from
      // cluster wide queue names.
      // There is no way to differentiate how this was set (user vs system).
      // Unset this after opening the session so that reopening of session uses the correct queue
      // names i.e, if client has not died and if the user has explicitly set a queue name
      // then reopened session will use user specified queue name else default cluster queue names.
      conf.unset(TezConfiguration.TEZ_QUEUE_NAME);
      return session;
    } finally {
      if (isOnThread && !isSuccessful) {
        closeAndIgnoreExceptions(session);
      }
    }
  }

  private static void closeAndIgnoreExceptions(TezClient session) {
    try {
      session.stop();
    } catch (SessionNotRunning nr) {
      // Ignore.
    } catch (IOException | TezException ex) {
      LOG.info("Failed to close Tez session after failure to initialize: " + ex.getMessage());
    }
  }

  public void endOpen() throws InterruptedException, CancellationException {
    if (this.session != null || this.sessionFuture == null) return;
    try {
      this.session = this.sessionFuture.get();
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private void setupSessionAcls(Configuration tezConf, HiveConf hiveConf) throws
      IOException {

    String user = SessionState.getUserFromAuthenticator();
    UserGroupInformation loginUserUgi = UserGroupInformation.getLoginUser();
    String loginUser =
        loginUserUgi == null ? null : loginUserUgi.getShortUserName();
    boolean addHs2User =
        HiveConf.getBoolVar(hiveConf, ConfVars.HIVETEZHS2USERACCESS);

    String viewStr = Utilities.getAclStringWithHiveModification(tezConf,
            TezConfiguration.TEZ_AM_VIEW_ACLS, addHs2User, user, loginUser);
    String modifyStr = Utilities.getAclStringWithHiveModification(tezConf,
            TezConfiguration.TEZ_AM_MODIFY_ACLS, addHs2User, user, loginUser);

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Setting Tez Session access for sessionId={} with viewAclString={}, modifyStr={}",
          SessionState.get().getSessionId(), viewStr, modifyStr);
    }

    tezConf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, viewStr);
    tezConf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, modifyStr);
  }

  public void refreshLocalResourcesFromConf(HiveConf conf)
    throws IOException, LoginException, IllegalArgumentException, URISyntaxException, TezException {

    String dir = tezScratchDir.toString();

    localizedResources.clear();

    // these are local resources set through add file, jar, etc
    List<LocalResource> lrs = utils.localizeTempFilesFromConf(dir, conf);
    if (lrs != null) {
      localizedResources.addAll(lrs);
    }

    // these are local resources that are set through the mr "tmpjars" property
    List<LocalResource> handlerLr = utils.localizeTempFiles(dir, conf,
      additionalFilesNotFromConf.toArray(new String[additionalFilesNotFromConf.size()]));

    if (handlerLr != null) {
      localizedResources.addAll(handlerLr);
    }
  }

  public boolean hasResources(String[] localAmResources) {
    if (localAmResources == null || localAmResources.length == 0) return true;
    if (additionalFilesNotFromConf.isEmpty()) return false;
    for (String s : localAmResources) {
      if (!additionalFilesNotFromConf.contains(s)) return false;
    }
    return true;
  }

  /**
   * Close a tez session. Will cleanup any tez/am related resources. After closing a session no
   * further DAGs can be executed against it.
   *
   * @param keepTmpDir
   *          whether or not to remove the scratch dir at the same time.
   * @throws Exception
   */
  public void close(boolean keepTmpDir) throws Exception {
    if (session != null) {
      LOG.info("Closing Tez Session");
      closeClient(session);
    } else if (sessionFuture != null) {
      sessionFuture.cancel(true);
      TezClient asyncSession = null;
      try {
        asyncSession = sessionFuture.get(); // In case it was done and noone looked at it.
      } catch (ExecutionException | CancellationException e) {
        // ignore
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // ignore
      }
      if (asyncSession != null) {
        LOG.info("Closing Tez Session");
        closeClient(asyncSession);
      }
    }

    if (!keepTmpDir) {
      cleanupScratchDir();
    }
    session = null;
    sessionFuture = null;
    console = null;
    tezScratchDir = null;
    conf = null;
    appJarLr = null;
    additionalFilesNotFromConf.clear();
    localizedResources.clear();
  }

  public Set<String> getAdditionalFilesNotFromConf() {
    return additionalFilesNotFromConf;
  }

  private void closeClient(TezClient client) throws TezException,
      IOException {
    try {
      client.stop();
    } catch (SessionNotRunning nr) {
      // ignore
    }
  }

  public void cleanupScratchDir () throws IOException {
    FileSystem fs = tezScratchDir.getFileSystem(conf);
    fs.delete(tezScratchDir, true);
    tezScratchDir = null;
  }

  public String getSessionId() {
    return sessionId;
  }

  public TezClient getSession() {
    if (session == null && sessionFuture != null) {
      if (!sessionFuture.isDone()) {
        console.printInfo("Waiting for Tez session and AM to be ready...");
      }
      try {
        session = sessionFuture.get();
      } catch (InterruptedException e) {
        console.printInfo("Interrupted while waiting for the session");
        Thread.currentThread().interrupt();
        return null;
      } catch (ExecutionException e) {
        console.printInfo("Failed to get session");
        throw new RuntimeException(e);
      } catch (CancellationException e) {
        console.printInfo("Cancelled while waiting for the session");
        return null;
      }
    }
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
  private Path createTezDir(String sessionId) throws IOException {
    // tez needs its own scratch dir (per session)
    Path tezDir = new Path(SessionState.get().getHdfsScratchDirURIString(), TEZ_DIR);
    tezDir = new Path(tezDir, sessionId);
    FileSystem fs = tezDir.getFileSystem(conf);
    FsPermission fsPermission = new FsPermission(HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCHDIRPERMISSION));
    fs.mkdirs(tezDir, fsPermission);
    // Make sure the path is normalized (we expect validation to pass since we just created it).
    tezDir = DagUtils.validateTargetDir(tezDir, conf).getPath();

    // Directory removal will be handled by cleanup at the SessionState level.
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
    // TODO Reduce the number of lookups that happen here. This shouldn't go to HDFS for each call.
    // The hiveJarDir can be determined once per client.
    FileStatus destDirStatus = utils.getHiveJarDirectory(conf);
    assert destDirStatus != null;
    Path destDirPath = destDirStatus.getPath();

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
    return utils.localizeResource(localFile, destFile, LocalResourceType.FILE, conf);
  }

  private void addJarLRByClassName(String className, final Map<String, LocalResource> lrMap) throws
      IOException, LoginException {
    Class<?> clazz;
    try {
      clazz = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new IOException("Cannot find " + className + " in classpath", e);
    }
    addJarLRByClass(clazz, lrMap);
  }

  private void addJarLRByClass(Class<?> clazz, final Map<String, LocalResource> lrMap) throws IOException,
      LoginException {
    final File jar =
        new File(Utilities.jarFinderGetJar(clazz));
    final LocalResource jarLr =
        createJarLocalResource(jar.toURI().toURL().toExternalForm());
    lrMap.put(utils.getBaseName(jarLr), jarLr);
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
    defaultQueue = true;
  }

  public boolean isDefault() {
    return defaultQueue;
  }

  public HiveConf getConf() {
    return conf;
  }

  public List<LocalResource> getLocalizedResources() {
    return new ArrayList<>(localizedResources);
  }

  public String getUser() {
    return user;
  }

  public boolean getDoAsEnabled() {
    return doAsEnabled;
  }

  /** Mark session as free for use from TezTask, for safety/debugging purposes. */
  public void markFree() {
    if (ownerThread.getAndSet(null) == null) throw new AssertionError("Not in use");
  }

  /** Mark session as being in use from TezTask, for safety/debugging purposes. */
  public void markInUse() {
    String newName = Thread.currentThread().getName();
    do {
      String oldName = ownerThread.get();
      if (oldName != null) {
        throw new AssertionError("Tez session is already in use from "
            + oldName + "; cannot use from " + newName);
      }
    } while (!ownerThread.compareAndSet(null, newName));
  }

}
