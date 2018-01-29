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
package org.apache.hadoop.hive.ql.exec.tez;

import org.apache.hadoop.registry.client.api.RegistryOperations;

import java.io.File;
import java.io.IOException;
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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.llap.coordinator.LlapCoordinator;
import org.apache.hadoop.hive.llap.impl.LlapProtocolClientImpl;
import org.apache.hadoop.hive.llap.security.LlapTokenClient;
import org.apache.hadoop.hive.llap.security.LlapTokenIdentifier;
import org.apache.hadoop.hive.llap.tez.LlapProtocolClientProxy;
import org.apache.hadoop.hive.llap.tezplugins.LlapContainerLauncher;
import org.apache.hadoop.hive.llap.tezplugins.LlapTaskCommunicator;
import org.apache.hadoop.hive.llap.tezplugins.LlapTaskSchedulerService;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.session.KillQuery;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.wm.WmContext;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.PreWarmVertex;
import org.apache.tez.dag.api.SessionNotRunning;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.mapreduce.hadoop.DeprecatedKeys;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.serviceplugins.api.ContainerLauncherDescriptor;
import org.apache.tez.serviceplugins.api.ServicePluginsDescriptor;
import org.apache.tez.serviceplugins.api.TaskCommunicatorDescriptor;
import org.apache.tez.serviceplugins.api.TaskSchedulerDescriptor;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.tez.monitoring.TezJobMonitor;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Holds session state related to Tez
 */
@JsonSerialize
public class TezSessionState {

  protected static final Logger LOG = LoggerFactory.getLogger(TezSessionState.class.getName());
  private static final String TEZ_DIR = "_tez_session_dir";
  public static final String LLAP_SERVICE = "LLAP";
  private static final String LLAP_SCHEDULER = LlapTaskSchedulerService.class.getName();
  private static final String LLAP_LAUNCHER = LlapContainerLauncher.class.getName();
  private static final String LLAP_TASK_COMMUNICATOR = LlapTaskCommunicator.class.getName();

  private final HiveConf conf;
  private Path tezScratchDir;
  private LocalResource appJarLr;
  private TezClient session;
  private Future<TezClient> sessionFuture;
  /** Console used for user feedback during async session opening. */
  private LogHelper console;
  @JsonProperty("sessionId")
  private String sessionId;
  private final DagUtils utils;
  @JsonProperty("queueName")
  private String queueName;
  @JsonProperty("defaultQueue")
  private boolean defaultQueue = false;
  @JsonProperty("user")
  private String user;

  private AtomicReference<String> ownerThread = new AtomicReference<>(null);

  public static final class HiveResources {
    public HiveResources(Path dagResourcesDir) {
      this.dagResourcesDir = dagResourcesDir;
    }
    /** A directory that will contain resources related to DAGs and specified in configs. */
    public final Path dagResourcesDir;
    public final Set<String> additionalFilesNotFromConf = new HashSet<>();
    /** Localized resources of this session; both from conf and not from conf (above). */
    public final Set<LocalResource> localizedResources = new HashSet<>();

    @Override
    public String toString() {
      return dagResourcesDir + "; " + additionalFilesNotFromConf.size() + " additional files, "
          + localizedResources.size() + " localized resources";
    }
  }

  private HiveResources resources;
  @JsonProperty("doAsEnabled")
  private boolean doAsEnabled;
  private boolean isLegacyLlapMode;
  private WmContext wmContext;
  private KillQuery killQuery;

  private static final Cache<String, String> shaCache = CacheBuilder.newBuilder().maximumSize(100).build();
  /**
   * Constructor. We do not automatically connect, because we only want to
   * load tez classes when the user has tez installed.
   */
  public TezSessionState(DagUtils utils, HiveConf conf) {
    this.utils = utils;
    this.conf = conf;
  }

  @Override
  public String toString() {
    return "sessionId=" + sessionId + ", queueName=" + queueName + ", user=" + user
        + ", doAs=" + doAsEnabled + ", isOpen=" + isOpen() + ", isDefault=" + defaultQueue;
  }

  /**
   * Constructor. We do not automatically connect, because we only want to
   * load tez classes when the user has tez installed.
   */
  public TezSessionState(String sessionId, HiveConf conf) {
    this(DagUtils.getInstance(), conf);
    this.sessionId = sessionId;
  }

  public boolean isOpening() {
    if (session != null || sessionFuture == null) {
      return false;
    }
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
    if (session != null) {
      return true;
    }
    if (sessionFuture == null) {
      return false;
    }
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

  public void open() throws IOException, LoginException, URISyntaxException, TezException {
    String[] noFiles = null;
    open(noFiles);
  }

  /**
   * Creates a tez session. A session is tied to either a cli/hs2 session. You can
   * submit multiple DAGs against a session (as long as they are executed serially).
   */
  public void open(String[] additionalFilesNotFromConf)
      throws IOException, LoginException, URISyntaxException, TezException {
    openInternal(additionalFilesNotFromConf, false, null, null);
  }


  public void open(HiveResources resources)
      throws LoginException, IOException, URISyntaxException, TezException {
    openInternal(null, false, null, resources);
  }

  public void beginOpen(String[] additionalFiles, LogHelper console)
      throws IOException, LoginException, URISyntaxException, TezException {
    openInternal(additionalFiles, true, console, null);
  }

  protected void openInternal(String[] additionalFilesNotFromConf,
      boolean isAsync, LogHelper console, HiveResources resources)
          throws IOException, LoginException, URISyntaxException, TezException {
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

    // Create the tez tmp dir and a directory for Hive resources.
    tezScratchDir = createTezDir(sessionId, null);
    if (resources != null) {
      // If we are getting the resources externally, don't relocalize anything.
      this.resources = resources;
      LOG.info("Setting resources to " + resources);
    } else {
      this.resources = new HiveResources(createTezDir(sessionId, "resources"));
      ensureLocalResources(conf, additionalFilesNotFromConf);
      LOG.info("Created new resources: " + resources);
    }

    // unless already installed on all the cluster nodes, we'll have to
    // localize hive-exec.jar as well.
    appJarLr = createJarLocalResource(utils.getExecJarPathLocal(conf));

    // configuration for the application master
    final Map<String, LocalResource> commonLocalResources = new HashMap<String, LocalResource>();
    commonLocalResources.put(DagUtils.getBaseName(appJarLr), appJarLr);
    for (LocalResource lr : this.resources.localizedResources) {
      commonLocalResources.put(DagUtils.getBaseName(lr), lr);
    }

    if (llapMode) {
      // localize llap client jars
      addJarLRByClass(LlapTaskSchedulerService.class, commonLocalResources);
      addJarLRByClass(LlapProtocolClientImpl.class, commonLocalResources);
      addJarLRByClass(LlapProtocolClientProxy.class, commonLocalResources);
      addJarLRByClass(RegistryOperations.class, commonLocalResources);
    }

    // Create environment for AM.
    Map<String, String> amEnv = new HashMap<String, String>();
    MRHelpers.updateEnvBasedOnMRAMEnv(conf, amEnv);

    // and finally we're ready to create and start the session
    // generate basic tez config
    final TezConfiguration tezConfig = new TezConfiguration(true);
    tezConfig.addResource(conf);

    setupTezParamsBasedOnMR(tezConfig);

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
    // TODO: De-link from SessionState. A TezSession can be linked to different Hive Sessions via the pool.
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
        if (isOnThread) {
          throw new IOException(ie);
          //ignore
        }
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
    if (this.session != null || this.sessionFuture == null) {
      return;
    }
    try {
      this.session = this.sessionFuture.get();
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This takes settings from MR and applies them to the appropriate Tez configuration. This is
   * similar to what Pig on Tez does (refer MRToTezHelper.java).
   *
   * @param conf configuration with MR settings
   */
  private void setupTezParamsBasedOnMR(TezConfiguration conf) {

    String env = conf.get(MRJobConfig.MR_AM_ADMIN_USER_ENV);
    if (conf.get(MRJobConfig.MR_AM_ENV) != null) {
      env = (env == null) ? conf.get(MRJobConfig.MR_AM_ENV) : env + "," + conf.get(MRJobConfig.MR_AM_ENV);
    }
    if (env != null) {
      conf.setIfUnset(TezConfiguration.TEZ_AM_LAUNCH_ENV, env);
    }

    conf.setIfUnset(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS,
        org.apache.tez.mapreduce.hadoop.MRHelpers.getJavaOptsForMRAM(conf));

    String queueName = conf.get(JobContext.QUEUE_NAME, YarnConfiguration.DEFAULT_QUEUE_NAME);
    conf.setIfUnset(TezConfiguration.TEZ_QUEUE_NAME, queueName);

    int amMemMB = conf.getInt(MRJobConfig.MR_AM_VMEM_MB, MRJobConfig.DEFAULT_MR_AM_VMEM_MB);
    conf.setIfUnset(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB, "" + amMemMB);

    int amCores = conf.getInt(MRJobConfig.MR_AM_CPU_VCORES, MRJobConfig.DEFAULT_MR_AM_CPU_VCORES);
    conf.setIfUnset(TezConfiguration.TEZ_AM_RESOURCE_CPU_VCORES, "" + amCores);

    conf.setIfUnset(TezConfiguration.TEZ_AM_MAX_APP_ATTEMPTS, ""
        + conf.getInt(MRJobConfig.MR_AM_MAX_ATTEMPTS, MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS));

    conf.setIfUnset(TezConfiguration.TEZ_AM_VIEW_ACLS,
        conf.get(MRJobConfig.JOB_ACL_VIEW_JOB, MRJobConfig.DEFAULT_JOB_ACL_VIEW_JOB));

    conf.setIfUnset(TezConfiguration.TEZ_AM_MODIFY_ACLS,
        conf.get(MRJobConfig.JOB_ACL_MODIFY_JOB, MRJobConfig.DEFAULT_JOB_ACL_MODIFY_JOB));


    // Refer to org.apache.tez.mapreduce.hadoop.MRHelpers.processDirectConversion.
    ArrayList<Map<String, String>> maps = new ArrayList<Map<String, String>>(2);
    maps.add(DeprecatedKeys.getMRToTezRuntimeParamMap());
    maps.add(DeprecatedKeys.getMRToDAGParamMap());

    boolean preferTez = true; // Can make this configurable.

    for (Map<String, String> map : maps) {
      for (Map.Entry<String, String> dep : map.entrySet()) {
        if (conf.get(dep.getKey()) != null) {
          // TODO Deprecation reason does not seem to reflect in the config ?
          // The ordering is important in case of keys which are also deprecated.
          // Unset will unset the deprecated keys and all its variants.
          final String mrValue = conf.get(dep.getKey());
          final String tezValue = conf.get(dep.getValue());
          conf.unset(dep.getKey());
          if (tezValue == null) {
            conf.set(dep.getValue(), mrValue, "TRANSLATED_TO_TEZ");
          } else if (!preferTez) {
            conf.set(dep.getValue(), mrValue, "TRANSLATED_TO_TEZ_AND_MR_OVERRIDE");
          }
          LOG.info("Config: mr(unset):" + dep.getKey() + ", mr initial value="
              + mrValue
              + ", tez(original):" + dep.getValue() + "=" + tezValue
              + ", tez(final):" + dep.getValue() + "=" + conf.get(dep.getValue()));
        }
      }
    }
  }

  private void setupSessionAcls(Configuration tezConf, HiveConf hiveConf) throws
      IOException {

    // TODO: De-link from SessionState. A TezSession can be linked to different Hive Sessions via the pool.
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
      // TODO: De-link from SessionState. A TezSession can be linked to different Hive Sessions via the pool.
      LOG.debug(
          "Setting Tez Session access for sessionId={} with viewAclString={}, modifyStr={}",
          SessionState.get().getSessionId(), viewStr, modifyStr);
    }

    tezConf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, viewStr);
    tezConf.set(TezConfiguration.TEZ_AM_MODIFY_ACLS, modifyStr);
  }

  /** This is called in openInternal and in TezTask.updateSession to localize conf resources. */
  public void ensureLocalResources(Configuration conf, String[] newFilesNotFromConf)
          throws IOException, LoginException, URISyntaxException, TezException {
    if (resources == null) {
      throw new AssertionError("Ensure called on an unitialized (or closed) session " + sessionId);
    }
    String dir = resources.dagResourcesDir.toString();
    resources.localizedResources.clear();

    // Always localize files from conf; duplicates are handled on FS level.
    // TODO: we could do the same thing as below and only localize if missing.
    //       That could be especially valuable given that this almost always the same set.
    List<LocalResource> lrs = utils.localizeTempFilesFromConf(dir, conf);
    if (lrs != null) {
      resources.localizedResources.addAll(lrs);
    }

    // Localize the non-conf resources that are missing from the current list.
    List<LocalResource> newResources = null;
    if (newFilesNotFromConf != null && newFilesNotFromConf.length > 0) {
      boolean hasResources = !resources.additionalFilesNotFromConf.isEmpty();
      if (hasResources) {
        for (String s : newFilesNotFromConf) {
          hasResources = resources.additionalFilesNotFromConf.contains(s);
          if (!hasResources) {
            break;
          }
        }
      }
      if (!hasResources) {
        String[] skipFilesFromConf = DagUtils.getTempFilesFromConf(conf);
        newResources = utils.localizeTempFiles(dir, conf, newFilesNotFromConf, skipFilesFromConf);
        if (newResources != null) {
          resources.localizedResources.addAll(newResources);
        }
        for (String fullName : newFilesNotFromConf) {
          resources.additionalFilesNotFromConf.add(fullName);
        }
      }
    }

    // Finally, add the files to the existing AM (if any). The old code seems to do this twice,
    // first for all the new resources regardless of type; and then for all the session resources
    // that are not of type file (see branch-1 calls to addAppMasterLocalFiles: from updateSession
    // and with resourceMap from submit).
    // TODO: Do we really need all this nonsense?
    if (session != null) {
      if (newResources != null && !newResources.isEmpty()) {
        session.addAppMasterLocalFiles(DagUtils.createTezLrMap(null, newResources));
      }
      if (!resources.localizedResources.isEmpty()) {
        session.addAppMasterLocalFiles(
            DagUtils.getResourcesUpdatableForAm(resources.localizedResources));
      }
    }
  }

  /**
   * Close a tez session. Will cleanup any tez/am related resources. After closing a session no
   * further DAGs can be executed against it. Only called by session management classes; some
   * sessions should not simply be closed by users - e.g. pool sessions need to be restarted.
   *
   * @param keepDagFilesDir
   *          whether or not to remove the scratch dir at the same time.
   * @throws Exception
   */
  void close(boolean keepDagFilesDir) throws Exception {
    console = null;
    appJarLr = null;

    try {
      if (session != null) {
        LOG.info("Closing Tez Session");
        closeClient(session);
        session = null;
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
        sessionFuture = null;
        if (asyncSession != null) {
          LOG.info("Closing Tez Session");
          closeClient(asyncSession);
        }
      }
    } finally {
      try {
        cleanupScratchDir();
      } finally {
        if (!keepDagFilesDir) {
          cleanupDagResources();
        }
      }
    }
  }

  private void closeClient(TezClient client) throws TezException,
      IOException {
    try {
      client.stop();
    } catch (SessionNotRunning nr) {
      // ignore
    }
  }

  protected final void cleanupScratchDir() throws IOException {
    if (tezScratchDir != null) {
      FileSystem fs = tezScratchDir.getFileSystem(conf);
      fs.delete(tezScratchDir, true);
      tezScratchDir = null;
    }
  }

  protected final void cleanupDagResources() throws IOException {
    LOG.info("Attemting to clean up resources for " + sessionId + ": " + resources);
    if (resources != null) {
      FileSystem fs = resources.dagResourcesDir.getFileSystem(conf);
      fs.delete(resources.dagResourcesDir, true);
      resources = null;
    }
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

  public LocalResource getAppJarLr() {
    return appJarLr;
  }

  /**
   * createTezDir creates a temporary directory in the scratchDir folder to
   * be used with Tez. Assumes scratchDir exists.
   */
  private Path createTezDir(String sessionId, String suffix) throws IOException {
    // tez needs its own scratch dir (per session)
    // TODO: De-link from SessionState. A TezSession can be linked to different Hive Sessions via the pool.
    Path tezDir = new Path(SessionState.get().getHdfsScratchDirURIString(), TEZ_DIR);
    tezDir = new Path(tezDir, sessionId + ((suffix == null) ? "" : ("-" + suffix)));
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
      throws IOException, LoginException, IllegalArgumentException {
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

  private String getKey(final FileStatus fileStatus) {
    return fileStatus.getPath() + ":" + fileStatus.getLen() + ":" + fileStatus.getModificationTime();
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
    final String localJarPath = jar.toURI().toURL().toExternalForm();
    final LocalResource jarLr = createJarLocalResource(localJarPath);
    lrMap.put(DagUtils.getBaseName(jarLr), jarLr);
  }

  private String getSha(final Path localFile) throws IOException, IllegalArgumentException {
    FileSystem localFs = FileSystem.getLocal(conf);
    FileStatus fileStatus = localFs.getFileStatus(localFile);
    String key = getKey(fileStatus);
    String sha256 = shaCache.getIfPresent(key);
    if (sha256 == null) {
      FSDataInputStream is = null;
      try {
        is = localFs.open(localFile);
        long start = System.currentTimeMillis();
        sha256 = DigestUtils.sha256Hex(is);
        long end = System.currentTimeMillis();
        LOG.info("Computed sha: {} for file: {} of length: {} in {} ms", sha256, localFile,
          LlapUtil.humanReadableByteCount(fileStatus.getLen()), end - start);
        shaCache.put(key, sha256);
      } finally {
        if (is != null) {
          is.close();
        }
      }
    }
    return sha256;
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
    return new ArrayList<>(resources.localizedResources);
  }

  public String getUser() {
    return user;
  }

  public boolean getDoAsEnabled() {
    return doAsEnabled;
  }

  /** Mark session as free for use from TezTask, for safety/debugging purposes. */
  public void markFree() {
    if (ownerThread.getAndSet(null) == null) {
      throw new AssertionError("Not in use");
    }
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

  void setLegacyLlapMode(boolean value) {
    this.isLegacyLlapMode = value;
  }

  boolean getLegacyLlapMode() {
    return this.isLegacyLlapMode;
  }

  public void returnToSessionManager() throws Exception {
    // By default, TezSessionPoolManager handles this for both pool and non-pool session.
    TezSessionPoolManager.getInstance().returnSession(this);
  }

  public TezSessionState reopen() throws Exception {
    // By default, TezSessionPoolManager handles this for both pool and non-pool session.
    return TezSessionPoolManager.getInstance().reopen(this);
  }

  public void destroy() throws Exception {
    // By default, TezSessionPoolManager handles this for both pool and non-pool session.
    TezSessionPoolManager.getInstance().destroy(this);
  }

  public WmContext getWmContext() {
    return wmContext;
  }

  public void setWmContext(final WmContext wmContext) {
    this.wmContext = wmContext;
  }

  public void setKillQuery(final KillQuery killQuery) {
    this.killQuery = killQuery;
  }

  public KillQuery getKillQuery() {
    return killQuery;
  }

  public HiveResources extractHiveResources() {
    HiveResources result = resources;
    resources = null;
    return result;
  }

  public Path replaceHiveResources(HiveResources resources, boolean isAsync) {
    Path dir = null;
    if (this.resources != null) {
      dir = this.resources.dagResourcesDir;
      if (!isAsync) {
        try {
          dir.getFileSystem(conf).delete(dir, true);
        } catch (Exception ex) {
          LOG.error("Failed to delete the old resources directory "
              + dir + "; ignoring " + ex.getLocalizedMessage());
        }
        dir = null;
      }
    }
    this.resources = resources;
    return dir;
  }
}
