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

package org.apache.hadoop.hive.llap.cli.status;


import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.cli.LlapSliderUtils;
import org.apache.hadoop.hive.llap.configuration.LlapDaemonConfiguration;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstance;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.records.Container;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.client.ServiceClient;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Checks the status of the Llap.
 */
public class LlapStatusServiceDriver {
  private static final Logger LOG = LoggerFactory.getLogger(LlapStatusServiceDriver.class);
  private static final Logger CONSOLE_LOGGER = LoggerFactory.getLogger("LlapStatusServiceDriverConsole");

  private static final EnumSet<State> NO_YARN_SERVICE_INFO_STATES = EnumSet.of(
      State.APP_NOT_FOUND, State.COMPLETE, State.LAUNCHING);
  private static final EnumSet<State> LAUNCHING_STATES = EnumSet.of(
      State.LAUNCHING, State.RUNNING_PARTIAL, State.RUNNING_ALL);

  // Defining a bunch of configs here instead of in HiveConf. These are experimental, and mainly
  // for use when retry handling is fixed in Yarn/Hadoop

  private static final String CONF_PREFIX = "hive.llapcli.";

  // The following two keys should ideally be used to control RM connect timeouts. However,
  // they don't seem to work. The IPC timeout needs to be set instead.
  private static final String CONFIG_YARN_RM_TIMEOUT_MAX_WAIT_MS = CONF_PREFIX + "yarn.rm.connect.max-wait-ms";
  private static final long CONFIG_YARN_RM_TIMEOUT_MAX_WAIT_MS_DEFAULT = 10000L;
  private static final String CONFIG_YARN_RM_RETRY_INTERVAL_MS = CONF_PREFIX + "yarn.rm.connect.retry-interval.ms";
  private static final long CONFIG_YARN_RM_RETRY_INTERVAL_MS_DEFAULT = 5000L;

  // As of Hadoop 2.7 - this is what controls the RM timeout.
  private static final String CONFIG_IPC_CLIENT_CONNECT_MAX_RETRIES = CONF_PREFIX + "ipc.client.max-retries";
  private static final int CONFIG_IPC_CLIENT_CONNECT_MAX_RETRIES_DEFAULT = 2;
  private static final String CONFIG_IPC_CLIENT_CONNECT_RETRY_INTERVAL_MS =
      CONF_PREFIX + "ipc.client.connect.retry-interval-ms";
  private static final long CONFIG_IPC_CLIENT_CONNECT_RETRY_INTERVAL_MS_DEFAULT = 1500L;

  // As of Hadoop 2.8 - this timeout spec behaves in a strnage manner. "2000,1" means 2000s with 1 retry.
  // However it does this - but does it thrice. Essentially - #retries+2 is the number of times the entire config
  // is retried. "2000,1" means 3 retries - each with 1 retry with a random 2000ms sleep.
  private static final String CONFIG_TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_RETRY_POLICY_SPEC =
      CONF_PREFIX + "timeline.service.fs-store.retry.policy.spec";
  private static final String
      CONFIG_TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_RETRY_POLICY_SPEC_DEFAULT = "2000, 1";

  private static final String CONFIG_LLAP_ZK_REGISTRY_TIMEOUT_MS = CONF_PREFIX + "zk-registry.timeout-ms";
  private static final long CONFIG_LLAP_ZK_REGISTRY_TIMEOUT_MS_DEFAULT = 20000L;

  private static final long LOG_SUMMARY_INTERVAL = 15000L; // Log summary every ~15 seconds.
  private static final String LLAP_KEY = "llap";

  private final Configuration conf;
  private String appName = null;
  private String applicationId = null;
  private ServiceClient serviceClient = null;
  private Configuration llapRegistryConf = null;
  private LlapRegistryService llapRegistry = null;

  private AppStatusBuilder appStatusBuilder;

  private static LlapStatusServiceDriver createServiceDriver() {
    LlapStatusServiceDriver statusServiceDriver = null;
    try {
      statusServiceDriver = new LlapStatusServiceDriver();
    } catch (Throwable t) {
      logError(t);
      System.exit(ExitCode.INTERNAL_ERROR.getCode());
    }
    return statusServiceDriver;
  }

  public LlapStatusServiceDriver() {
    SessionState ss = SessionState.get();
    conf = (ss != null) ? ss.getConf() : new HiveConf(SessionState.class);
    setupConf();
  }

  private void setupConf() {
    for (String f : LlapDaemonConfiguration.DAEMON_CONFIGS) {
      conf.addResource(f);
    }
    conf.reloadConfiguration();

    // Setup timeouts for various services.

    // Once we move to a Hadoop-2.8 dependency, the following paramteer can be used.
    // conf.set(YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_RETRY_POLICY_SPEC);
    conf.set("yarn.timeline-service.entity-group-fs-store.retry-policy-spec",
        conf.get(CONFIG_TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_RETRY_POLICY_SPEC,
            CONFIG_TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_RETRY_POLICY_SPEC_DEFAULT));

    conf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS,
        conf.getLong(CONFIG_YARN_RM_TIMEOUT_MAX_WAIT_MS, CONFIG_YARN_RM_TIMEOUT_MAX_WAIT_MS_DEFAULT));
    conf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS,
        conf.getLong(CONFIG_YARN_RM_RETRY_INTERVAL_MS, CONFIG_YARN_RM_RETRY_INTERVAL_MS_DEFAULT));

    conf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        conf.getInt(CONFIG_IPC_CLIENT_CONNECT_MAX_RETRIES, CONFIG_IPC_CLIENT_CONNECT_MAX_RETRIES_DEFAULT));
    conf.setLong(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY,
        conf.getLong(CONFIG_IPC_CLIENT_CONNECT_RETRY_INTERVAL_MS, CONFIG_IPC_CLIENT_CONNECT_RETRY_INTERVAL_MS_DEFAULT));

    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT, (
        conf.getLong(CONFIG_LLAP_ZK_REGISTRY_TIMEOUT_MS, CONFIG_LLAP_ZK_REGISTRY_TIMEOUT_MS_DEFAULT) + "ms"));

    llapRegistryConf = new Configuration(conf);
  }

  public ExitCode run(LlapStatusServiceCommandLine cl, long watchTimeoutMs) {
    appStatusBuilder = new AppStatusBuilder();
    try {
      if (appName == null) {
        // user provided configs
        for (Map.Entry<Object, Object> props : cl.getHiveConf().entrySet()) {
          conf.set((String) props.getKey(), (String) props.getValue());
        }

        appName = cl.getName();
        if (StringUtils.isEmpty(appName)) {
          appName = HiveConf.getVar(conf, HiveConf.ConfVars.LLAP_DAEMON_SERVICE_HOSTS);
          if (appName.startsWith("@") && appName.length() > 1) {
            // This is a valid YARN Service name. Parse it out.
            appName = appName.substring(1);
          } else {
            // Invalid app name. Checked later.
            appName = null;
          }
        }
        if (StringUtils.isEmpty(appName)) {
          LOG.error("Invalid app name. This must be setup via config or passed in as a parameter." +
              " This tool works with clusters deployed by YARN Service");
          return ExitCode.INCORRECT_USAGE;
        }
        LOG.debug("Using appName: {}", appName);

        llapRegistryConf.set(HiveConf.ConfVars.LLAP_DAEMON_SERVICE_HOSTS.varname, "@" + appName);
      }

      try {
        if (serviceClient == null) {
          serviceClient = LlapSliderUtils.createServiceClient(conf);
        }
      } catch (Exception e) {
        LlapStatusCliException le = new LlapStatusCliException(
            ExitCode.SERVICE_CLIENT_ERROR_CREATE_FAILED, "Failed to create service client", e);
        logError(le);
        return le.getExitCode();
      }

      // Get the App report from YARN
      ApplicationReport appReport;
      try {
        appReport = getAppReport(appName, cl.getFindAppTimeoutMs());
      } catch (LlapStatusCliException e) {
        logError(e);
        return e.getExitCode();
      }

      // Process the report
      ExitCode ret;
      try {
        ret = processAppReport(appReport, appStatusBuilder);
      } catch (LlapStatusCliException e) {
        logError(e);
        return e.getExitCode();
      }

      if (ret != ExitCode.SUCCESS) {
        return ret;
      } else if (NO_YARN_SERVICE_INFO_STATES.contains(appStatusBuilder.getState())) {
        return ExitCode.SUCCESS;
      } else {
        // Get information from YARN Service
        try {
          ret = populateAppStatusFromServiceStatus(appName, serviceClient, appStatusBuilder);
        } catch (LlapStatusCliException e) {
          // In case of failure, send back whatever is constructed so far - which would be from the AppReport
          logError(e);
          return e.getExitCode();
        }
      }

      if (ret != ExitCode.SUCCESS) {
        return ret;
      } else {
        try {
          ret = populateAppStatusFromLlapRegistry(appStatusBuilder, watchTimeoutMs);
        } catch (LlapStatusCliException e) {
          logError(e);
          return e.getExitCode();
        }
      }

      return ret;
    } finally {
      LOG.debug("Final AppState: " + appStatusBuilder.toString());
    }
  }

  private ApplicationReport getAppReport(String appName, long timeoutMs)
      throws LlapStatusCliException {
    Clock clock = SystemClock.getInstance();
    long startTime = clock.getTime();
    long timeoutTime = timeoutMs < 0 ? Long.MAX_VALUE : (startTime + timeoutMs);
    ApplicationReport appReport = null;
    ApplicationId appId;
    try {
      appId = serviceClient.getAppId(appName);
    } catch (YarnException | IOException e) {
      return null;
    }

    while (appReport == null) {
      try {
        appReport = serviceClient.getYarnClient().getApplicationReport(appId);
        if (timeoutMs == 0) {
          // break immediately if timeout is 0
          break;
        }
        // Otherwise sleep, and try again.
        if (appReport == null) {
          long remainingTime = Math.min(timeoutTime - clock.getTime(), 500L);
          if (remainingTime > 0) {
            Thread.sleep(remainingTime);
          } else {
            break;
          }
        }
      } catch (Exception e) {
        if (e instanceof ApplicationNotFoundException) {
          //This might happen when serviceClient caches an appId from the past which is now not
          // valid (i.e. Yarn RM restart). This will force re-creation of service client in the
          // next check (if watch mode is on..) which effectively invalidates such cache.
          serviceClient = null;
        }
        throw new LlapStatusCliException(ExitCode.YARN_ERROR, "Failed to get Yarn AppReport", e);
      }
    }
    return appReport;
  }

  public void outputJson(PrintWriter writer) throws LlapStatusCliException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    mapper.setSerializationInclusion(Include.NON_NULL);
    mapper.setSerializationInclusion(Include.NON_EMPTY);
    mapper.setVisibility(PropertyAccessor.ALL, Visibility.NON_PRIVATE);
    try {
      writer.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(appStatusBuilder));
    } catch (IOException e) {
      LOG.warn("Failed to create JSON", e);
      throw new LlapStatusCliException(ExitCode.LLAP_JSON_GENERATION_ERROR, "Failed to create JSON", e);
    }
  }

  /**
   * Populates parts of the AppStatus.
   *
   * @return an ExitCode. An ExitCode other than ExitCode.SUCCESS implies future progress not possible
   * @throws LlapStatusCliException
   */
  private ExitCode processAppReport(ApplicationReport appReport, AppStatusBuilder appStatusBuilder)
      throws LlapStatusCliException {
    if (appReport == null) {
      appStatusBuilder.setState(State.APP_NOT_FOUND);
      LOG.info("No Application Found");
      return ExitCode.SUCCESS;
    }

    applicationId = appReport.getApplicationId().toString();

    // TODO Maybe add the YARN URL for the app.
    appStatusBuilder.setAmInfo(
        new AmInfo().setAppName(appReport.getName()).setAppType(appReport.getApplicationType()));
    appStatusBuilder.setAppStartTime(appReport.getStartTime());
    switch (appReport.getYarnApplicationState()) {
    case NEW:
    case NEW_SAVING:
    case SUBMITTED:
      appStatusBuilder.setState(State.LAUNCHING);
      return ExitCode.SUCCESS;
    case ACCEPTED:
      appStatusBuilder.maybeCreateAndGetAmInfo().setAppId(applicationId);
      appStatusBuilder.setState(State.LAUNCHING);
      return ExitCode.SUCCESS;
    case RUNNING:
      appStatusBuilder.maybeCreateAndGetAmInfo().setAppId(applicationId);
      // If the app state is running, get additional information from YARN Service
      return ExitCode.SUCCESS;
    case FINISHED:
    case FAILED:
    case KILLED:
      appStatusBuilder.maybeCreateAndGetAmInfo().setAppId(applicationId);
      appStatusBuilder.setAppFinishTime(appReport.getFinishTime());
      appStatusBuilder.setState(State.COMPLETE);
      // add log links and other diagnostics from YARN Service
      return ExitCode.SUCCESS;
    default:
      throw new LlapStatusCliException(ExitCode.INTERNAL_ERROR,
          "Unknown Yarn Application State: " + appReport.getYarnApplicationState());
    }
  }

  /**
   * Populates information from YARN Service Status.
   *
   * @return an ExitCode. An ExitCode other than ExitCode.SUCCESS implies future progress not possible
   * @throws LlapStatusCliException
   */
  private ExitCode populateAppStatusFromServiceStatus(String appName, ServiceClient serviceClient,
      AppStatusBuilder appStatusBuilder) throws LlapStatusCliException {
    ExitCode exitCode = ExitCode.YARN_ERROR;
    try {
      Service service = serviceClient.getStatus(appName);
      if (service != null) {
        // How to get config paths and AmInfo
        ServiceState state = service.getState();
        appStatusBuilder.setAppStartTime(service.getLaunchTime() == null ? 0 : service.getLaunchTime().getTime());
        appStatusBuilder.setDesiredInstances(service.getComponent(LLAP_KEY).getNumberOfContainers() == null ? 0
            : service.getComponent(LLAP_KEY).getNumberOfContainers().intValue());
        appStatusBuilder.setLiveInstances(service.getComponent(LLAP_KEY).getContainers().size());
        for (Container cont : service.getComponent(LLAP_KEY).getContainers()) {
          LlapInstance llapInstance = new LlapInstance(cont.getHostname(), cont.getId());
          appStatusBuilder.addNewRunningLlapInstance(llapInstance);
        }
        if (state == ServiceState.STARTED || state == ServiceState.STABLE || state == ServiceState.FLEX) {
          exitCode = ExitCode.SUCCESS;
        }
      } else {
        exitCode = ExitCode.SERVICE_CLIENT_ERROR_OTHER;
      }
    } catch (IOException | YarnException e) {
      LlapStatusCliException le = new LlapStatusCliException(
          ExitCode.SERVICE_CLIENT_ERROR_OTHER, "Failed to get service status", e);
      logError(le);
      exitCode = le.getExitCode();
    }
    return exitCode;
  }

  /**
   * Populate additional information for containers from the LLAP registry. Must be invoked
   * after YARN Service status and diagnostics.
   * @return an ExitCode. An ExitCode other than ExitCode.SUCCESS implies future progress not possible
   * @throws LlapStatusCliException
   */
  private ExitCode populateAppStatusFromLlapRegistry(AppStatusBuilder appStatusBuilder, long watchTimeoutMs)
      throws LlapStatusCliException {

    if (llapRegistry == null) {
      try {
        llapRegistry = LlapRegistryService.getClient(llapRegistryConf);
      } catch (Exception e) {
        throw new LlapStatusCliException(ExitCode.LLAP_REGISTRY_ERROR,
          "Failed to create llap registry client", e);
      }
    }

    Collection<LlapServiceInstance> serviceInstances;
    try {
      serviceInstances = llapRegistry.getInstances(watchTimeoutMs).getAll();
    } catch (Exception e) {
      throw new LlapStatusCliException(ExitCode.LLAP_REGISTRY_ERROR, "Failed to get instances from llap registry", e);
    }

    if (serviceInstances == null || serviceInstances.isEmpty()) {
      LOG.debug("No information found in the LLAP registry");
      appStatusBuilder.setLiveInstances(0);
      appStatusBuilder.setState(State.LAUNCHING);
      appStatusBuilder.clearRunningLlapInstances();
      return ExitCode.SUCCESS;
    } else {
      // Tracks instances known by both YARN Service and llap.
      List<LlapInstance> validatedInstances = new LinkedList<>();
      List<String> llapExtraInstances = new LinkedList<>();

      for (LlapServiceInstance serviceInstance : serviceInstances) {
        String containerIdString = serviceInstance.getProperties().get(
            HiveConf.ConfVars.LLAP_DAEMON_CONTAINER_ID.varname);

        LlapInstance llapInstance = appStatusBuilder.removeAndGetRunningLlapInstanceForContainer(containerIdString);
        if (llapInstance != null) {
          llapInstance.setMgmtPort(serviceInstance.getManagementPort());
          llapInstance.setRpcPort(serviceInstance.getRpcPort());
          llapInstance.setShufflePort(serviceInstance.getShufflePort());
          llapInstance.setWebUrl(serviceInstance.getServicesAddress());
          llapInstance.setStatusUrl(serviceInstance.getServicesAddress() + "/status");
          validatedInstances.add(llapInstance);
        } else {
          // This likely indicates that an instance has recently restarted
          // (the old instance has not been unregistered), and the new instances has not registered yet.
          llapExtraInstances.add(containerIdString);
          // This instance will not be added back, since it's services are not up yet.
        }

      }

      appStatusBuilder.setLiveInstances(validatedInstances.size());
      appStatusBuilder.setLaunchingInstances(llapExtraInstances.size());
      if (appStatusBuilder.getDesiredInstances() != null &&
          validatedInstances.size() >= appStatusBuilder.getDesiredInstances()) {
        appStatusBuilder.setState(State.RUNNING_ALL);
        if (validatedInstances.size() > appStatusBuilder.getDesiredInstances()) {
          LOG.warn("Found more entries in LLAP registry, as compared to desired entries");
        }
      } else {
        if (validatedInstances.size() > 0) {
          appStatusBuilder.setState(State.RUNNING_PARTIAL);
        } else {
          appStatusBuilder.setState(State.LAUNCHING);
        }
      }

      // At this point, everything that can be consumed from AppStatusBuilder has been consumed.
      // Debug only
      if (appStatusBuilder.allRunningInstances().size() > 0) {
        // Containers likely to come up soon.
        LOG.debug("Potential instances starting up: {}", appStatusBuilder.allRunningInstances());
      }
      if (llapExtraInstances.size() > 0) {
        // Old containers which are likely shutting down, or new containers which
        // launched between YARN Service status/diagnostics. Skip for this iteration.
        LOG.debug("Instances likely to shutdown soon: {}", llapExtraInstances);
      }

      appStatusBuilder.clearAndAddPreviouslyKnownRunningInstances(validatedInstances);

    }
    return ExitCode.SUCCESS;
  }

  private void close() {
    if (serviceClient != null) {
      serviceClient.stop();
    }
    if (llapRegistry != null) {
      llapRegistry.stop();
    }
  }

  public static void main(String[] args) {
    LlapStatusServiceCommandLine cl = LlapStatusServiceCommandLine.parseArguments(args);
    LlapStatusServiceDriver statusServiceDriver = createServiceDriver();

    ExitCode ret = ExitCode.SUCCESS;
    Clock clock = SystemClock.getInstance();
    long lastSummaryLogTime = -1;

    boolean firstAttempt = true;
    final long refreshInterval = cl.getRefreshIntervalMs();
    final boolean watchMode = cl.isWatchMode();
    final long watchTimeout = cl.getWatchTimeoutMs();
    long numAttempts = watchTimeout / refreshInterval;
    numAttempts = watchMode ? numAttempts : 1; // Break out of the loop fast if watchMode is disabled.
    State launchingState = null;
    State currentState = null;
    boolean desiredStateAttained = false;
    final float runningNodesThreshold = cl.getRunningNodesThreshold();
    try (OutputStream os = cl.getOutputFile() == null ? System.out : new FileOutputStream(cl.getOutputFile());
         Writer w = new OutputStreamWriter(os, Charset.defaultCharset());
         PrintWriter pw = new PrintWriter(w)) {

      LOG.info("Configured refresh interval: {}s. Watch timeout: {}s. Attempts remaining: {}." +
          " Watch mode: {}. Running nodes threshold: {}.", refreshInterval/1000, watchTimeout/1000,
          numAttempts, watchMode, new DecimalFormat("#.###").format(runningNodesThreshold));
      while (numAttempts > 0) {
        if (!firstAttempt) {
          if (watchMode) {
            try {
              Thread.sleep(refreshInterval);
            } catch (InterruptedException e) {
              // ignore
            }
          } else {
            // reported once, so break
            break;
          }
        } else {
          firstAttempt = false;
        }
        ret = statusServiceDriver.run(cl, watchMode ? watchTimeout : 0);
        currentState = statusServiceDriver.appStatusBuilder.getState();
        try {
          lastSummaryLogTime = LlapStatusServiceDriver.maybeLogSummary(clock, lastSummaryLogTime,
              statusServiceDriver, watchMode, watchTimeout, launchingState);
        } catch (Exception e) {
          LOG.warn("Failed to log summary", e);
        }

        if (ret == ExitCode.SUCCESS) {
          if (watchMode) {

            // YARN Service has started llap application, now if for some reason
            // state changes to COMPLETE then fail fast
            if (launchingState == null && LAUNCHING_STATES.contains(currentState)) {
              launchingState = currentState;
            }

            if (currentState.equals(State.COMPLETE)) {
              if (launchingState != null || cl.isLaunched()) {
                LOG.warn("COMPLETE state reached while waiting for RUNNING state. Failing.");
                System.err.println("Final diagnostics: " + statusServiceDriver.appStatusBuilder.getDiagnostics());
                break;
              } else {
                LOG.info("Found a stopped application; assuming it was a previous attempt "
                    + "and waiting for the next one. Omit the -l flag to avoid this.");
              }
            }

            if (!(currentState.equals(State.RUNNING_PARTIAL) || currentState.equals(State.RUNNING_ALL))) {
              LOG.debug(
                  "Current state: {}. Desired state: {}. {}/{} instances.",
                  currentState,
                  runningNodesThreshold == 1.0f ?
                      State.RUNNING_ALL :
                      State.RUNNING_PARTIAL,
                  statusServiceDriver.appStatusBuilder.getLiveInstances(),
                  statusServiceDriver.appStatusBuilder.getDesiredInstances());
              numAttempts--;
              continue;
            }

            // we have reached RUNNING state, now check if running nodes threshold is met
            final int liveInstances = statusServiceDriver.appStatusBuilder.getLiveInstances();
            final int desiredInstances = statusServiceDriver.appStatusBuilder.getDesiredInstances();
            if (desiredInstances > 0) {
              final float ratio = (float) liveInstances / (float) desiredInstances;
              if (ratio < runningNodesThreshold) {
                LOG.debug(
                    "Waiting until running nodes threshold is reached. Current: {} Desired: {}." +
                        " {}/{} instances.",
                    new DecimalFormat("#.###").format(ratio),
                    new DecimalFormat("#.###").format(runningNodesThreshold),
                    statusServiceDriver.appStatusBuilder.getLiveInstances(),
                    statusServiceDriver.appStatusBuilder.getDesiredInstances());
                numAttempts--;
                continue;
              } else {
                desiredStateAttained = true;
                statusServiceDriver.appStatusBuilder.setRunningThresholdAchieved(true);
              }
            } else {
              numAttempts--;
              continue;
            }
          }
        } else if (ret == ExitCode.YARN_ERROR && watchMode) {
          LOG.warn("Watch mode enabled and got YARN error. Retrying..");
          numAttempts--;
          continue;
        } else if (ret == ExitCode.SERVICE_CLIENT_ERROR_CREATE_FAILED && watchMode) {
          LOG.warn("Watch mode enabled and YARN Service client creation failed. Retrying..");
          numAttempts--;
          continue;
        } else if (ret == ExitCode.SERVICE_CLIENT_ERROR_OTHER && watchMode) {
          LOG.warn("Watch mode enabled and got YARN Service client error. Retrying..");
          numAttempts--;
          continue;
        } else if (ret == ExitCode.LLAP_REGISTRY_ERROR && watchMode) {
          LOG.warn("Watch mode enabled and got LLAP registry error. Retrying..");
          numAttempts--;
          continue;
        }
        break;
      }
      // Log final state to CONSOLE_LOGGER
      maybeLogSummary(clock, 0L, statusServiceDriver, watchMode, watchTimeout, launchingState);
      CONSOLE_LOGGER.info("\n\n\n");

      statusServiceDriver.outputJson(pw); // print current state before exiting
      pw.flush();
      if (numAttempts == 0 && watchMode && !desiredStateAttained) {
        LOG.warn("Watch timeout {}s exhausted before desired state RUNNING is attained.", watchTimeout/1000);
      }
    } catch (Throwable t) {
      logError(t);
      if (t instanceof LlapStatusCliException) {
        LlapStatusCliException ce = (LlapStatusCliException) t;
        ret = ce.getExitCode();
      } else {
        ret = ExitCode.INTERNAL_ERROR;
      }
    } finally {
      LOG.info("LLAP status finished");
      if (ret != ExitCode.SUCCESS) {
        LOG.error("LLAP did not start. Check the application log for more info:\n" +
            "\tyarn logs --applicationId {} -out <path>", statusServiceDriver.applicationId);
      }
      statusServiceDriver.close();
    }
    LOG.debug("Completed processing - exiting with " + ret);

    // HACK: due to the System.exit some log messages may not be present.
    try {
      Thread.sleep(1000);
    } catch (Exception e) {
      // ignore
    }
    System.exit(ret.getCode());
  }

  private static long maybeLogSummary(Clock clock, long lastSummaryLogTime, LlapStatusServiceDriver statusServiceDriver,
                                      boolean watchMode, long watchTimeout, State launchingState) {
    long currentTime = clock.getTime();
    if (lastSummaryLogTime < currentTime - LOG_SUMMARY_INTERVAL) {
      String diagString = null;
      if (launchingState == null && statusServiceDriver.appStatusBuilder.getState() == State.COMPLETE && watchMode) {
        // First known state was COMPLETED. Wait for the app launch to start.
        diagString = "Awaiting LLAP launch";
        // Clear completed instances in this case. Don't want to provide information from the previous run.
        statusServiceDriver.appStatusBuilder.clearCompletedLlapInstances();
      } else {
        diagString = constructDiagnostics(statusServiceDriver.appStatusBuilder);
      }

      if (lastSummaryLogTime == -1) {
        if (watchMode) {
          CONSOLE_LOGGER.info("\nLLAPSTATUS WatchMode with timeout={} s", watchTimeout/1000);
        } else {
          CONSOLE_LOGGER.info("\nLLAPSTATUS");
        }
        CONSOLE_LOGGER.info("--------------------------------------------------------------------------------");
      }
      CONSOLE_LOGGER.info(diagString);
      CONSOLE_LOGGER.info("--------------------------------------------------------------------------------");
      lastSummaryLogTime = currentTime;
    }
    return lastSummaryLogTime;
  }

  /**
   * Helper method to construct a diagnostic message from a complete AppStatusBuilder.
   */
  private static String constructDiagnostics(AppStatusBuilder appStatusBuilder) {
    StringBuilder sb = new StringBuilder();

    switch (appStatusBuilder.getState()) {
    case APP_NOT_FOUND:
      sb.append("LLAP status unknown. Awaiting app launch");
      break;
    case LAUNCHING:
      // This is a catch all state - when containers have not started yet, or LLAP has not started yet.
      if (StringUtils.isNotBlank(appStatusBuilder.getAmInfo().getAppId())) {
        sb.append("LLAP Starting up with AppId=").append(appStatusBuilder.getAmInfo().getAppId()).append(".");
        if (appStatusBuilder.getDesiredInstances() != null) {
          sb.append(" Started 0/").append(appStatusBuilder.getDesiredInstances()).append(" instances");
        }

        String containerDiagnostics = constructCompletedContainerDiagnostics(
            appStatusBuilder.getCompletedInstances());
        if (StringUtils.isNotEmpty(containerDiagnostics)) {
          sb.append("\n").append(containerDiagnostics);
        }
      } else {
        sb.append("Awaiting LLAP startup");
      }
      break;
    case RUNNING_PARTIAL:
      sb.append("LLAP Starting up with ApplicationId=").append(appStatusBuilder.getAmInfo().getAppId());
      sb.append(" Started").append(appStatusBuilder.getLiveInstances()).append("/")
        .append(appStatusBuilder.getDesiredInstances()).append(" instances");
      String containerDiagnostics = constructCompletedContainerDiagnostics(appStatusBuilder.getCompletedInstances());
      if (StringUtils.isNotEmpty(containerDiagnostics)) {
        sb.append("\n").append(containerDiagnostics);
      }

      // TODO HIVE-15865: Include information about pending requests, and last
      // allocation time once YARN Service provides this information.
      break;
    case RUNNING_ALL:
      sb.append("LLAP Application running with ApplicationId=").append(appStatusBuilder.getAmInfo().getAppId());
      break;
    case COMPLETE:
      sb.append("LLAP Application already complete. ApplicationId=").append(appStatusBuilder.getAmInfo().getAppId());
      containerDiagnostics = constructCompletedContainerDiagnostics(appStatusBuilder.getCompletedInstances());
      if (StringUtils.isNotEmpty(containerDiagnostics)) {
        sb.append("\n").append(containerDiagnostics);
      }

      break;
    case UNKNOWN:
      sb.append("LLAP status unknown");
      break;
    default:
      throw new IllegalStateException("Unknown State: " + appStatusBuilder.getState());
    }
    if (StringUtils.isNotBlank(appStatusBuilder.getDiagnostics())) {
      sb.append("\n").append(appStatusBuilder.getDiagnostics());
    }

    return sb.toString();
  }

  private static String constructCompletedContainerDiagnostics(List<LlapInstance> completedInstances) {
    StringBuilder sb = new StringBuilder();
    if (completedInstances == null || completedInstances.size() == 0) {
      return "";
    } else {
      // TODO HIVE-15865 Ideally sort these by completion time, once that is available.
      boolean isFirst = true;
      for (LlapInstance instance : completedInstances) {
        if (!isFirst) {
          sb.append("\n");
        } else {
          isFirst = false;
        }

        if (instance.getYarnContainerExitStatus() == ContainerExitStatus.KILLED_EXCEEDED_PMEM ||
            instance.getYarnContainerExitStatus() == ContainerExitStatus.KILLED_EXCEEDED_VMEM) {
          sb.append("\tKILLED container (by YARN for exceeding memory limits): ");
        } else {
          // TODO HIVE-15865 Handle additional reasons like OS launch failed
          sb.append("\tFAILED container: ");
        }
        sb.append(" ").append(instance.getContainerId());
        sb.append(", Logs at: ").append(instance.getLogUrl());
      }
    }
    return sb.toString();
  }

  private static void logError(Throwable t) {
    LOG.error("FAILED: " + t.getMessage(), t);
    System.err.println("FAILED: " + t.getMessage());
  }
}
