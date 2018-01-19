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

package org.apache.hadoop.hive.llap.cli;


import com.google.common.annotations.VisibleForTesting;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.cli.LlapStatusOptionsProcessor.LlapStatusOptions;
import org.apache.hadoop.hive.llap.cli.status.LlapStatusHelpers;
import org.apache.hadoop.hive.llap.cli.status.LlapStatusHelpers.AppStatusBuilder;
import org.apache.hadoop.hive.llap.cli.status.LlapStatusHelpers.LlapInstance;
import org.apache.hadoop.hive.llap.cli.status.LlapStatusHelpers.State;
import org.apache.hadoop.hive.llap.configuration.LlapDaemonConfiguration;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstance;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.ClusterDescriptionKeys;
import org.apache.slider.api.StateValues;
import org.apache.slider.api.StatusKeys;
import org.apache.slider.api.types.ApplicationDiagnostics;
import org.apache.slider.api.types.ContainerInformation;
import org.apache.slider.client.SliderClient;
import org.apache.slider.common.params.ActionDiagnosticArgs;
import org.apache.slider.core.exceptions.SliderException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LlapStatusServiceDriver {

  private static final EnumSet<State> NO_SLIDER_INFO_STATES = EnumSet.of(
      State.APP_NOT_FOUND, State.COMPLETE, State.LAUNCHING);
  private static final EnumSet<State> LAUNCHING_STATES = EnumSet.of(
      State.LAUNCHING, State.RUNNING_PARTIAL, State.RUNNING_ALL);
  private static final Logger LOG = LoggerFactory.getLogger(LlapStatusServiceDriver.class);
  private static final Logger CONSOLE_LOGGER = LoggerFactory.getLogger("LlapStatusServiceDriverConsole");

  // Defining a bunch of configs here instead of in HiveConf. These are experimental, and mainly
  // for use when retry handling is fixed in Yarn/Hadoop

  private static final String CONF_PREFIX = "hive.llapcli.";

  // The following two keys should ideally be used to control RM connect timeouts. However,
  // they don't seem to work. The IPC timeout needs to be set instead.
  @InterfaceAudience.Private
  private static final String CONFIG_YARN_RM_TIMEOUT_MAX_WAIT_MS =
      CONF_PREFIX + "yarn.rm.connect.max-wait-ms";
  private static final long CONFIG_YARN_RM_TIMEOUT_MAX_WAIT_MS_DEFAULT = 10000l;
  @InterfaceAudience.Private
  private static final String CONFIG_YARN_RM_RETRY_INTERVAL_MS =
      CONF_PREFIX + "yarn.rm.connect.retry-interval.ms";
  private static final long CONFIG_YARN_RM_RETRY_INTERVAL_MS_DEFAULT = 5000l;

  // As of Hadoop 2.7 - this is what controls the RM timeout.
  @InterfaceAudience.Private
  private static final String CONFIG_IPC_CLIENT_CONNECT_MAX_RETRIES =
      CONF_PREFIX + "ipc.client.max-retries";
  private static final int CONFIG_IPC_CLIENT_CONNECT_MAX_RETRIES_DEFAULT = 2;
  @InterfaceAudience.Private
  private static final String CONFIG_IPC_CLIENT_CONNECT_RETRY_INTERVAL_MS =
      CONF_PREFIX + "ipc.client.connect.retry-interval-ms";
  private static final long CONFIG_IPC_CLIENT_CONNECT_RETRY_INTERVAL_MS_DEFAULT = 1500l;

  // As of Hadoop 2.8 - this timeout spec behaves in a strnage manner. "2000,1" means 2000s with 1 retry.
  // However it does this - but does it thrice. Essentially - #retries+2 is the number of times the entire config
  // is retried. "2000,1" means 3 retries - each with 1 retry with a random 2000ms sleep.
  @InterfaceAudience.Private
  private static final String CONFIG_TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_RETRY_POLICY_SPEC =
      CONF_PREFIX + "timeline.service.fs-store.retry.policy.spec";
  private static final String
      CONFIG_TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_RETRY_POLICY_SPEC_DEFAULT = "2000, 1";

  private static final String CONFIG_LLAP_ZK_REGISTRY_TIMEOUT_MS =
      CONF_PREFIX + "zk-registry.timeout-ms";
  private static final long CONFIG_LLAP_ZK_REGISTRY_TIMEOUT_MS_DEFAULT = 20000l;

  private static final long LOG_SUMMARY_INTERVAL = 15000L; // Log summary every ~15 seconds.

  private static final String LLAP_KEY = "LLAP";
  private final Configuration conf;
  private final Clock clock = new SystemClock();
  private String appName = null;
  private SliderClient sliderClient = null;
  private Configuration llapRegistryConf = null;
  private LlapRegistryService llapRegistry = null;

  @VisibleForTesting
  AppStatusBuilder appStatusBuilder;

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
      conf.getLong(CONFIG_YARN_RM_TIMEOUT_MAX_WAIT_MS,
        CONFIG_YARN_RM_TIMEOUT_MAX_WAIT_MS_DEFAULT));
    conf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS,
      conf.getLong(CONFIG_YARN_RM_RETRY_INTERVAL_MS, CONFIG_YARN_RM_RETRY_INTERVAL_MS_DEFAULT));

    conf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
      conf.getInt(CONFIG_IPC_CLIENT_CONNECT_MAX_RETRIES,
        CONFIG_IPC_CLIENT_CONNECT_MAX_RETRIES_DEFAULT));
    conf.setLong(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY,
      conf.getLong(CONFIG_IPC_CLIENT_CONNECT_RETRY_INTERVAL_MS,
        CONFIG_IPC_CLIENT_CONNECT_RETRY_INTERVAL_MS_DEFAULT));

    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT, (conf
      .getLong(CONFIG_LLAP_ZK_REGISTRY_TIMEOUT_MS, CONFIG_LLAP_ZK_REGISTRY_TIMEOUT_MS_DEFAULT) +
      "ms"));

    llapRegistryConf = new Configuration(conf);
  }

  /**
   * Parse command line options.
   *
   * @param args
   * @return command line options.
   */
  public LlapStatusOptions parseOptions(String[] args) throws
      LlapStatusCliException {

    LlapStatusOptionsProcessor optionsProcessor = new LlapStatusOptionsProcessor();
    LlapStatusOptions options;
    try {
      options = optionsProcessor.processOptions(args);
      return options;
    } catch (Exception e) {
      LOG.info("Failed to parse arguments", e);
      throw new LlapStatusCliException(ExitCode.INCORRECT_USAGE, "Incorrect usage");
    }
  }

  public int run(LlapStatusOptions options, long watchTimeoutMs) {
    appStatusBuilder = new AppStatusBuilder();
    try {
      if (appName == null) {
        // user provided configs
        for (Map.Entry<Object, Object> props : options.getConf().entrySet()) {
          conf.set((String) props.getKey(), (String) props.getValue());
        }

        appName = options.getName();
        if (StringUtils.isEmpty(appName)) {
          appName = HiveConf.getVar(conf, HiveConf.ConfVars.LLAP_DAEMON_SERVICE_HOSTS);
          if (appName.startsWith("@") && appName.length() > 1) {
            // This is a valid slider app name. Parse it out.
            appName = appName.substring(1);
          } else {
            // Invalid app name. Checked later.
            appName = null;
          }
        }
        if (StringUtils.isEmpty(appName)) {
          String message =
            "Invalid app name. This must be setup via config or passed in as a parameter." +
              " This tool works with clusters deployed by Slider/YARN";
          LOG.info(message);
          return ExitCode.INCORRECT_USAGE.getInt();
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Using appName: {}", appName);
        }

        llapRegistryConf.set(HiveConf.ConfVars.LLAP_DAEMON_SERVICE_HOSTS.varname, "@" + appName);
      }

      try {
        if (sliderClient == null) {
          sliderClient = LlapSliderUtils.createSliderClient(conf);
        }
      } catch (Exception e) {
        LlapStatusCliException le = new LlapStatusCliException(
            LlapStatusServiceDriver.ExitCode.SLIDER_CLIENT_ERROR_CREATE_FAILED,
            "Failed to create slider client", e);
        logError(le);
        return le.getExitCode().getInt();
      }

      // Get the App report from YARN
      ApplicationReport appReport;
      try {
        appReport = LlapSliderUtils.getAppReport(appName, sliderClient, options.getFindAppTimeoutMs());
      } catch (LlapStatusCliException e) {
        logError(e);
        return e.getExitCode().getInt();
      }

      // Process the report to decide whether to go to slider.
      ExitCode ret;
      try {
        ret = processAppReport(appReport, appStatusBuilder);
      } catch (LlapStatusCliException e) {
        logError(e);
        return e.getExitCode().getInt();
      }

      if (ret != ExitCode.SUCCESS) {
        return ret.getInt();
      } else if (NO_SLIDER_INFO_STATES.contains(appStatusBuilder.getState())) {
        return ExitCode.SUCCESS.getInt();
      } else {
        // Get information from slider.
        try {
          ret = populateAppStatusFromSliderStatus(appName, sliderClient, appStatusBuilder);
        } catch (LlapStatusCliException e) {
          // In case of failure, send back whatever is constructed sop far - which wouldbe from the AppReport
          logError(e);
          return e.getExitCode().getInt();
        }
      }


      if (ret != ExitCode.SUCCESS) {
        return ret.getInt();
      } else {
        try {
          ret = populateAppStatusFromSliderDiagnostics(appName, sliderClient, appStatusBuilder);
        } catch (LlapStatusCliException e) {
          logError(e);
          return e.getExitCode().getInt();
        }
      }

      if (ret != ExitCode.SUCCESS) {
        return ret.getInt();
      } else {
        try {
          ret = populateAppStatusFromLlapRegistry(appStatusBuilder, watchTimeoutMs);
        } catch (LlapStatusCliException e) {
          logError(e);
          return e.getExitCode().getInt();
        }
      }

      return ret.getInt();
    } finally {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Final AppState: " + appStatusBuilder.toString());
      }
    }
  }

  public void outputJson(PrintWriter writer) throws
      LlapStatusCliException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
    mapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
    mapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_EMPTY);
    try {
      writer.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(appStatusBuilder));
    } catch (IOException e) {
      LOG.warn("Failed to create JSON", e);
      throw new LlapStatusCliException(ExitCode.LLAP_JSON_GENERATION_ERROR, "Failed to create JSON",
          e);
    }
  }

  private SliderClient createSliderClient() throws LlapStatusCliException {
    if (sliderClient != null) {
      return sliderClient;
    }

    try {
      sliderClient = LlapSliderUtils.createSliderClient(conf);
    } catch (Exception e) {
      throw new LlapStatusCliException(ExitCode.SLIDER_CLIENT_ERROR_CREATE_FAILED,
        "Failed to create slider client", e);
    }
    return sliderClient;
  }

  private ApplicationReport getAppReport(String appName, SliderClient sliderClient,
                                         long timeoutMs) throws LlapStatusCliException {

    long startTime = clock.getTime();
    long timeoutTime = timeoutMs < 0 ? Long.MAX_VALUE : (startTime + timeoutMs);
    ApplicationReport appReport = null;

    // TODO HIVE-13454 Maybe add an option to wait for a certain amount of time for the app to
    // move to running state. Potentially even wait for the containers to be launched.

//    while (clock.getTime() < timeoutTime && appReport == null) {

    while (appReport == null) {
      try {
        appReport = sliderClient.getYarnAppListClient().findInstance(appName);
        if (timeoutMs == 0) {
          // break immediately if timeout is 0
          break;
        }
        // Otherwise sleep, and try again.
        if (appReport == null) {
          long remainingTime = Math.min(timeoutTime - clock.getTime(), 500l);
          if (remainingTime > 0) {
            Thread.sleep(remainingTime);
          } else {
            break;
          }
        }
      } catch (Exception e) { // No point separating IOException vs YarnException vs others
        throw new LlapStatusCliException(ExitCode.YARN_ERROR,
            "Failed to get Yarn AppReport", e);
      }
    }
    return appReport;
  }


  /**
   * Populates parts of the AppStatus
   *
   * @param appReport
   * @param appStatusBuilder
   * @return an ExitCode. An ExitCode other than ExitCode.SUCCESS implies future progress not possible
   * @throws LlapStatusCliException
   */
  private ExitCode processAppReport(ApplicationReport appReport,
                               AppStatusBuilder appStatusBuilder) throws
      LlapStatusCliException {
    if (appReport == null) {
      appStatusBuilder.setState(State.APP_NOT_FOUND);
      LOG.info("No Application Found");
      return ExitCode.SUCCESS;
    }

    // TODO Maybe add the YARN URL for the app.
    appStatusBuilder.setAmInfo(
        new LlapStatusHelpers.AmInfo().setAppName(appReport.getName()).setAppType(appReport.getApplicationType()));
    appStatusBuilder.setAppStartTime(appReport.getStartTime());
    switch (appReport.getYarnApplicationState()) {
      case NEW:
      case NEW_SAVING:
      case SUBMITTED:
        appStatusBuilder.setState(State.LAUNCHING);
        return ExitCode.SUCCESS;
      case ACCEPTED:
        appStatusBuilder.maybeCreateAndGetAmInfo().setAppId(appReport.getApplicationId().toString());
        appStatusBuilder.setState(State.LAUNCHING);
        return ExitCode.SUCCESS;
      case RUNNING:
        appStatusBuilder.maybeCreateAndGetAmInfo().setAppId(appReport.getApplicationId().toString());
        // If the app state is running, get additional information from Slider itself.
        return ExitCode.SUCCESS;
      case FINISHED:
      case FAILED:
      case KILLED:
        appStatusBuilder.maybeCreateAndGetAmInfo().setAppId(appReport.getApplicationId().toString());
        appStatusBuilder.setAppFinishTime(appReport.getFinishTime());
        appStatusBuilder.setState(State.COMPLETE);
        ApplicationDiagnostics appDiagnostics =
            LlapSliderUtils.getApplicationDiagnosticsFromYarnDiagnostics(
                appReport, appStatusBuilder, LOG);
        if (appDiagnostics == null) {
          LOG.warn("AppDiagnostics not available for YARN application report");
        } else {
          processAppDiagnostics(appStatusBuilder, appDiagnostics, true);
        }
        return ExitCode.SUCCESS;
      default:
        throw new LlapStatusCliException(ExitCode.INTERNAL_ERROR,
            "Unknown Yarn Application State: " + appReport.getYarnApplicationState());
    }
  }





  /**
   * Populates information from SliderStatus.
   *
   * @param appName
   * @param sliderClient
   * @param appStatusBuilder
   * @return an ExitCode. An ExitCode other than ExitCode.SUCCESS implies future progress not possible
   * @throws LlapStatusCliException
   */
  private ExitCode populateAppStatusFromSliderStatus(String appName, SliderClient sliderClient, AppStatusBuilder appStatusBuilder) throws
      LlapStatusCliException {

    ClusterDescription clusterDescription;
    try {
      clusterDescription = sliderClient.getClusterDescription(appName);
    } catch (SliderException e) {
      throw new LlapStatusCliException(ExitCode.SLIDER_CLIENT_ERROR_OTHER,
          "Failed to get cluster description from slider. SliderErrorCode=" + (e).getExitCode(), e);
    } catch (Exception e) {
      throw new LlapStatusCliException(ExitCode.SLIDER_CLIENT_ERROR_OTHER,
          "Failed to get cluster description from slider", e);
    }

    if (clusterDescription == null) {
      LOG.info("Slider ClusterDescription not available");
      return ExitCode.SLIDER_CLIENT_ERROR_OTHER; // ClusterDescription should always be present.
    } else {
      // Process the Cluster Status returned by slider.
      appStatusBuilder.setOriginalConfigurationPath(clusterDescription.originConfigurationPath);
      appStatusBuilder.setGeneratedConfigurationPath(clusterDescription.generatedConfigurationPath);
      appStatusBuilder.setAppStartTime(clusterDescription.createTime);

      // Finish populating AMInfo
      appStatusBuilder.maybeCreateAndGetAmInfo().setAmWebUrl(clusterDescription.getInfo(StatusKeys.INFO_AM_WEB_URL));
      appStatusBuilder.maybeCreateAndGetAmInfo().setHostname(clusterDescription.getInfo(StatusKeys.INFO_AM_HOSTNAME));
      appStatusBuilder.maybeCreateAndGetAmInfo().setContainerId(clusterDescription.getInfo(StatusKeys.INFO_AM_CONTAINER_ID));


      if (clusterDescription.statistics != null) {
        Map<String, Integer> llapStats = clusterDescription.statistics.get(LLAP_KEY);
        if (llapStats != null) {
          int desiredContainers = llapStats.get(StatusKeys.STATISTICS_CONTAINERS_DESIRED);
          int liveContainers = llapStats.get(StatusKeys.STATISTICS_CONTAINERS_LIVE);
          appStatusBuilder.setDesiredInstances(desiredContainers);
          appStatusBuilder.setLiveInstances(liveContainers);
        } else {
          throw new LlapStatusCliException(ExitCode.SLIDER_CLIENT_ERROR_OTHER,
              "Failed to get statistics for LLAP"); // Error since LLAP should always exist.
        }
        // TODO HIVE-13454 Use some information from here such as containers.start.failed
        // and containers.failed.recently to provide an estimate of whether this app is healthy or not.
      } else {
        throw new LlapStatusCliException(ExitCode.SLIDER_CLIENT_ERROR_OTHER,
            "Failed to get statistics"); // Error since statistics should always exist.
      }

      // Code to locate container status via slider. Not using this at the moment.
      if (clusterDescription.status != null) {
        Object liveObject = clusterDescription.status.get(ClusterDescriptionKeys.KEY_CLUSTER_LIVE);
        if (liveObject != null) {
          Map<String, Map<String, Map<String, Object>>> liveEntity =
              (Map<String, Map<String, Map<String, Object>>>) liveObject;
          Map<String, Map<String, Object>> llapEntity = liveEntity.get(LLAP_KEY);

          if (llapEntity != null) { // Not a problem. Nothing has come up yet.
            for (Map.Entry<String, Map<String, Object>> containerEntry : llapEntity.entrySet()) {
              String containerIdString = containerEntry.getKey();
              Map<String, Object> containerParams = containerEntry.getValue();

              String host = (String) containerParams.get("host");

              LlapInstance
                  llapInstance = new LlapInstance(host, containerIdString);

              appStatusBuilder.addNewRunningLlapInstance(llapInstance);
            }
          }

        }
      }

      return ExitCode.SUCCESS;

    }
  }

  /**
   * Populates information based on the slider diagnostics call. Must be invoked
   * after populating status from slider status.
   * @param appName
   * @param sliderClient
   * @param appStatusBuilder
   * @return
   * @throws LlapStatusCliException
   */
  private ExitCode populateAppStatusFromSliderDiagnostics(String appName,
                                                          SliderClient sliderClient,
                                                          AppStatusBuilder appStatusBuilder) throws
      LlapStatusCliException {

    ApplicationDiagnostics appDiagnostics;
    try {
      ActionDiagnosticArgs args = new ActionDiagnosticArgs();
      args.containers = true;
      args.name = appName;
      appDiagnostics =
          sliderClient.actionDiagnosticContainers(args);
    } catch (YarnException | IOException | URISyntaxException e) {
      throw new LlapStatusCliException(
          ExitCode.SLIDER_CLIENT_ERROR_OTHER,
          "Failed to get container diagnostics from slider", e);
    }
    if (appDiagnostics == null) {
      LOG.info("Slider container diagnostics not available");
      return ExitCode.SLIDER_CLIENT_ERROR_OTHER;
    }

    processAppDiagnostics(appStatusBuilder, appDiagnostics, false);

    return ExitCode.SUCCESS;
  }

  /**
   * Populate additional information for containers from the LLAP registry. Must be invoked
   * after Slider status. Also after slider-diagnostics.
   * @param appStatusBuilder
   * @return an ExitCode. An ExitCode other than ExitCode.SUCCESS implies future progress not possible
   * @throws LlapStatusCliException
   */
  private ExitCode populateAppStatusFromLlapRegistry(
      AppStatusBuilder appStatusBuilder, long watchTimeoutMs) throws
    LlapStatusCliException {

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
      if (LOG.isDebugEnabled()) {
        LOG.debug("No information found in the LLAP registry");
      }
      appStatusBuilder.setLiveInstances(0);
      appStatusBuilder.setState(State.LAUNCHING);
      appStatusBuilder.clearRunningLlapInstances();
      return ExitCode.SUCCESS;
    } else {
      // Tracks instances known by both slider and llap.
      List<LlapInstance> validatedInstances = new LinkedList<>();
      List<String> llapExtraInstances = new LinkedList<>();

      for (LlapServiceInstance serviceInstance : serviceInstances) {
        String containerIdString = serviceInstance.getProperties().get(
          HiveConf.ConfVars.LLAP_DAEMON_CONTAINER_ID.varname);

        LlapInstance llapInstance = appStatusBuilder.removeAndGetRunningLlapInstanceForContainer(
          containerIdString);
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
      if (validatedInstances.size() >= appStatusBuilder.getDesiredInstances()) {
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
        // launched between slider-status/slider-diagnostics. Skip for this iteration.
        LOG.debug("Instances likely to shutdown soon: {}", llapExtraInstances);
      }

      appStatusBuilder.clearAndAddPreviouslyKnownRunningInstances(validatedInstances);

    }
    return ExitCode.SUCCESS;
  }


  private static void processAppDiagnostics(AppStatusBuilder appStatusBuilder,
                                            ApplicationDiagnostics appDiagnostics, boolean appComplete) {
    // For a running app this should be empty.
    String finalMessage = appDiagnostics.getFinalMessage();
    Collection<ContainerInformation> containerInfos =
        appDiagnostics.getContainers();
    appStatusBuilder.setDiagnostics(finalMessage);
    if (containerInfos != null) {
      for (ContainerInformation containerInformation : containerInfos) {
        if (containerInformation.getState() == StateValues.STATE_LIVE && !appComplete) {
          LlapInstance instance = appStatusBuilder
              .removeAndGetCompletedLlapInstanceForContainer(
                  containerInformation.getContainerId());
          if (instance ==
              null) { // New launch. Not available during slider status, but available now.
            instance = new LlapInstance(containerInformation.getHost(),
                containerInformation.getContainerId());
          }
          instance.setLogUrl(containerInformation.getLogLink());
          appStatusBuilder.addNewRunningLlapInstance(instance);
        } else if (containerInformation.getState() ==
            StateValues.STATE_STOPPED || appComplete) {
          LlapInstance instance =
              new LlapInstance(containerInformation.getHost(),
                  containerInformation.getContainerId());
          instance.setLogUrl(containerInformation.getLogLink());
          if (appComplete && containerInformation.getExitCode() !=
              ContainerExitStatus.INVALID) {
            instance
                .setYarnContainerExitStatus(containerInformation.getExitCode());
          }
          instance.setDiagnostics(containerInformation.getDiagnostics());
          appStatusBuilder.addNewCompleteLlapInstance(instance);
        } else {
          LOG.warn("Unexpected containerstate={}, for container={}",
              containerInformation.getState(), containerInformation);
        }
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("ContainerInfos is null");
      }
    }
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

        if (instance.getYarnContainerExitStatus() ==
            ContainerExitStatus.KILLED_EXCEEDED_PMEM ||
            instance.getYarnContainerExitStatus() ==
                ContainerExitStatus.KILLED_EXCEEDED_VMEM) {
          sb.append("\tKILLED container (by YARN for exceeding memory limits): ");
        } else {
          // TODO HIVE-15865 Handle additional reasons like OS launch failed (Slider needs to give this info)
          sb.append("\tFAILED container: ");
        }
        sb.append(" ").append(instance.getContainerId());
        sb.append(", Logs at: ").append(instance.getLogUrl());
      }
    }
    return sb.toString();
  }

  /**
   * Helper method to construct a diagnostic message from a complete
   * AppStatusBuilder.
   *
   * @return
   */
  private static String constructDiagnostics(
      AppStatusBuilder appStatusBuilder) {
    StringBuilder sb = new StringBuilder();

    switch (appStatusBuilder.getState()) {
      case APP_NOT_FOUND:
        sb.append("LLAP status unknown. Awaiting app launch");
        break;
      case LAUNCHING:
        // This is a catch all state - when containers have not started yet, or LLAP has not started yet.
        if (StringUtils.isNotBlank(appStatusBuilder.getAmInfo().getAppId())) {
          sb.append("LLAP Starting up with AppId=")
              .append(appStatusBuilder.getAmInfo().getAppId()).append(".");
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
        sb.append("LLAP Starting up with ApplicationId=")
            .append(appStatusBuilder.getAmInfo().getAppId());
        sb.append(" Started").append(appStatusBuilder.getLiveInstances())
            .append("/").append(appStatusBuilder.getDesiredInstances())
            .append(" instances");
        String containerDiagnostics = constructCompletedContainerDiagnostics(
            appStatusBuilder.getCompletedInstances());
        if (StringUtils.isNotEmpty(containerDiagnostics)) {
          sb.append("\n").append(containerDiagnostics);
        }

        // TODO HIVE-15865: Include information about pending requests, and last allocation time
        // once Slider provides this information.
        break;
      case RUNNING_ALL:
        sb.append("LLAP Application running with ApplicationId=")
            .append(appStatusBuilder.getAmInfo().getAppId());
        break;
      case COMPLETE:

        sb.append("LLAP Application already complete. ApplicationId=")
            .append(appStatusBuilder.getAmInfo().getAppId());
        containerDiagnostics = constructCompletedContainerDiagnostics(
            appStatusBuilder.getCompletedInstances());
        if (StringUtils.isNotEmpty(containerDiagnostics)) {
          sb.append("\n").append(containerDiagnostics);
        }

        break;
      case UNKNOWN:
        sb.append("LLAP status unknown");
        break;
    }
    if (StringUtils.isNotBlank(appStatusBuilder.getDiagnostics())) {
      sb.append("\n").append(appStatusBuilder.getDiagnostics());
    }

    return sb.toString();
  }

  public enum ExitCode {
    SUCCESS(0),
    INCORRECT_USAGE(10),
    YARN_ERROR(20),
    SLIDER_CLIENT_ERROR_CREATE_FAILED(30),
    SLIDER_CLIENT_ERROR_OTHER(31),
    LLAP_REGISTRY_ERROR(40),
    LLAP_JSON_GENERATION_ERROR(50),
    // Error in the script itself - likely caused by an incompatible change, or new functionality / states added.
    INTERNAL_ERROR(100);

    private final int exitCode;

    ExitCode(int exitCode) {
      this.exitCode = exitCode;
    }

    public int getInt() {
      return exitCode;
    }
  }


  public static class LlapStatusCliException extends Exception {
    final LlapStatusServiceDriver.ExitCode exitCode;


    public LlapStatusCliException(LlapStatusServiceDriver.ExitCode exitCode, String message) {
      super(exitCode.getInt() +": " + message);
      this.exitCode = exitCode;
    }

    public LlapStatusCliException(LlapStatusServiceDriver.ExitCode exitCode, String message, Throwable cause) {
      super(message, cause);
      this.exitCode = exitCode;
    }

    public LlapStatusServiceDriver.ExitCode getExitCode() {
      return exitCode;
    }
  }


  private static void logError(Throwable t) {
    LOG.error("FAILED: " + t.getMessage(), t);
    System.err.println("FAILED: " + t.getMessage());
  }


  public static void main(String[] args) {
    LOG.info("LLAP status invoked with arguments = {}", Arrays.toString(args));
    int ret = ExitCode.SUCCESS.getInt();
    Clock clock = new SystemClock();
    long startTime = clock.getTime();
    long lastSummaryLogTime = -1;

    LlapStatusServiceDriver statusServiceDriver = null;
    LlapStatusOptions options = null;
    try {
      statusServiceDriver = new LlapStatusServiceDriver();
      options = statusServiceDriver.parseOptions(args);
    } catch (Throwable t) {
      statusServiceDriver.close();
      logError(t);
      if (t instanceof LlapStatusCliException) {
        LlapStatusCliException
            ce = (LlapStatusCliException) t;
        ret = ce.getExitCode().getInt();
      } else {
        ret = ExitCode.INTERNAL_ERROR.getInt();
      }
    }
    if (ret != 0 || options == null) { // Failure / help
      if (statusServiceDriver != null) {
        statusServiceDriver.close();
      }
      System.exit(ret);
    }

    boolean firstAttempt = true;
    final long refreshInterval = options.getRefreshIntervalMs();
    final boolean watchMode = options.isWatchMode();
    final long watchTimeout = options.getWatchTimeoutMs();
    long numAttempts = watchTimeout / refreshInterval;
    numAttempts = watchMode ? numAttempts : 1; // Break out of the loop fast if watchMode is disabled.
    LlapStatusHelpers.State launchingState = null;
    LlapStatusHelpers.State currentState = null;
    boolean desiredStateAttained = false;
    final float runningNodesThreshold = options.getRunningNodesThreshold();
    try (OutputStream os = options.getOutputFile() == null ? System.out :
      new BufferedOutputStream(new FileOutputStream(options.getOutputFile()));
         PrintWriter pw = new PrintWriter(os)) {

      LOG.info("Configured refresh interval: {}s. Watch timeout: {}s. Attempts remaining: {}." +
          " Watch mode: {}. Running nodes threshold: {}.",
        TimeUnit.SECONDS.convert(refreshInterval, TimeUnit.MILLISECONDS),
        TimeUnit.SECONDS.convert(watchTimeout, TimeUnit.MILLISECONDS),
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
        ret = statusServiceDriver.run(options, watchMode ? watchTimeout : 0);
        currentState = statusServiceDriver.appStatusBuilder.getState();
        try {
          lastSummaryLogTime = LlapStatusServiceDriver
              .maybeLogSummary(clock, lastSummaryLogTime, statusServiceDriver,
                  watchMode, watchTimeout, launchingState);
        } catch (Exception e) {
          LOG.warn("Failed to log summary", e);
        }

        if (ret == ExitCode.SUCCESS.getInt()) {
          if (watchMode) {

            // slider has started llap application, now if for some reason state changes to COMPLETE then fail fast
            if (launchingState == null && LAUNCHING_STATES.contains(currentState)) {
              launchingState = currentState;
            }

            if (currentState.equals(State.COMPLETE)) {
              if (launchingState != null || options.isLaunched()) {
                LOG.warn("COMPLETE state reached while waiting for RUNNING state. Failing.");
                System.err.println("Final diagnostics: " +
                    statusServiceDriver.appStatusBuilder.getDiagnostics());
                break;
              } else {
                LOG.info("Found a stopped application; assuming it was a previous attempt "
                    + "and waiting for the next one. Omit the -l flag to avoid this.");
              }
            }

            if (!(currentState.equals(State.RUNNING_PARTIAL) || currentState.equals(
                State.RUNNING_ALL))) {
              if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "Current state: {}. Desired state: {}. {}/{} instances.",
                    currentState,
                    runningNodesThreshold == 1.0f ?
                        State.RUNNING_ALL :
                        State.RUNNING_PARTIAL,
                    statusServiceDriver.appStatusBuilder.getLiveInstances(),
                    statusServiceDriver.appStatusBuilder
                        .getDesiredInstances());
              }
              numAttempts--;
              continue;
            }

            // we have reached RUNNING state, now check if running nodes threshold is met
            final int liveInstances = statusServiceDriver.appStatusBuilder.getLiveInstances();
            final int desiredInstances = statusServiceDriver.appStatusBuilder.getDesiredInstances();
            if (desiredInstances > 0) {
              final float ratio = (float) liveInstances / (float) desiredInstances;
              if (ratio < runningNodesThreshold) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug(
                      "Waiting until running nodes threshold is reached. Current: {} Desired: {}." +
                          " {}/{} instances.",
                      new DecimalFormat("#.###").format(ratio),
                      new DecimalFormat("#.###")
                          .format(runningNodesThreshold),
                      statusServiceDriver.appStatusBuilder.getLiveInstances(),
                      statusServiceDriver.appStatusBuilder
                          .getDesiredInstances());
                }
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
        } else if (ret == ExitCode.YARN_ERROR.getInt() && watchMode) {
          LOG.warn("Watch mode enabled and got YARN error. Retrying..");
          numAttempts--;
          continue;
        } else if (ret == ExitCode.SLIDER_CLIENT_ERROR_CREATE_FAILED.getInt() && watchMode) {
          LOG.warn("Watch mode enabled and slider client creation failed. Retrying..");
          numAttempts--;
          continue;
        } else if (ret == ExitCode.SLIDER_CLIENT_ERROR_OTHER.getInt() && watchMode) {
          LOG.warn("Watch mode enabled and got slider client error. Retrying..");
          numAttempts--;
          continue;
        } else if (ret == ExitCode.LLAP_REGISTRY_ERROR.getInt() && watchMode) {
          LOG.warn("Watch mode enabled and got LLAP registry error. Retrying..");
          numAttempts--;
          continue;
        }
        break;
      }
      // Log final state to CONSOLE_LOGGER
      LlapStatusServiceDriver
          .maybeLogSummary(clock, 0L, statusServiceDriver,
              watchMode, watchTimeout, launchingState);
      CONSOLE_LOGGER.info("\n\n\n");
      // print current state before exiting
      statusServiceDriver.outputJson(pw);
      os.flush();
      pw.flush();
      if (numAttempts == 0 && watchMode && !desiredStateAttained) {
        LOG.warn("Watch timeout {}s exhausted before desired state RUNNING is attained.",
          TimeUnit.SECONDS.convert(watchTimeout, TimeUnit.MILLISECONDS));
      }
    } catch (Throwable t) {
      logError(t);
      if (t instanceof LlapStatusCliException) {
        LlapStatusCliException
            ce = (LlapStatusCliException) t;
        ret = ce.getExitCode().getInt();
      } else {
        ret = ExitCode.INTERNAL_ERROR.getInt();
      }
    } finally {
      LOG.info("LLAP status finished");
      statusServiceDriver.close();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Completed processing - exiting with " + ret);
    }
    System.exit(ret);
  }

  private static long maybeLogSummary(Clock clock, long lastSummaryLogTime,
                                      LlapStatusServiceDriver statusServiceDriver,
                                      boolean watchMode, long watchTimeout, LlapStatusHelpers.State launchingState) {
    long currentTime = clock.getTime();
    if (lastSummaryLogTime < currentTime - LOG_SUMMARY_INTERVAL) {
      String diagString = null;
      if (launchingState == null && statusServiceDriver.appStatusBuilder.getState() ==
          State.COMPLETE && watchMode) {
        // First known state was COMPLETED. Wait for the app launch to start.
        diagString = "Awaiting LLAP launch";
        // Clear completed instances in this case. Don't want to provide information from the previous run.
        statusServiceDriver.appStatusBuilder.clearCompletedLlapInstances();
      } else {
        diagString = constructDiagnostics(statusServiceDriver.appStatusBuilder);
      }

      if (lastSummaryLogTime == -1) {
        if (watchMode) {
          CONSOLE_LOGGER.info("\nLLAPSTATUS WatchMode with timeout={} s",
              TimeUnit.SECONDS.convert(watchTimeout, TimeUnit.MILLISECONDS));
        } else {
          CONSOLE_LOGGER.info("\nLLAPSTATUS");
        }
        CONSOLE_LOGGER.info(
            "--------------------------------------------------------------------------------");
      }
      CONSOLE_LOGGER.info(diagString);
      CONSOLE_LOGGER.info(
          "--------------------------------------------------------------------------------");
      lastSummaryLogTime = currentTime;
    }
    return lastSummaryLogTime;
  }

  private void close() {
    if (sliderClient != null) {
      sliderClient.stop();
    }
    if (llapRegistry != null) {
      llapRegistry.stop();
    }
  }
}
