/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.log;

import java.util.regex.Pattern;

import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.session.OperationLog;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.routing.Route;
import org.apache.logging.log4j.core.appender.routing.Routes;
import org.apache.logging.log4j.core.appender.routing.RoutingAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.config.plugins.processor.PluginEntry;
import org.apache.logging.log4j.core.config.plugins.util.PluginType;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Divert appender to redirect operation logs to separate files.
 */
public class LogDivertAppender {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LogDivertAppender.class.getName());
  public static final String verboseLayout = "%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n";
  public static final String nonVerboseLayout = "%-5p : %m%n";
  /**
   * Name of the query routine appender.
   */
  public static final String QUERY_ROUTING_APPENDER = "query-routing";

  /**
   * A log filter that filters messages coming from the logger with the given names.
   * It be used as a white list filter or a black list filter.
   * We apply black list filter on the Loggers used by the log diversion stuff, so that
   * they don't generate more logs for themselves when they process logs.
   * White list filter is used for less verbose log collection
   */
  @Plugin(name = "NameFilter", category = "Core", elementType="filter", printObject = true)
  private static class NameFilter extends AbstractFilter {
    private Pattern namePattern;
    private OperationLog.LoggingLevel loggingMode;

    /* Patterns that are excluded in verbose logging level.
     * Filter out messages coming from log processing classes, or we'll run an infinite loop.
     */
    private static final Pattern verboseExcludeNamePattern = Pattern.compile(Joiner.on("|").
        join(new String[]{LOG.getName(), OperationLog.class.getName()}));

    /* Patterns that are included in execution logging level.
     * In execution mode, show only select logger messages.
     */
    private static final Pattern executionIncludeNamePattern = Pattern.compile(Joiner.on("|").
        join(new String[]{"org.apache.hadoop.mapreduce.JobSubmitter",
          "org.apache.hadoop.mapreduce.Job", "SessionState", "ReplState", Task.class.getName(),
          TezTask.class.getName(), Driver.class.getName(),
          "org.apache.hadoop.hive.ql.exec.spark.status.SparkJobMonitor"}));

    /* Patterns that are included in performance logging level.
     * In performance mode, show execution and performance logger messages.
     */
    private static final Pattern performanceIncludeNamePattern = Pattern.compile(
        executionIncludeNamePattern.pattern() + "|" + PerfLogger.class.getName());

    private void setCurrentNamePattern(OperationLog.LoggingLevel mode) {
      if (mode == OperationLog.LoggingLevel.VERBOSE) {
        this.namePattern = verboseExcludeNamePattern;
      } else if (mode == OperationLog.LoggingLevel.EXECUTION) {
        this.namePattern = executionIncludeNamePattern;
      } else if (mode == OperationLog.LoggingLevel.PERFORMANCE) {
        this.namePattern = performanceIncludeNamePattern;
      }
    }

    public NameFilter(OperationLog.LoggingLevel loggingMode) {
      this.loggingMode = loggingMode;
      setCurrentNamePattern(loggingMode);
    }

    @Override
    public Result filter(LogEvent event) {
      boolean excludeMatches = (loggingMode == OperationLog.LoggingLevel.VERBOSE);

      String logLevel = event.getContextMap().get(LogUtils.OPERATIONLOG_LEVEL_KEY);
      logLevel = logLevel == null ? "" : logLevel;
      OperationLog.LoggingLevel currentLoggingMode = OperationLog.getLoggingLevel(logLevel);
      // If logging is disabled, deny everything.
      if (currentLoggingMode == OperationLog.LoggingLevel.NONE) {
        return Result.DENY;
      }
      // Look at the current session's setting
      // and set the pattern and excludeMatches accordingly.
      if (currentLoggingMode != loggingMode) {
        loggingMode = currentLoggingMode;
        excludeMatches = (loggingMode == OperationLog.LoggingLevel.VERBOSE);
        setCurrentNamePattern(loggingMode);
      }

      boolean isMatch = namePattern.matcher(event.getLoggerName()).matches();

      if (excludeMatches == isMatch) {
        // Deny if this is black-list filter (excludeMatches = true) and it
        // matched or if this is whitelist filter and it didn't match
        return Result.DENY;
      }

      return Result.NEUTRAL;
    }

    @PluginFactory
    public static NameFilter createFilter(
        @PluginAttribute("loggingLevel") final String loggingLevel) {
      // Name required for routing. Error out if it is not set.
      Preconditions.checkNotNull(loggingLevel,
          "loggingLevel must be specified for " + NameFilter.class.getName());

      return new NameFilter(OperationLog.getLoggingLevel(loggingLevel));
    }
  }

  /**
   * Programmatically register a routing appender to Log4J configuration, which
   * automatically writes the log of each query to an individual file.
   * The equivalent property configuration is as follows:
   * # queryId based routing file appender
      appender.query-routing.type = Routing
      appender.query-routing.name = query-routing
      appender.query-routing.routes.type = Routes
      appender.query-routing.routes.pattern = $${ctx:queryId}
      # default route
      appender.query-routing.routes.route-default.type = Route
      appender.query-routing.routes.route-default.key = $${ctx:queryId}
      appender.query-routing.routes.route-default.app.type = null
      appender.query-routing.routes.route-default.app.name = Null
      # queryId based route
      appender.query-routing.routes.route-mdc.type = Route
      appender.query-routing.routes.route-mdc.name = IrrelevantName-query-routing
      appender.query-routing.routes.route-mdc.app.type = RandomAccessFile
      appender.query-routing.routes.route-mdc.app.name = query-file-appender
      appender.query-routing.routes.route-mdc.app.fileName = ${sys:hive.log.dir}/${ctx:sessionId}/${ctx:queryId}
      appender.query-routing.routes.route-mdc.app.layout.type = PatternLayout
      appender.query-routing.routes.route-mdc.app.layout.pattern = %d{ISO8601} %5p %c{2}: %m%n
   * @param conf  the configuration for HiveServer2 instance
   */
  public static void registerRoutingAppender(org.apache.hadoop.conf.Configuration conf) {
    String loggingLevel = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LEVEL);
    OperationLog.LoggingLevel loggingMode = OperationLog.getLoggingLevel(loggingLevel);
    String layout = loggingMode == OperationLog.LoggingLevel.VERBOSE ? verboseLayout : nonVerboseLayout;
    String logLocation = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LOG_LOCATION);

    // Create NullAppender
    PluginEntry nullEntry = new PluginEntry();
    nullEntry.setClassName(NullAppender.class.getName());
    nullEntry.setKey("null");
    nullEntry.setName("appender");
    PluginType<NullAppender> nullChildType = new PluginType<NullAppender>(nullEntry, NullAppender.class, "appender");
    Node nullChildNode = new Node(null, "Null", nullChildType);

    // Create default route
    PluginEntry defaultEntry = new PluginEntry();
    defaultEntry.setClassName(Route.class.getName());
    defaultEntry.setKey("route");
    defaultEntry.setName("Route");
    PluginType<Route> defaultType = new PluginType<Route>(defaultEntry, Route.class, "Route");
    Node nullNode = new Node(null, "Route", defaultType);
    nullNode.getChildren().add(nullChildNode);
    Route defaultRoute = Route.createRoute(null, "${ctx:queryId}", nullNode);

    // Create queryId based route
    PluginEntry entry = new PluginEntry();
    entry.setClassName(Route.class.getName());
    entry.setKey("route");
    entry.setName("Route");
    PluginType<Route> type = new PluginType<Route>(entry, Route.class, "Route");
    Node node = new Node(null, "Route", type);

    PluginEntry childEntry = new PluginEntry();
    childEntry.setClassName(HushableRandomAccessFileAppender.class.getName());
    childEntry.setKey("HushableMutableRandomAccess");
    childEntry.setName("appender");
    PluginType<HushableRandomAccessFileAppender> childType = new PluginType<>(childEntry, HushableRandomAccessFileAppender.class, "appender");
    Node childNode = new Node(node, "HushableMutableRandomAccess", childType);
    childNode.getAttributes().put("name", "query-file-appender");
    childNode.getAttributes().put("fileName", logLocation + "/${ctx:sessionId}/${ctx:queryId}");
    node.getChildren().add(childNode);

    PluginEntry filterEntry = new PluginEntry();
    filterEntry.setClassName(NameFilter.class.getName());
    filterEntry.setKey("namefilter");
    filterEntry.setName("namefilter");
    PluginType<NameFilter> filterType = new PluginType<>(filterEntry, NameFilter.class, "filter");
    Node filterNode = new Node(childNode, "NameFilter", filterType);
    filterNode.getAttributes().put("loggingLevel", loggingMode.name());
    childNode.getChildren().add(filterNode);

    PluginEntry layoutEntry = new PluginEntry();
    layoutEntry.setClassName(PatternLayout.class.getName());
    layoutEntry.setKey("patternlayout");
    layoutEntry.setName("layout");
    PluginType<PatternLayout> layoutType = new PluginType<>(layoutEntry, PatternLayout.class, "layout");
    Node layoutNode = new Node(childNode, "PatternLayout", layoutType);
    layoutNode.getAttributes().put("pattern", layout);
    childNode.getChildren().add(layoutNode);

    Route mdcRoute = Route.createRoute(null, null, node);
    Routes routes = Routes.createRoutes("${ctx:queryId}", defaultRoute, mdcRoute);

    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration configuration = context.getConfiguration();

    RoutingAppender routingAppender = RoutingAppender.createAppender(QUERY_ROUTING_APPENDER,
        "true",
        routes,
        configuration,
        null,
        null,
        null);

    LoggerConfig loggerConfig = configuration.getRootLogger();
    loggerConfig.addAppender(routingAppender, null, null);
    context.updateLoggers();
    routingAppender.start();
  }

}
