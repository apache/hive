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
package org.apache.hadoop.hive.ql.log;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.logging.log4j.Level;
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
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.config.plugins.processor.PluginEntry;
import org.apache.logging.log4j.core.config.plugins.util.PluginType;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.core.layout.PatternLayout;

/**
 * Divert appender to redirect and filter test operation logs to match the output of the original
 * CLI qtest results.
 */
public final class LogDivertAppenderForTest {

  /**
   * Name of the test query routine appender.
   */
  public static final String TEST_QUERY_ROUTING_APPENDER = "test-query-routing";

  private LogDivertAppenderForTest() {
    // Prevent instantiation
  }

  /**
   * A log filter that filters test messages coming from the logger.
   */
  @Plugin(name = "TestFilter", category = "Core", elementType="filter", printObject = true)
  private static class TestFilter extends AbstractFilter {
    @Override
    public Result filter(LogEvent event) {
      if (event.getLevel().equals(Level.INFO) && "SessionState".equals(event.getLoggerName())) {
        if (event.getMessage().getFormattedMessage().startsWith("PREHOOK:")
            || event.getMessage().getFormattedMessage().startsWith("POSTHOOK:")
            || event.getMessage().getFormattedMessage().startsWith("unix_timestamp(void)")
            || event.getMessage().getFormattedMessage().startsWith("Warning: ")
            ) {
          return Result.ACCEPT;
        }
      }
      return Result.DENY;
    }

    @PluginFactory
    public static TestFilter createFilter() {
      return new TestFilter();
    }
  }

  /**
   * If the HIVE_IN_TEST is set, then programmatically register a routing appender to Log4J
   * configuration, which automatically writes the test log of each query to an individual file.
   * The equivalent property configuration is as follows:
   *  # queryId based routing file appender
      appender.test-query-routing.type = Routing
      appender.test-query-routing.name = test-query-routing
      appender.test-query-routing.routes.type = Routes
      appender.test-query-routing.routes.pattern = $${ctx:queryId}
      # default route
      appender.test-query-routing.routes.test-route-default.type = Route
      appender.test-query-routing.routes.test-route-default.key = $${ctx:queryId}
      appender.test-query-routing.routes.test-route-default.app.type = NullAppender
      appender.test-query-routing.routes.test-route-default.app.name = test-null-appender
      # queryId based route
      appender.test-query-routing.routes.test-route-mdc.type = Route
      appender.test-query-routing.routes.test-route-mdc.name = test-query-routing
      appender.test-query-routing.routes.test-route-mdc.app.type = RandomAccessFile
      appender.test-query-routing.routes.test-route-mdc.app.name = test-query-file-appender
      appender.test-query-routing.routes.test-route-mdc.app.fileName = ${sys:hive.log.dir}/${ctx:sessionId}/${ctx:queryId}.test
      appender.test-query-routing.routes.test-route-mdc.app.layout.type = PatternLayout
      appender.test-query-routing.routes.test-route-mdc.app.layout.pattern = %d{ISO8601} %5p %c{2}: %m%n
      appender.test-query-routing.routes.test-route-mdc.app.filter.type = TestFilter
   * @param conf the configuration for HiveServer2 instance
   */
  public static void registerRoutingAppenderIfInTest(org.apache.hadoop.conf.Configuration conf) {
    if (!conf.getBoolean(HiveConf.ConfVars.HIVE_IN_TEST.varname,
        HiveConf.ConfVars.HIVE_IN_TEST.defaultBoolVal)) {
      // If not in test mode, then do no create the appender
      return;
    }

    String logLocation =
        HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LOG_LOCATION);

    // Create test-null-appender to drop events without queryId
    PluginEntry nullAppenderEntry = new PluginEntry();
    nullAppenderEntry.setClassName(NullAppender.class.getName());
    PluginType<NullAppender> nullAppenderType =
        new PluginType<>(nullAppenderEntry, NullAppender.class, "appender");
    Node nullAppenderChildNode = new Node(null, "test-null-appender", nullAppenderType);

    // Create default route where events go without queryId
    PluginEntry defaultRouteEntry = new PluginEntry();
    defaultRouteEntry.setClassName(Route.class.getName());
    PluginType<Route> defaultRouteType = new PluginType<>(defaultRouteEntry, Route.class, "");
    Node defaultRouteNode = new Node(null, "test-route-default", defaultRouteType);
    // Add the test-null-appender to the default route
    defaultRouteNode.getChildren().add(nullAppenderChildNode);

    // Create queryId based route
    PluginEntry queryIdRouteEntry = new PluginEntry();
    queryIdRouteEntry.setClassName(Route.class.getName());
    PluginType<Route> queryIdRouteType = new PluginType<>(queryIdRouteEntry, Route.class, "");
    Node queryIdRouteNode = new Node(null, "test-route-mdc", queryIdRouteType);

    // Create the queryId appender for the queryId route
    PluginEntry queryIdAppenderEntry = new PluginEntry();
    queryIdAppenderEntry.setClassName(HushableRandomAccessFileAppender.class.getName());
    PluginType<HushableRandomAccessFileAppender> queryIdAppenderType =
        new PluginType<>(queryIdAppenderEntry,
            HushableRandomAccessFileAppender.class, "appender");
    Node queryIdAppenderNode =
        new Node(queryIdRouteNode, "test-query-file-appender", queryIdAppenderType);
    queryIdAppenderNode.getAttributes().put("fileName", logLocation
        + "/${ctx:sessionId}/${ctx:queryId}.test");
    queryIdAppenderNode.getAttributes().put("name", "test-query-file-appender");
    // Add the queryId appender to the queryId based route
    queryIdRouteNode.getChildren().add(queryIdAppenderNode);

    // Create the filter for the queryId appender
    PluginEntry filterEntry = new PluginEntry();
    filterEntry.setClassName(TestFilter.class.getName());
    PluginType<TestFilter> filterType =
        new PluginType<>(filterEntry, TestFilter.class, "");
    Node filterNode = new Node(queryIdAppenderNode, "test-filter", filterType);
    // Add the filter to the queryId appender
    queryIdAppenderNode.getChildren().add(filterNode);

    // Create the layout for the queryId appender
    PluginEntry layoutEntry = new PluginEntry();
    layoutEntry.setClassName(PatternLayout.class.getName());
    PluginType<PatternLayout> layoutType =
        new PluginType<>(layoutEntry, PatternLayout.class, "");
    Node layoutNode = new Node(queryIdAppenderNode, "PatternLayout", layoutType);
    layoutNode.getAttributes().put("pattern", LogDivertAppender.nonVerboseLayout);
    // Add the layout to the queryId appender
    queryIdAppenderNode.getChildren().add(layoutNode);

    // Create the route objects based on the Nodes
    Route defaultRoute = Route.createRoute(null, "${ctx:queryId}", defaultRouteNode);
    Route mdcRoute = Route.createRoute(null, null, queryIdRouteNode);
    // Create the routes group
    Routes routes = Routes.createRoutes("${ctx:queryId}", defaultRoute, mdcRoute);

    LoggerContext context = (LoggerContext)LogManager.getContext(false);
    Configuration configuration = context.getConfiguration();

    // Create the appender
    RoutingAppender routingAppender = RoutingAppender.createAppender(TEST_QUERY_ROUTING_APPENDER,
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
