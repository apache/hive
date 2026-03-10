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
package org.apache.iceberg.rest.standalone;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.context.WebServerInitializedEvent;
import org.springframework.boot.web.server.Ssl;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Runtime lifecycle for the Standalone REST Catalog Server.
 * Holds port, rest endpoint, and handles web server initialization.
 */
@Component
public class RestCatalogServerRuntime {
  private static final Logger LOG = LoggerFactory.getLogger(RestCatalogServerRuntime.class);

  private final Configuration conf;
  private final ServerProperties serverProperties;
  private String restEndpoint;
  private int port;

  public RestCatalogServerRuntime(Configuration conf, ServerProperties serverProperties) {
    this.conf = conf;
    this.serverProperties = serverProperties;

    String thriftUris = MetastoreConf.getVar(conf, ConfVars.THRIFT_URIS);
    if (thriftUris == null || thriftUris.isEmpty()) {
      throw new IllegalArgumentException("metastore.thrift.uris must be configured to connect to HMS");
    }

    LOG.info("Hadoop Configuration initialized");
    LOG.info("  HMS Thrift URIs: {}", thriftUris);

    if (LOG.isInfoEnabled()) {
      LOG.info("  Warehouse: {}", MetastoreConf.getVar(conf, ConfVars.WAREHOUSE));
      LOG.info("  Warehouse (external): {}", MetastoreConf.getVar(conf, ConfVars.WAREHOUSE_EXTERNAL));
    }
  }

  @EventListener
  public void onWebServerInitialized(WebServerInitializedEvent event) {
    int actualPort = event.getWebServer().getPort();

    if (actualPort > 0) {
      port = actualPort;
      String servletPath = MetastoreConf.getVar(conf, ConfVars.ICEBERG_CATALOG_SERVLET_PATH);

      if (servletPath == null || servletPath.isEmpty()) {
        servletPath = IcebergCatalogConfiguration.DEFAULT_SERVLET_PATH;
      }

      String scheme = isSslEnabled() ? "https" : "http";
      restEndpoint = UriComponentsBuilder.newInstance()
          .scheme(scheme)
          .host("localhost")
          .port(actualPort)
          .pathSegment(servletPath)
          .toUriString();

      LOG.info("REST endpoint set to actual server port: {}", restEndpoint);
    }
  }

  private boolean isSslEnabled() {
    Ssl ssl = serverProperties != null ? serverProperties.getSsl() : null;
    return ssl != null && ssl.isEnabled();
  }

  @VisibleForTesting
  public int getPort() {
    return port;
  }

  public String getRestEndpoint() {
    return restEndpoint;
  }
}
