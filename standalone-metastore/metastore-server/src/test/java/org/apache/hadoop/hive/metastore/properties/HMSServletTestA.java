/* * Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.hadoop.hive.metastore.properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.junit.Assert;
import java.nio.file.Files;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;

/**
 * Test using the servlet server created by the MetaStore.
 */
public class HMSServletTestA extends HMSServletTest {
  protected int thriftPort;

  @Override
  protected int createServer(Configuration conf) throws Exception {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.PROPERTIES_SERVLET_AUTH, "JWT");
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.PROPERTIES_SERVLET_PORT, 0);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_METASTORE_AUTHENTICATION_JWT_JWKS_URL,
        "http://localhost:" + MOCK_JWKS_SERVER_PORT + "/jwks");
    MOCK_JWKS_SERVER.stubFor(get("/jwks")
        .willReturn(ok()
            .withBody(Files.readAllBytes(jwtVerificationJWKSFile.toPath()))));
    thriftPort = MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf);
    servletServer = HiveMetaStore.getPropertyServer();
    if (servletServer == null || !servletServer.isStarted()) {
      Assert.fail("http server did not start");
    }
    sport = servletServer.getURI().getPort();
    return sport;
  }

  @Override
  protected void stopServer(int port) throws Exception {
    super.stopServer(port);
    MetaStoreTestUtils.close(thriftPort);
  }
}
