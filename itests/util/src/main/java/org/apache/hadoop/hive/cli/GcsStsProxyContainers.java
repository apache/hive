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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.cli;

import java.time.Duration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Forwards {@code sts.googleapis.com:443} inside a Docker network to a host-reachable OAuth/STS
 * mock ({@link GcsOAuthTokenMock}). Gravitino {@code GCSTokenGenerator} uses Google STS over HTTPS
 * on port 443; {@code extraHost} alone cannot redirect that to an ephemeral mock port.
 */
public final class GcsStsProxyContainers {

  private static final DockerImageName SOCAT_IMAGE = DockerImageName.parse("alpine/socat:1.0.5");
  private static final String STS_HOST = "sts.googleapis.com";
  private static final int STS_PORT = 443;

  private GenericContainer<?> socat;

  /**
   * Starts a socat listener on {@code network} as {@code sts.googleapis.com}, forwarding to
   * {@code hostTokenPort} on the test host.
   */
  @SuppressWarnings("resource")
  public void start(Network network, int hostTokenPort) {
    String forwardTarget = String.format("TCP:host.testcontainers.internal:%d", hostTokenPort);
    socat = new GenericContainer<>(SOCAT_IMAGE)
        .withNetwork(network)
        .withNetworkAliases(STS_HOST)
        .withExtraHost("host.testcontainers.internal", "host-gateway")
        .withCommand(String.format(
            "TCP-LISTEN:%d,fork,reuseaddr", STS_PORT), forwardTarget)
        .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(1)));
    socat.start();
  }

  public void stop() {
    if (socat != null) {
      socat.stop();
      socat = null;
    }
  }
}
