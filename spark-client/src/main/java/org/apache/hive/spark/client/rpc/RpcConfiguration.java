/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.spark.client.rpc;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;

/**
 * Definitions of configuration keys and default values for the RPC layer.
 */
@InterfaceAudience.Private
public final class RpcConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(RpcConfiguration.class);

  /** Connection timeout for RPC clients. */
  public static final String CONNECT_TIMEOUT_MS_KEY = "hive.spark.client.connect.timeout.ms";
  private static final int CONNECT_TIMEOUT_MS_DEFAULT = 1000;

  /**
   * How long the server should wait for clients to connect back after they're
   * registered. Also used to time out the client waiting for the server to
   * reply to its "hello" message.
   */
  public static final String SERVER_CONNECT_TIMEOUT_MS_KEY = "hive.spark.client.server.connect.timeout.ms";
  private static final long SERVER_CONNECT_TIMEOUT_MS_DEFAULT = 10000L;

  /**
   * Number of bits of randomness in the generated client secrets. Rounded down
   * to the nearest multiple of 8.
   */
  public static final String SECRET_RANDOM_BITS_KEY = "hive.spark.client.secret.bits";
  private static final int SECRET_RANDOM_BITS_DEFAULT = 256;

  /** Hostname or IP address to advertise for the server. */
  public static final String SERVER_LISTEN_ADDRESS_KEY = "hive.spark.client.server.address";

  /** Maximum number of threads to use for the RPC event loop. */
  public static final String RPC_MAX_THREADS_KEY = "hive.spark.client.rpc.threads";
  public static final int RPC_MAX_THREADS_DEFAULT = 8;

  /** Maximum message size. Default = 10MB. */
  public static final String RPC_MAX_MESSAGE_SIZE_KEY = "hive.spark.client.rpc.max.size";
  public static final int RPC_MAX_MESSAGE_SIZE_DEFAULT = 50 * 1024 * 1024;

  /** Channel logging level. */
  public static final String RPC_CHANNEL_LOG_LEVEL_KEY = "hive.spark.client.channel.log.level";

  private final Map<String, String> config;

  public RpcConfiguration(Map<String, String> config) {
    this.config = config;
  }

  int getConnectTimeoutMs() {
    String value = config.get(CONNECT_TIMEOUT_MS_KEY);
    return value != null ? Integer.parseInt(value) : CONNECT_TIMEOUT_MS_DEFAULT;
  }

  int getMaxMessageSize() {
    String value = config.get(RPC_MAX_MESSAGE_SIZE_KEY);
    return value != null ? Integer.parseInt(value) : RPC_MAX_MESSAGE_SIZE_DEFAULT;
  }

  long getServerConnectTimeoutMs() {
    String value = config.get(SERVER_CONNECT_TIMEOUT_MS_KEY);
    return value != null ? Long.parseLong(value) : SERVER_CONNECT_TIMEOUT_MS_DEFAULT;
  }

  int getSecretBits() {
    String value = config.get(SECRET_RANDOM_BITS_KEY);
    return value != null ? Integer.parseInt(value) : SECRET_RANDOM_BITS_DEFAULT;
  }

  String getServerAddress() throws IOException {
    String value = config.get(SERVER_LISTEN_ADDRESS_KEY);
    if (value != null) {
      return value;
    }

    InetAddress address = InetAddress.getLocalHost();
    if (address.isLoopbackAddress()) {
      // Address resolves to something like 127.0.1.1, which happens on Debian;
      // try to find
      // a better address using the local network interfaces
      Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
      while (ifaces.hasMoreElements()) {
        NetworkInterface ni = ifaces.nextElement();
        Enumeration<InetAddress> addrs = ni.getInetAddresses();
        while (addrs.hasMoreElements()) {
          InetAddress addr = addrs.nextElement();
          if (!addr.isLinkLocalAddress() && !addr.isLoopbackAddress()
              && addr instanceof Inet4Address) {
            // We've found an address that looks reasonable!
            LOG.warn("Your hostname, {}, resolves to a loopback address; using {} "
                + " instead (on interface {})", address.getHostName(), addr.getHostAddress(),
                ni.getName());
            LOG.warn("Set '{}' if you need to bind to another address.", SERVER_LISTEN_ADDRESS_KEY);
            return addr.getHostAddress();
          }
        }
      }
    }

    LOG.warn("Your hostname, {}, resolves to a loopback address, but we couldn't find "
        + " any external IP address!", address.getHostName());
    LOG.warn("Set {} if you need to bind to another address.", SERVER_LISTEN_ADDRESS_KEY);
    return address.getHostName();
  }

  String getRpcChannelLogLevel() {
    return config.get(RPC_CHANNEL_LOG_LEVEL_KEY);
  }

  public int getRpcThreadCount() {
    String value = config.get(RPC_MAX_THREADS_KEY);
    return value != null ? Integer.parseInt(value) : RPC_MAX_THREADS_DEFAULT;
  }

}
