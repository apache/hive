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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.security.sasl.Sasl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Definitions of configuration keys and default values for the RPC layer.
 */
@InterfaceAudience.Private
public final class RpcConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(RpcConfiguration.class);

  public static final ImmutableSet<String> HIVE_SPARK_RSC_CONFIGS = ImmutableSet.of(
    HiveConf.ConfVars.SPARK_RPC_CLIENT_CONNECT_TIMEOUT.varname,
    HiveConf.ConfVars.SPARK_RPC_CLIENT_HANDSHAKE_TIMEOUT.varname,
    HiveConf.ConfVars.SPARK_RPC_CHANNEL_LOG_LEVEL.varname,
    HiveConf.ConfVars.SPARK_RPC_MAX_MESSAGE_SIZE.varname,
    HiveConf.ConfVars.SPARK_RPC_MAX_THREADS.varname,
    HiveConf.ConfVars.SPARK_RPC_SECRET_RANDOM_BITS.varname
  );
  public static final ImmutableSet<String> HIVE_SPARK_TIME_CONFIGS = ImmutableSet.of(
    HiveConf.ConfVars.SPARK_RPC_CLIENT_CONNECT_TIMEOUT.varname,
    HiveConf.ConfVars.SPARK_RPC_CLIENT_HANDSHAKE_TIMEOUT.varname
  );

  public static final String SERVER_LISTEN_ADDRESS_KEY = "hive.spark.client.server.address";

  /** Prefix for other SASL options. */
  public static final String RPC_SASL_OPT_PREFIX = "hive.spark.client.rpc.sasl.";

  private final Map<String, String> config;

  private static final HiveConf DEFAULT_CONF = new HiveConf();

  public RpcConfiguration(Map<String, String> config) {
    this.config = config;
  }

  long getConnectTimeoutMs() {
    String value = config.get(HiveConf.ConfVars.SPARK_RPC_CLIENT_CONNECT_TIMEOUT.varname);
    return value != null ? Integer.parseInt(value) : DEFAULT_CONF.getTimeVar(
      HiveConf.ConfVars.SPARK_RPC_CLIENT_CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
  }

  int getMaxMessageSize() {
    String value = config.get(HiveConf.ConfVars.SPARK_RPC_MAX_MESSAGE_SIZE.varname);
    return value != null ? Integer.parseInt(value) : HiveConf.ConfVars.SPARK_RPC_MAX_MESSAGE_SIZE.defaultIntVal;
  }

  long getServerConnectTimeoutMs() {
    String value = config.get(HiveConf.ConfVars.SPARK_RPC_CLIENT_HANDSHAKE_TIMEOUT.varname);
    return value != null ? Long.parseLong(value) : DEFAULT_CONF.getTimeVar(
      HiveConf.ConfVars.SPARK_RPC_CLIENT_HANDSHAKE_TIMEOUT, TimeUnit.MILLISECONDS);
  }

  int getSecretBits() {
    String value = config.get(HiveConf.ConfVars.SPARK_RPC_SECRET_RANDOM_BITS.varname);
    return value != null ? Integer.parseInt(value) : HiveConf.ConfVars.SPARK_RPC_SECRET_RANDOM_BITS.defaultIntVal;
  }

  String getServerAddress() throws IOException {
    String value = config.get(SERVER_LISTEN_ADDRESS_KEY);
    if (value != null) {
      return value;
    }

    InetAddress address = InetAddress.getLocalHost();
    if (address.isLoopbackAddress()) {
      // Address resolves to something like 127.0.1.1, which happens on Debian;
      // try to find a better address using the local network interfaces
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
    return config.get(HiveConf.ConfVars.SPARK_RPC_CHANNEL_LOG_LEVEL.varname);
  }

  public int getRpcThreadCount() {
    String value = config.get(HiveConf.ConfVars.SPARK_RPC_MAX_THREADS.varname);
    return value != null ? Integer.parseInt(value) : HiveConf.ConfVars.SPARK_RPC_MAX_THREADS.defaultIntVal;
  }

  /**
   * Utility method for a given RpcConfiguration key, to convert value to millisecond if it is a time value,
   * and return as string in either case.
   * @param conf hive configuration
   * @param key Rpc configuration to lookup (hive.spark.*)
   * @return string form of the value
   */
  public static String getValue(HiveConf conf, String key) {
    if (HIVE_SPARK_TIME_CONFIGS.contains(key)) {
      HiveConf.ConfVars confVar = HiveConf.getConfVars(key);
      return String.valueOf(conf.getTimeVar(confVar, TimeUnit.MILLISECONDS));
    } else {
      return conf.get(key);
    }
  }

  String getSaslMechanism() {
    String value = config.get(HiveConf.ConfVars.SPARK_RPC_SASL_MECHANISM.varname);
    return value != null ? value : HiveConf.ConfVars. SPARK_RPC_SASL_MECHANISM.defaultStrVal;
  }

  /**
   * SASL options are namespaced under "hive.spark.client.rpc.sasl.*"; each option is the
   * lower-case version of the constant in the "javax.security.sasl.Sasl" class (e.g. "strength"
   * for cipher strength).
   */
  Map<String, String> getSaslOptions() {
    Map<String, String> opts = new HashMap<String, String>();
    Map<String, String> saslOpts = ImmutableMap.<String, String>builder()
      .put(Sasl.CREDENTIALS, "credentials")
      .put(Sasl.MAX_BUFFER, "max_buffer")
      .put(Sasl.POLICY_FORWARD_SECRECY, "policy_forward_secrecy")
      .put(Sasl.POLICY_NOACTIVE, "policy_noactive")
      .put(Sasl.POLICY_NOANONYMOUS, "policy_noanonymous")
      .put(Sasl.POLICY_NODICTIONARY, "policy_nodictionary")
      .put(Sasl.POLICY_NOPLAINTEXT, "policy_noplaintext")
      .put(Sasl.POLICY_PASS_CREDENTIALS, "policy_pass_credentials")
      .put(Sasl.QOP, "qop")
      .put(Sasl.RAW_SEND_SIZE, "raw_send_size")
      .put(Sasl.REUSE, "reuse")
      .put(Sasl.SERVER_AUTH, "server_auth")
      .put(Sasl.STRENGTH, "strength")
      .build();
    for (Map.Entry<String, String> e : saslOpts.entrySet()) {
      String value = config.get(RPC_SASL_OPT_PREFIX + e.getValue());
      if (value != null) {
        opts.put(e.getKey(), value);
      }
    }
    return opts;
  }

}
