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

package org.apache.hive.spark.client;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.Map;
import java.util.UUID;

import akka.actor.ActorSystem;
import akka.actor.ExtendedActorSystem;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ClientUtils {

  private final static Logger LOG = LoggerFactory.getLogger(ClientUtils.class);

  static final String CONF_KEY_SECRET = "spark.client.authentication.secret";
  static final String CONF_KEY_IN_PROCESS = "spark.client.do_not_use_this.run_driver_in_process";
  static final String CONF_KEY_SERIALIZER = "spark-remote.akka.serializer";

  /**
   * Create a new ActorSystem based on the given configuration.
   *
   * The akka configs are the same used to configure Akka in Spark.
   *
   * @param conf Configuration.
   * @return 2-tuple (actor system, akka root url)
   */
  static ActorSystemInfo createActorSystem(Map<String, String> conf) throws IOException {
    int akkaThreads = toInt(conf.get("spark.akka.threads"), 4);
    int akkaBatchSize = toInt(conf.get("spark.akka.batchSize"), 15);
    int akkaTimeout = toInt(conf.get("spark.akka.timeout"), 100);
    int akkaFrameSize = toInt(conf.get("spark.akka.frameSize"), 10) * 1024 * 1024;
    String lifecycleEvents = toBoolean(conf.get("spark.akka.logLifecycleEvents")) ? "on" : "off";
    String logAkkaConfig = toBoolean(conf.get("spark.akka.logAkkaConfig")) ? "on" : "off";

    int akkaHeartBeatPauses = toInt(conf.get("spark.akka.heartbeat.pauses"), 600);
    double akkaFailureDetector =
      toDouble(conf.get("spark.akka.failure-detector.threshold"), 300.0);
    int akkaHeartBeatInterval = toInt(conf.get("spark.akka.heartbeat.interval"), 1000);

     // Disabled due to chill-akka depending on kryo 2.21, which is incompatible with 2.22
     // due to packaging changes (relocated org.objenesis classes).
     // String akkaSerializer = Optional.fromNullable(conf.get(CONF_KEY_SERIALIZER)).or("java");
     String akkaSerializer = "java";

    String host = findLocalIpAddress();
    String secret = conf.get(CONF_KEY_SECRET);
    Preconditions.checkArgument(secret != null, "%s not set.", CONF_KEY_SECRET);

    Map<String, String> sparkConf = Maps.newHashMap();
    for (Map.Entry<String, String> e : sparkConf.entrySet()) {
      if (e.getKey().startsWith("akka.")) {
        sparkConf.put(e.getKey(), e.getValue());
      }
    }

    Config fallback = ConfigFactory.parseString(""
        + "akka.daemonic = on\n"
        + "akka.loggers = [ \"akka.event.slf4j.Slf4jLogger\" ]\n"
        + "akka.stdout-loglevel = \"ERROR\"\n"
        + "akka.jvm-exit-on-fatal-error = off\n"
        + "akka.actor.default-dispatcher.throughput = " + akkaBatchSize + "\n"
        + "akka.actor.serializers.java = \"akka.serialization.JavaSerializer\"\n"
        /* Disabled due to chill-akka depending on kryo 2.21, which is incompatible with 2.22
           due to packaging changes (relocated org.objenesis classes).
        + "akka.actor.serializers.kryo = \"com.twitter.chill.akka.AkkaSerializer\"\n"
        */
        + String.format(
              "akka.actor.serialization-bindings = { \"java.io.Serializable\" = \"%s\" }\n",
              akkaSerializer)
        + "akka.log-config-on-start = " + logAkkaConfig + "\n"
        + "akka.log-dead-letters = " + lifecycleEvents + "\n"
        + "akka.log-dead-letters-during-shutdown = " + lifecycleEvents + "\n"
        + "akka.actor.provider = \"akka.remote.RemoteActorRefProvider\"\n"
        + "akka.remote.log-remote-lifecycle-events = " + lifecycleEvents + "\n"
        + String.format("akka.remote.netty.tcp.connection-timeout = %d s\n", akkaTimeout)
        + "akka.remote.netty.tcp.execution-pool-size = " + akkaThreads + "\n"
        + "akka.remote.netty.tcp.hostname = \"" + host + "\"\n"
        + String.format("akka.remote.netty.tcp.maximum-frame-size = %d B\n", akkaFrameSize)
        + "akka.remote.netty.tcp.port = 0\n"
        + "akka.remote.netty.tcp.tcp-nodelay = on\n"
        + "akka.remote.netty.tcp.transport-class = \"akka.remote.transport.netty.NettyTransport\"\n"
        + "akka.remote.require-cookie = on\n"
        + "akka.remote.secure-cookie = \"" + secret + "\"\n"
        + String.format(
              "akka.remote.transport-failure-detector.acceptable-heartbeat-pause = %d s\n",
              akkaHeartBeatPauses)
        + String.format(
              "akka.remote.transport-failure-detector.heartbeat-interval = %d s\n",
              akkaHeartBeatInterval)
        + "akka.remote.transport-failure-detector.threshold = " +
            String.valueOf(akkaFailureDetector) + "\n");

    String name = randomName();
    Config akkaConf = ConfigFactory.parseMap(sparkConf).withFallback(fallback);
    ActorSystem actorSystem = ActorSystem.create(name, akkaConf);
    ExtendedActorSystem extActorSystem = (ExtendedActorSystem) actorSystem;
    int boundPort =
      ((Integer)extActorSystem.provider().getDefaultAddress().port().get()).intValue();
    return new ActorSystemInfo(actorSystem,
        String.format("akka.tcp://%s@%s:%d/user", name, host, boundPort));
  }

  static String randomName() {
    return  UUID.randomUUID().toString();
  }

  private static boolean toBoolean(String value) {
    return Boolean.parseBoolean(Optional.fromNullable(value).or("false"));
  }

  private static double toDouble(String value, double defaultValue) {
    return Double.parseDouble(Optional.fromNullable(value).or(String.valueOf(defaultValue)));
  }

  private static int toInt(String value, int defaultValue) {
    return Integer.parseInt(Optional.fromNullable(value).or(String.valueOf(defaultValue)));
  }

  // Copied from Utils.scala.
  private static String findLocalIpAddress() throws IOException {
    String ip = System.getenv("SPARK_LOCAL_IP");
    if (ip == null) {
      InetAddress address = InetAddress.getLocalHost();
      if (address.isLoopbackAddress()) {
        // Address resolves to something like 127.0.1.1, which happens on Debian; try to find
        // a better address using the local network interfaces
        Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
        while (ifaces.hasMoreElements()) {
          NetworkInterface ni = ifaces.nextElement();
          Enumeration<InetAddress> addrs = ni.getInetAddresses();
          while (addrs.hasMoreElements()) {
            InetAddress addr = addrs.nextElement();
            if (!addr.isLinkLocalAddress() &&
                !addr.isLoopbackAddress() &&
                addr instanceof Inet4Address) {
              // We've found an address that looks reasonable!
              LOG.warn("Your hostname, {}, resolves to a loopback address; using {} " +
                " instead (on interface {})",
                address.getHostName(),
                addr.getHostAddress(),
                ni.getName());
              LOG.warn("Set SPARK_LOCAL_IP if you need to bind to another address");
              return addr.getHostAddress();
            }
          }
        }
        LOG.warn("Your hostname, {}, resolves to, but we couldn't find any external IP address!",
          address.getHostName());
        LOG.warn("Set SPARK_LOCAL_IP if you need to bind to another address");
      }
      return address.getHostAddress();
    }
    return ip;
  }

  static class ActorSystemInfo {
    final ActorSystem system;
    final String url;

    private ActorSystemInfo(ActorSystem system, String url) {
      this.system = system;
      this.url = url;
    }

  }

}
