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

package org.apache.hadoop.hive.common;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * ServerUtils (specific to HiveServer version 1)
 */
public class ServerUtils {

  public static final Logger LOG = LoggerFactory.getLogger(ServerUtils.class);

  public static void cleanUpScratchDir(HiveConf hiveConf) {
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_START_CLEANUP_SCRATCHDIR)) {
      String hiveScratchDir = hiveConf.get(HiveConf.ConfVars.SCRATCH_DIR.varname);
      try {
        Path jobScratchDir = new Path(hiveScratchDir);
        LOG.info("Cleaning scratchDir : " + hiveScratchDir);
        FileSystem fileSystem = jobScratchDir.getFileSystem(hiveConf);
        fileSystem.delete(jobScratchDir, true);
      }
      // Even if the cleanup throws some exception it will continue.
      catch (Throwable e) {
        LOG.warn("Unable to delete scratchDir : " + hiveScratchDir, e);
      }
    }
  }

  /**
   * Get the Inet address of the machine of the given host name.
   * @param hostname The name of the host
   * @return The network address of the the host
   * @throws UnknownHostException
   */
  public static InetAddress getHostAddress(String hostname) throws UnknownHostException {
    InetAddress serverIPAddress;
    if (hostname != null && !hostname.isEmpty()) {
      serverIPAddress = InetAddress.getByName(hostname);
    } else {
      serverIPAddress = InetAddress.getLocalHost();
    }
    return serverIPAddress;
  }

  /**
   * Get the IP address string (e.g. "192.168.1.50") for the given host name.
   * @param hostname The name of the host
   * @return The network address of the host and UNKNOWN if resolution fails
   */
  public static String getHostAddressString(String hostname) {
    try {
      return getHostAddress(hostname).getHostAddress();
    } catch (UnknownHostException e) {
      LOG.warn("Error trying to get host address for {}: {}", hostname, e.getMessage());
      return "UNKNOWN";
    }
  }

  /**
   * @return name of current host
   */
  public static String hostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.warn("Error fetching hostname! Falling back to UNKNOWN.{}", e.getMessage());
      return "UNKNOWN";
    }
  }

  /**
   * Method to get canonical-ized hostname of localhost
   * @return the canonical-ized hostname of localhost is returned. If not found, fallback to UNKNOWN.
   */
  public static String canonicalHostname() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      LOG.warn("Error fetching canonical hostname! Falling back to UNKNOWN.{}", e.getMessage());
      return "UNKNOWN";
    }
  }

  /**
   * Method to get canonical-ized hostname, given a hostname (possibly a CNAME).
   * This should allow for service-principals to use simplified CNAMEs.
   * @param hostname The hostname to be canonical-ized.
   * @return Given a CNAME, the canonical-ized hostname is returned. If not found, fallback to UNKNOWN.
   */
  public static String canonicalHostname(String hostname) {
    try {
      return InetAddress.getByName(hostname).getCanonicalHostName();
    } catch (UnknownHostException e) {
      LOG.warn("Error fetching canonical hostname! Falling back to UNKNOWN.{}", e.getMessage());
      return "UNKNOWN";
    }
  }

  public static int findFreePort() throws IOException {
    ServerSocket socket= new ServerSocket(0);
    int port = socket.getLocalPort();
    socket.close();
    return port;
  }
}
