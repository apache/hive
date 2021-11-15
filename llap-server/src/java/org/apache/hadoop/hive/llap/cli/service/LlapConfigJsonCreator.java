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

package org.apache.hadoop.hive.llap.cli.service;

import java.io.OutputStreamWriter;
import java.nio.charset.Charset;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * Creates the config json for llap start.
 */
class LlapConfigJsonCreator {
  // This is not a config that users set in hive-site. It's only use is to share information
  // between the java component of the service driver and the python component.
  private static final String CONFIG_CLUSTER_NAME = "private.hive.llap.servicedriver.cluster.name";

  private final HiveConf conf;
  private final FileSystem fs;
  private final Path tmpDir;

  private final long cache;
  private final long xmx;
  private final String javaHome;

  LlapConfigJsonCreator(HiveConf conf, FileSystem fs, Path tmpDir, long cache, long xmx, String javaHome) {
    this.conf = conf;
    this.fs = fs;
    this.tmpDir = tmpDir;
    this.cache = cache;
    this.xmx = xmx;
    this.javaHome = javaHome;
  }

  void createLlapConfigJson() throws Exception {
    JSONObject configs = createConfigJson();
    writeConfigJson(configs);
  }

  private JSONObject createConfigJson() throws JSONException {
    // extract configs for processing by the python fragments in YARN Service
    JSONObject configs = new JSONObject();

    configs.put("java.home", javaHome);

    configs.put(ConfVars.LLAP_DAEMON_YARN_CONTAINER_MB.varname,
        conf.getLongVar(ConfVars.LLAP_DAEMON_YARN_CONTAINER_MB));

    configs.put(ConfVars.LLAP_IO_MEMORY_MAX_SIZE.varname, conf.getSizeVar(ConfVars.LLAP_IO_MEMORY_MAX_SIZE));

    configs.put(ConfVars.LLAP_ALLOCATOR_DIRECT.varname, conf.getBoolVar(HiveConf.ConfVars.LLAP_ALLOCATOR_DIRECT));

    configs.put(ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB.varname,
        conf.getIntVar(ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB));

    configs.put(ConfVars.LLAP_DAEMON_VCPUS_PER_INSTANCE.varname,
        conf.getIntVar(ConfVars.LLAP_DAEMON_VCPUS_PER_INSTANCE));

    configs.put(ConfVars.LLAP_DAEMON_NUM_EXECUTORS.varname, conf.getIntVar(ConfVars.LLAP_DAEMON_NUM_EXECUTORS));

    // Let YARN pick the queue name, if it isn't provided in hive-site, or via the command-line
    if (conf.getVar(ConfVars.LLAP_DAEMON_QUEUE_NAME) != null) {
      configs.put(ConfVars.LLAP_DAEMON_QUEUE_NAME.varname, conf.getVar(ConfVars.LLAP_DAEMON_QUEUE_NAME));
    }

    // Propagate the cluster name to the script.
    String clusterHosts = conf.getVar(ConfVars.LLAP_DAEMON_SERVICE_HOSTS);
    if (!StringUtils.isEmpty(clusterHosts) && clusterHosts.startsWith("@") && clusterHosts.length() > 1) {
      configs.put(CONFIG_CLUSTER_NAME, clusterHosts.substring(1));
    }

    configs.put(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, -1));

    configs.put(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, -1));

    long maxDirect = (xmx > 0 && cache > 0 && xmx < cache * 1.25) ? (long) (cache * 1.25) : -1;
    configs.put("max_direct_memory", Long.toString(maxDirect));

    configs.put(ConfVars.LLAP_HDFS_PACKAGE_DIR.varname,
            conf.getVar(ConfVars.LLAP_HDFS_PACKAGE_DIR));

    return configs;
  }

  private void writeConfigJson(JSONObject configs) throws Exception {
    try (FSDataOutputStream fsdos = fs.create(new Path(tmpDir, "config.json"));
         OutputStreamWriter w = new OutputStreamWriter(fsdos, Charset.defaultCharset())) {
      configs.write(w);
    }
  }
}
