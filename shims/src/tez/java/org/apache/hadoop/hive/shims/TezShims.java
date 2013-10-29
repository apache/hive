/**
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
package org.apache.hadoop.hive.shims;

import org.apache.hadoop.hive.shims.Hadoop23Shims;

import java.io.IOException;
import java.lang.Integer;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.tez.mapreduce.*;
import org.apache.tez.test.MiniTezCluster;

/**
 * Implemention of shims against Tez
 */
public class TezShims extends Hadoop23Shims {

  private MiniDFSCluster dfsCluster;
  /**
   * Returns a shim to wrap MiniMrCluster
   */
  @Override
  public MiniMrShim getMiniMrCluster(Configuration conf, int numberOfTaskTrackers, 
                                     String nameNode, int numDir) throws IOException {
    return new MiniMrShim(conf, numberOfTaskTrackers, nameNode, numDir);
  }

  /**
   * Shim for MiniMrCluster
   */
  public class MiniMrShim extends Hadoop23Shims.MiniMrShim {

    private final MiniTezCluster mr;
    private final Configuration conf;

    public MiniMrShim(Configuration conf, int numberOfTaskTrackers, 
                      String nameNode, int numDir) throws IOException {

      mr = new MiniTezCluster("hive", numberOfTaskTrackers);
      conf.set("fs.defaultFS", nameNode);
      conf.set(MRJobConfig.MR_AM_STAGING_DIR, "/apps_staging_dir");
      mr.init(conf);
      mr.start();
      this.conf = mr.getConfig();
    }

    @Override
    public int getJobTrackerPort() throws UnsupportedOperationException {
      String address = conf.get("yarn.resourcemanager.address");
      address = StringUtils.substringAfterLast(address, ":");

      if (StringUtils.isBlank(address)) {
        throw new IllegalArgumentException("Invalid YARN resource manager port.");
      }
      
      return Integer.parseInt(address);
    }

    @Override
    public void shutdown() throws IOException {
      mr.stop();
    }
    
    @Override
    public void setupConfiguration(Configuration conf) {
      Configuration config = mr.getConfig();
      for (Map.Entry<String, String> pair: config) {
        conf.set(pair.getKey(), pair.getValue());
      }

      Path jarPath = new Path("hdfs:///user/hive");
      Path hdfsPath = new Path("hdfs:///user/");
      try {
        FileSystem fs = dfsCluster.getFileSystem();
        jarPath = fs.makeQualified(jarPath);
        conf.set("hive.jar.directory", jarPath.toString());
        fs.mkdirs(jarPath);
        hdfsPath = fs.makeQualified(hdfsPath);
        conf.set("hive.user.install.directory", hdfsPath.toString());
        fs.mkdirs(hdfsPath);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public Hadoop23Shims.MiniDFSShim getMiniDfs(Configuration conf,
      int numDataNodes,
      boolean format,
      String[] racks) throws IOException {
    this.dfsCluster = new MiniDFSCluster(conf, numDataNodes, format, racks);
    return new MiniDFSShim(dfsCluster);
  }

  @Override
  public boolean isLocalMode(Configuration conf) {
    return true;
  }
}
