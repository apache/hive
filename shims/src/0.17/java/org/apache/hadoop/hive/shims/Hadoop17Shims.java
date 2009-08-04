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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import java.io.IOException;

/**
 * Implemention of shims against Hadoop 0.17.0
 */
public class Hadoop17Shims implements HadoopShims {
  public boolean usesJobShell() {
    return true;
  }

  public boolean fileSystemDeleteOnExit(FileSystem fs, Path path)
    throws IOException {
    return false;
  }

  public void inputFormatValidateInput(InputFormat fmt, JobConf conf)
    throws IOException {
    fmt.validateInput(conf);
  }

  /**
   * workaround for hadoop-17 - jobclient only looks at commandlineconfig
   */
  public void setTmpFiles(String prop, String files) {
    Configuration conf = JobClient.getCommandLineConfig();
    if (conf != null) {
      conf.set(prop, files);
    }
  }

  public HadoopShims.MiniDFSShim getMiniDfs(Configuration conf,
                                int numDataNodes,
                                boolean format,
                                String[] racks) throws IOException {
    return new MiniDFSShim(new MiniDFSCluster(conf, numDataNodes, format, racks));
  }

  public class MiniDFSShim implements HadoopShims.MiniDFSShim {
    private MiniDFSCluster cluster;
    public MiniDFSShim(MiniDFSCluster cluster) {
      this.cluster = cluster;
    }

    public FileSystem getFileSystem() throws IOException {
      return cluster.getFileSystem();
    }

    public void shutdown() {
      cluster.shutdown();
    }
  }
}
