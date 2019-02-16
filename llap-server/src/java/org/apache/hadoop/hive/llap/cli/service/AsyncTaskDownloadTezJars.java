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

import java.util.concurrent.Callable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.CompressionUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Download tez related jars for the tarball. */
class AsyncTaskDownloadTezJars implements Callable<Void> {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncTaskDownloadTezJars.class.getName());

  private final HiveConf conf;
  private final FileSystem fs;
  private final FileSystem rawFs;
  private final Path libDir;
  private final Path tezDir;

  AsyncTaskDownloadTezJars(HiveConf conf, FileSystem fs, FileSystem rawFs, Path libDir, Path tezDir) {
    this.conf = conf;
    this.fs = fs;
    this.rawFs = rawFs;
    this.libDir = libDir;
    this.tezDir = tezDir;
  }

  @Override
  public Void call() throws Exception {
    synchronized (fs) {
      String tezLibs = conf.get(TezConfiguration.TEZ_LIB_URIS);
      if (tezLibs == null) {
        LOG.warn("Missing tez.lib.uris in tez-site.xml");
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Copying tez libs from " + tezLibs);
      }
      rawFs.mkdirs(tezDir);
      fs.copyToLocalFile(new Path(tezLibs), new Path(libDir, "tez.tar.gz"));
      CompressionUtils.unTar(new Path(libDir, "tez.tar.gz").toString(), tezDir.toString(), true);
      rawFs.delete(new Path(libDir, "tez.tar.gz"), false);
    }
    return null;
  }
}
