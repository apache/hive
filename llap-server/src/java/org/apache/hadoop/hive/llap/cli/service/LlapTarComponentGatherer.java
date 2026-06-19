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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gathers all the jar files necessary to start llap.
 */
class LlapTarComponentGatherer {
  private static final Logger LOG = LoggerFactory.getLogger(LlapTarComponentGatherer.class.getName());

  //using Callable<Void> instead of Runnable to be able to throw Exception
  private final Map<String, Future<Void>> tasks = new HashMap<>();

  private final LlapServiceCommandLine cl;
  private final HiveConf conf;
  private final Properties directProperties;
  private final FileSystem fs;
  private final FileSystem rawFs;
  private final ExecutorService executor;
  private final Path libDir;
  private final Path tezDir;
  private final Path udfDir;
  private final Path confDir;

  LlapTarComponentGatherer(LlapServiceCommandLine cl, HiveConf conf, Properties directProperties, FileSystem fs,
      FileSystem rawFs, ExecutorService executor, Path tmpDir) {
    this.cl = cl;
    this.conf = conf;
    this.directProperties = directProperties;
    this.fs = fs;
    this.rawFs = rawFs;
    this.executor = executor;
    this.libDir = new Path(tmpDir, "lib");
    this.tezDir = new Path(libDir, "tez");
    this.udfDir = new Path(libDir, "udfs");
    this.confDir = new Path(tmpDir, "conf");
  }

  void createDirs() throws Exception {
    if (!rawFs.mkdirs(tezDir)) {
      LOG.warn("mkdirs for " + tezDir + " returned false");
    }
    if (!rawFs.mkdirs(udfDir)) {
      LOG.warn("mkdirs for " + udfDir + " returned false");
    }
    if (!rawFs.mkdirs(confDir)) {
      LOG.warn("mkdirs for " + confDir + " returned false");
    }
  }

  void submitTarComponentGatherTasks() {
    CompletionService<Void> asyncRunner = new ExecutorCompletionService<Void>(executor);

    tasks.put("downloadTezJars", asyncRunner.submit(new AsyncTaskDownloadTezJars(conf, fs, rawFs, libDir, tezDir)));
    tasks.put("copyLocalJars", asyncRunner.submit(new AsyncTaskCopyLocalJars(rawFs, libDir)));
    tasks.put("copyAuxJars", asyncRunner.submit(new AsyncTaskCopyAuxJars(cl, conf, rawFs, libDir)));
    tasks.put("createUdfFile", asyncRunner.submit(new AsyncTaskCreateUdfFile(conf, fs, rawFs, udfDir, confDir)));
    tasks.put("copyConfigs", asyncRunner.submit(new AsyncTaskCopyConfigs(cl, conf, directProperties, rawFs,
        confDir)));
  }

  void waitForFinish() throws Exception {
    for (Map.Entry<String, Future<Void>> task : tasks.entrySet()) {
      long t1 = System.nanoTime();
      task.getValue().get();
      long t2 = System.nanoTime();
      LOG.debug(task.getKey() + " waited for " + (t2 - t1) + " ns");
    }
  }
}
