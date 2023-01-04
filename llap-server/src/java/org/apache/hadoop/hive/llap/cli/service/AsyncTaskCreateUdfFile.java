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

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.daemon.impl.StaticPermanentFunctionChecker;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.util.ResourceDownloader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Create the list of allowed UDFs for the tarball. */
class AsyncTaskCreateUdfFile implements Callable<Void> {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncTaskCreateUdfFile.class.getName());

  private final HiveConf conf;
  private final FileSystem fs;
  private final FileSystem rawFs;
  private final Path udfDir;
  private final Path confDir;

  AsyncTaskCreateUdfFile(HiveConf conf, FileSystem fs, FileSystem rawFs, Path udfDir, Path confDir) {
    this.conf = conf;
    this.fs = fs;
    this.rawFs = rawFs;
    this.udfDir = udfDir;
    this.confDir = confDir;
  }

  @Override
  public Void call() throws Exception {
    // UDFs
    final Set<String> allowedUdfs;

    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_ALLOW_PERMANENT_FNS)) {
      synchronized (fs) {
        allowedUdfs = downloadPermanentFunctions();
      }
    } else {
      allowedUdfs = Collections.emptySet();
    }

    OutputStream os = rawFs.create(new Path(confDir, StaticPermanentFunctionChecker.PERMANENT_FUNCTIONS_LIST));
    OutputStreamWriter osw = new OutputStreamWriter(os, Charset.defaultCharset());
    PrintWriter udfStream = new PrintWriter(osw);
    for (String udfClass : allowedUdfs) {
      udfStream.println(udfClass);
    }

    udfStream.close();
    return null;
  }

  private Set<String> downloadPermanentFunctions() throws HiveException, URISyntaxException, IOException {
    Map<String, String> udfs = new HashMap<String, String>();
    HiveConf hiveConf = new HiveConf(conf);
    // disable expensive operations on the metastore
    hiveConf.setBoolean(MetastoreConf.ConfVars.METRICS_ENABLED.getVarname(), false);
    // performance problem: ObjectStore does its own new HiveConf()
    Hive hive = Hive.getWithFastCheck(hiveConf, false);
    ResourceDownloader resourceDownloader = new ResourceDownloader(conf, udfDir.toUri().normalize().getPath());
    List<Function> fns = hive.getAllFunctions();
    Set<URI> srcUris = new HashSet<>();
    for (Function fn : fns) {
      String fqfn = fn.getDbName() + "." + fn.getFunctionName();
      if (udfs.containsKey(fn.getClassName())) {
        LOG.warn("Duplicate function names found for " + fn.getClassName() + " with " + fqfn + " and " +
            udfs.get(fn.getClassName()));
      }
      udfs.put(fn.getClassName(), fqfn);
      List<ResourceUri> resources = fn.getResourceUris();
      if (resources == null || resources.isEmpty()) {
        LOG.warn("Missing resources for " + fqfn);
        continue;
      }
      for (ResourceUri resource : resources) {
        srcUris.add(ResourceDownloader.createURI(resource.getUri()));
      }
    }
    for (URI srcUri : srcUris) {
      List<URI> localUris = resourceDownloader.downloadExternal(srcUri, null, false);
      for(URI dst : localUris) {
        LOG.warn("Downloaded " + dst + " from " + srcUri);
      }
    }
    return udfs.keySet();
  }
}
