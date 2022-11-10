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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Copy auxiliary jars for the tarball. */
class AsyncTaskCopyAuxJars implements Callable<Void> {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncTaskCopyAuxJars.class.getName());

  private static final String[] DEFAULT_AUX_CLASSES =
      new String[] {"org.apache.hive.hcatalog.data.JsonSerDe", "org.apache.hadoop.hive.druid.DruidStorageHandler",
          "org.apache.hive.storage.jdbc.JdbcStorageHandler", "org.apache.commons.dbcp.BasicDataSourceFactory",
          "org.apache.commons.pool.impl.GenericObjectPool", "org.apache.hadoop.hive.kafka.KafkaStorageHandler",
          "org.apache.hadoop.hive.kudu.KuduStorageHandler"};
  private static final String HBASE_SERDE_CLASS = "org.apache.hadoop.hive.hbase.HBaseSerDe";

  private final LlapServiceCommandLine cl;
  private final HiveConf conf;
  private final FileSystem rawFs;
  private final Path libDir;

  AsyncTaskCopyAuxJars(LlapServiceCommandLine cl, HiveConf conf, FileSystem rawFs, Path libDir) {
    this.cl = cl;
    this.conf = conf;
    this.rawFs = rawFs;
    this.libDir = libDir;
  }

  @Override
  public Void call() throws Exception {
    localizeJarForClass(Arrays.asList(DEFAULT_AUX_CLASSES), false);
    localizeJarForClass(conf.getStringCollection("io.compression.codecs"), false);
    localizeJarForClass(getDbSpecificJdbcJars(), false);

    Set<String> auxJars = new HashSet<>();
    // There are many ways to have AUX jars in Hive... sigh
    if (cl.getIsHiveAux()) {
      // Note: we don't add ADDED jars, RELOADABLE jars, etc. That is by design; there are too many ways
      // to add jars in Hive, some of which are session/etc. specific. Env + conf + arg should be enough.
      addAuxJarsToSet(auxJars, conf.getAuxJars(), ",");
      addAuxJarsToSet(auxJars, System.getenv("HIVE_AUX_JARS_PATH"), ":");
      LOG.info("Adding the following aux jars from the environment and configs: " + auxJars);
    }

    if (cl.getIsHBase()) {
      try {
        localizeJarForClass(Arrays.asList(HBASE_SERDE_CLASS), true);
        Job fakeJob = Job.getInstance(new JobConf()); // HBase API is convoluted.
        TableMapReduceUtil.addDependencyJars(fakeJob);
        Collection<String> hbaseJars = fakeJob.getConfiguration().getStringCollection("tmpjars");
        for (String jarPath : hbaseJars) {
          if (!jarPath.isEmpty()) {
            rawFs.copyFromLocalFile(new Path(jarPath), libDir);
          }
        }
        addAuxJarsToSet(auxJars, cl.getHBaseJars(), ",");
      } catch (Throwable t) {
        String err = "Failed to add HBase jars. Use --auxhbase=false to avoid localizing them";
        LOG.error(err);
        System.err.println(err);
        throw new RuntimeException(t);
      }
    }

    addAuxJarsToSet(auxJars, cl.getAuxJars(), ",");
    for (String jarPath : auxJars) {
      rawFs.copyFromLocalFile(new Path(jarPath), libDir);
    }
    return null;
  }

  private void localizeJarForClass(Collection<String> classNames, boolean doThrow) throws IOException {
    if (CollectionUtils.isEmpty(classNames)) {
      return;
    }

    for (String className : classNames) {
      String jarPath = null;
      boolean hasException = false;
      try {
        Class<?> clazz = Class.forName(className);
        jarPath = Utilities.jarFinderGetJar(clazz);
      } catch (Throwable t) {
        if (doThrow) {
          throw (t instanceof IOException) ? (IOException)t : new IOException(t);
        }
        hasException = true;
        String err = "Cannot find a jar for [" + className + "] due to an exception (" +
            t.getMessage() + "); not packaging the jar";
        LOG.error(err);
        System.err.println(err);
      }

      if (jarPath != null) {
        rawFs.copyFromLocalFile(new Path(jarPath), libDir);
      } else if (!hasException) {
        String err = "Cannot find a jar for [" + className + "]; not packaging the jar";
        if (doThrow) {
          throw new IOException(err);
        }
        LOG.error(err);
        System.err.println(err);
      }
    }
  }

  private List<String> getDbSpecificJdbcJars() {
    List<String> jdbcJars = new ArrayList<String>();
    addJarForClassToListIfExists("com.mysql.jdbc.Driver", jdbcJars); // add mysql jdbc driver
    addJarForClassToListIfExists("org.postgresql.Driver", jdbcJars); // add postgresql jdbc driver
    addJarForClassToListIfExists("oracle.jdbc.OracleDriver", jdbcJars); // add oracle jdbc driver
    addJarForClassToListIfExists("com.microsoft.sqlserver.jdbc.SQLServerDriver", jdbcJars); // add mssql jdbc driver
    return jdbcJars;
  }

  private void addJarForClassToListIfExists(String cls, List<String> jarList) {
    try {
      Class.forName(cls);
      jarList.add(cls);
    } catch (Exception e) {
    }
  }

  private void addAuxJarsToSet(Set<String> auxJarSet, String auxJars, String delimiter) {
    if (StringUtils.isNotEmpty(auxJars)) {
      // TODO: transitive dependencies warning?
      String[] jarPaths = auxJars.split(delimiter);
      for (String jarPath : jarPaths) {
        if (!jarPath.isEmpty()) {
          auxJarSet.add(jarPath);
        }
      }
    }
  }
}
