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

package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.*;

public class SparkClient implements Serializable {
  private static final long serialVersionUID = 1L;
  protected static transient final Log LOG = LogFactory.getLog(SparkClient.class);

  private static final String SPARK_DEFAULT_CONF_FILE = "spark-defaults.conf";
  private static final String SPARK_DEFAULT_MASTER = "local";
  private static final String SAPRK_DEFAULT_APP_NAME = "Hive on Spark";

  private static SparkClient client;

  public static synchronized SparkClient getInstance(Configuration hiveConf) {
    if (client == null) {
      client = new SparkClient(hiveConf);
    }
    return client;
  }

  private JavaSparkContext sc;

  private List<String> localJars = new ArrayList<String>();

  private List<String> localFiles = new ArrayList<String>();

  private SparkClient(Configuration hiveConf) {
    sc = new JavaSparkContext(initiateSparkConf(hiveConf));
  }

  private SparkConf initiateSparkConf(Configuration hiveConf) {
    SparkConf sparkConf = new SparkConf();

    // set default spark configurations.
    sparkConf.set("spark.master", SPARK_DEFAULT_MASTER);
    sparkConf.set("spark.app.name", SAPRK_DEFAULT_APP_NAME);
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.default.parallelism",  "1");
    // load properties from spark-defaults.conf.
    InputStream inputStream = null;
    try {
      inputStream = this.getClass().getClassLoader().getResourceAsStream(SPARK_DEFAULT_CONF_FILE);
      if (inputStream != null) {
        LOG.info("loading spark properties from:" + SPARK_DEFAULT_CONF_FILE);
        Properties properties = new Properties();
        properties.load(inputStream);
        for (String propertyName : properties.stringPropertyNames()) {
          if (propertyName.startsWith("spark")) {
            String value = properties.getProperty(propertyName);
            sparkConf.set(propertyName, properties.getProperty(propertyName));
            LOG.info(String.format("load spark configuration from %s (%s -> %s).",
                SPARK_DEFAULT_CONF_FILE, propertyName, value));
          }
        }
      }
    } catch (IOException e) {
      LOG.info("Failed to open spark configuration file:" + SPARK_DEFAULT_CONF_FILE, e);
    } finally {
      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (IOException e) {
          LOG.debug("Failed to close inputstream.", e);
        }
      }
    }

    // load properties from hive configurations.
    Iterator<Map.Entry<String, String>> iterator = hiveConf.iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, String> entry = iterator.next();
      String propertyName = entry.getKey();
      if (propertyName.startsWith("spark")) {
        String value = entry.getValue();
        sparkConf.set(propertyName, value);
        LOG.info(String.format("load spark configuration from hive configuration (%s -> %s).",
            propertyName, value));
      }
    }

    return sparkConf;
  }

  public int execute(DriverContext driverContext, SparkWork sparkWork) {
    Context ctx = driverContext.getCtx();
    HiveConf hiveConf = (HiveConf)ctx.getConf();
    refreshLocalResources(sparkWork, hiveConf);
    JobConf jobConf = new JobConf(hiveConf);

    // Create temporary scratch dir
    Path emptyScratchDir;
    try {
      emptyScratchDir = ctx.getMRTmpPath();
      FileSystem fs = emptyScratchDir.getFileSystem(jobConf);
      fs.mkdirs(emptyScratchDir);
    } catch (IOException e) {
      LOG.error("Error launching map-reduce job", e);
      return 5;
    }

    // Generate Spark plan
    SparkPlanGenerator gen = new SparkPlanGenerator(sc, ctx, jobConf, emptyScratchDir);
    SparkPlan plan;
    try {
      plan = gen.generate(sparkWork);
    } catch (Exception e) {
      LOG.error("Error generating Spark Plan", e);
      return 2;
    }

    // Execute generated plan.
    // TODO: we should catch any exception and return more meaningful error code.
    plan.execute();
    return 0;
  }

  private void refreshLocalResources(SparkWork sparkWork, HiveConf conf) {
    // add hive-exec jar
    String hiveJar = conf.getJar();
    if (!localJars.contains(hiveJar)) {
      localJars.add(hiveJar);
      sc.addJar(hiveJar);
    }
    // add aux jars
    String auxJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS);
    if (StringUtils.isNotEmpty(auxJars) && StringUtils.isNotBlank(auxJars)) {
      addJars(auxJars);
    }

    // add added jars
    String addedJars = Utilities.getResourceFiles(conf, SessionState.ResourceType.JAR);
    if (StringUtils.isNotEmpty(addedJars) && StringUtils.isNotBlank(addedJars)) {
      HiveConf.setVar(conf, HiveConf.ConfVars.HIVEADDEDJARS, addedJars);
      addJars(addedJars);
    }

    // add plugin module jars on demand
    final String MR_JAR_PROPERTY = "tmpjars";
    // jobConf will hold all the configuration for hadoop, tez, and hive
    JobConf jobConf = new JobConf(conf);
    jobConf.setStrings(MR_JAR_PROPERTY, new String[0]);

    for (BaseWork work : sparkWork.getAllWork()) {
      work.configureJobConf(jobConf);
    }

    String[] newTmpJars = jobConf.getStrings(MR_JAR_PROPERTY);
    if (newTmpJars != null && newTmpJars.length > 0) {
      for (String tmpJar : newTmpJars) {
        if (StringUtils.isNotEmpty(tmpJar) && StringUtils.isNotBlank(tmpJar)
            && !localJars.contains(tmpJar)) {
          localJars.add(tmpJar);
          sc.addJar(tmpJar);
        }
      }
    }

    //add added files
    String addedFiles = Utilities.getResourceFiles(conf, SessionState.ResourceType.FILE);
    if (StringUtils.isNotEmpty(addedFiles) && StringUtils.isNotBlank(addedFiles)) {
      HiveConf.setVar(conf, HiveConf.ConfVars.HIVEADDEDFILES, addedFiles);
      addResources(addedFiles);
    }

    // add added archives
    String addedArchives = Utilities.getResourceFiles(conf, SessionState.ResourceType.ARCHIVE);
    if (StringUtils.isNotEmpty(addedArchives) && StringUtils.isNotBlank(addedArchives)) {
      HiveConf.setVar(conf, HiveConf.ConfVars.HIVEADDEDARCHIVES, addedArchives);
      addResources(addedArchives);
    }
  }

  private void addResources(String addedFiles) {
    for (String addedFile : addedFiles.split(",")) {
      if (StringUtils.isNotEmpty(addedFile) && StringUtils.isNotBlank(addedFile)
          && !localFiles.contains(addedFile)) {
        localFiles.add(addedFile);
        sc.addFile(addedFile);
      }
    }
  }

  private void addJars(String addedJars) {
    for (String addedJar : addedJars.split(",")) {
      if (StringUtils.isNotEmpty(addedJar) && StringUtils.isNotBlank(addedJar)
        && !localJars.contains(addedJar)) {
        localJars.add(addedJar);
        sc.addJar(addedJar);
      }
    }
  }
}
