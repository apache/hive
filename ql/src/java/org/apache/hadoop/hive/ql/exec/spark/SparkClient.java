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
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
    int rc = 1;
//    System.out.println("classpath=\n"+System.getProperty("java.class.path") + "\n");

    HiveConf hiveConf = (HiveConf)driverContext.getCtx().getConf();
    refreshLocalResources(sparkWork, hiveConf);

    MapWork mapWork = sparkWork.getMapWork();
    ReduceWork redWork = sparkWork.getReduceWork();

    // TODO: need to combine spark conf and hive conf
    JobConf jobConf = new JobConf(hiveConf);

    Context ctx = driverContext.getCtx();
    Path emptyScratchDir;
    try {
      if (ctx == null) {
        ctx = new Context(jobConf);
      }

      emptyScratchDir = ctx.getMRTmpPath();
      FileSystem fs = emptyScratchDir.getFileSystem(jobConf);
      fs.mkdirs(emptyScratchDir);
    } catch (IOException e) {
      e.printStackTrace();
      System.err.println("Error launching map-reduce job" + "\n"
        + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return 5;
    }

    List<Path> inputPaths;
    try {
      inputPaths = Utilities.getInputPaths(jobConf, mapWork, emptyScratchDir, ctx, false);
    } catch (Exception e2) {
      e2.printStackTrace();
      return -1;
    }
    Utilities.setInputPaths(jobConf, inputPaths);
    Utilities.setMapWork(jobConf, mapWork, emptyScratchDir, true);
    if (redWork != null)
      Utilities.setReduceWork(jobConf, redWork, emptyScratchDir, true);

    try {
      Utilities.createTmpDirs(jobConf, mapWork);
      Utilities.createTmpDirs(jobConf, redWork);
    } catch (IOException e1) {
      e1.printStackTrace();
    }

    try {
      Path planPath = new Path(jobConf.getWorkingDirectory(), "plan.xml");
      System.out.println("Serializing plan to path: " + planPath);
      OutputStream os2 = planPath.getFileSystem(jobConf).create(planPath);
      Utilities.serializePlan(mapWork, os2, jobConf);
    } catch (IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
      return 1;
    }
    
    JavaPairRDD rdd = createRDD(sc, jobConf, mapWork);
    byte[] confBytes = KryoSerializer.serializeJobConf(jobConf);
    HiveMapFunction mf = new HiveMapFunction(confBytes);
    JavaPairRDD rdd2 = rdd.mapPartitionsToPair(mf);
    if (redWork == null) {
      rdd2.foreach(HiveVoidFunction.getInstance());
      if (mapWork.getAliasToWork() != null) {
        for (Operator<? extends OperatorDesc> op : mapWork.getAliasToWork().values()) {
          try {
            op.jobClose(jobConf, true);
          } catch (HiveException e) {
            System.out.println("Calling jobClose() failed: " + e);
            e.printStackTrace();
          }
        }
      }
    } else {
      JavaPairRDD rdd3 = rdd2.partitionBy(new HashPartitioner(1/*redWork.getNumReduceTasks()*/)); // Two partitions.
      HiveReduceFunction rf = new HiveReduceFunction(confBytes);
      JavaPairRDD rdd4 = rdd3.mapPartitionsToPair(rf);
      rdd4.foreach(HiveVoidFunction.getInstance());
      try {
        redWork.getReducer().jobClose(jobConf, true);
      } catch (HiveException e) {
        System.out.println("Calling jobClose() failed: " + e);
        e.printStackTrace();
      }
    }
    
    return 0;
  }
  
  private JavaPairRDD createRDD(JavaSparkContext sc, JobConf jobConf, MapWork mapWork) {
    Class ifClass = HiveInputFormat.class;

    // The mapper class is expected by the HiveInputFormat.
    jobConf.set("mapred.mapper.class", ExecMapper.class.getName());
    return sc.hadoopRDD(jobConf, ifClass, WritableComparable.class, Writable.class);
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
      addResources(auxJars, localJars);
    }

    // add added jars
    String addedJars = Utilities.getResourceFiles(conf, SessionState.ResourceType.JAR);
    if (StringUtils.isNotEmpty(addedJars) && StringUtils.isNotBlank(addedJars)) {
      HiveConf.setVar(conf, HiveConf.ConfVars.HIVEADDEDJARS, addedJars);
      addResources(addedJars, localJars);
    }
    // add plugin module jars on demand
    final String MR_JAR_PROPERTY = "tmpjars";
    // jobConf will hold all the configuration for hadoop, tez, and hive
    JobConf jobConf = new JobConf(conf);
    jobConf.setStrings(MR_JAR_PROPERTY, new String[0]);
    // TODO update after SparkCompiler finished.
    //    for (BaseWork work : sparkWork.getAllWork()) {
    //      work.configureJobConf(jobConf);
    //    }
    sparkWork.getMapWork().configureJobConf(jobConf);
    ReduceWork redWork = sparkWork.getReduceWork();
    if (redWork != null) {
      redWork.configureJobConf(jobConf);
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
      addResources(addedFiles, localFiles);
    }

    // add added archives
    String addedArchives = Utilities.getResourceFiles(conf, SessionState.ResourceType.ARCHIVE);
    if (StringUtils.isNotEmpty(addedArchives) && StringUtils.isNotBlank(addedArchives)) {
      HiveConf.setVar(conf, HiveConf.ConfVars.HIVEADDEDARCHIVES, addedArchives);
      addResources(addedArchives, localFiles);
    }
  }

  private void addResources(String addedFiles, List<String> localCache) {
    for (String addedFile : addedFiles.split(",")) {
      if (StringUtils.isNotEmpty(addedFile) && StringUtils.isNotBlank(addedFile)
        && !localCache.contains(addedFile)) {
        localCache.add(addedFile);
        sc.addFile(addedFile);
      }
    }
  }
}
