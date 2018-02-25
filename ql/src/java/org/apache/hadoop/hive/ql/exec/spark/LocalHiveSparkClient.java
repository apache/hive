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

package org.apache.hadoop.hive.ql.exec.spark;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.DagUtils;
import org.apache.hive.spark.client.SparkClientUtilities;
import org.apache.spark.util.CallSite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfUtil;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobRef;
import org.apache.hadoop.hive.ql.exec.spark.status.impl.JobMetricsListener;
import org.apache.hadoop.hive.ql.exec.spark.status.impl.LocalSparkJobRef;
import org.apache.hadoop.hive.ql.exec.spark.status.impl.LocalSparkJobStatus;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.spark.counter.SparkCounters;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;

/**
 * LocalSparkClient submit Spark job in local driver, it's responsible for build spark client
 * environment and execute spark work.
 */
public class LocalHiveSparkClient implements HiveSparkClient {
  private static final long serialVersionUID = 1L;

  private static final String MR_JAR_PROPERTY = "tmpjars";
  protected static final transient Logger LOG = LoggerFactory
      .getLogger(LocalHiveSparkClient.class);

  private static final Splitter CSV_SPLITTER = Splitter.on(",").omitEmptyStrings();

  private static LocalHiveSparkClient client;

  public static synchronized LocalHiveSparkClient getInstance(
      SparkConf sparkConf, HiveConf hiveConf) throws FileNotFoundException, MalformedURLException {
    if (client == null) {
      client = new LocalHiveSparkClient(sparkConf, hiveConf);
    }
    return client;
  }

  private final JavaSparkContext sc;

  private final List<String> localJars = new ArrayList<String>();

  private final List<String> localFiles = new ArrayList<String>();

  private final JobMetricsListener jobMetricsListener;

  private LocalHiveSparkClient(SparkConf sparkConf, HiveConf hiveConf)
      throws FileNotFoundException, MalformedURLException {
    String regJar = null;
    // the registrator jar should already be in CP when not in test mode
    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_IN_TEST)) {
      String kryoReg = sparkConf.get("spark.kryo.registrator", "");
      if (SparkClientUtilities.HIVE_KRYO_REG_NAME.equals(kryoReg)) {
        regJar = SparkClientUtilities.findKryoRegistratorJar(hiveConf);
        SparkClientUtilities.addJarToContextLoader(new File(regJar));
      }
    }
    sc = new JavaSparkContext(sparkConf);
    if (regJar != null) {
      sc.addJar(regJar);
    }
    jobMetricsListener = new JobMetricsListener();
    sc.sc().listenerBus().addListener(jobMetricsListener);
  }

  @Override
  public SparkConf getSparkConf() {
    return sc.sc().conf();
  }

  @Override
  public int getExecutorCount() {
    return sc.sc().getExecutorMemoryStatus().size();
  }

  @Override
  public int getDefaultParallelism() throws Exception {
    return sc.sc().defaultParallelism();
  }

  @Override
  public SparkJobRef execute(DriverContext driverContext, SparkWork sparkWork) throws Exception {
    Context ctx = driverContext.getCtx();
    HiveConf hiveConf = (HiveConf) ctx.getConf();
    refreshLocalResources(sparkWork, hiveConf);
    JobConf jobConf = new JobConf(hiveConf);

    // Create temporary scratch dir
    Path emptyScratchDir;
    emptyScratchDir = ctx.getMRTmpPath();
    FileSystem fs = emptyScratchDir.getFileSystem(jobConf);
    fs.mkdirs(emptyScratchDir);

    // Update credential provider location
    // the password to the credential provider in already set in the sparkConf
    // in HiveSparkClientFactory
    HiveConfUtil.updateJobCredentialProviders(jobConf);

    SparkCounters sparkCounters = new SparkCounters(sc);
    Map<String, List<String>> prefixes = sparkWork.getRequiredCounterPrefix();
    if (prefixes != null) {
      for (String group : prefixes.keySet()) {
        for (String counterName : prefixes.get(group)) {
          sparkCounters.createCounter(group, counterName);
        }
      }
    }
    SparkReporter sparkReporter = new SparkReporter(sparkCounters);

    // Generate Spark plan
    SparkPlanGenerator gen =
      new SparkPlanGenerator(sc, ctx, jobConf, emptyScratchDir, sparkReporter);
    SparkPlan plan = gen.generate(sparkWork);

    if (driverContext.isShutdown()) {
      throw new HiveException("Operation is cancelled.");
    }

    // Execute generated plan.
    JavaPairRDD<HiveKey, BytesWritable> finalRDD = plan.generateGraph();

    sc.setJobGroup("queryId = " + sparkWork.getQueryId(), DagUtils.getQueryName(jobConf));

    // We use Spark RDD async action to submit job as it's the only way to get jobId now.
    JavaFutureAction<Void> future = finalRDD.foreachAsync(HiveVoidFunction.getInstance());

    // As we always use foreach action to submit RDD graph, it would only trigger one job.
    int jobId = future.jobIds().get(0);
    LocalSparkJobStatus sparkJobStatus = new LocalSparkJobStatus(
      sc, jobId, jobMetricsListener, sparkCounters, plan.getCachedRDDIds(), future);
    return new LocalSparkJobRef(Integer.toString(jobId), hiveConf,  sparkJobStatus, sc);
  }

  /**
   * At this point single SparkContext is used by more than one thread, so make this
   * method synchronized.
   *
   * This method can't remove a jar/resource from SparkContext. Looks like this is an
   * issue we have to live with until multiple SparkContexts are supported in a single JVM.
   */
  private synchronized void refreshLocalResources(SparkWork sparkWork, HiveConf conf) {
    // add hive-exec jar
    addJars((new JobConf(this.getClass())).getJar());

    // add aux jars
    addJars(conf.getAuxJars());
    addJars(SessionState.get() == null ? null : SessionState.get().getReloadableAuxJars());

    // add added jars
    String addedJars = Utilities.getResourceFiles(conf, SessionState.ResourceType.JAR);
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVEADDEDJARS, addedJars);
    addJars(addedJars);

    // add plugin module jars on demand
    // jobConf will hold all the configuration for hadoop, tez, and hive
    JobConf jobConf = new JobConf(conf);
    jobConf.set(MR_JAR_PROPERTY, "");
    for (BaseWork work : sparkWork.getAllWork()) {
      work.configureJobConf(jobConf);
    }
    addJars(jobConf.get(MR_JAR_PROPERTY));

    // add added files
    String addedFiles = Utilities.getResourceFiles(conf, SessionState.ResourceType.FILE);
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVEADDEDFILES, addedFiles);
    addResources(addedFiles);

    // add added archives
    String addedArchives = Utilities.getResourceFiles(conf, SessionState.ResourceType.ARCHIVE);
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVEADDEDARCHIVES, addedArchives);
    addResources(addedArchives);
  }

  private void addResources(String addedFiles) {
    for (String addedFile : CSV_SPLITTER.split(Strings.nullToEmpty(addedFiles))) {
      if (!localFiles.contains(addedFile)) {
        localFiles.add(addedFile);
        sc.addFile(addedFile);
      }
    }
  }

  private void addJars(String addedJars) {
    for (String addedJar : CSV_SPLITTER.split(Strings.nullToEmpty(addedJars))) {
      if (!localJars.contains(addedJar)) {
        localJars.add(addedJar);
        sc.addJar(addedJar);
      }
    }
  }

  @Override
  public void close() {
    synchronized (LocalHiveSparkClient.class) {
      client = null;
    }
    if (sc != null) {
      sc.stop();
    }
  }
}
