/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.spark;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.io.NullScanFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveConfUtil;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.DagUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobRef;
import org.apache.hadoop.hive.ql.exec.spark.status.impl.RemoteSparkJobRef;
import org.apache.hadoop.hive.ql.exec.spark.status.impl.RemoteSparkJobStatus;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.spark.client.Job;
import org.apache.hive.spark.client.JobContext;
import org.apache.hive.spark.client.JobHandle;
import org.apache.hive.spark.client.SparkClient;
import org.apache.hive.spark.client.SparkClientFactory;
import org.apache.hive.spark.client.SparkClientUtilities;
import org.apache.hive.spark.counter.SparkCounters;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaPairRDD;

/**
 * RemoteSparkClient is a wrapper of {@link org.apache.hive.spark.client.SparkClient}, which
 * wrap a spark job request and send to an remote SparkContext.
 */
public class RemoteHiveSparkClient implements HiveSparkClient {
  private static final long serialVersionUID = 1L;

  private static final String MR_JAR_PROPERTY = "tmpjars";
  private static final String MR_CREDENTIALS_LOCATION_PROPERTY = "mapreduce.job.credentials.binary";
  private static final transient Logger LOG = LoggerFactory.getLogger(RemoteHiveSparkClient.class);
  private static final transient Splitter CSV_SPLITTER = Splitter.on(",").omitEmptyStrings();

  private transient Map<String, String> conf;
  private transient SparkClient remoteClient;
  private transient SparkConf sparkConf;
  private transient HiveConf hiveConf;

  private transient List<URI> localJars = new ArrayList<URI>();
  private transient List<URI> localFiles = new ArrayList<URI>();

  private final transient long sparkClientTimtout;
  private final String sessionId;

  RemoteHiveSparkClient(HiveConf hiveConf, Map<String, String> conf, String sessionId) throws Exception {
    this.hiveConf = hiveConf;
    sparkClientTimtout = hiveConf.getTimeVar(HiveConf.ConfVars.SPARK_CLIENT_FUTURE_TIMEOUT,
        TimeUnit.SECONDS);
    sparkConf = HiveSparkClientFactory.generateSparkConf(conf);
    this.conf = conf;
    this.sessionId = sessionId;
    createRemoteClient();
  }

  private void createRemoteClient() throws Exception {
    remoteClient = SparkClientFactory.createClient(conf, hiveConf, sessionId);

    if (HiveConf.getBoolVar(hiveConf, ConfVars.HIVE_PREWARM_ENABLED) &&
            (SparkClientUtilities.isYarnMaster(hiveConf.get("spark.master")) ||
             SparkClientUtilities.isLocalMaster(hiveConf.get("spark.master")))) {
      int minExecutors = getExecutorsToWarm();
      if (minExecutors <= 0) {
        return;
      }

      LOG.info("Prewarm Spark executors. The minimum number of executors to warm is " + minExecutors);

      // Spend at most HIVE_PREWARM_SPARK_TIMEOUT to wait for executors to come up.
      int curExecutors = 0;
      long maxPrewarmTime = HiveConf.getTimeVar(hiveConf, ConfVars.HIVE_PREWARM_SPARK_TIMEOUT,
          TimeUnit.MILLISECONDS);
      long ts = System.currentTimeMillis();
      do {
        try {
          curExecutors = getExecutorCount(maxPrewarmTime, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
          // let's don't fail on future timeout since we have a timeout for pre-warm
          LOG.warn("Timed out getting executor count.", e);
        }
        if (curExecutors >= minExecutors) {
          LOG.info("Finished prewarming Spark executors. The current number of executors is " + curExecutors);
          return;
        }
        Thread.sleep(500); // sleep half a second
      } while (System.currentTimeMillis() - ts < maxPrewarmTime);

      LOG.info("Timeout (" + maxPrewarmTime / 1000 + "s) occurred while prewarming executors. " +
          "The current number of executors is " + curExecutors);
    }
  }

  /**
   * Please note that the method is very tied with Spark documentation 1.4.1 regarding
   * dynamic allocation, such as default values.
   * @return
   */
  private int getExecutorsToWarm() {
    int minExecutors =
        HiveConf.getIntVar(hiveConf, HiveConf.ConfVars.HIVE_PREWARM_NUM_CONTAINERS);
    boolean dynamicAllocation = hiveConf.getBoolean("spark.dynamicAllocation.enabled", false);
    if (dynamicAllocation) {
      int min = sparkConf.getInt("spark.dynamicAllocation.minExecutors", 0);
      int initExecutors = sparkConf.getInt("spark.dynamicAllocation.initialExecutors", min);
      minExecutors = Math.min(minExecutors, initExecutors);
    } else {
      int execInstances = sparkConf.getInt("spark.executor.instances", 2);
      minExecutors = Math.min(minExecutors, execInstances);
    }
    return minExecutors;
  }

  private int getExecutorCount(long timeout, TimeUnit unit) throws Exception {
    Future<Integer> handler = remoteClient.getExecutorCount();
    return handler.get(timeout, unit);
  }

  @Override
  public SparkConf getSparkConf() {
    return sparkConf;
  }

  @Override
  public int getExecutorCount() throws Exception {
    return getExecutorCount(sparkClientTimtout, TimeUnit.SECONDS);
  }

  @Override
  public int getDefaultParallelism() throws Exception {
    Future<Integer> handler = remoteClient.getDefaultParallelism();
    return handler.get(sparkClientTimtout, TimeUnit.SECONDS);
  }

  @Override
  public SparkJobRef execute(final DriverContext driverContext, final SparkWork sparkWork)
      throws Exception {
    if (SparkClientUtilities.isYarnMaster(hiveConf.get("spark.master")) &&
        !remoteClient.isActive()) {
      // Re-create the remote client if not active any more
      close();
      createRemoteClient();
    }

    try {
      return submit(driverContext, sparkWork);
    } catch (Throwable cause) {
      throw new Exception("Failed to submit Spark work, please retry later", cause);
    }
  }

  private SparkJobRef submit(final DriverContext driverContext, final SparkWork sparkWork) throws Exception {
    final Context ctx = driverContext.getCtx();
    final HiveConf hiveConf = (HiveConf) ctx.getConf();
    refreshLocalResources(sparkWork, hiveConf);
    final JobConf jobConf = new JobConf(hiveConf);

    //update the credential provider location in the jobConf
    HiveConfUtil.updateJobCredentialProviders(jobConf);

    // Create temporary scratch dir
    final Path emptyScratchDir = ctx.getMRTmpPath();
    FileSystem fs = emptyScratchDir.getFileSystem(jobConf);
    fs.mkdirs(emptyScratchDir);

    // make sure NullScanFileSystem can be loaded - HIVE-18442
    jobConf.set("fs." + NullScanFileSystem.getBaseScheme() + ".impl",
        NullScanFileSystem.class.getCanonicalName());

    byte[] jobConfBytes = KryoSerializer.serializeJobConf(jobConf);
    byte[] scratchDirBytes = KryoSerializer.serialize(emptyScratchDir);
    byte[] sparkWorkBytes = KryoSerializer.serialize(sparkWork);

    JobStatusJob job = new JobStatusJob(jobConfBytes, scratchDirBytes, sparkWorkBytes);
    if (driverContext.isShutdown()) {
      throw new HiveException("Operation is cancelled.");
    }

    JobHandle<Serializable> jobHandle = remoteClient.submit(job);
    RemoteSparkJobStatus sparkJobStatus = new RemoteSparkJobStatus(remoteClient, jobHandle, sparkClientTimtout);
    return new RemoteSparkJobRef(hiveConf, jobHandle, sparkJobStatus);
  }

  private void refreshLocalResources(SparkWork sparkWork, HiveConf conf) throws IOException {
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

    // remove the location of container tokens
    conf.unset(MR_CREDENTIALS_LOCATION_PROPERTY);
    // add added files
    String addedFiles = Utilities.getResourceFiles(conf, SessionState.ResourceType.FILE);
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVEADDEDFILES, addedFiles);
    addResources(addedFiles);

    // add added archives
    String addedArchives = Utilities.getResourceFiles(conf, SessionState.ResourceType.ARCHIVE);
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVEADDEDARCHIVES, addedArchives);
    addResources(addedArchives);
  }

  private void addResources(String addedFiles) throws IOException {
    for (String addedFile : CSV_SPLITTER.split(Strings.nullToEmpty(addedFiles))) {
      try {
        URI fileUri = FileUtils.getURI(addedFile);
        if (fileUri != null && !localFiles.contains(fileUri)) {
          localFiles.add(fileUri);
          if (SparkUtilities.needUploadToHDFS(fileUri, sparkConf)) {
            fileUri = SparkUtilities.uploadToHDFS(fileUri, hiveConf);
          }
          remoteClient.addFile(fileUri);
        }
      } catch (URISyntaxException e) {
        LOG.warn("Failed to add file:" + addedFile, e);
      }
    }
  }

  private void addJars(String addedJars) throws IOException {
    for (String addedJar : CSV_SPLITTER.split(Strings.nullToEmpty(addedJars))) {
      try {
        URI jarUri = FileUtils.getURI(addedJar);
        if (jarUri != null && !localJars.contains(jarUri)) {
          localJars.add(jarUri);
          if (SparkUtilities.needUploadToHDFS(jarUri, sparkConf)) {
            jarUri = SparkUtilities.uploadToHDFS(jarUri, hiveConf);
          }
          remoteClient.addJar(jarUri);
        }
      } catch (URISyntaxException e) {
        LOG.warn("Failed to add jar:" + addedJar, e);
      }
    }
  }

  @Override
  public void close() {
    if (remoteClient != null) {
      remoteClient.stop();
    }
    localFiles.clear();
    localJars.clear();
  }

  private static class JobStatusJob implements Job<Serializable> {

    private static final long serialVersionUID = 1L;
    private final byte[] jobConfBytes;
    private final byte[] scratchDirBytes;
    private final byte[] sparkWorkBytes;

    @SuppressWarnings("unused")
    private JobStatusJob() {
      // For deserialization.
      this(null, null, null);
    }

    JobStatusJob(byte[] jobConfBytes, byte[] scratchDirBytes, byte[] sparkWorkBytes) {
      this.jobConfBytes = jobConfBytes;
      this.scratchDirBytes = scratchDirBytes;
      this.sparkWorkBytes = sparkWorkBytes;
    }

    @Override
    public Serializable call(JobContext jc) throws Exception {
      JobConf localJobConf = KryoSerializer.deserializeJobConf(jobConfBytes);

      // Add jar to current thread class loader dynamically, and add jar paths to JobConf as Spark
      // may need to load classes from this jar in other threads.
      Map<String, Long> addedJars = jc.getAddedJars();
      if (addedJars != null && !addedJars.isEmpty()) {
        List<String> localAddedJars = SparkClientUtilities.addToClassPath(addedJars,
            localJobConf, jc.getLocalTmpDir());
        localJobConf.set(Utilities.HIVE_ADDED_JARS, StringUtils.join(localAddedJars, ";"));
      }

      Path localScratchDir = KryoSerializer.deserialize(scratchDirBytes, Path.class);
      SparkWork localSparkWork = KryoSerializer.deserialize(sparkWorkBytes, SparkWork.class);
      logConfigurations(localJobConf);

      SparkCounters sparkCounters = new SparkCounters(jc.sc());
      Map<String, List<String>> prefixes = localSparkWork.getRequiredCounterPrefix();
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
        new SparkPlanGenerator(jc.sc(), null, localJobConf, localScratchDir, sparkReporter);
      SparkPlan plan = gen.generate(localSparkWork);

      jc.sc().setJobGroup("queryId = " + localSparkWork.getQueryId(), DagUtils.getQueryName(localJobConf));

      // Execute generated plan.
      JavaPairRDD<HiveKey, BytesWritable> finalRDD = plan.generateGraph();
      // We use Spark RDD async action to submit job as it's the only way to get jobId now.
      JavaFutureAction<Void> future = finalRDD.foreachAsync(HiveVoidFunction.getInstance());
      jc.monitor(future, sparkCounters, plan.getCachedRDDIds());
      return null;
    }

    private void logConfigurations(JobConf localJobConf) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Logging job configuration: ");
        StringBuilder outWriter = new StringBuilder();
        // redact sensitive information before logging
        HiveConfUtil.dumpConfig(localJobConf, outWriter);
        LOG.debug(outWriter.toString());
      }
    }
  }
}
