/**
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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobRef;
import org.apache.hadoop.hive.ql.exec.spark.status.impl.RemoteSparkJobRef;
import org.apache.hadoop.hive.ql.exec.spark.status.impl.RemoteSparkJobStatus;
import org.apache.hadoop.hive.ql.io.HiveKey;
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
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaPairRDD;

/**
 * RemoteSparkClient is a wrapper of {@link org.apache.hive.spark.client.SparkClient}, which
 * wrap a spark job request and send to an remote SparkContext.
 */
public class RemoteHiveSparkClient implements HiveSparkClient {
  private static final long serialVersionUID = 1L;

  private static final String MR_JAR_PROPERTY = "tmpjars";
  protected static final transient Log LOG = LogFactory
    .getLog(RemoteHiveSparkClient.class);

  private static final transient Splitter CSV_SPLITTER = Splitter.on(",").omitEmptyStrings();

  private transient SparkClient remoteClient;
  private transient SparkConf sparkConf;
  private transient HiveConf hiveConf;

  private transient List<URL> localJars = new ArrayList<URL>();
  private transient List<URL> localFiles = new ArrayList<URL>();

  private final transient long sparkClientTimtout;

  RemoteHiveSparkClient(HiveConf hiveConf, Map<String, String> conf) throws IOException, SparkException {
    this.hiveConf = hiveConf;
    sparkConf = HiveSparkClientFactory.generateSparkConf(conf);
    remoteClient = SparkClientFactory.createClient(conf, hiveConf);
    sparkClientTimtout = hiveConf.getTimeVar(HiveConf.ConfVars.SPARK_CLIENT_FUTURE_TIMEOUT, TimeUnit.SECONDS);
  }

  @Override
  public SparkConf getSparkConf() {
    return sparkConf;
  }

  @Override
  public int getExecutorCount() throws Exception {
    Future<Integer> handler = remoteClient.getExecutorCount();
    return handler.get(sparkClientTimtout, TimeUnit.SECONDS).intValue();
  }

  @Override
  public int getDefaultParallelism() throws Exception {
    Future<Integer> handler = remoteClient.getDefaultParallelism();
    return handler.get(sparkClientTimtout, TimeUnit.SECONDS);
  }

  @Override
  public SparkJobRef execute(final DriverContext driverContext, final SparkWork sparkWork) throws Exception {
    final Context ctx = driverContext.getCtx();
    final HiveConf hiveConf = (HiveConf) ctx.getConf();
    refreshLocalResources(sparkWork, hiveConf);
    final JobConf jobConf = new JobConf(hiveConf);

    // Create temporary scratch dir
    final Path emptyScratchDir = ctx.getMRTmpPath();
    FileSystem fs = emptyScratchDir.getFileSystem(jobConf);
    fs.mkdirs(emptyScratchDir);

    byte[] jobConfBytes = KryoSerializer.serializeJobConf(jobConf);
    byte[] scratchDirBytes = KryoSerializer.serialize(emptyScratchDir);
    byte[] sparkWorkBytes = KryoSerializer.serialize(sparkWork);

    JobStatusJob job = new JobStatusJob(jobConfBytes, scratchDirBytes, sparkWorkBytes);
    JobHandle<Serializable> jobHandle = remoteClient.submit(job);
    RemoteSparkJobStatus sparkJobStatus = new RemoteSparkJobStatus(remoteClient, jobHandle, sparkClientTimtout);
    return new RemoteSparkJobRef(hiveConf, jobHandle, sparkJobStatus);
  }

  private void refreshLocalResources(SparkWork sparkWork, HiveConf conf) {
    // add hive-exec jar
    addJars((new JobConf(this.getClass())).getJar());

    // add aux jars
    addJars(HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS));

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
    addJars(conf.get(MR_JAR_PROPERTY));

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
      try {
        URL fileUrl = SparkUtilities.getURL(addedFile);
        if (fileUrl != null && !localFiles.contains(fileUrl)) {
          localFiles.add(fileUrl);
          remoteClient.addFile(fileUrl);
        }
      } catch (MalformedURLException e) {
        LOG.warn("Failed to add file:" + addedFile);
      }
    }
  }

  private void addJars(String addedJars) {
    for (String addedJar : CSV_SPLITTER.split(Strings.nullToEmpty(addedJars))) {
      try {
        URL jarUrl = SparkUtilities.getURL(addedJar);
        if (jarUrl != null && !localJars.contains(jarUrl)) {
          localJars.add(jarUrl);
          remoteClient.addJar(jarUrl);
        }
      } catch (MalformedURLException e) {
        LOG.warn("Failed to add jar:" + addedJar);
      }
    }
  }

  @Override
  public void close() {
    remoteClient.stop();
  }

  private static class JobStatusJob implements Job<Serializable> {

    private final byte[] jobConfBytes;
    private final byte[] scratchDirBytes;
    private final byte[] sparkWorkBytes;

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
      List<String> addedJars = jc.getAddedJars();
      if (addedJars != null && !addedJars.isEmpty()) {
        SparkClientUtilities.addToClassPath(addedJars.toArray(new String[addedJars.size()]));
        localJobConf.set(Utilities.HIVE_ADDED_JARS, StringUtils.join(addedJars, ";"));
      }

      Path localScratchDir = KryoSerializer.deserialize(scratchDirBytes, Path.class);
      SparkWork localSparkWork = KryoSerializer.deserialize(sparkWorkBytes, SparkWork.class);

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

      // Execute generated plan.
      JavaPairRDD<HiveKey, BytesWritable> finalRDD = plan.generateGraph();
      // We use Spark RDD async action to submit job as it's the only way to get jobId now.
      JavaFutureAction<Void> future = finalRDD.foreachAsync(HiveVoidFunction.getInstance());
      jc.monitor(future, sparkCounters, plan.getCachedRDDIds());
      return null;
    }
  }

}
