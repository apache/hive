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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.spark.counter.SparkCounters;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobRef;
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
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaPairRDD;

import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * RemoteSparkClient is a wrapper of {@link org.apache.hive.spark.client.SparkClient}, which
 * wrap a spark job request and send to an remote SparkContext.
 */
public class RemoteHiveSparkClient implements HiveSparkClient {
  private static final long serialVersionUID = 1L;

  private static final String MR_JAR_PROPERTY = "tmpjars";
  protected static transient final Log LOG = LogFactory
    .getLog(RemoteHiveSparkClient.class);

  private static transient final Splitter CSV_SPLITTER = Splitter.on(",").omitEmptyStrings();

  private transient SparkClient remoteClient;

  private transient List<String> localJars = new ArrayList<String>();

  private transient List<String> localFiles = new ArrayList<String>();

  RemoteHiveSparkClient(Map<String, String> sparkConf) throws IOException, SparkException {
    SparkClientFactory.initialize(sparkConf);
    remoteClient = SparkClientFactory.createClient(sparkConf);
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

    final byte[] jobConfBytes = KryoSerializer.serializeJobConf(jobConf);
    final byte[] scratchDirBytes = KryoSerializer.serialize(emptyScratchDir);
    final byte[] sparkWorkBytes = KryoSerializer.serialize(sparkWork);

    JobHandle<Serializable> jobHandle = remoteClient.submit(new Job<Serializable>() {
      @Override
      public Serializable call(JobContext jc) throws Exception {
        JobConf localJobConf = KryoSerializer.deserializeJobConf(jobConfBytes);
        Path localScratchDir = KryoSerializer.deserialize(scratchDirBytes, Path.class);
        SparkWork localSparkWork = KryoSerializer.deserialize(sparkWorkBytes, SparkWork.class);

        SparkCounters sparkCounters = new SparkCounters(jc.sc(), localJobConf);
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
        jc.monitor(future);
        return null;
      }
    });
    jobHandle.get();
    return new SparkJobRef(jobHandle.getClientJobId());
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
      if (!localFiles.contains(addedFile)) {
        localFiles.add(addedFile);
        try {
          remoteClient.addFile(SparkUtilities.getURL(addedFile));
        } catch (MalformedURLException e) {
          LOG.warn("Failed to add file:" + addedFile);
        }
      }
    }
  }

  private void addJars(String addedJars) {
    for (String addedJar : CSV_SPLITTER.split(Strings.nullToEmpty(addedJars))) {
      if (!localJars.contains(addedJar)) {
        localJars.add(addedJar);
        try {
          remoteClient.addJar(SparkUtilities.getURL(addedJar));
        } catch (MalformedURLException e) {
          LOG.warn("Failed to add jar:" + addedJar);
        }
      }
    }
  }

  @Override
  public void close() {
    remoteClient.stop();
  }
}
