/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.spark;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManager;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.spark.client.SparkClientUtilities;
import org.apache.spark.Dependency;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.UnionRDD;
import scala.collection.JavaConversions;

/**
 * Contains utilities methods used as part of Spark tasks.
 */
public class SparkUtilities {

  public static HiveKey copyHiveKey(HiveKey key) {
    HiveKey copy = new HiveKey();
    copy.setDistKeyLength(key.getDistKeyLength());
    copy.setHashCode(key.hashCode());
    copy.set(key);
    return copy;
  }

  public static BytesWritable copyBytesWritable(BytesWritable bw) {
    BytesWritable copy = new BytesWritable();
    copy.set(bw);
    return copy;
  }

  /**
   * Uploads a local file to HDFS
   *
   * @param source
   * @param conf
   * @return
   * @throws IOException
   */
  public static URI uploadToHDFS(URI source, HiveConf conf) throws IOException {
    Path localFile = new Path(source.getPath());
    Path remoteFile = new Path(SessionState.get().getSparkSession().getHDFSSessionDir(),
        getFileName(source));
    FileSystem fileSystem = FileSystem.get(conf);
    // Overwrite if the remote file already exists. Whether the file can be added
    // on executor is up to spark, i.e. spark.files.overwrite
    fileSystem.copyFromLocalFile(false, true, localFile, remoteFile);
    Path fullPath = fileSystem.getFileStatus(remoteFile).getPath();
    return fullPath.toUri();
  }

  // checks if a resource has to be uploaded to HDFS for yarn-cluster mode
  public static boolean needUploadToHDFS(URI source, SparkConf sparkConf) {
    String master = sparkConf.get("spark.master");
    String deployMode = sparkConf.contains("spark.submit.deployMode") ?
        sparkConf.get("spark.submit.deployMode") : null;
    return SparkClientUtilities.isYarnClusterMode(master, deployMode) &&
        !source.getScheme().equals("hdfs");
  }

  private static String getFileName(URI uri) {
    if (uri == null) {
      return null;
    }

    String name = FilenameUtils.getName(uri.getPath());
    return name;
  }

  public static boolean isDedicatedCluster(Configuration conf) {
    String master = conf.get("spark.master");
    return SparkClientUtilities.isYarnMaster(master) || SparkClientUtilities.isLocalMaster(master);
  }

  public static SparkSession getSparkSession(HiveConf conf,
      SparkSessionManager sparkSessionManager) throws HiveException {
    SparkSession sparkSession = SessionState.get().getSparkSession();
    HiveConf sessionConf = SessionState.get().getConf();

    // Spark configurations are updated close the existing session
    // In case of async queries or confOverlay is not empty,
    // sessionConf and conf are different objects
    if (sessionConf.getSparkConfigUpdated() || conf.getSparkConfigUpdated()) {
      sparkSessionManager.closeSession(sparkSession);
      sparkSession =  null;
      conf.setSparkConfigUpdated(false);
      sessionConf.setSparkConfigUpdated(false);
    }
    sparkSession = sparkSessionManager.getSession(sparkSession, conf, true);
    SessionState.get().setSparkSession(sparkSession);
    return sparkSession;
  }


  public static String rddGraphToString(JavaPairRDD rdd) {
    StringBuilder sb = new StringBuilder();
    rddToString(rdd.rdd(), sb, "");
    return sb.toString();
  }

  private static void rddToString(RDD rdd, StringBuilder sb, String offset) {
    sb.append(offset).append(rdd.getClass().getCanonicalName()).append("[").append(rdd.hashCode()).append("]");
    if (rdd.getStorageLevel().useMemory()) {
      sb.append("(cached)");
    }
    sb.append("\n");
    Collection<Dependency> dependencies = JavaConversions.asJavaCollection(rdd.dependencies());
    if (dependencies != null) {
      offset += "\t";
      for (Dependency dependency : dependencies) {
        RDD parentRdd = dependency.rdd();
        rddToString(parentRdd, sb, offset);
      }
    } else if (rdd instanceof UnionRDD) {
      UnionRDD unionRDD = (UnionRDD) rdd;
      offset += "\t";
      Collection<RDD> parentRdds = JavaConversions.asJavaCollection(unionRDD.rdds());
      for (RDD parentRdd : parentRdds) {
        rddToString(parentRdd, sb, offset);
      }
    }
  }

  /**
   * Generate a temporary path for dynamic partition pruning in Spark branch
   * TODO: no longer need this if we use accumulator!
   * @param basePath
   * @param id
   * @return
   */
  public static Path generateTmpPathForPartitionPruning(Path basePath, String id) {
    return new Path(basePath, id);
  }

  /**
   * Return the ID for this BaseWork, in String form.
   * @param work the input BaseWork
   * @return the unique ID for this BaseWork
   */
  public static String getWorkId(BaseWork work) {
    String workName = work.getName();
    return workName.substring(workName.indexOf(" ") + 1);
  }

  public static SparkTask createSparkTask(HiveConf conf) {
    return (SparkTask) TaskFactory.get(
        new SparkWork(conf.getVar(HiveConf.ConfVars.HIVEQUERYID)), conf);
  }

  public static SparkTask createSparkTask(SparkWork work, HiveConf conf) {
    return (SparkTask) TaskFactory.get(work, conf);
  }

  /**
   * Recursively find all operators under root, that are of class clazz, and
   * put them in result.
   * @param result all operators under root that are of class clazz
   * @param root the root operator under which all operators will be examined
   * @param clazz clas to collect. Must NOT be null.
   */
  public static void collectOp(Collection<Operator<?>> result, Operator<?> root, Class<?> clazz) {
    Preconditions.checkArgument(clazz != null, "AssertionError: clazz should not be null");
    if (root == null) {
      return;
    }
    if (clazz.equals(root.getClass())) {
      result.add(root);
    }
    for (Operator<?> child : root.getChildOperators()) {
      collectOp(result, child, clazz);
    }
  }
}
