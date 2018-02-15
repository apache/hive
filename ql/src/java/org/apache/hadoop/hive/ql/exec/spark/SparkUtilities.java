/*
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

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManager;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.spark.OptimizeSparkProcContext;
import org.apache.hadoop.hive.ql.parse.spark.SparkPartitionPruningSinkOperator;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.spark.client.SparkClientUtilities;
import org.apache.spark.SparkConf;


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
    FileSystem fileSystem = FileSystem.get(remoteFile.toUri(), conf);
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
        !(source.getScheme().equals("hdfs") || source.getScheme().equals("viewfs"));
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
   * Recursively find all operators under root, that are of class clazz or are the sub-class of clazz, and
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
    if (clazz.isAssignableFrom(root.getClass())) {
      result.add(root);
    }
    for (Operator<?> child : root.getChildOperators()) {
      collectOp(result, child, clazz);
    }
  }

  /**
   * Collect operators of type T starting from root. Matching operators will be put into result.
   * Set seen can be used to skip search in certain branches.
   */
  public static <T extends Operator<?>> void collectOp(Operator<?> root, Class<T> cls,
      Collection<T> result, Set<Operator<?>> seen) {
    if (seen.contains(root)) {
      return;
    }
    Deque<Operator<?>> deque = new ArrayDeque<>();
    deque.add(root);
    while (!deque.isEmpty()) {
      Operator<?> op = deque.remove();
      seen.add(op);
      if (cls.isInstance(op)) {
        result.add((T) op);
      }
      if (op.getChildOperators() != null) {
        for (Operator<?> child : op.getChildOperators()) {
          if (!seen.contains(child)) {
            deque.add(child);
          }
        }
      }
    }
  }

  /**
   * remove currTask from the children of its parentTask
   * remove currTask from the parent of its childrenTask
   * @param currTask
   */
  public static void removeEmptySparkTask(SparkTask currTask) {
    //remove currTask from parentTasks
    ArrayList<Task> parTasks = new ArrayList<Task>();
    parTasks.addAll(currTask.getParentTasks());

    Object[] parTaskArr = parTasks.toArray();
    for (Object parTask : parTaskArr) {
      ((Task) parTask).removeDependentTask(currTask);
    }
    //remove currTask from childTasks
    currTask.removeFromChildrenTasks();
  }

  /**
   * For DPP sinks w/ common join, we'll split the tree and what's above the branching
   * operator is computed multiple times. Therefore it may not be good for performance to support
   * nested DPP sinks, i.e. one DPP sink depends on other DPP sinks.
   * The following is an example:
   *
   *             TS          TS
   *             |           |
   *            ...         FIL
   *            |           |  \
   *            RS         RS  SEL
   *              \        /    |
   *     TS          JOIN      GBY
   *     |         /     \      |
   *    RS        RS    SEL   DPP2
   *     \       /       |
   *       JOIN         GBY
   *                     |
   *                    DPP1
   *
   * where DPP1 depends on DPP2.
   *
   * To avoid such case, we'll visit all the branching operators. If a branching operator has any
   * further away DPP branches w/ common join in its sub-tree, such branches will be removed.
   * In the above example, the branch of DPP1 will be removed.
   */
  public static void removeNestedDPP(OptimizeSparkProcContext procContext) {
    Set<SparkPartitionPruningSinkOperator> allDPPs = new HashSet<>();
    Set<Operator<?>> seen = new HashSet<>();
    // collect all DPP sinks
    for (TableScanOperator root : procContext.getParseContext().getTopOps().values()) {
      SparkUtilities.collectOp(root, SparkPartitionPruningSinkOperator.class, allDPPs, seen);
    }
    // collect all branching operators
    Set<Operator<?>> branchingOps = new HashSet<>();
    for (SparkPartitionPruningSinkOperator dpp : allDPPs) {
      branchingOps.add(dpp.getBranchingOp());
    }
    // remember the branching ops we have visited
    Set<Operator<?>> visited = new HashSet<>();
    for (Operator<?> branchingOp : branchingOps) {
      if (!visited.contains(branchingOp)) {
        visited.add(branchingOp);
        seen.clear();
        Set<SparkPartitionPruningSinkOperator> nestedDPPs = new HashSet<>();
        for (Operator<?> branch : branchingOp.getChildOperators()) {
          if (!isDirectDPPBranch(branch)) {
            SparkUtilities.collectOp(branch, SparkPartitionPruningSinkOperator.class, nestedDPPs,
                seen);
          }
        }
        for (SparkPartitionPruningSinkOperator nestedDPP : nestedDPPs) {
          visited.add(nestedDPP.getBranchingOp());
          // if a DPP is with MJ, the tree won't be split and so we don't have to remove it
          if (!nestedDPP.isWithMapjoin()) {
            OperatorUtils.removeBranch(nestedDPP);
          }
        }
      }
    }
  }

  // whether of pattern "SEL - GBY - DPP"
  private static boolean isDirectDPPBranch(Operator<?> op) {
    if (op instanceof SelectOperator && op.getChildOperators() != null
        && op.getChildOperators().size() == 1) {
      op = op.getChildOperators().get(0);
      if (op instanceof GroupByOperator && op.getChildOperators() != null
          && op.getChildOperators().size() == 1) {
        op = op.getChildOperators().get(0);
        return op instanceof SparkPartitionPruningSinkOperator;
      }
    }
    return false;
  }

  public static String reverseDNSLookupURL(String url) throws UnknownHostException {
    // Run a reverse DNS lookup on the URL
    URI uri = URI.create(url);
    InetAddress address = InetAddress.getByName(uri.getHost());
    return uri.getScheme() + "://" + address.getCanonicalHostName() + ":" + uri.getPort();
  }
}
