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

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.mapred.InputFormat;
import parquet.hadoop.ParquetInputFormat;

/**
 * A Hive query may scan the same table multi times, or even share same part in the query,
 * so cache the shared RDD would reduce IO and CPU cost, which help to improve Hive performance.
 * SparkRddCachingResolver is in charge of walking through Tasks, and parttern matching all
 * the cacheable MapWork/ReduceWork.
 */
public class SparkRddCachingResolver implements PhysicalPlanResolver {

  private static final Log LOG = LogFactory.getLog(SparkRddCachingResolver.class);

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    List<SparkTask> sparkTasks = getAllSparkTasks(pctx);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    if (sparkTasks.size() > 0) {
      TaskGraphWalker sparkWorkOgw = new TaskGraphWalker(new SparkWorkMapInputMatching());
      DummyTask dummyTask = new DummyTask(pctx.getConf(), sparkTasks);
      topNodes.add(dummyTask);
      sparkWorkOgw.startWalking(topNodes, null);

      TaskGraphWalker sparkWorkOgw2 = new TaskGraphWalker(new ShareParentWorkMatching());
      sparkWorkOgw2.startWalking(topNodes, null);
    } else {
      LOG.info("No SparkWork found, skip SparkRddCachingResolver.");
    }

    return pctx;
  }

  class DummyTask extends Task {

    private HiveConf hiveConf;

    private List<? extends Node> children;

    public DummyTask(HiveConf hiveConf, List<? extends Node> children) {
      this.hiveConf = hiveConf;
      this.children = children;
    }

    @Override
    protected int execute(DriverContext driverContext) {
      return 0;
    }

    @Override
    public List<? extends Node> getChildren() {
      return children;
    }

    @Override
    public StageType getType() {
      return null;
    }


    @Override
    public String getName() {
      return "DummyTask";
    }

    public HiveConf getHiveConf() {
      return hiveConf;
    }
  }

  class SparkWorkMapInputMatching implements Dispatcher {

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs) throws SemanticException {
      if (nd instanceof DummyTask) {
        HiveConf hiveConf = ((DummyTask) nd).getHiveConf();
        List<? extends Node> children = nd.getChildren();
        List<MapWork> allMapWork = new LinkedList<MapWork>();
        List<SparkWork> allSparkWork = new LinkedList<SparkWork>();
        for (Node task : children) {
          if (task instanceof SparkTask) {
            SparkTask sparkTask = (SparkTask) task;
            SparkWork sparkWork = sparkTask.getWork();
            allSparkWork.add(sparkWork);
            for (MapWork mapWork : sparkWork.getAllMapWork()) {
              boolean add = true;
              for (Operator operator : mapWork.getWorks()) {
                if (operator instanceof TableScanOperator) {
                  TableScanOperator tsop = (TableScanOperator) operator;
                  Table table = tsop.getConf().getTableMetadata();
                  long rawDataSize = StatsUtils.getRawDataSize(table);
                  long threshold = HiveConf.getLongVar(hiveConf, HiveConf.ConfVars.SPARK_DYNAMIC_RDD_CACHING_THRESHOLD);
                  // If rawDataSize is not collected or rawDataSize is beyond the threshold, we should not cache this table.
                  if (rawDataSize == 0 || rawDataSize > threshold) {
                    add = false;
                    break;
                  }
                }
              }
              if (add) {
                allMapWork.add(mapWork);
              }
            }
          }
        }

        if (allMapWork.size() > 1) {
          Map<BaseWork, Pair<String, Integer>> equivalentWorks = matchSameMapWorkInput(allMapWork);
          for (SparkWork sparkWork : allSparkWork) {
            sparkWork.setEquivalentWorks(equivalentWorks);
          }
        }
      }
      return null;
    }
  }

  private Map<BaseWork, Pair<String, Integer>> matchSameMapWorkInput(List<MapWork> allMapWorks) {
    Map<BaseWork, Pair<String, Integer>> equivalentWorks = new HashMap<BaseWork, Pair<String, Integer>>();

    int size = allMapWorks.size();
    for (int i = 0; i < size; i++) {
      MapWork first = allMapWorks.get(i);
      for (int j = i + 1; j < size; j++) {
        MapWork second = allMapWorks.get(j);
        if (isScanTheSameData(first, second)) {
          Pair<String, Integer> container = null;
          Pair<String, Integer> container1 = equivalentWorks.get(first);
          Pair<String, Integer> container2 = equivalentWorks.get(second);
          if (container1 == null && container2 == null) {
            container = new MutablePair<String, Integer>(UUID.randomUUID().toString(), 2);
            equivalentWorks.put(first, container);
            equivalentWorks.put(second, container);
          } else if (container1 == null && container2 != null) {
            container2.setValue(container2.getValue() + 1);
            equivalentWorks.put(first, container2);
          } else if (container1 != null && container2 == null) {
            container1.setValue(container1.getValue() + 1);
            equivalentWorks.put(second, container1);
          } else {
            Preconditions.checkArgument(container1 == container2);
          }
        }
      }
    }

    return equivalentWorks;
  }

  /**
   * check whether 2 input MapWork scan the same data.
   *
   * @param mapWork1
   * @param mapWork2
   * @return
   */
  private boolean isScanTheSameData(MapWork mapWork1, MapWork mapWork2) {
    boolean result = true;
    Map<String, ArrayList<String>> pathToAliases1 = mapWork1.getPathToAliases();
    Map<String, ArrayList<String>> pathToAliases2 = mapWork2.getPathToAliases();
    Map<String, PartitionDesc> aliasToPartnInfo1 = mapWork1.getAliasToPartnInfo();
    Map<String, PartitionDesc> aliasToPartnInfo2 = mapWork2.getAliasToPartnInfo();
    Map<String, Operator<? extends OperatorDesc>> aliasToWork1 = mapWork1.getAliasToWork();
    Map<String, Operator<? extends OperatorDesc>> aliasToWork2 = mapWork2.getAliasToWork();

    if (pathToAliases1.size() != pathToAliases2.size()) {
      result = false;
    }

    Iterator<String> pathIter1 = pathToAliases1.keySet().iterator();
    Iterator<String> pathIter2 = pathToAliases2.keySet().iterator();
    while (pathIter1.hasNext()) {
      String path1 = pathIter1.next();
      String path2 = pathIter2.next();

      if (!path1.equals(path2)) {
        result = false;
        break;
      }

      List<String> alias1 = pathToAliases1.get(path1);
      List<String> alias2 = pathToAliases2.get(path1);

      // Skip this optimization if get multi alias.
      if (alias1.size() > 1 || alias2.size() > 1) {
        result = false;
        break;
      }

      PartitionDesc partitionDesc1 = aliasToPartnInfo1.get(alias1.get(0));
      PartitionDesc partitionDesc2 = aliasToPartnInfo2.get(alias2.get(0));
      // Compare TableDesc, make sure scan the same table.
      TableDesc tableDesc1 = partitionDesc1.getTableDesc();
      TableDesc tableDesc2 = partitionDesc2.getTableDesc();
      if (!tableDesc1.equals(tableDesc2)) {
        result = false;
        break;
      }
      // compare the partition info, make sure scan the same partitions.
      Map<String, String> partSpec1 = partitionDesc1.getPartSpec();
      Map<String, String> partSpec2 = partitionDesc2.getPartSpec();
      if ((partSpec1 == null && partSpec2 != null) || (partSpec1 != null && partSpec2 == null)) {
        result = false;
        break;
      }
      if (partSpec1 != null && partSpec2 != null && !Maps.difference(partSpec1, partSpec2).areEqual()) {
        result = false;
        break;
      }

      Operator<? extends OperatorDesc> operator1 = aliasToWork1.get(alias1.get(0));
      Operator<? extends OperatorDesc> operator2 = aliasToWork2.get(alias2.get(0));
      if (!(operator1 instanceof TableScanOperator) || !(operator2 instanceof TableScanOperator)) {
        result = false;
        break;
      }
      TableScanDesc tableScanDesc1 = (TableScanDesc) operator1.getConf();
      TableScanDesc tableScanDesc2 = (TableScanDesc) operator2.getConf();
      // For columnar storage format, Hive may push projection and filter down to InputFormat level,
      // so Hive may scan the same table twice, but with different output data from columnar InputFormat.
      // we should not cache RDD in this case.
      // TODO is there any more storage format which may push projection and filter down to InputFormat level?
      Class<? extends InputFormat> inputFileFormatClass = tableDesc1.getInputFileFormatClass();
      if (inputFileFormatClass.equals(ParquetInputFormat.class) ||
        inputFileFormatClass.equals(OrcInputFormat.class) ||
        inputFileFormatClass.equals(OrcNewInputFormat.class)) {
        // check the filter information.
        if (!isFilterInfoEqual(tableScanDesc1, tableScanDesc2)) {
          result = false;
          break;
        }

        // check the projection columns.
        List<String> neededColumns1 = tableScanDesc1.getNeededColumns();
        List<String> neededColumns2 = tableScanDesc2.getNeededColumns();
        if (neededColumns1.size() != neededColumns2.size()) {
          result = false;
          break;
        }
        for (String columnName : neededColumns1) {
          if (!neededColumns2.contains(columnName)) {
            result = false;
            break;
          }
        }
        if (!result) {
          break;
        }
      }
    }

    return result;
  }

  private boolean isFilterInfoEqual(TableScanDesc tableScanDesc1, TableScanDesc tableScanDesc2) {
    boolean result = true;
    if (tableScanDesc1.getFilterExpr() == null && tableScanDesc2.getFilterExpr() == null) {
      result = true;
    } else if (tableScanDesc1.getFilterExpr() == null && tableScanDesc2.getFilterExpr() != null) {
      result = false;
    } else if (tableScanDesc1.getFilterExpr() != null && tableScanDesc2.getFilterExpr() == null) {
      result = false;
    } else {
      if (!tableScanDesc1.getFilterExprString().equals(tableScanDesc2.getFilterExprString())) {
        result = false;
      }
    }
    return result;
  }

  /**
   * A Work graph like:
   *        MapWork1
   *       /        \
   * ReduceWork1  ReduceWork2
   * would be translated into RDD graph like:
   *          MapPartitionsRDD1
   *             /          \
   *        ShuffledRDD1   ShuffledRDD2
   *            /             \
   *    MapPartitionsRDD3    MapPartitionsRDD4
   * In the Spark implementation, MapPartitionsRDD1 would be computed twice, so cache it may improve performance.
   * ShareParentWorkMatching try to match all the works with multi children in SparkWork here.
   */
  class ShareParentWorkMatching implements Dispatcher {

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs) throws SemanticException {
      if (nd instanceof DummyTask) {
        HiveConf hiveConf = ((DummyTask) nd).getHiveConf();
        List<? extends Node> children = nd.getChildren();
        for (Node child : children) {
          if (child instanceof SparkTask) {
            SparkTask sparkTask = (SparkTask) child;
            SparkWork sparkWork = sparkTask.getWork();
            List<BaseWork> toCacheWorks = Lists.newLinkedList();
            List<Pair<BaseWork, BaseWork>> pairs = Lists.newArrayList(sparkWork.getEdgeProperties().keySet());
            int size = pairs.size();
            for (int i = 0; i < size; i++) {
              Pair<BaseWork, BaseWork> first = pairs.get(i);
              for (int j = i + 1; j < size; j++) {
                Pair<BaseWork, BaseWork> second = pairs.get(j);
                if (first.getKey().equals(second.getKey()) && !first.getValue().equals(second.getValue())) {
                  BaseWork work = first.getKey();
                  long estimatedDataSize = getWorkDataSize(work);
                  long threshold = HiveConf.getLongVar(hiveConf, HiveConf.ConfVars.SPARK_DYNAMIC_RDD_CACHING_THRESHOLD);
                  if (estimatedDataSize > threshold) {
                    continue;
                  } else {
                    toCacheWorks.add(work);
                  }
                }
              }
            }
            sparkWork.setCachingWorks(toCacheWorks);
          }
        }
      }
      return null;
    }
  }

  private List<SparkTask> getAllSparkTasks(PhysicalContext pctx) {
    List<SparkTask> sparkTasks = new LinkedList<SparkTask>();
    List<Task<? extends Serializable>> rootTasks = pctx.getRootTasks();
    if (rootTasks != null) {
      for (Task<? extends Serializable> task : rootTasks) {
        getSparkTask(task, sparkTasks);
      }
    }
    return sparkTasks;
  }

  private void getSparkTask(Task task, List<SparkTask> sparkTasks) {
    if (task instanceof SparkTask) {
      sparkTasks.add((SparkTask) task);
      List<Task> childTasks = task.getChildTasks();
      if (childTasks != null) {
        for (Task childTask : childTasks) {
          getSparkTask(childTask, sparkTasks);
        }
      }
    }
  }

  private long getWorkDataSize(BaseWork work) {
    long size = 0;
    Set<Operator<?>> leafOperators = work.getAllLeafOperators();
    for (Operator operator : leafOperators) {
      size += operator.getStatistics().getDataSize();
    }
    return size;
  }
}
