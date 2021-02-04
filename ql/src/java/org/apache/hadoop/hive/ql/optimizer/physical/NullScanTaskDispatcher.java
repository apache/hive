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

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.StringInternUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.NullScanFileSystem;
import org.apache.hadoop.hive.ql.io.OneNullRowInputFormat;
import org.apache.hadoop.hive.ql.io.ZeroRowsInputFormat;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.PreOrderOnceWalker;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.optimizer.physical.MetadataOnlyOptimizer.WalkerCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.NullStructSerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterate over all tasks one by one and removes all input paths from task if
 * conditions as defined in rules match.
 */
public class NullScanTaskDispatcher implements SemanticDispatcher {

  static final Logger LOG =
      LoggerFactory.getLogger(NullScanTaskDispatcher.class);

  private final int maxAsyncLookupCount;
  private final PhysicalContext physicalContext;
  private final Map<SemanticRule, SemanticNodeProcessor> rules;

  public NullScanTaskDispatcher(PhysicalContext context,
      Map<SemanticRule, SemanticNodeProcessor> rules) {
    super();
    this.physicalContext = context;
    this.rules = rules;
    this.maxAsyncLookupCount = physicalContext.getConf()
        .getIntVar(HiveConf.ConfVars.HIVE_COMPUTE_SPLITS_NUM_THREADS);
  }

  private String getAliasForTableScanOperator(MapWork work,
      TableScanOperator tso) {
    for (Map.Entry<String, Operator<? extends OperatorDesc>> entry : work
        .getAliasToWork().entrySet()) {
      if (entry.getValue() == tso) {
        return entry.getKey();
      }
    }
    return null;
  }

  private void lookupAndProcessPath(MapWork work, Path path,
                           Collection<String> aliasesToOptimize) {
    try {
      boolean isEmpty = Utilities.listNonHiddenFileStatus(physicalContext.getConf(), path).length == 0;
      processPath(work, path, aliasesToOptimize, isEmpty);
    } catch (IOException e) {
      LOG.warn("Could not determine if path {} was empty." +
          "Cannot use null scan optimization for this path.", path, e);
    }
  }

  private synchronized void processPath(MapWork work, Path path, Collection<String> aliasesToOptimize,
                                        boolean isEmpty) {
    PartitionDesc partDesc = work.getPathToPartitionInfo().get(path).clone();
    partDesc.setInputFileFormatClass(isEmpty ? ZeroRowsInputFormat.class : OneNullRowInputFormat.class);
    partDesc.setOutputFileFormatClass(HiveIgnoreKeyTextOutputFormat.class);
    partDesc.getProperties().setProperty(serdeConstants.SERIALIZATION_LIB,
        NullStructSerDe.class.getName());
    Path fakePath =
        new Path(NullScanFileSystem.getBase() + partDesc.getTableName()
            + "/part" + encode(partDesc.getPartSpec()));
    StringInternUtils.internUriStringsInPath(fakePath);
    work.addPathToPartitionInfo(fakePath, partDesc);
    work.addPathToAlias(fakePath, new ArrayList<>(aliasesToOptimize));
    Collection<String> aliasesContainingPath = work.getPathToAliases().get(path);
    aliasesContainingPath.removeAll(aliasesToOptimize);
    if (aliasesContainingPath.isEmpty()) {
      work.removePathToAlias(path);
      work.removePathToPartitionInfo(path);
    }
  }

  private void processTableScans(MapWork work, Set<TableScanOperator> tableScans) {
    Map<Path, Boolean> managedEmptyPathMap = new HashMap<>();
    Map<Path, Collection<String>> candidatePathsToAliases = new HashMap<>();
    Map<String, Boolean> aliasTypeMap = new HashMap<>();
    Map<String, Map<Path, Partition>> allowedAliasesToPartitions = new HashMap<>();
    int numberOfUnmanagedPaths = 0;
    for (TableScanOperator tso : tableScans) {
      // use LinkedHashMap<String, Operator<? extends OperatorDesc>>
      // getAliasToWork() should not apply this for non-native table
      if (tso.getConf().getTableMetadata().getStorageHandler() != null) {
        continue;
      }
      String alias = getAliasForTableScanOperator(work, tso);
      boolean isManagedTable = !MetaStoreUtils.isExternalTable(tso.getConf().getTableMetadata().getTTable());
      aliasTypeMap.put(alias, isManagedTable);
      allowedAliasesToPartitions.put(alias, getPathToPartitionMap(alias, tso));
      tso.getConf().setIsMetadataOnly(true);
    }

    for (Path path : work.getPaths()) {
      List<String> aliases = work.getPathToAliases().get(path);
      for (String alias: aliases) {
        Map<Path, Partition> pathToPartitionMap = allowedAliasesToPartitions.get(alias);
        if (pathToPartitionMap == null) {
          continue;
        }
        candidatePathsToAliases.computeIfAbsent(path, k -> new ArrayList<>()).add(alias);
        Partition partitionObject = pathToPartitionMap.get(path);
        Map<String, String> partitionParameters = partitionObject != null ? partitionObject.getParameters() : null;
        boolean isManagedTable = aliasTypeMap.get(alias);
        if (isManagedTable && partitionParameters != null && StatsSetupConst.areBasicStatsUptoDate(partitionParameters)) {
          long rwCount = Long.parseLong(partitionParameters.get(StatsSetupConst.ROW_COUNT));
          if (rwCount == 0) {
            managedEmptyPathMap.put(path, true);
          } else {
            managedEmptyPathMap.put(path, false);
          }
        } else {
          numberOfUnmanagedPaths++;
        }
      }
    }

    int fetcherPoolParallelism = Math.min(maxAsyncLookupCount, numberOfUnmanagedPaths);
    ExecutorService executorService = null;
    if (fetcherPoolParallelism > 1) { // don't create executor service for one partition
      executorService = Executors.newFixedThreadPool(fetcherPoolParallelism,
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("NullScanOptimizer_lookup#%d").build());
    }
    List<Future<?>> lookupFutures = new ArrayList<>(numberOfUnmanagedPaths);
    for (Entry<Path, Collection<String>> entry : candidatePathsToAliases.entrySet()) {
      Path path = entry.getKey();
      Collection<String> allowed = entry.getValue();
      Boolean isEmpty = managedEmptyPathMap.get(path);
      // if isEmpty is null, either stats are not up to date or this is external table
      if (isEmpty == null) {
        if (executorService != null) {
          Future<?> f = executorService.submit(() -> lookupAndProcessPath(work, path, allowed));
          lookupFutures.add(f);
        } else {
          lookupAndProcessPath(work, path, allowed);
        }
      } else {
        processPath(work, path, allowed, isEmpty);
      }
    }
    try {
      for (Future<?> future : lookupFutures) {
          future.get();
      }
    } catch (ExecutionException | InterruptedException e) {
      for (Future<?> f : lookupFutures) {
        f.cancel(true);
      }
      LOG.error("NullScanOptimizer could not complete. It may miss eliminating some null scans", e);
    }
    if (executorService != null) {
      executorService.shutdown();
    }
  }

  private Map<Path, Partition> getPathToPartitionMap(String alias, TableScanOperator tso) {
    Map<Path, Partition> pathToPartitionMap = new HashMap<>();
    try {
      Set<Partition> partitions = physicalContext.getParseContext()
          .getPrunedPartitions(alias, tso).getPartitions();
      for (Partition partition : partitions) {
        pathToPartitionMap.put(partition.getPartitionPath(), partition);
      }
    } catch (SemanticException e) {
      LOG.warn("Error while determining partitions of {}." +
          "We cannot determine its empty partitions from stats.", alias, e);
    }
    return pathToPartitionMap;
  }

  // considered using URLEncoder, but it seemed too much
  private String encode(Map<String, String> partSpec) {
    return partSpec.toString().replaceAll("[{}:/#\\?, ]+", "_");
  }

  @Override
  public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs)
      throws SemanticException {
    Task<?> task = (Task<?>) nd;

    // create a the context for walking operators
    ParseContext parseContext = physicalContext.getParseContext();
    WalkerCtx walkerCtx = new WalkerCtx();

    List<MapWork> mapWorks = new ArrayList<MapWork>(task.getMapWork());
    Collections.sort(mapWorks, new Comparator<MapWork>() {
      @Override
      public int compare(MapWork o1, MapWork o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });

    for (MapWork mapWork : mapWorks) {
      LOG.debug("Looking at: {}", mapWork.getName());
      Collection<Operator<? extends OperatorDesc>> topOperators =
          mapWork.getAliasToWork().values();
      if (topOperators.isEmpty()) {
        LOG.debug("No top operators");
        return null;
      }

      LOG.debug("Looking for table scans where optimization is applicable");

      // The dispatcher fires the processor corresponding to the closest
      // matching rule and passes the context along
      SemanticDispatcher disp = new DefaultRuleDispatcher(null, rules, walkerCtx);
      SemanticGraphWalker ogw = new PreOrderOnceWalker(disp);

      // Create a list of topOp nodes
      ArrayList<Node> topNodes = new ArrayList<>();
      // Get the top Nodes for this task
      Collection<TableScanOperator> topOps = parseContext.getTopOps().values();
      for (Operator<? extends OperatorDesc> workOperator : topOperators) {
        if (topOps.contains(workOperator)) {
          topNodes.add(workOperator);
        }
      }

      Operator<? extends OperatorDesc> reducer = task.getReducer(mapWork);
      if (reducer != null) {
        topNodes.add(reducer);
      }

      ogw.startWalking(topNodes, null);

      int scanTableSize = walkerCtx.getMetadataOnlyTableScans().size();
      LOG.debug("Found {} null table scans", scanTableSize);
      if (scanTableSize > 0) {
        processTableScans(mapWork, walkerCtx.getMetadataOnlyTableScans());
      }
    }
    return null;
  }
}
