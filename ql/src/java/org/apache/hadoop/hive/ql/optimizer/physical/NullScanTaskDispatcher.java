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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StringInternUtils;
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

  private final PhysicalContext physicalContext;
  private final Map<SemanticRule, SemanticNodeProcessor> rules;

  public NullScanTaskDispatcher(PhysicalContext context,
      Map<SemanticRule, SemanticNodeProcessor> rules) {
    super();
    this.physicalContext = context;
    this.rules = rules;
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

  private PartitionDesc changePartitionToMetadataOnly(PartitionDesc desc,
      Path path) {
    if (desc == null) {
      return null;
    }
    boolean isEmpty = false;
    try {
      isEmpty = Utilities.isEmptyPath(physicalContext.getConf(), path);
    } catch (IOException e) {
      LOG.error("Cannot determine if the table is empty", e);
    }
    desc.setInputFileFormatClass(
        isEmpty ? ZeroRowsInputFormat.class : OneNullRowInputFormat.class);
    desc.setOutputFileFormatClass(HiveIgnoreKeyTextOutputFormat.class);
    desc.getProperties().setProperty(serdeConstants.SERIALIZATION_LIB,
        NullStructSerDe.class.getName());
    return desc;
  }

  private void processAlias(MapWork work, Path path,
      Collection<String> aliasesAffected, Set<String> aliases) {
    // the aliases that are allowed to map to a null scan.
    Collection<String> allowed = aliasesAffected.stream()
        .filter(a -> aliases.contains(a)).collect(Collectors.toList());
    if (!allowed.isEmpty()) {
      PartitionDesc partDesc = work.getPathToPartitionInfo().get(path).clone();
      PartitionDesc newPartition =
          changePartitionToMetadataOnly(partDesc, path);
      // Prefix partition with something to avoid it being a hidden file.
      Path fakePath =
          new Path(NullScanFileSystem.getBase() + newPartition.getTableName()
              + "/part" + encode(newPartition.getPartSpec()));
      StringInternUtils.internUriStringsInPath(fakePath);
      work.addPathToPartitionInfo(fakePath, newPartition);
      work.addPathToAlias(fakePath, new ArrayList<>(allowed));
      aliasesAffected.removeAll(allowed);
      if (aliasesAffected.isEmpty()) {
        work.removePathToAlias(path);
        work.removePathToPartitionInfo(path);
      }
    }
  }

  private void processAlias(MapWork work, Set<TableScanOperator> tableScans) {
    Set<String> aliases = new HashSet<>();
    for (TableScanOperator tso : tableScans) {
      // use LinkedHashMap<String, Operator<? extends OperatorDesc>>
      // getAliasToWork() should not apply this for non-native table
      if (tso.getConf().getTableMetadata().getStorageHandler() != null) {
        continue;
      }
      String alias = getAliasForTableScanOperator(work, tso);
      aliases.add(alias);
      tso.getConf().setIsMetadataOnly(true);
    }
    // group path alias according to work
    Map<Path, List<String>> candidates = new HashMap<>();
    for (Path path : work.getPaths()) {
      List<String> aliasesAffected = work.getPathToAliases().get(path);
      if (CollectionUtils.isNotEmpty(aliasesAffected)) {
        candidates.put(path, aliasesAffected);
      }
    }
    for (Entry<Path, List<String>> entry : candidates.entrySet()) {
      processAlias(work, entry.getKey(), entry.getValue(), aliases);
    }
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
        processAlias(mapWork, walkerCtx.getMetadataOnlyTableScans());
      }
    }
    return null;
  }
}
