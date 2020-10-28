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
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StringInternUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
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
  private final float minRecursiveListingRatio;

  public NullScanTaskDispatcher(PhysicalContext context,
      Map<SemanticRule, SemanticNodeProcessor> rules) {
    super();
    this.physicalContext = context;
    this.rules = rules;
    this.minRecursiveListingRatio = HiveConf.getFloatVar(physicalContext.getConf(),
        HiveConf.ConfVars.HIVENULLSCAN_RECURSIVE_LISTING_RATIO);
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

  private PartitionDesc maybeChangePartitionToMetadataOnly(PartitionDesc desc, Path path, Set<Path> notAllowed, Set<Path> nonEmptyDirs) {
    if (notAllowed.contains(path)) {
      return desc;
    }
    if (nonEmptyDirs.contains(path)) {
      desc.setInputFileFormatClass(OneNullRowInputFormat.class);
    } else {
      desc.setInputFileFormatClass(ZeroRowsInputFormat.class);
    }
    desc.setOutputFileFormatClass(HiveIgnoreKeyTextOutputFormat.class);
    desc.getProperties().setProperty(serdeConstants.SERIALIZATION_LIB,
        NullStructSerDe.class.getName());
    return desc;
  }

  private PartitionDesc maybeChangePartitionToMetadataOnly(PartitionDesc desc,
                                                           Path path) {
    FileStatus[] filesFoundInPartitionDir = null;
    try {
      filesFoundInPartitionDir = Utilities.listNonHiddenFileStatus(physicalContext.getConf(), path);
    } catch (IOException e) {
      LOG.error("Cannot determine if the table is empty", e);
    }
    if (!isMetadataOnlyAllowed(filesFoundInPartitionDir)) {
      return desc;
    }

    boolean isEmpty = filesFoundInPartitionDir == null || filesFoundInPartitionDir.length == 0;
    desc.setInputFileFormatClass(isEmpty ? ZeroRowsInputFormat.class : OneNullRowInputFormat.class);
    desc.setOutputFileFormatClass(HiveIgnoreKeyTextOutputFormat.class);
    desc.getProperties().setProperty(serdeConstants.SERIALIZATION_LIB,
        NullStructSerDe.class.getName());
    return desc;
  }

  private boolean isMetadataOnlyAllowed(FileStatus[] filesFoundInPartitionDir) {
    if (filesFoundInPartitionDir == null || filesFoundInPartitionDir.length == 0) {
      return true; // empty folders are safe to convert to metadata-only
    }
    for (FileStatus f : filesFoundInPartitionDir) {
      if (AcidUtils.isDeleteDelta(f.getPath())) {
        /*
         * as described in HIVE-23712, an acid partition is not a safe subject of metadata-only
         * optimization, because there is a chance that it contains no data but contains folders
         * (e.g: delta_0000002_0000002_0000, delete_delta_0000003_0000003_0000), without scanning
         * the underlying file contents, we cannot tell whether this partition contains data or not
         */
        return false;
      }
    }
    return true;
  }

  private void processPaths(MapWork work, List<Path> queriedPaths,
                            Map<Path, List<String>> pathToAffectedAliases,
                            Set<String> tableScanAliases,
                            final boolean useRecursiveListing, Path parent) {
    Set<Path> metadataOnlyNotAllowed = null;
    Set<Path> nonEmptyPartitionDirs = null;
    if (useRecursiveListing) {
      metadataOnlyNotAllowed = new HashSet<>();
      nonEmptyPartitionDirs = new HashSet<>();
      fillMetadataFromRecursiveListing(parent, metadataOnlyNotAllowed, nonEmptyPartitionDirs);
    }
    for (Path partitionDir: queriedPaths) {
      Collection<String> allowed = pathToAffectedAliases.get(partitionDir).stream()
          .filter(a -> tableScanAliases.contains(a)).collect(Collectors.toList());
      if (!allowed.isEmpty()) {
        PartitionDesc partDesc = work.getPathToPartitionInfo().get(partitionDir).clone();
        PartitionDesc newPartition;
        if (useRecursiveListing) {
          newPartition = maybeChangePartitionToMetadataOnly(partDesc, partitionDir,
              metadataOnlyNotAllowed, nonEmptyPartitionDirs);
        } else {
          newPartition = maybeChangePartitionToMetadataOnly(partDesc, partitionDir);
        }
        Path fakePath =
            new Path(NullScanFileSystem.getBase() + newPartition.getTableName()
                + "/part" + encode(newPartition.getPartSpec()));
        StringInternUtils.internUriStringsInPath(fakePath);
        work.addPathToPartitionInfo(fakePath, newPartition);
        work.addPathToAlias(fakePath, new ArrayList<>(allowed));
        pathToAffectedAliases.get(partitionDir).removeAll(allowed);
        if (pathToAffectedAliases.get(partitionDir).isEmpty()) {
          work.removePathToAlias(partitionDir);
          work.removePathToPartitionInfo(partitionDir);
        }
      }
    }
  }

  private void addToParentToPathMap(Map<Path, List<Path>> map, Path path) {
    Path parent = path.getParent();
    List<Path> paths = map.get(parent);
    if (paths == null) {
      paths = new ArrayList<>();
      map.put(parent, paths);
    }
    paths.add(path);
  }

  private void processTableScans(MapWork work, Set<TableScanOperator> tableScans) {
    Set<String> allAliasesInMapWork = new HashSet<>();
    for (TableScanOperator tso : tableScans) {
      // use LinkedHashMap<String, Operator<? extends OperatorDesc>>
      // getAliasToWork() should not apply this for non-native table
      if (tso.getConf().getTableMetadata().getStorageHandler() != null) {
        continue;
      }
      String alias = getAliasForTableScanOperator(work, tso);
      allAliasesInMapWork.add(alias);
      tso.getConf().setIsMetadataOnly(true);
    }
    // group path alias according to work
    Map<Path, List<String>> pathToAffectedAliasesMap = new HashMap<>();
    Map<Path, List<Path>> parentToPathMap = new HashMap<>();
    for (Path path : work.getPaths()) {
      List<String> aliasesAffectedByPathScan = work.getPathToAliases().get(path);
      if (CollectionUtils.isNotEmpty(aliasesAffectedByPathScan)) {
        pathToAffectedAliasesMap.put(path, aliasesAffectedByPathScan);
        addToParentToPathMap(parentToPathMap, path);
      }
    }

    for (Map.Entry<Path, List<Path>> parentToChildrenEntry : parentToPathMap.entrySet()) {
      Path parent = parentToChildrenEntry.getKey();
      List<Path> children = parentToChildrenEntry.getValue();
      // if we need to list only a few partitions, we will listStatus them one
      // by one. Otherwise, we will do a recursive file listing on table directory
      // to save round trips
      boolean useRecursiveListing = false;
      // If there is only one directory, just get the listing. It would be
      // a waste to do extra listing on its parent directory.
      if (children.size() > 1) {
        try {
          FileStatus[] fileStatuses = Utilities.listNonHiddenFileStatus(physicalContext.getConf(), parent);
          if (fileStatuses.length * minRecursiveListingRatio < children.size()) {
            useRecursiveListing = true;
          }
        } catch (IOException e) {
          LOG.warn("Cannot query parent directory of data files: {}", parent);
        }
      }
      processPaths(work, children, pathToAffectedAliasesMap, allAliasesInMapWork, useRecursiveListing, parent);
    }
  }

  private void fillMetadataFromRecursiveListing(Path parent,Set<Path> metadataOnlyNotAllowed,
                                                Set<Path> nonEmptyPartitionDirs) {
    try {
      FileSystem fs = parent.getFileSystem(physicalContext.getConf());
      RemoteIterator<LocatedFileStatus> it = fs.listFiles(parent, true);
      while (it.hasNext()) {
        LocatedFileStatus fileStatus = it.next();
        if (!FileUtils.HIDDEN_FILES_PATH_FILTER.accept(fileStatus.getPath())) {
          continue;
        }
        Path cur = fileStatus.getPath().getParent();
        // mark non empty directories from here all the way to
        // parent. Unless the table is non-acid and oddly nested
        // the following loop will not loop more than once
        while (!cur.equals(parent)) {
          if (AcidUtils.isDeleteDelta(cur)) {
            metadataOnlyNotAllowed.add(cur.getParent());
          }
          nonEmptyPartitionDirs.add(cur);
          cur = cur.getParent();
        }
      }
    } catch (IOException e) {
      LOG.error("Cannot determine if subdirectories of {} are empty." +
          "Bailing out of null scan optimizer for this table. Caused by {}", parent, e);
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
        processTableScans(mapWork, walkerCtx.getMetadataOnlyTableScans());
      }
    }
    return null;
  }
}
