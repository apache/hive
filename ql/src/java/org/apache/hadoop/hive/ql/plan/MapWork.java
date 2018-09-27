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

package org.apache.hadoop.hive.ql.plan;

import org.apache.hadoop.hive.common.StringInternUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.IConfigureJobConf;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedSupport.Support;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.optimizer.physical.BucketingSortingCtx.BucketCol;
import org.apache.hadoop.hive.ql.optimizer.physical.BucketingSortingCtx.SortCol;
import org.apache.hadoop.hive.ql.optimizer.physical.VectorizerReason;
import org.apache.hadoop.hive.ql.parse.SplitSample;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.Explain.Vectorization;
import org.apache.hadoop.mapred.JobConf;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Interner;

/**
 * MapWork represents all the information used to run a map task on the cluster.
 * It is first used when the query planner breaks the logical plan into tasks and
 * used throughout physical optimization to track map-side operator plans, input
 * paths, aliases, etc.
 *
 * ExecDriver will serialize the contents of this class and make sure it is
 * distributed on the cluster. The ExecMapper will ultimately deserialize this
 * class on the data nodes and setup it's operator pipeline accordingly.
 *
 * This class is also used in the explain command any property with the
 * appropriate annotation will be displayed in the explain output.
 */
@SuppressWarnings({"serial"})
public class MapWork extends BaseWork {

  public enum LlapIODescriptor {
    DISABLED(null, false),
    NO_INPUTS("no inputs", false),
    UNKNOWN("unknown", false),
    SOME_INPUTS("some inputs", false),
    ACID("may be used (ACID table)", true),
    ALL_INPUTS("all inputs", true),
    CACHE_ONLY("all inputs (cache only)", true);

    final String desc;
    final boolean cached;

    LlapIODescriptor(String desc, boolean cached) {
      this.desc = desc;
      this.cached = cached;
    }

  }

  // use LinkedHashMap to make sure the iteration order is
  // deterministic, to ease testing
  private LinkedHashMap<Path, ArrayList<String>> pathToAliases = new LinkedHashMap<>();

  private LinkedHashMap<Path, PartitionDesc> pathToPartitionInfo = new LinkedHashMap<>();

  private LinkedHashMap<String, Operator<? extends OperatorDesc>> aliasToWork = new LinkedHashMap<String, Operator<? extends OperatorDesc>>();

  private LinkedHashMap<String, PartitionDesc> aliasToPartnInfo = new LinkedHashMap<String, PartitionDesc>();

  private HashMap<String, SplitSample> nameToSplitSample = new LinkedHashMap<String, SplitSample>();

  // If this map task has a FileSinkOperator, and bucketing/sorting metadata can be
  // inferred about the data being written by that operator, these are mappings from the directory
  // that operator writes into to the bucket/sort columns for that data.
  private final Map<String, List<BucketCol>> bucketedColsByDirectory =
      new HashMap<String, List<BucketCol>>();
  private final Map<String, List<SortCol>> sortedColsByDirectory =
      new HashMap<String, List<SortCol>>();

  private Path tmpHDFSPath;

  private Path tmpPathForPartitionPruning;

  private String inputformat;

  private Integer numMapTasks;
  private Long maxSplitSize;
  private Long minSplitSize;
  private Long minSplitSizePerNode;
  private Long minSplitSizePerRack;

  //use sampled partitioning
  private int samplingType;

  public static final int SAMPLING_ON_PREV_MR = 1;  // todo HIVE-3841
  public static final int SAMPLING_ON_START = 2;    // sampling on task running

  // the following two are used for join processing
  private boolean leftInputJoin;
  private String[] baseSrc;
  private List<String> mapAliases;

  private boolean mapperCannotSpanPartns;

  // used to indicate the input is sorted, and so a BinarySearchRecordReader shoudl be used
  private boolean inputFormatSorted = false;

  private boolean useBucketizedHiveInputFormat;

  private boolean dummyTableScan = false;

  // used for dynamic partitioning
  private Map<String, List<TableDesc>> eventSourceTableDescMap =
      new LinkedHashMap<String, List<TableDesc>>();
  private Map<String, List<String>> eventSourceColumnNameMap =
      new LinkedHashMap<String, List<String>>();
  private Map<String, List<String>> eventSourceColumnTypeMap =
      new LinkedHashMap<String, List<String>>();
  private Map<String, List<ExprNodeDesc>> eventSourcePartKeyExprMap =
      new LinkedHashMap<String, List<ExprNodeDesc>>();

  private boolean doSplitsGrouping = true;

  private VectorizedRowBatch vectorizedRowBatch;

  private VectorizerReason notEnabledInputFileFormatReason;

  private Set<String> vectorizationInputFileFormatClassNameSet;
  private List<VectorPartitionDesc> vectorPartitionDescList;
  private List<String> vectorizationEnabledConditionsMet;
  private List<String> vectorizationEnabledConditionsNotMet;

  // bitsets can't be correctly serialized by Kryo's default serializer
  // BitSet::wordsInUse is transient, so force dumping into a lower form
  private byte[] includedBuckets;

  /** Whether LLAP IO will be used for inputs. */
  private LlapIODescriptor llapIoDesc;

  private boolean isMergeFromResolver;

  public MapWork() {}

  public MapWork(String name) {
    super(name);
  }

  @Explain(displayName = "Path -> Alias", explainLevels = { Level.EXTENDED })
  public LinkedHashMap<Path, ArrayList<String>> getPathToAliases() {
    //
    return pathToAliases;
  }

  public void setPathToAliases(final LinkedHashMap<Path, ArrayList<String>> pathToAliases) {
    for (Path p : pathToAliases.keySet()) {
      StringInternUtils.internUriStringsInPath(p);
    }
    this.pathToAliases = pathToAliases;
  }

  public void addPathToAlias(Path path, ArrayList<String> aliases){
    StringInternUtils.internUriStringsInPath(path);
    pathToAliases.put(path, aliases);
  }

  public void addPathToAlias(Path path, String newAlias){
    ArrayList<String> aliases = pathToAliases.get(path);
    if (aliases == null) {
      aliases = new ArrayList<>();
      StringInternUtils.internUriStringsInPath(path);
      pathToAliases.put(path, aliases);
    }
    aliases.add(newAlias.intern());
  }


  public void removePathToAlias(Path path){
    pathToAliases.remove(path);
  }

  /**
   * This is used to display and verify output of "Path -> Alias" in test framework.
   *
   * QTestUtil masks "Path -> Alias" and makes verification impossible.
   * By keeping "Path -> Alias" intact and adding a new display name which is not
   * masked by QTestUtil by removing prefix.
   *
   * Notes: we would still be masking for intermediate directories.
   *
   * @return
   */
  @Explain(displayName = "Truncated Path -> Alias", explainLevels = { Level.EXTENDED })
  public Map<String, ArrayList<String>> getTruncatedPathToAliases() {
    Map<String, ArrayList<String>> trunPathToAliases = new LinkedHashMap<String,
        ArrayList<String>>();
    Iterator<Entry<Path, ArrayList<String>>> itr = this.pathToAliases.entrySet().iterator();
    while (itr.hasNext()) {
      final Entry<Path, ArrayList<String>> entry = itr.next();
      Path origiKey = entry.getKey();
      String newKey = PlanUtils.removePrefixFromWarehouseConfig(origiKey.toString());
      ArrayList<String> value = entry.getValue();
      trunPathToAliases.put(newKey, value);
    }
    return trunPathToAliases;
  }

  @Explain(displayName = "Path -> Partition", explainLevels = { Level.EXTENDED })
  public LinkedHashMap<Path, PartitionDesc> getPathToPartitionInfo() {
    return pathToPartitionInfo;
  }

  public void setPathToPartitionInfo(final LinkedHashMap<Path, PartitionDesc> pathToPartitionInfo) {
    for (Path p : pathToPartitionInfo.keySet()) {
      StringInternUtils.internUriStringsInPath(p);
    }
    this.pathToPartitionInfo = pathToPartitionInfo;
  }

  public void addPathToPartitionInfo(Path path, PartitionDesc partitionInfo) {
    if (pathToPartitionInfo == null) {
      pathToPartitionInfo=new LinkedHashMap<>();
    }
    pathToPartitionInfo.put(path, partitionInfo);
  }

  public void removePathToPartitionInfo(Path path) {
    pathToPartitionInfo.remove(path);
  }

  /**
   * Derive additional attributes to be rendered by EXPLAIN.
   * TODO: this method is relied upon by custom input formats to set jobconf properties.
   *       This is madness? - This is Hive Storage Handlers!
   */
  public void deriveExplainAttributes() {
    if (pathToPartitionInfo != null) {
      for (Map.Entry<Path, PartitionDesc> entry : pathToPartitionInfo.entrySet()) {
        entry.getValue().deriveBaseFileName(entry.getKey());
      }
    }
    MapredLocalWork mapLocalWork = getMapRedLocalWork();
    if (mapLocalWork != null) {
      mapLocalWork.deriveExplainAttributes();
    }
  }

  public void deriveLlap(Configuration conf, boolean isExecDriver) {
    boolean hasLlap = false, hasNonLlap = false, hasAcid = false, hasCacheOnly = false;
    // Assume the IO is enabled on the daemon by default. We cannot reasonably check it here.
    boolean isLlapOn = HiveConf.getBoolVar(conf, ConfVars.LLAP_IO_ENABLED, llapMode);
    boolean canWrapAny = false, doCheckIfs = false;
    if (isLlapOn) {
      // We can wrap inputs if the execution is vectorized, or if we use a wrapper.
      canWrapAny = Utilities.getIsVectorized(conf, this);
      // ExecDriver has no plan path, so we cannot derive VRB stuff for the wrapper.
      if (!canWrapAny && !isExecDriver) {
        canWrapAny = HiveConf.getBoolVar(conf, ConfVars.LLAP_IO_NONVECTOR_WRAPPER_ENABLED);
        doCheckIfs = true;
      }
    }
    boolean hasPathToPartInfo = (pathToPartitionInfo != null && !pathToPartitionInfo.isEmpty());
    if (hasPathToPartInfo) {
      for (PartitionDesc part : pathToPartitionInfo.values()) {
        boolean isUsingLlapIo = canWrapAny && HiveInputFormat.canWrapForLlap(
            part.getInputFileFormatClass(), doCheckIfs);
        if (isUsingLlapIo) {
          if (part.getTableDesc() != null &&
              AcidUtils.isTablePropertyTransactional(part.getTableDesc().getProperties())) {
            hasAcid = true;
          } else {
            hasLlap = true;
          }
        } else if (isLlapOn && HiveInputFormat.canInjectCaches(part.getInputFileFormatClass())) {
          hasCacheOnly = true;
        } else {
          hasNonLlap = true;
        }
      }
    }

    llapIoDesc = deriveLlapIoDescString(
        isLlapOn, canWrapAny, hasPathToPartInfo, hasLlap, hasNonLlap, hasAcid, hasCacheOnly);
  }

  private static LlapIODescriptor deriveLlapIoDescString(boolean isLlapOn, boolean canWrapAny,
      boolean hasPathToPartInfo, boolean hasLlap, boolean hasNonLlap, boolean hasAcid,
      boolean hasCacheOnly) {
    if (!isLlapOn) {
      return LlapIODescriptor.DISABLED; // LLAP IO is off, don't output.
    }
    if (!canWrapAny && !hasCacheOnly) {
      return LlapIODescriptor.NO_INPUTS; //"no inputs"; // Cannot use with input formats.
    }
    if (!hasPathToPartInfo) {
      return LlapIODescriptor.UNKNOWN; //"unknown"; // No information to judge.
    }
    int varieties = (hasAcid ? 1 : 0) + (hasLlap ? 1 : 0) + (hasCacheOnly ? 1 : 0) + (hasNonLlap ? 1 : 0);
    if (varieties > 1) {
      return LlapIODescriptor.SOME_INPUTS; //"some inputs"; // Will probably never actually happen.
    }
    if (hasAcid) {
      return LlapIODescriptor.ACID; //"may be used (ACID table)";
    }
    if (hasLlap) {
      return LlapIODescriptor.ALL_INPUTS;
    }
    if (hasCacheOnly) {
      return LlapIODescriptor.CACHE_ONLY;
    }
    return LlapIODescriptor.NO_INPUTS;
  }

  public void internTable(Interner<TableDesc> interner) {
    if (aliasToPartnInfo != null) {
      for (PartitionDesc part : aliasToPartnInfo.values()) {
        if (part == null) {
          continue;
        }
        part.intern(interner);
      }
    }
    if (pathToPartitionInfo != null) {
      for (PartitionDesc part : pathToPartitionInfo.values()) {
        part.intern(interner);
      }
    }
  }

  /**
   * @return the aliasToPartnInfo
   */
  public LinkedHashMap<String, PartitionDesc> getAliasToPartnInfo() {
    return aliasToPartnInfo;
  }

  /**
   * @param aliasToPartnInfo
   *          the aliasToPartnInfo to set
   */
  public void setAliasToPartnInfo(
      LinkedHashMap<String, PartitionDesc> aliasToPartnInfo) {
    this.aliasToPartnInfo = aliasToPartnInfo;
  }

  public LinkedHashMap<String, Operator<? extends OperatorDesc>> getAliasToWork() {
    return aliasToWork;
  }

  public void setAliasToWork(
      final LinkedHashMap<String, Operator<? extends OperatorDesc>> aliasToWork) {
    this.aliasToWork = aliasToWork;
  }

  @Explain(displayName = "Split Sample", explainLevels = { Level.EXTENDED })
  public HashMap<String, SplitSample> getNameToSplitSample() {
    return nameToSplitSample;
  }

  @Explain(displayName = "LLAP IO", vectorization = Vectorization.SUMMARY_PATH)
  public String getLlapIoDescString() {
    return llapIoDesc.desc;
  }

  public boolean getCacheAffinity() {
    return llapIoDesc.cached;
  }

 public void setNameToSplitSample(HashMap<String, SplitSample> nameToSplitSample) {
    this.nameToSplitSample = nameToSplitSample;
  }

  public Integer getNumMapTasks() {
    return numMapTasks;
  }

  public void setNumMapTasks(Integer numMapTasks) {
    this.numMapTasks = numMapTasks;
  }

  @SuppressWarnings("nls")
  @VisibleForTesting
  public void addMapWork(Path path, String alias, Operator<?> work,
      PartitionDesc pd) {
    StringInternUtils.internUriStringsInPath(path);
    ArrayList<String> curAliases = pathToAliases.get(path);
    if (curAliases == null) {
      assert (pathToPartitionInfo.get(path) == null);
      curAliases = new ArrayList<>();
      pathToAliases.put(path, curAliases);
      pathToPartitionInfo.put(path, pd);
    } else {
      assert (pathToPartitionInfo.get(path) != null);
    }

    for (String oneAlias : curAliases) {
      if (oneAlias.equals(alias)) {
        throw new RuntimeException("Multiple aliases named: " + alias
            + " for path: " + path);
      }
    }
    curAliases.add(alias);

    if (aliasToWork.get(alias) != null) {
      throw new RuntimeException("Existing work for alias: " + alias);
    }
    aliasToWork.put(alias, work);
  }

  public boolean isInputFormatSorted() {
    return inputFormatSorted;
  }

  public void setInputFormatSorted(boolean inputFormatSorted) {
    this.inputFormatSorted = inputFormatSorted;
  }

  public void resolveDynamicPartitionStoredAsSubDirsMerge(HiveConf conf, Path path,
      TableDesc tblDesc, ArrayList<String> aliases, PartitionDesc partDesc) {
    StringInternUtils.internUriStringsInPath(path);
    pathToAliases.put(path, aliases);
    pathToPartitionInfo.put(path, partDesc);
  }

  /**
   * For each map side operator - stores the alias the operator is working on
   * behalf of in the operator runtime state. This is used by reduce sink
   * operator - but could be useful for debugging as well.
   */
  private void setAliases() {
    if(aliasToWork == null) {
      return;
    }
    for (String oneAlias : aliasToWork.keySet()) {
      aliasToWork.get(oneAlias).setAlias(oneAlias);
    }
  }

  @Explain(displayName = "Execution mode", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED },
      vectorization = Vectorization.SUMMARY_PATH)
  public String getExecutionMode() {
    if (vectorMode &&
        !(getIsTestForcedVectorizationEnable() &&
          getIsTestVectorizationSuppressExplainExecutionMode())) {
      if (llapMode) {
        if (uberMode) {
          return "vectorized, uber";
        } else {
          return "vectorized, llap";
        }
      } else {
        return "vectorized";
      }
    } else if (llapMode) {
      return uberMode? "uber" : "llap";
    }
    return null;
  }

  @Override
  public void replaceRoots(Map<Operator<?>, Operator<?>> replacementMap) {
    LinkedHashMap<String, Operator<?>> newAliasToWork = new LinkedHashMap<String, Operator<?>>();

    for (Map.Entry<String, Operator<?>> entry: aliasToWork.entrySet()) {
      newAliasToWork.put(entry.getKey(), replacementMap.get(entry.getValue()));
    }

    setAliasToWork(newAliasToWork);
  }

  @Override
  @Explain(displayName = "Map Operator Tree", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED },
      vectorization = Vectorization.OPERATOR_PATH)
  public Set<Operator<? extends OperatorDesc>> getAllRootOperators() {
    Set<Operator<?>> opSet = new LinkedHashSet<Operator<?>>();

    for (Operator<?> op : getAliasToWork().values()) {
      opSet.add(op);
    }
    return opSet;
  }

  @Override
  public Operator<? extends OperatorDesc> getAnyRootOperator() {
    return aliasToWork.isEmpty() ? null : aliasToWork.values().iterator().next();
  }

  public void mergeAliasedInput(String alias, Path pathDir, PartitionDesc partitionInfo) {
    StringInternUtils.internUriStringsInPath(pathDir);
    alias = alias.intern();
    ArrayList<String> aliases = pathToAliases.get(pathDir);
    if (aliases == null) {
      aliases = new ArrayList<>(Arrays.asList(alias));
      pathToAliases.put(pathDir, aliases);
      pathToPartitionInfo.put(pathDir, partitionInfo);
    } else {
      aliases.add(alias);
    }
  }

  public void initialize() {
    setAliases();
  }

  public Long getMaxSplitSize() {
    return maxSplitSize;
  }

  public void setMaxSplitSize(Long maxSplitSize) {
    this.maxSplitSize = maxSplitSize;
  }

  public Long getMinSplitSize() {
    return minSplitSize;
  }

  public void setMinSplitSize(Long minSplitSize) {
    this.minSplitSize = minSplitSize;
  }

  public Long getMinSplitSizePerNode() {
    return minSplitSizePerNode;
  }

  public void setMinSplitSizePerNode(Long minSplitSizePerNode) {
    this.minSplitSizePerNode = minSplitSizePerNode;
  }

  public Long getMinSplitSizePerRack() {
    return minSplitSizePerRack;
  }

  public void setMinSplitSizePerRack(Long minSplitSizePerRack) {
    this.minSplitSizePerRack = minSplitSizePerRack;
  }

  public String getInputformat() {
    return inputformat;
  }

  public void setInputformat(String inputformat) {
    this.inputformat = inputformat;
  }

  public boolean isUseBucketizedHiveInputFormat() {
    return useBucketizedHiveInputFormat;
  }

  public void setUseBucketizedHiveInputFormat(boolean useBucketizedHiveInputFormat) {
    this.useBucketizedHiveInputFormat = useBucketizedHiveInputFormat;
  }

  public void setMapperCannotSpanPartns(boolean mapperCannotSpanPartns) {
    this.mapperCannotSpanPartns = mapperCannotSpanPartns;
  }

  public boolean isMapperCannotSpanPartns() {
    return this.mapperCannotSpanPartns;
  }

  public ArrayList<String> getAliases() {
    return new ArrayList<String>(aliasToWork.keySet());
  }

  public ArrayList<Operator<?>> getWorks() {
    return new ArrayList<Operator<?>>(aliasToWork.values());
  }

  public ArrayList<Path> getPaths() {
    return new ArrayList<Path>(pathToAliases.keySet());
  }

  public ArrayList<PartitionDesc> getPartitionDescs() {
    return new ArrayList<PartitionDesc>(aliasToPartnInfo.values());
  }

  public Path getTmpHDFSPath() {
    return tmpHDFSPath;
  }

  public void setTmpHDFSPath(Path tmpHDFSPath) {
    this.tmpHDFSPath = tmpHDFSPath;
  }

  public Path getTmpPathForPartitionPruning() {
    return this.tmpPathForPartitionPruning;
  }

  public void setTmpPathForPartitionPruning(Path tmpPathForPartitionPruning) {
    this.tmpPathForPartitionPruning = tmpPathForPartitionPruning;
  }

  public void mergingInto(MapWork mapWork) {
    // currently, this is sole field affecting mergee task
    mapWork.useBucketizedHiveInputFormat |= useBucketizedHiveInputFormat;
  }

  @Explain(displayName = "Path -> Bucketed Columns", explainLevels = { Level.EXTENDED })
  public Map<String, List<BucketCol>> getBucketedColsByDirectory() {
    return bucketedColsByDirectory;
  }

  @Explain(displayName = "Path -> Sorted Columns", explainLevels = { Level.EXTENDED })
  public Map<String, List<SortCol>> getSortedColsByDirectory() {
    return sortedColsByDirectory;
  }

  public int getSamplingType() {
    return samplingType;
  }

  public void setSamplingType(int samplingType) {
    this.samplingType = samplingType;
  }

  @Explain(displayName = "Sampling", explainLevels = { Level.EXTENDED })
  public String getSamplingTypeString() {
    return samplingType == 1 ? "SAMPLING_ON_PREV_MR" :
        samplingType == 2 ? "SAMPLING_ON_START" : null;
  }

  @Override
  public void configureJobConf(JobConf job) {
    for (PartitionDesc partition : aliasToPartnInfo.values()) {
      PlanUtils.configureJobConf(partition.getTableDesc(), job);
    }
    Collection<Operator<?>> mappers = aliasToWork.values();
    for (FileSinkOperator fs : OperatorUtils.findOperators(mappers, FileSinkOperator.class)) {
      PlanUtils.configureJobConf(fs.getConf().getTableInfo(), job);
    }
    for (IConfigureJobConf icjc : OperatorUtils.findOperators(mappers, IConfigureJobConf.class)) {
      icjc.configureJobConf(job);
    }
  }

  public void setDummyTableScan(boolean dummyTableScan) {
    this.dummyTableScan = dummyTableScan;
  }

  public boolean getDummyTableScan() {
    return dummyTableScan;
  }

  public void setEventSourceTableDescMap(Map<String, List<TableDesc>> map) {
    this.eventSourceTableDescMap = map;
  }

  public Map<String, List<TableDesc>> getEventSourceTableDescMap() {
    return eventSourceTableDescMap;
  }

  public void setEventSourceColumnNameMap(Map<String, List<String>> map) {
    this.eventSourceColumnNameMap = map;
  }

  public Map<String, List<String>> getEventSourceColumnNameMap() {
    return eventSourceColumnNameMap;
  }

  public Map<String, List<String>> getEventSourceColumnTypeMap() {
    return eventSourceColumnTypeMap;
  }

  public void setEventSourceColumnTypeMap(Map<String, List<String>> eventSourceColumnTypeMap) {
    this.eventSourceColumnTypeMap = eventSourceColumnTypeMap;
 }

  public Map<String, List<ExprNodeDesc>> getEventSourcePartKeyExprMap() {
    return eventSourcePartKeyExprMap;
  }

  public void setEventSourcePartKeyExprMap(Map<String, List<ExprNodeDesc>> map) {
    this.eventSourcePartKeyExprMap = map;
  }

  public void setDoSplitsGrouping(boolean doSplitsGrouping) {
    this.doSplitsGrouping = doSplitsGrouping;
  }

  public boolean getDoSplitsGrouping() {
    return this.doSplitsGrouping;
  }

  public boolean isLeftInputJoin() {
    return leftInputJoin;
  }

  public void setLeftInputJoin(boolean leftInputJoin) {
    this.leftInputJoin = leftInputJoin;
  }

  public String[] getBaseSrc() {
    return baseSrc;
  }

  public void setBaseSrc(String[] baseSrc) {
    this.baseSrc = baseSrc;
  }

  public List<String> getMapAliases() {
    return mapAliases;
  }

  public void setMapAliases(List<String> mapAliases) {
    this.mapAliases = mapAliases;
  }

  public BitSet getIncludedBuckets() {
    return includedBuckets != null ? BitSet.valueOf(includedBuckets) : null;
  }

  public void setIncludedBuckets(BitSet includedBuckets) {
    // see comment next to the field
    this.includedBuckets = includedBuckets == null ? null : includedBuckets.toByteArray();
  }

  public void setVectorizedRowBatch(VectorizedRowBatch vectorizedRowBatch) {
    this.vectorizedRowBatch = vectorizedRowBatch;
  }

  public VectorizedRowBatch getVectorizedRowBatch() {
    return vectorizedRowBatch;
  }

  public void setIsMergeFromResolver(boolean b) {
    this.isMergeFromResolver = b;
  }

  public boolean isMergeFromResolver() {
    return this.isMergeFromResolver;
  }

  /*
   * Whether the HiveConf.ConfVars.HIVE_VECTORIZATION_USE_VECTORIZED_INPUT_FILE_FORMAT variable
   * (hive.vectorized.use.vectorized.input.format) was true when the Vectorizer class evaluated
   * vectorizing this node.
   *
   * When Vectorized Input File Format looks at this flag, it can determine whether it should
   * operate vectorized or not.  In some modes, the node can be vectorized but use row
   * serialization.
   */
  public void setUseVectorizedInputFileFormat(boolean useVectorizedInputFileFormat) {
    this.useVectorizedInputFileFormat = useVectorizedInputFileFormat;
  }

  public boolean getUseVectorizedInputFileFormat() {
    return useVectorizedInputFileFormat;
  }

  public void setInputFormatSupportSet(Set<Support> inputFormatSupportSet) {
    this.inputFormatSupportSet = inputFormatSupportSet;
  }

  public Set<Support> getInputFormatSupportSet() {
    return inputFormatSupportSet;
  }

  public void setSupportSetInUse(Set<Support> supportSetInUse) {
    this.supportSetInUse = supportSetInUse;
  }

  public Set<Support> getSupportSetInUse() {
    return supportSetInUse;
  }

  public void setSupportRemovedReasons(List<String> supportRemovedReasons) {
    this.supportRemovedReasons =supportRemovedReasons;
  }

  public List<String> getSupportRemovedReasons() {
    return supportRemovedReasons;
  }

  public void setNotEnabledInputFileFormatReason(VectorizerReason notEnabledInputFileFormatReason) {
    this.notEnabledInputFileFormatReason = notEnabledInputFileFormatReason;
  }

  public VectorizerReason getNotEnabledInputFileFormatReason() {
    return notEnabledInputFileFormatReason;
  }

  public void setVectorizationInputFileFormatClassNameSet(Set<String> vectorizationInputFileFormatClassNameSet) {
    this.vectorizationInputFileFormatClassNameSet = vectorizationInputFileFormatClassNameSet;
  }

  public Set<String> getVectorizationInputFileFormatClassNameSet() {
    return vectorizationInputFileFormatClassNameSet;
  }

  public void setVectorPartitionDescList(List<VectorPartitionDesc> vectorPartitionDescList) {
    this.vectorPartitionDescList = vectorPartitionDescList;
  }

  public List<VectorPartitionDesc> getVectorPartitionDescList() {
    return vectorPartitionDescList;
  }

  public void setVectorizationEnabledConditionsMet(ArrayList<String> vectorizationEnabledConditionsMet) {
    this.vectorizationEnabledConditionsMet = vectorizationEnabledConditionsMet == null ? null : VectorizationCondition.addBooleans(
            vectorizationEnabledConditionsMet, true);
  }

  public List<String> getVectorizationEnabledConditionsMet() {
    return vectorizationEnabledConditionsMet;
  }

  public void setVectorizationEnabledConditionsNotMet(List<String> vectorizationEnabledConditionsNotMet) {
    this.vectorizationEnabledConditionsNotMet = vectorizationEnabledConditionsNotMet == null ? null : VectorizationCondition.addBooleans(
            vectorizationEnabledConditionsNotMet, false);
  }

  public List<String> getVectorizationEnabledConditionsNotMet() {
    return vectorizationEnabledConditionsNotMet;
  }

  public class MapExplainVectorization extends BaseExplainVectorization {

    private final MapWork mapWork;

    public MapExplainVectorization(MapWork mapWork) {
      super(mapWork);
      this.mapWork = mapWork;
    }

    @Explain(vectorization = Vectorization.SUMMARY, displayName = "inputFileFormats", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public Set<String> inputFileFormats() {
      return mapWork.getVectorizationInputFileFormatClassNameSet();
    }

    /*
    // Too many Q out file changes for the moment...
    @Explain(vectorization = Vectorization.DETAIL, displayName = "vectorPartitionDescs", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String vectorPartitionDescs() {
      return mapWork.getVectorPartitionDescList().toString();
    }
    */

    @Explain(vectorization = Vectorization.SUMMARY, displayName = "inputFormatFeatureSupport", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getInputFormatSupport() {
      Set<Support> inputFormatSupportSet = mapWork.getInputFormatSupportSet();
      if (inputFormatSupportSet == null) {
        return null;
      }
      return inputFormatSupportSet.toString();
    }

    @Explain(vectorization = Vectorization.SUMMARY, displayName = "featureSupportInUse", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getVectorizationSupportInUse() {
      Set<Support> supportSet = mapWork.getSupportSetInUse();
      if (supportSet == null) {
        return null;
      }
      return supportSet.toString();
    }

    @Explain(vectorization = Vectorization.SUMMARY, displayName = "vectorizationSupportRemovedReasons", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getSupportRemovedReasons() {
      List<String> supportRemovedReasons = mapWork.getSupportRemovedReasons();
      if (supportRemovedReasons == null || supportRemovedReasons.isEmpty()) {
        return null;
      }
      return supportRemovedReasons.toString();
    }

    @Explain(vectorization = Vectorization.SUMMARY, displayName = "enabledConditionsMet", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> enabledConditionsMet() {
      return mapWork.getVectorizationEnabledConditionsMet();
    }

    @Explain(vectorization = Vectorization.SUMMARY, displayName = "enabledConditionsNotMet", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> enabledConditionsNotMet() {
      return mapWork.getVectorizationEnabledConditionsNotMet();
    }
  }

  @Explain(vectorization = Vectorization.SUMMARY, displayName = "Map Vectorization", explainLevels = { Level.DEFAULT, Level.EXTENDED })
  public MapExplainVectorization getMapExplainVectorization() {
    if (!getVectorizationExamined()) {
      return null;
    }
    return new MapExplainVectorization(this);
  }
}
