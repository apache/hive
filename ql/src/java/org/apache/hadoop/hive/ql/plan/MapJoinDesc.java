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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.MemoryMonitorInfo;
import org.apache.hadoop.hive.ql.optimizer.signature.Signature;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.Explain.Vectorization;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableImplementationType;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.VectorMapJoinVariation;

/**
 * Map Join operator Descriptor implementation.
 *
 */
@Explain(displayName = "Map Join Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class MapJoinDesc extends JoinDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  private Map<Byte, List<ExprNodeDesc>> keys;
  private TableDesc keyTblDesc;
  private List<TableDesc> valueTblDescs;
  private List<TableDesc> valueFilteredTblDescs;

  private int posBigTable;

  private Map<Byte, int[]> valueIndices;
  private Map<Byte, List<Integer>> retainList;

  private transient String bigTableAlias;

  // for tez. used to remember which position maps to which logical input
  // TODO: should these rather be arrays?
  private Map<Integer, String> parentToInput = new HashMap<Integer, String>();
  private Map<Integer, Long> parentKeyCounts = new HashMap<Integer, Long>();
  private Map<Integer, Long> parentDataSizes = new HashMap<Integer, Long>();

  // table alias (small) --> input file name (big) --> target file names (small)
  private Map<String, Map<String, List<String>>> aliasBucketFileNameMapping;
  private Map<String, Integer> bigTableBucketNumMapping;
  private Map<String, List<String>> bigTablePartSpecToFileMapping;

  //map join dump file name
  private String dumpFilePrefix;

  // flag for bucket map join. One usage is to set BucketizedHiveInputFormat
  private boolean isBucketMapJoin;

  // Hash table memory usage allowed; used in case of non-staged mapjoin.
  private float hashtableMemoryUsage;   // This is a percentage value between 0 and 1
  protected boolean genJoinKeys = true;

  private boolean isHybridHashJoin;
  private boolean isDynamicPartitionHashJoin = false;

  public MapJoinDesc() {
    bigTableBucketNumMapping = new LinkedHashMap<String, Integer>();
  }

  public MapJoinDesc(MapJoinDesc clone) {
    super(clone);
    this.keys = clone.keys;
    this.keyTblDesc = clone.keyTblDesc;
    this.valueTblDescs = clone.valueTblDescs;
    this.posBigTable = clone.posBigTable;
    this.valueIndices = clone.valueIndices;
    this.retainList = clone.retainList;
    this.bigTableAlias = clone.bigTableAlias;
    this.aliasBucketFileNameMapping = clone.aliasBucketFileNameMapping;
    this.bigTableBucketNumMapping = clone.bigTableBucketNumMapping;
    this.bigTablePartSpecToFileMapping = clone.bigTablePartSpecToFileMapping;
    this.dumpFilePrefix = clone.dumpFilePrefix;
    this.parentToInput = clone.parentToInput;
    this.parentKeyCounts = clone.parentKeyCounts;
    this.parentDataSizes = clone.parentDataSizes;
    this.isBucketMapJoin = clone.isBucketMapJoin;
    this.isHybridHashJoin = clone.isHybridHashJoin;
  }

  public MapJoinDesc(final Map<Byte, List<ExprNodeDesc>> keys,
    final TableDesc keyTblDesc, final Map<Byte, List<ExprNodeDesc>> values,
    final List<TableDesc> valueTblDescs, final List<TableDesc> valueFilteredTblDescs, List<String> outputColumnNames,
    final int posBigTable, final JoinCondDesc[] conds,
    final Map<Byte, List<ExprNodeDesc>> filters, boolean noOuterJoin, String dumpFilePrefix,
    final MemoryMonitorInfo memoryMonitorInfo, final long inMemoryDataSize) {
    super(values, outputColumnNames, noOuterJoin, conds, filters, null, memoryMonitorInfo);
    this.keys = keys;
    this.keyTblDesc = keyTblDesc;
    this.valueTblDescs = valueTblDescs;
    this.valueFilteredTblDescs = valueFilteredTblDescs;
    this.posBigTable = posBigTable;
    this.bigTableBucketNumMapping = new LinkedHashMap<String, Integer>();
    this.dumpFilePrefix = dumpFilePrefix;
    this.inMemoryDataSize = inMemoryDataSize;
    initRetainExprList();
  }

  private void initRetainExprList() {
    retainList = new HashMap<Byte, List<Integer>>();
    Set<Entry<Byte, List<ExprNodeDesc>>> set = super.getExprs().entrySet();
    Iterator<Entry<Byte, List<ExprNodeDesc>>> setIter = set.iterator();
    while (setIter.hasNext()) {
      Entry<Byte, List<ExprNodeDesc>> current = setIter.next();
      List<Integer> list = new ArrayList<Integer>();
      for (int i = 0; i < current.getValue().size(); i++) {
        list.add(i);
      }
      retainList.put(current.getKey(), list);
    }
  }

  @Explain(displayName = "input vertices", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  @Signature
  public Map<Integer, String> getParentToInput() {
    return parentToInput;
  }

  public void setParentToInput(Map<Integer, String> parentToInput) {
    this.parentToInput = parentToInput;
  }

  public Map<Integer, Long> getParentKeyCounts() {
    return parentKeyCounts;
  }

  public Map<Integer, Long> getParentDataSizes() {
    return parentDataSizes;
  }

  @Explain(displayName = "Estimated key counts", explainLevels = { Level.EXTENDED })
  @Signature
  public String getKeyCountsExplainDesc() {
    StringBuilder result = null;
    for (Map.Entry<Integer, Long> entry : parentKeyCounts.entrySet()) {
      if (result == null) {
        result = new StringBuilder();
      } else {
        result.append(", ");
      }
      result.append(parentToInput.get(entry.getKey())).append(" => ").append(entry.getValue());
    }
    return result == null ? null : result.toString();
  }

  public void setParentKeyCount(Map<Integer, Long> parentKeyCounts) {
    this.parentKeyCounts = parentKeyCounts;
  }

  public Map<Byte, int[]> getValueIndices() {
    return valueIndices;
  }

  public void setValueIndices(Map<Byte, int[]> valueIndices) {
    this.valueIndices = valueIndices;
  }

  public int[] getValueIndex(byte alias) {
    return valueIndices == null ? null : valueIndices.get(alias);
  }

  public Map<Byte, List<Integer>> getRetainList() {
    return retainList;
  }

  public void setRetainList(Map<Byte, List<Integer>> retainList) {
    this.retainList = retainList;
  }

  /**
   * @return the dumpFilePrefix
   */
  public String getDumpFilePrefix() {
    return dumpFilePrefix;
  }

  /**
   * @param dumpFilePrefix
   *          the dumpFilePrefix to set
   */
  public void setDumpFilePrefix(String dumpFilePrefix) {
    this.dumpFilePrefix = dumpFilePrefix;
  }

  /**
   * @return the keys in string form
   */
  @Override
  @Explain(displayName = "keys")
  public Map<Byte, String> getKeysString() {
    Map<Byte, String> keyMap = new LinkedHashMap<Byte, String>();
    for (Map.Entry<Byte, List<ExprNodeDesc>> k: getKeys().entrySet()) {
      keyMap.put(k.getKey(), PlanUtils.getExprListString(k.getValue()));
    }
    return keyMap;
  }

  @Override
  @Explain(displayName = "keys", explainLevels = { Level.USER })
  public Map<Byte, String> getUserLevelExplainKeysString() {
    Map<Byte, String> keyMap = new LinkedHashMap<Byte, String>();
    for (Map.Entry<Byte, List<ExprNodeDesc>> k: getKeys().entrySet()) {
      keyMap.put(k.getKey(), PlanUtils.getExprListString(k.getValue(), true));
    }
    return keyMap;
  }

  /**
   * @return the keys
   */
  public Map<Byte, List<ExprNodeDesc>> getKeys() {
    return keys;
  }

  /**
   * @param keys
   *          the keys to set
   */
  public void setKeys(Map<Byte, List<ExprNodeDesc>> keys) {
    this.keys = keys;
  }

  /**
   * @return the position of the big table not in memory
   */
  @Explain(displayName = "Position of Big Table", explainLevels = { Level.EXTENDED })
  @Signature
  public int getPosBigTable() {
    return posBigTable;
  }

  /**
   * @param posBigTable
   *          the position of the big table not in memory
   */
  public void setPosBigTable(int posBigTable) {
    this.posBigTable = posBigTable;
  }

  /**
   * @return the keyTblDesc
   */
  public TableDesc getKeyTblDesc() {
    return keyTblDesc;
  }

  /**
   * @param keyTblDesc
   *          the keyTblDesc to set
   */
  public void setKeyTblDesc(TableDesc keyTblDesc) {
    this.keyTblDesc = keyTblDesc;
  }

  public List<TableDesc> getValueFilteredTblDescs() {
    return valueFilteredTblDescs;
  }

  public void setValueFilteredTblDescs(List<TableDesc> valueFilteredTblDescs) {
    this.valueFilteredTblDescs = valueFilteredTblDescs;
  }

  /**
   * @return the valueTblDescs
   */
  public List<TableDesc> getValueTblDescs() {
    return valueTblDescs;
  }

  /**
   * @param valueTblDescs
   *          the valueTblDescs to set
   */
  public void setValueTblDescs(List<TableDesc> valueTblDescs) {
    this.valueTblDescs = valueTblDescs;
  }

  /**
   * @return bigTableAlias
   */
  public String getBigTableAlias() {
    return bigTableAlias;
  }

  /**
   * @param bigTableAlias
   */
  public void setBigTableAlias(String bigTableAlias) {
    this.bigTableAlias = bigTableAlias;
  }

  public Map<String, Map<String, List<String>>> getAliasBucketFileNameMapping() {
    return aliasBucketFileNameMapping;
  }

  public void setAliasBucketFileNameMapping(
      Map<String, Map<String, List<String>>> aliasBucketFileNameMapping) {
    this.aliasBucketFileNameMapping = aliasBucketFileNameMapping;
  }

  public Map<String, Integer> getBigTableBucketNumMapping() {
    return bigTableBucketNumMapping;
  }

  public void setBigTableBucketNumMapping(Map<String, Integer> bigTableBucketNumMapping) {
    this.bigTableBucketNumMapping = bigTableBucketNumMapping;
  }

  public Map<String, List<String>> getBigTablePartSpecToFileMapping() {
    return bigTablePartSpecToFileMapping;
  }

  public void setBigTablePartSpecToFileMapping(Map<String, List<String>> partToFileMapping) {
    this.bigTablePartSpecToFileMapping = partToFileMapping;
  }

  @Explain(displayName = "BucketMapJoin", explainLevels = { Level.USER, Level.EXTENDED }, displayOnlyOnTrue = true)
  @Signature
  public boolean isBucketMapJoin() {
    return isBucketMapJoin;
  }

  public void setBucketMapJoin(boolean isBucketMapJoin) {
    this.isBucketMapJoin = isBucketMapJoin;
  }

  @Explain(displayName = "HybridGraceHashJoin", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED }, displayOnlyOnTrue = true)
  public boolean isHybridHashJoin() {
    return isHybridHashJoin;
  }

  public void setHybridHashJoin(boolean isHybridHashJoin) {
    this.isHybridHashJoin = isHybridHashJoin;
  }

  public void setHashTableMemoryUsage(float hashtableMemoryUsage) {
    this.hashtableMemoryUsage = hashtableMemoryUsage;
  }

  public float getHashTableMemoryUsage() {
    return hashtableMemoryUsage;
  }

  @Override
  public boolean isMapSideJoin() {
    return true;
  }

  public void setGenJoinKeys(boolean genJoinKeys) {
    this.genJoinKeys = genJoinKeys;
  }

  public boolean getGenJoinKeys() {
    return genJoinKeys;
  }

  public boolean isDynamicPartitionHashJoin() {
    return isDynamicPartitionHashJoin;
  }

  public void setDynamicPartitionHashJoin(boolean isDistributedHashJoin) {
    this.isDynamicPartitionHashJoin = isDistributedHashJoin;
  }

  // Use LinkedHashSet to give predictable display order.
  private static final Set<String> vectorizableMapJoinNativeEngines =
      new LinkedHashSet<String>(Arrays.asList("tez", "spark"));

  public class MapJoinOperatorExplainVectorization extends OperatorExplainVectorization {

    private final MapJoinDesc mapJoinDesc;
    private final VectorMapJoinDesc vectorMapJoinDesc;
    private final VectorMapJoinInfo vectorMapJoinInfo;

    private VectorizationCondition[] nativeConditions;

    public MapJoinOperatorExplainVectorization(MapJoinDesc mapJoinDesc,
        VectorMapJoinDesc vectorMapJoinDesc) {
      // VectorMapJoinOperator is not native vectorized.
      super(vectorMapJoinDesc, vectorMapJoinDesc.getHashTableImplementationType() != HashTableImplementationType.NONE);
      this.mapJoinDesc = mapJoinDesc;
      this.vectorMapJoinDesc = vectorMapJoinDesc;
      vectorMapJoinInfo =
          (vectorMapJoinDesc != null ? vectorMapJoinDesc.getVectorMapJoinInfo() : null);
    }

    private VectorizationCondition[] createNativeConditions() {

      boolean enabled = vectorMapJoinDesc.getIsVectorizationMapJoinNativeEnabled();

      String engine = vectorMapJoinDesc.getEngine();
      String engineInSupportedCondName =
          HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname + " " + engine + " IN " + vectorizableMapJoinNativeEngines;
      boolean engineInSupported = vectorizableMapJoinNativeEngines.contains(engine);

      boolean isFastHashTableEnabled = vectorMapJoinDesc.getIsFastHashTableEnabled();

      List<VectorizationCondition> conditionList = new ArrayList<VectorizationCondition>();
      conditionList.add(
          new VectorizationCondition(
              vectorMapJoinDesc.getUseOptimizedTable(),
              HiveConf.ConfVars.HIVEMAPJOINUSEOPTIMIZEDTABLE.varname));
      conditionList.add(
          new VectorizationCondition(
              enabled,
              HiveConf.ConfVars.HIVE_VECTORIZATION_MAPJOIN_NATIVE_ENABLED.varname));
      conditionList.add(
          new VectorizationCondition(
              engineInSupported,
              engineInSupportedCondName));
      conditionList.add(
          new VectorizationCondition(
              vectorMapJoinDesc.getOneMapJoinCondition(),
              "One MapJoin Condition"));
      conditionList.add(
          new VectorizationCondition(
              !vectorMapJoinDesc.getHasNullSafes(),
              "No nullsafe"));
      conditionList.add(
          new VectorizationCondition(
              vectorMapJoinDesc.getSmallTableExprVectorizes(),
              "Small table vectorizes"));
      if (!mapJoinDesc.isNoOuterJoin()) {
        conditionList.add(
            new VectorizationCondition(
                !vectorMapJoinDesc.getOuterJoinHasNoKeys(),
                "Outer Join has keys"));
      }

      if (isFastHashTableEnabled) {
        conditionList.add(
            new VectorizationCondition(
                !vectorMapJoinDesc.getIsHybridHashJoin(),
                "Fast Hash Table and No Hybrid Hash Join"));
      } else {
        conditionList.add(
            new VectorizationCondition(
                vectorMapJoinDesc.getSupportsKeyTypes(),
                "Optimized Table and Supports Key Types"));
      }

      VectorizationCondition[] conditions =
          conditionList.toArray(new VectorizationCondition[0]);

      return conditions;
    }

    @Explain(vectorization = Vectorization.OPERATOR, displayName = "nativeConditionsMet", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> getNativeConditionsMet() {
      if (nativeConditions == null) {
        nativeConditions = createNativeConditions();
      }
      return VectorizationCondition.getConditionsMet(nativeConditions);
    }

    @Explain(vectorization = Vectorization.OPERATOR, displayName = "nativeConditionsNotMet", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> getNativeConditionsNotMet() {
      if (nativeConditions == null) {
        nativeConditions = createNativeConditions();
      }
      return VectorizationCondition.getConditionsNotMet(nativeConditions);
    }

    @Explain(vectorization = Vectorization.EXPRESSION, displayName = "bigTableKeyExpressions", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> getBigTableKeyExpressions() {
      return vectorExpressionsToStringList(
          isNative ?
              vectorMapJoinInfo.getSlimmedBigTableKeyExpressions() :
              vectorMapJoinDesc.getAllBigTableKeyExpressions());
    }

    @Explain(vectorization = Vectorization.DETAIL, displayName = "bigTableKeyColumnNums", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getBigTableKeyColumnNums() {
      if (!isNative) {
        return null;
      }
      int[] bigTableKeyColumnMap = vectorMapJoinInfo.getBigTableKeyColumnMap();
      if (bigTableKeyColumnMap.length == 0) {
        return null;
      }
      return Arrays.toString(bigTableKeyColumnMap);
    }

    @Explain(vectorization = Vectorization.EXPRESSION, displayName = "bigTableValueExpressions", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> getBigTableValueExpressions() {
      return vectorExpressionsToStringList(
          isNative ?
              vectorMapJoinInfo.getSlimmedBigTableValueExpressions() :
              vectorMapJoinDesc.getAllBigTableValueExpressions());
    }

    @Explain(vectorization = Vectorization.DETAIL, displayName = "bigTableValueColumnNums", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getBigTableValueColumnNums() {
      if (!isNative) {
        return null;
      }
      int[] bigTableValueColumnMap = vectorMapJoinInfo.getBigTableValueColumnMap();
      if (bigTableValueColumnMap.length == 0) {
        return null;
      }
      return Arrays.toString(bigTableValueColumnMap);
    }

    @Explain(vectorization = Vectorization.DETAIL, displayName = "smallTableMapping", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getSmallTableColumns() {
      if (!isNative) {
        return null;
      }
      return outputColumnsToStringList(vectorMapJoinInfo.getSmallTableMapping());
    }

    @Explain(vectorization = Vectorization.DETAIL, displayName = "projectedOutputColumnNums", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getProjectedOutputColumnNums() {
      if (!isNative) {
        return null;
      }
      return outputColumnsToStringList(vectorMapJoinInfo.getProjectionMapping());
    }

    @Explain(vectorization = Vectorization.DETAIL, displayName = "bigTableOuterKeyMapping", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> getBigTableOuterKey() {
      if (!isNative || vectorMapJoinDesc.getVectorMapJoinVariation() != VectorMapJoinVariation.OUTER) {
        return null;
      }
      return columnMappingToStringList(vectorMapJoinInfo.getBigTableOuterKeyMapping());
    }

    @Explain(vectorization = Vectorization.DETAIL, displayName = "bigTableRetainedColumnNums", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getBigTableRetainedColumnNums() {
      if (!isNative) {
        return null;
      }
      return outputColumnsToStringList(vectorMapJoinInfo.getBigTableRetainedMapping());
    }

    @Explain(vectorization = Vectorization.OPERATOR, displayName = "nativeNotSupportedKeyTypes", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> getNativeNotSupportedKeyTypes() {
      return vectorMapJoinDesc.getNotSupportedKeyTypes();
    }
  }

  @Explain(vectorization = Vectorization.OPERATOR, displayName = "Map Join Vectorization", explainLevels = { Level.DEFAULT, Level.EXTENDED })
  public MapJoinOperatorExplainVectorization getMapJoinVectorization() {
    VectorMapJoinDesc vectorMapJoinDesc = (VectorMapJoinDesc) getVectorDesc();
    if (vectorMapJoinDesc == null || this instanceof SMBJoinDesc) {
      return null;
    }
    return new MapJoinOperatorExplainVectorization(this, vectorMapJoinDesc);
  }

  public class SMBJoinOperatorExplainVectorization extends OperatorExplainVectorization {

    private final SMBJoinDesc smbJoinDesc;
    private final VectorSMBJoinDesc vectorSMBJoinDesc;

    public SMBJoinOperatorExplainVectorization(SMBJoinDesc smbJoinDesc,
        VectorSMBJoinDesc vectorSMBJoinDesc) {
      // Native vectorization NOT supported.
      super(vectorSMBJoinDesc, false);
      this.smbJoinDesc = smbJoinDesc;
      this.vectorSMBJoinDesc = vectorSMBJoinDesc;
    }
  }

  // Handle dual nature.
  @Explain(vectorization = Vectorization.OPERATOR, displayName = "SMB Map Join Vectorization", explainLevels = { Level.DEFAULT, Level.EXTENDED })
  public SMBJoinOperatorExplainVectorization getSMBJoinVectorization() {
    VectorSMBJoinDesc vectorSMBJoinDesc = (VectorSMBJoinDesc) getVectorDesc();
    if (vectorSMBJoinDesc == null || !(this instanceof SMBJoinDesc)) {
      return null;
    }
    return new SMBJoinOperatorExplainVectorization((SMBJoinDesc) this, vectorSMBJoinDesc);
  }

  @Override
  public boolean isSame(OperatorDesc other) {
    if (super.isSame(other)) {
      MapJoinDesc otherDesc = (MapJoinDesc) other;
      return Objects.equals(getParentToInput(), otherDesc.getParentToInput()) &&
          Objects.equals(getKeyCountsExplainDesc(), otherDesc.getKeyCountsExplainDesc()) &&
          getPosBigTable() == otherDesc.getPosBigTable() &&
          isBucketMapJoin() == otherDesc.isBucketMapJoin();
    }
    return false;
  }

}
