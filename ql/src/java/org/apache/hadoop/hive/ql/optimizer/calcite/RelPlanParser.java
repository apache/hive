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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveCostModel;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveDefaultCostModel;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveOnTezCostModel;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveGroupingID;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * Class responsible for parsing a given plan from a json file.
 */
public class RelPlanParser {
  private static final TypeReference<LinkedHashMap<String, Object>> TYPE_REF =
      new TypeReference<LinkedHashMap<String, Object>>() {
      };

  private final RelOptCluster cluster;
  private final RelOptHiveTableFactory relOptHiveTableFactory;
  private final Map<String, PrunedPartitionList> partitionCache;
  private final HiveRelJson relJson = new HiveRelJson(null);
  private final Map<String, RelNode> relMap = new LinkedHashMap<>();
  private RelNode lastRel;

  public RelPlanParser(RelOptCluster cluster,
                       RelOptHiveTableFactory relOptHiveTableFactory,
                       Map<String, PrunedPartitionList> partitionCache) {
    this.cluster = cluster;
    this.relOptHiveTableFactory = relOptHiveTableFactory;
    this.partitionCache = partitionCache;
  }

  public RelNode parse(String json) throws IOException {
    byte[] mapData = json.getBytes(Charset.defaultCharset());
    return read(new String(mapData, Charset.defaultCharset()));
  }

  public RelNode read(String s) throws IOException {
    JsonFactory jsonFactory = new JsonFactory();
    Map<String, Object> o;
    try (JsonParser parser = jsonFactory.createParser(s)) {
      parser.nextToken();
      assert parser.currentToken() == JsonToken.START_OBJECT;
      parser.nextToken();
      assert parser.currentToken() == JsonToken.FIELD_NAME;
      parser.nextToken();
      assert parser.currentToken() == JsonToken.VALUE_STRING;

      lastRel = null;
      final ObjectMapper mapper = new ObjectMapper();
      o = mapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
          .readValue(parser.getValueAsString(), TYPE_REF);
    } catch (AssertionError e) {
      throw new IOException(e);
    }
    @SuppressWarnings("unchecked") final List<Map<String, Object>> rels = (List) o.get("rels");
    readRels(rels);
    return lastRel;
  }

  private void readRels(List<Map<String, Object>> jsonRels) {
    for (Map<String, Object> jsonRel : jsonRels) {
      readRel(jsonRel);
    }
  }

  private void readRel(final Map<String, Object> jsonRel) {
    String id = (String) jsonRel.get("id");
    String type = (String) jsonRel.get("relOp");
    Constructor constructor = relJson.getConstructor(type);
    RelInput input = new HiveRelInput(jsonRel);
    try {
      final HiveRelNode rel = (HiveRelNode) constructor.newInstance(input);
      relMap.put(id, rel);
      lastRel = rel;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      final Throwable e2 = e.getCause();
      if (e2 instanceof RuntimeException) {
        throw (RuntimeException) e2;
      }
      throw new RuntimeException(e2);
    }
  }

  AggregateCall toAggCall(RelInput input, Map<String, Object> jsonAggCall) {
    final Map aggMap = (Map) jsonAggCall.get("agg");
    final String aggName = (String) aggMap.get("name");
    final Boolean distinct = (Boolean) jsonAggCall.get("distinct");
    @SuppressWarnings("unchecked") final List<Integer> operands = (List<Integer>) jsonAggCall.get("operands");
    final Integer filterOperand = (Integer) jsonAggCall.get("filter");
    final RelDataType type = relJson.toType(input.getCluster().getTypeFactory(), jsonAggCall.get("type"));
    final RelCollation collation = jsonAggCall.containsKey("collation") ?
        relJson.toCollation((List<Map<String, Object>>) jsonAggCall.get("collation")) :
        RelCollations.EMPTY;

    // GROUPING__ID requires special handling, otherwise this will create different
    // optimized AST.
    // tok_selexpr (tok_functionstar grouping__id) grouping__id)
    // vs
    // tok_selexpr (tok_table_or_col grouping__id) grouping__id)
    //
    // Since we don't have a function grouping__id in FunctionRegistry, the former
    // AST will fail.
    if (HiveGroupingID.INSTANCE.getName().equals(jsonAggCall.get("name"))) {
      return AggregateCall.create(HiveGroupingID.INSTANCE,
          false, new ImmutableList.Builder<Integer>().build(), -1,
          this.cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT),
          HiveGroupingID.INSTANCE.getName());
    }

    return AggregateCall.create(
        relJson.toAggregation(input, aggName, jsonAggCall),
        distinct,
        false,
        false,
        operands,
        filterOperand == null ? -1 : filterOperand,
        collation,
        type,
        (String) jsonAggCall.get("name"));
  }

  private class HiveRelInput implements RelInput {

    final Map<String, Object> jsonRel;

    public HiveRelInput(Map<String, Object> jsonRel) {
      this.jsonRel = jsonRel;
    }

    public RelOptCluster getCluster() {
      return cluster;
    }

    public RelTraitSet getTraitSet() {
      return cluster.traitSet().plus(HiveRelNode.CONVENTION).plus(RelCollations.EMPTY);
    }

    public RelOptHiveTable getTable(String table) {
      @SuppressWarnings("unchecked")
      List<String> qualifiedName = (List<String>) jsonRel.get("table");
      RelDataType rowType = relJson.toType(cluster.getTypeFactory(), jsonRel.get("rowType"));
      String tableAlias = (String) jsonRel.get("table:alias");

      Map<Boolean, List<ColumnInfo>> columnInfo = computeColumnInfos(rowType, tableAlias);
      List<ColumnInfo> nonPartitionColumns = columnInfo.getOrDefault(false, new ArrayList<>());
      List<ColumnInfo> partitionColumns = columnInfo.getOrDefault(true, new ArrayList<>());

      List<VirtualColumn> virtualColumns = getVirtualColumns();

      TableName tableName = new TableName(
          null, qualifiedName.get(0), qualifiedName.get(1), (String) jsonRel.get("snapshotRef"));

      RelOptHiveTable relOptHiveTable = relOptHiveTableFactory.createRelOptHiveTable(
          tableAlias, tableName, rowType, nonPartitionColumns, partitionColumns, virtualColumns);

      // set partition list
      String plKey = (String) jsonRel.get("plKey");
      if (isNotBlank(plKey) && partitionCache.containsKey(plKey)) {
        relOptHiveTable.partitionList = partitionCache.get(plKey);
      }

      return relOptHiveTable;
    }

    private List<VirtualColumn> getVirtualColumns() {
      List<VirtualColumn> virtualColumns = new ArrayList<>();
      if (!jsonRel.containsKey("virtualColumns")) {
        return virtualColumns;
      }
      for (String colName : (List<String>) jsonRel.get("virtualColumns")) {
        virtualColumns.add(VirtualColumn.VIRTUAL_COLUMN_NAME_MAP.get(colName));
      }

      return virtualColumns;
    }

    private Map<Boolean, List<ColumnInfo>> computeColumnInfos(RelDataType rowType, String tableAlias) {
      Set<String> partColsSet = new HashSet<>();
      if (jsonRel.containsKey("partitionColumns")) {
        partColsSet.addAll((List<String>) jsonRel.get("partitionColumns"));
      }

      Predicate<RelDataTypeField> notVirtualColumn = f -> !VirtualColumn.VIRTUAL_COLUMN_NAMES.contains(f.getName());

      // MAP of
      // true -> list of partition columns
      // false -> list of non partition columns
      return rowType.getFieldList().stream()
          .filter(notVirtualColumn)
          .map(f -> {
                boolean isPartitionedColumn = partColsSet.contains(f.getName());
                return new ColumnInfo(
                    f.getName(),
                    TypeConverter.convert(f.getType()),
                    f.getType().isNullable(),
                    tableAlias,
                    isPartitionedColumn
                );
              }
          )
          .collect(Collectors.partitioningBy(ColumnInfo::getIsVirtualCol));
    }

    public RelNode getInput() {
      final List<RelNode> inputs = getInputs();
      assert inputs.size() == 1;
      return inputs.get(0);
    }

    public List<RelNode> getInputs() {
      final List<String> jsonInputs = getStringList("inputs");
      if (jsonInputs == null) {
        return ImmutableList.of(lastRel);
      }
      final List<RelNode> inputs = new ArrayList<>();
      for (String jsonInput : jsonInputs) {
        inputs.add(lookupInput(jsonInput));
      }
      return inputs;
    }

    private RelNode lookupInput(String jsonInput) {
      RelNode node = relMap.get(jsonInput);
      if (node == null) {
        throw new RuntimeException("unknown id " + jsonInput + " for relational expression");
      }
      return node;
    }

    public RexNode getExpression(String tag) {
      return relJson.toRex(this, jsonRel.get(tag));
    }

    public ImmutableBitSet getBitSet(String tag) {
      return ImmutableBitSet.of(getIntegerList(tag));
    }

    public List<ImmutableBitSet> getBitSetList(String tag) {
      List<List<Integer>> list = getIntegerListList(tag);
      if (list == null) {
        return null;
      }
      final ImmutableList.Builder<ImmutableBitSet> builder = ImmutableList.builder();
      for (List<Integer> integers : list) {
        builder.add(ImmutableBitSet.of(integers));
      }
      return builder.build();
    }

    public List<String> getStringList(String tag) {
      //noinspection unchecked
      return (List<String>) jsonRel.get(tag);
    }

    public List<Integer> getIntegerList(String tag) {
      //noinspection unchecked
      return (List<Integer>) jsonRel.get(tag);
    }

    public List<List<Integer>> getIntegerListList(String tag) {
      //noinspection unchecked
      return (List<List<Integer>>) jsonRel.get(tag);
    }

    public List<AggregateCall> getAggregateCalls(String tag) {
      @SuppressWarnings("unchecked") final List<Map<String, Object>> jsonAggs = (List) jsonRel.get(tag);
      final List<AggregateCall> inputs = new ArrayList<>();
      for (Map<String, Object> jsonAggCall : jsonAggs) {
        inputs.add(toAggCall(this, jsonAggCall));
      }
      return inputs;
    }

    public Object get(String tag) {
      if ("getJoinInputsForHiveMultiJoin".equals(tag)) {
        return getJoinInputsForHiveMultiJoin();
      }
      if ("getJoinTypesForHiveMultiJoin".equals(tag)) {
        return getJoinTypesForHiveMultiJoin();
      }
      if ("joinAlgorithm".equals(tag)) {
        return getJoinAlgorithm();
      }

      return jsonRel.get(tag);
    }

    public String getString(String tag) {
      return (String) jsonRel.get(tag);
    }

    public float getFloat(String tag) {
      return ((Number) jsonRel.get(tag)).floatValue();
    }

    public boolean getBoolean(String tag, boolean defaultBool) {
      final Boolean b = (Boolean) jsonRel.get(tag);
      return b != null ? b : defaultBool;
    }

    public <E extends Enum<E>> E getEnum(String tag, Class<E> enumClass) {
      return Util.enumVal(enumClass, getString(tag).toUpperCase(Locale.ROOT));
    }

    public List<RexNode> getExpressionList(String tag) {
      @SuppressWarnings("unchecked") final List<Object> jsonNodes = (List) jsonRel.get(tag);
      final List<RexNode> nodes = new ArrayList<>();
      for (Object jsonNode : jsonNodes) {
        nodes.add(relJson.toRex(this, jsonNode));
      }
      return nodes;
    }

    public RelDataType getRowType(String tag) {
      final Object o = jsonRel.get(tag);
      return relJson.toType(cluster.getTypeFactory(), o);
    }

    public RelDataType getRowType(String expressionsTag, String fieldsTag) {
      final List<RexNode> expressionList = getExpressionList(expressionsTag);
      @SuppressWarnings("unchecked") final List<String> names = (List<String>) get(fieldsTag);
      return cluster.getTypeFactory().createStructType(new AbstractList<Entry<String, RelDataType>>() {
        @Override
        public Entry<String, RelDataType> get(int index) {
          return Pair.of(names.get(index), expressionList.get(index).getType());
        }

        @Override
        public int size() {
          return names.size();
        }
      });
    }

    public RelCollation getCollation() {
      //noinspection unchecked
      RelCollation result = relJson.toCollation((List) get("collation"));
      if (result != null) {
        return result;
      }
      return RelCollationTraitDef.INSTANCE.getDefault();
    }

    public RelDistribution getDistribution() {
      return relJson.toDistribution(get("distribution"));
    }

    public ImmutableList<ImmutableList<RexLiteral>> getTuples(String tag) {
      //noinspection unchecked
      final List<List> jsonTuples = (List) get(tag);
      final ImmutableList.Builder<ImmutableList<RexLiteral>> builder = ImmutableList.builder();
      for (List jsonTuple : jsonTuples) {
        builder.add(getTuple(jsonTuple));
      }
      return builder.build();
    }

    public ImmutableList<RexLiteral> getTuple(List jsonTuple) {
      final ImmutableList.Builder<RexLiteral> builder = ImmutableList.builder();
      for (Object jsonValue : jsonTuple) {
        builder.add((RexLiteral) relJson.toRex(this, jsonValue));
      }
      return builder.build();
    }

    public List<Pair<Integer, Integer>> getJoinInputsForHiveMultiJoin() {
      List<Pair<Integer, Integer>> result = new ArrayList<>();
      List<String> joinsDescription = getStringList("joinsDescription");
      if (joinsDescription == null || joinsDescription.isEmpty()) {
        return result;
      }

      return joinsDescription.stream()
          .map(s -> s.split(" : ", 2)[0])
          .map(s -> s.split(" - ", 2))
          .map(s -> Pair.of(Integer.valueOf(s[0]), Integer.valueOf(s[1])))
          .collect(Collectors.toList());
    }

    public List<JoinRelType> getJoinTypesForHiveMultiJoin() {
      List<JoinRelType> result = new ArrayList<>();
      List<String> joinsDescription = getStringList("joinsDescription");
      if (joinsDescription == null || joinsDescription.isEmpty()) {
        return result;
      }

      return joinsDescription.stream()
          .map(s -> s.split(" : ", 2)[1])
          .map(s -> Util.enumVal(JoinRelType.class, s))
          .collect(Collectors.toList());
    }

    private HiveCostModel.JoinAlgorithm getJoinAlgorithm() {
      String algoName = (String) get("algorithm");
      algoName = algoName == null ? "" : algoName;

      switch (algoName) {
        case "BucketJoin":
          return HiveOnTezCostModel.TezBucketJoinAlgorithm.INSTANCE;
        case "CommonJoin":
          return HiveOnTezCostModel.TezCommonJoinAlgorithm.INSTANCE;
        case "MapJoin":
          return HiveOnTezCostModel.TezMapJoinAlgorithm.INSTANCE;
        case "SMBJoin":
          return HiveOnTezCostModel.TezSMBJoinAlgorithm.INSTANCE;
        case "none":
        default:
          return HiveDefaultCostModel.DefaultJoinAlgorithm.INSTANCE;
      }
    }
  }
}
