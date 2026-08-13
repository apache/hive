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

package org.apache.hadoop.hive.ql.parse;

import static org.apache.hadoop.hive.ql.metadata.HiveUtils.unparseIdentifier;
import static org.apache.hadoop.hive.ql.metadata.VirtualColumn.PARTITION_SPEC_ID;
import static org.apache.hadoop.hive.ql.parse.rewrite.sql.SqlGeneratorFactory.SUB_QUERY_ALIAS;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.VariableSubstitution;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.stats.ColStatsProcessor.ColumnStatsField;
import org.apache.hadoop.hive.ql.stats.ColStatsProcessor.ColumnStatsType;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ColumnStatsSemanticAnalyzer.
 * Handles semantic analysis and rewrite for gathering column statistics both at the level of a
 * partition and a table. Note that table statistics are implemented in SemanticAnalyzer.
 *
 */
public class ColumnStatsSemanticAnalyzer extends SemanticAnalyzer {
  private static final Logger LOG = LoggerFactory
      .getLogger(ColumnStatsSemanticAnalyzer.class);
  private static final LogHelper CONSOLE = new LogHelper(LOG);

  private ASTNode originalTree;
  private ASTNode rewrittenTree;
  private String rewrittenQuery;

  private Context ctx;
  private boolean isRewritten;

  private boolean isTableLevel;
  private FieldSchemas rewrittenColumnSchemas;
  private Table tbl;

  public ColumnStatsSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  private boolean shouldRewrite(ASTNode tree) {
    boolean rwt = false;
    if (tree.getChildCount() > 1) {
      ASTNode child0 = (ASTNode) tree.getChild(0);
      ASTNode child1;
      if (child0.getToken().getType() == HiveParser.TOK_TAB) {
        child0 = (ASTNode) child0.getChild(0);
        if (child0.getToken().getType() == HiveParser.TOK_TABNAME) {
          child1 = (ASTNode) tree.getChild(1);
          if (child1.getToken().getType() == HiveParser.KW_COLUMNS) {
            rwt = true;
          }
        }
      }
    }
    return rwt;
  }

  /**
   * Get the Field Schemas of the columns that support column statistics.
   */
  private static FieldSchemas getStatsEligibleFieldSchemas(Table tbl) {
    List<FieldSchema> result = new ArrayList<>();
    List<FieldSchema> colsToLookUp = tbl.hasNonNativePartitionSupport() ? tbl.getAllCols() : tbl.getCols();
    for (FieldSchema col : colsToLookUp) {
      String type = col.getType();
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(type);
      boolean isSupported = ColumnStatsAutoGatherContext.isColumnSupported(typeInfo.getCategory(), () -> typeInfo);
      if (isSupported) {
        result.add(col);
      }
    }
    return new FieldSchemas(result);
  }

  private List<String> getExplicitColumnNamesFromAst(ASTNode tree) throws SemanticException {
    // The parser stores this statement as three pieces in order: which table (or partition) to
    // analyze, a flag that this is column-level stats (not scanning the whole table for table
    // stats alone), then the listed column names from "FOR COLUMNS (a, b, ...)". That layout is the reason
    // we expect exactly three children and read the identifiers from the last one.
    if (tree.getChildCount() != 3) {
      throw new SemanticException("Internal error. Expected number of children of ASTNode should be 3. Found : "
          + tree.getChildCount());
    }
    int numCols = tree.getChild(2).getChildCount();
    List<String> colName = new ArrayList<>(numCols);
    for (int i = 0; i < numCols; i++) {
      colName.add(getUnescapedName((ASTNode) tree.getChild(2).getChild(i)));
    }
    return colName;
  }

  private void handlePartialPartitionSpec(Map<String, String> partSpec, ColumnStatsAutoGatherContext context) throws
    SemanticException {

    // If user has fully specified partition, validate that partition exists
    int partValsSpecified = 0;
    for (String partKey : partSpec.keySet()) {
      partValsSpecified += partSpec.get(partKey) == null ? 0 : 1;
    }
    try {
      // for static partition, it may not exist when HIVE_STATS_COL_AUTOGATHER is
      // set to true
      if (context == null && partValsSpecified > 0) {
        if ((partValsSpecified == tbl.getPartitionKeys().size())
            && (db.getPartition(tbl, partSpec, false, null, false) == null)) {
          throw new SemanticException(ErrorMsg.COLUMNSTATSCOLLECTOR_INVALID_PARTITION.getMsg()
              + " : " + partSpec);
        }
      }
    } catch (HiveException he) {
      throw new SemanticException(ErrorMsg.COLUMNSTATSCOLLECTOR_INVALID_PARTITION.getMsg() + " : "
          + partSpec);
    }

    // User might have only specified partial list of partition keys, in which case add other partition keys in partSpec
    List<String> partKeys = Utilities.getColumnNamesFromFieldSchema(tbl.getPartitionKeys());
    for (String partKey : partKeys) {
      if (!partSpec.containsKey(partKey)) {
        partSpec.put(partKey, null);
      }
    }

    // Check if user have erroneously specified non-existent partitioning columns
    for (String partKey : partSpec.keySet()) {
      if (!partKeys.contains(partKey)) {
        throw new SemanticException(ErrorMsg.COLUMNSTATSCOLLECTOR_INVALID_PART_KEY.getMsg() + " : " + partKey);
      }
    }
  }

  private static CharSequence genPartitionClause(Table tbl, List<TransformSpec> partTransformSpec,
    Map<String, String> partSpec, HiveConf conf) {
    boolean predPresent = partSpec.values().stream().anyMatch(Objects::nonNull);
    
    StringBuilder whereClause = new StringBuilder(" where ").append(
      partSpec.entrySet().stream()
        .filter(part -> part.getValue() != null)
        .map(part -> unparseIdentifier(part.getKey(), conf) + " = " 
            + genPartValueString(getColTypeOf(tbl, part.getKey()), part.getValue()))
        .collect(Collectors.joining(" and "))
    );

    StringBuilder groupByClause = new StringBuilder(" group by ").append((
      (partTransformSpec != null) ?
          partTransformSpec.stream().map(spec -> spec.toHiveExpr(conf)) :
          tbl.getPartColNames().stream().map(col -> unparseIdentifier(col, conf))
      )
      .collect(Collectors.joining(", "))
    );

    // attach the predicate and group by to the return clause
    return predPresent ? whereClause.append(groupByClause) : groupByClause;
  }

  /** Renders the predicate selecting the rows written under any of the given specs. */
  private static String genSpecIdPredicate(List<Integer> specIds, HiveConf conf) {
    return "%s in (%s)".formatted(
        unparseIdentifier(PARTITION_SPEC_ID.getName(), conf),
        StringUtils.join(specIds, ", "));
  }

  /** The unified-partition-tuple rewrite fragments: the projection, group by clause and CTE keys. */
  private record PartitionTupleSql(String projection, String groupByClause, String cteKeys) {
  }

  /**
   * Builds the rewrite fragments over the unified partition tuple - the union of every spec's
   * fields, mirroring Iceberg's Partitioning.partitionType: a single scan groups every row by the
   * spec that wrote it and that spec's transform values, computed once in a CTE as the single
   * group key. For specs 0 (unpartitioned history), 1 = bucket[8](a), 2 = bucket[4](a) + year(b):
   * <pre>
   *   with s as (
   *     select a, b, id,
   *       PARTITION__SPEC__ID as k0,
   *       if(PARTITION__SPEC__ID in (1), iceberg_bucket(a, 8), null) as k1,
   *       if(PARTITION__SPEC__ID in (2), iceberg_bucket(a, 4), null) as k2,
   *       if(PARTITION__SPEC__ID in (2), iceberg_year(b), null) as k3
   *     from t
   *   )
   *   select compute_stats(...),
   *          named_struct('PARTITION__SPEC__ID', k0, 'a_bucket_8', k1, 'a_bucket_4', k2, 'b_year', k3)
   *   from s
   *   group by k0, k1, k2, k3
   * </pre>
   * The stats processor passes the tuple through as a provisional partition name; the storage
   * handler renders each group's final name from it at persist time - the spec that wrote the rows
   * knows its own fields, so the values of the specs not owning a field, nulled by the guards, are
   * ignored, and a void spec renders the synthetic no-partition name.
   * Returns null for a native table (no transform specs) and for a single-spec table with no void
   * specs: the rewrite is then the master-shaped query.
   */
  private static PartitionTupleSql genPartitionTupleSql(Map<Integer, List<TransformSpec>> transformSpecsBySpecId,
      HiveConf conf) throws SemanticException {
    if (transformSpecsBySpecId == null) {
      return null;
    }
    List<Integer> voidSpecIds = transformSpecsBySpecId.entrySet().stream()
        .filter(e -> e.getValue().isEmpty())
        .map(Map.Entry::getKey)
        .sorted().toList();
    List<TransformSpec> partitionedFields = transformSpecsBySpecId.keySet().stream()
        .filter(specId -> !voidSpecIds.contains(specId))
        .sorted()
        .flatMap(specId -> transformSpecsBySpecId.get(specId).stream())
        .toList();
    List<Integer> partitionedSpecIds = partitionedFields.stream()
        .map(TransformSpec::getSpecId)
        .distinct().toList();
    if (voidSpecIds.isEmpty() && partitionedSpecIds.size() == 1) {
      return null;
    }

    // the unified fields: distinct field names across the partitioned specs, with their owning specs
    // (mirrors Iceberg's Partitioning.partitionType). The tuple is joined by name, so a name reused
    // for a different transform is rejected - Iceberg's own partition stats fail on such reuse too,
    // and evolution-generated names encode the transform, so they never conflict
    Map<String, TransformSpec> fieldsByName = new LinkedHashMap<>();
    Map<String, List<Integer>> ownerSpecIds = new LinkedHashMap<>();
    for (TransformSpec field : partitionedFields) {
      TransformSpec known = fieldsByName.putIfAbsent(field.getFieldName(), field);
      if (known != null && (known.getTransformType() != field.getTransformType() ||
          !Objects.equals(known.getColumnName(), field.getColumnName()) ||
          !Objects.equals(known.getTransformParam(), field.getTransformParam()))) {
        throw new SemanticException("Conflicting transforms for partition field '%s': %s(%s) vs %s(%s)"
            .formatted(field.getFieldName(), known.getTransformType(), known.getColumnName(),
                field.getTransformType(), field.getColumnName()));
      }
      ownerSpecIds.computeIfAbsent(field.getFieldName(), name -> new ArrayList<>()).add(field.getSpecId());
    }

    // group keys: one expression per unified field, nulled for the rows of the specs not owning it
    Map<String, String> fieldKeyExprs = new LinkedHashMap<>();
    fieldsByName.forEach((name, field) -> fieldKeyExprs.put(name,
        "if(%s, %s, null)".formatted(genSpecIdPredicate(ownerSpecIds.get(name), conf), field.toHiveExpr(conf))));

    // several void specs must fold into one group, keyed by a representative among them; a single
    // one groups by its own id (either way the void spec renders the empty tuple at persist time)
    String specIdIdentifier = unparseIdentifier(PARTITION_SPEC_ID.getName(), conf);
    String specKey = voidSpecIds.size() > 1 ?
        "if(%s, %d, %s)".formatted(genSpecIdPredicate(voidSpecIds, conf), voidSpecIds.get(0), specIdIdentifier) :
        specIdIdentifier;

    // the group keys are computed once in the CTE, under synthetic aliases: an identity transform's
    // field name is its column's, which the stats projection selects too; the keys stay scalar -
    // a complex-typed group key would disqualify the aggregation from vectorization - and the
    // unified tuple is assembled from them after the aggregation, carrying the real field names
    List<String> fieldNames = new ArrayList<>(fieldKeyExprs.keySet());
    StringBuilder cteKeys = new StringBuilder("%s as k0".formatted(specKey));
    StringBuilder tupleFields = new StringBuilder("'%s', k0".formatted(PARTITION_SPEC_ID.getName()));
    List<String> groupKeys = new ArrayList<>(List.of("k0"));
    for (int i = 0; i < fieldNames.size(); i++) {
      String key = "k" + (i + 1);
      cteKeys.append(",\n    %s as %s".formatted(fieldKeyExprs.get(fieldNames.get(i)), key));
      tupleFields.append(", '%s', %s".formatted(fieldNames.get(i), key));
      groupKeys.add(key);
    }
    return new PartitionTupleSql(
        ",\n  named_struct(%s)".formatted(tupleFields),
        "\ngroup by " + String.join(", ", groupKeys),
        cteKeys.toString());
  }

  private static String getColTypeOf(Table tbl, String partKey) {
    for (FieldSchema fs : tbl.getPartCols()) {
      if (partKey.equalsIgnoreCase(fs.getName())) {
        return fs.getType().toLowerCase();
      }
    }
    throw new RuntimeException("Unknown partition key : " + partKey);
  }

  protected static List<FieldSchema> getFieldSchemasByColName(Table tbl, List<String> colNames)
      throws SemanticException {
    Map<String, FieldSchema> specifiedColsMap = new HashMap<>();
    for (String colName : colNames) {
      specifiedColsMap.put(colName.toLowerCase(), new FieldSchema(colName, null, null));
    }

    for (FieldSchema pk : tbl.getPartitionKeys()) {
      FieldSchema fs = specifiedColsMap.get(pk.getName().toLowerCase());
      if (fs != null) {
        throw new SemanticException(ErrorMsg.COLUMNSTATSCOLLECTOR_INVALID_COLUMN.getMsg()
            + " [Try removing column '" + fs.getName() + "' from column list]");
      }
    }

    List<FieldSchema> colsToLookUp = tbl.hasNonNativePartitionSupport() ? tbl.getAllCols() : tbl.getCols();
    for (FieldSchema col : colsToLookUp) {
      specifiedColsMap.computeIfPresent(col.getName().toLowerCase(), (key, value) -> col);
    }

    List<FieldSchema> result = new ArrayList<>();
    List<String> tableColNames = new FieldSchemas(colsToLookUp).getColName();
    for (String colName : colNames) {
      FieldSchema fs = specifiedColsMap.get(colName.toLowerCase());

      // If the type is null, the column does not exist as its FieldSchema was not populated from tbl.getCols()
      if (fs.getType() == null) {
        String msg = "'" + colName + "' (possible columns are " + tableColNames + ")";
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(msg));
      }

      String type = fs.getType();
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(type);
      boolean isSupported = ColumnStatsAutoGatherContext.isColumnSupported(typeInfo.getCategory(), () -> typeInfo);
      if (!isSupported) {
        logTypeWarning(colName, type);
      } else {
        result.add(new FieldSchema(colName, type, fs.getComment()));
      }
    }
    return result;
  }

  private String genRewrittenQuery(FieldSchemas columnSchemas, HiveConf conf,
      List<TransformSpec> partTransformSpec, Map<String, String> partSpec,
      boolean isPartitionStats, PartitionTupleSql partitionTuple) {
    String rewritten = genRewrittenQuery(tbl, columnSchemas, conf, partTransformSpec, partSpec,
        isPartitionStats, false, partitionTuple);
    isRewritten = true;
    return rewritten;
  }

  /**
   * Generates a SQL statement that will compute the stats for all columns
   * included in the input table.
   */
  protected static String genRewrittenQuery(Table tbl,
      HiveConf conf, List<TransformSpec> partTransformSpec, Map<String, String> partSpec,
      boolean isPartitionStats) {
    return ColumnStatsSemanticAnalyzer.genRewrittenQuery(tbl, getStatsEligibleFieldSchemas(tbl), conf,
        partTransformSpec, partSpec, isPartitionStats, true, null);
  }

  /**
   * @param partTransformSpec the current spec's transforms, projected as the partition struct;
   *     null for a native table, whose raw partition columns are projected instead
   * @param partitionTuple the unified-partition-tuple fragments grouping all specs' rows in one scan;
   *     null on the auto-gather and single-spec paths, which keep the current spec's
   *     partition-struct projection
   */
  private static String genRewrittenQuery(Table tbl, FieldSchemas columnSchemas,
      HiveConf conf, List<TransformSpec> partTransformSpec, Map<String, String> partSpec,
      boolean isPartitionStats, boolean useTableValues, PartitionTupleSql partitionTuple) {
    StringBuilder rewrittenQueryBuilder = new StringBuilder("select ");

    StringBuilder columnNamesBuilder = new StringBuilder();
    StringBuilder columnDummyValuesBuilder = new StringBuilder();
    for (int i = 0; i < columnSchemas.size(); i++) {
      if (i > 0) {
        rewrittenQueryBuilder.append(", ");
        columnNamesBuilder.append(", ");
        columnDummyValuesBuilder.append(", ");
      }

      FieldSchema columnSchema = columnSchemas.get(i);
      final String columnName = unparseIdentifier(columnSchema.getName(), conf);
      final TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(columnSchema.getType());
      
      try {
        genComputeStats(rewrittenQueryBuilder, conf, i, columnName, typeInfo);
      } catch (SemanticException e) {
        throw new RuntimeException(e);
      }

      columnNamesBuilder.append(columnName);

      columnDummyValuesBuilder.append(
          "cast(null as " + typeInfo + ")");
    }

    if (isPartitionStats) {
      if (partitionTuple != null) {
        rewrittenQueryBuilder.append(partitionTuple.projection());
      } else if (partTransformSpec != null) {
        rewrittenQueryBuilder.append(", ")
          .append(TransformSpec.toNamedStruct(partTransformSpec, conf));
      } else {
        for (FieldSchema fs : tbl.getPartCols()) {
          String identifier = unparseIdentifier(fs.getName(), conf);
          rewrittenQueryBuilder.append(", ").append(identifier);
          columnNamesBuilder.append(", ").append(identifier);

          columnDummyValuesBuilder.append(", cast(null as ")
            .append(TypeInfoUtils.getTypeInfoFromTypeString(fs.getType()).toString())
            .append(")");
        }
      }
    }

    rewrittenQueryBuilder.append(" from ");
    if (useTableValues) {
      //TABLE(VALUES(cast(null as int),cast(null as string))) AS tablename(col1,col2)
      rewrittenQueryBuilder.append("table(values(")
        // Values
        .append(columnDummyValuesBuilder)
        .append(")) as ")
        .append(unparseIdentifier(tbl.getTableName() ,conf))
        .append("(")
        // Columns
        .append(columnNamesBuilder)
        .append(")");
    } else {
      rewrittenQueryBuilder.append(partitionTuple != null ? SUB_QUERY_ALIAS : genTableRef(tbl, conf));
    }

    // If partition level statistics is requested, add predicate and group by as needed to rewritten
    // query
    if (isPartitionStats) {
      rewrittenQueryBuilder.append(partitionTuple != null ?
          partitionTuple.groupByClause() :
          genPartitionClause(tbl, partTransformSpec, partSpec, conf));
    }

    String rewrittenQuery = rewrittenQueryBuilder.toString();
    if (partitionTuple != null) {
      // the unified partition tuple is computed once in a CTE and grouped on by name (Hive
      // resolves CTE output columns in group by, but not select aliases)
      String cteCols = columnSchemas.getSchemas().stream()
          .map(schema -> unparseIdentifier(schema.getName(), conf))
          .collect(Collectors.joining(", "));
      rewrittenQuery = "with %s as (\n  select %s,\n    %s\n  from %s\n)\n%s"
          .formatted(SUB_QUERY_ALIAS, cteCols, partitionTuple.cteKeys(), genTableRef(tbl, conf), rewrittenQuery);
    }
    rewrittenQuery = new VariableSubstitution(
        () -> SessionState.get().getHiveVariables()).substitute(conf, rewrittenQuery);
    return rewrittenQuery;
  }

  private static String genTableRef(Table tbl, HiveConf conf) {
    return unparseIdentifier(tbl.getDbName(), conf) + "." + unparseIdentifier(tbl.getTableName(), conf);
  }

  private static void genComputeStats(StringBuilder rewrittenQueryBuilder, HiveConf conf,
      int pos, String columnName, TypeInfo typeInfo) throws SemanticException {
    Preconditions.checkArgument(typeInfo.getCategory() == Category.PRIMITIVE);
    ColumnStatsType columnStatsType =
        ColumnStatsType.getColumnStatsType((PrimitiveTypeInfo) typeInfo);
    List<ColumnStatsField> columnStatsFields = columnStatsType.getColumnStats();
    columnStatsFields = ColumnStatsType.removeDisabledStatistics(conf, columnStatsFields);
    // The first column is always the type
    // The rest of columns will depend on the type itself
    for (int i = 0; i < columnStatsFields.size(); i++) {
      ColumnStatsField columnStatsField = columnStatsFields.get(i);
      if (i > 0) {
        rewrittenQueryBuilder.append(", ");
      }
      appendStatsField(rewrittenQueryBuilder, conf, columnStatsField, columnStatsType,
          columnName, pos);
    }
  }

  private static void appendStatsField(StringBuilder rewrittenQueryBuilder, HiveConf conf,
      ColumnStatsField columnStatsField, ColumnStatsType columnStatsType,
      String columnName, int pos) throws SemanticException {
    switch (columnStatsField) {
    case COLUMN_STATS_TYPE:
      appendColumnType(rewrittenQueryBuilder, conf, columnStatsType, pos);
      break;
    case COUNT_TRUES:
      appendCountTrues(rewrittenQueryBuilder, conf, columnName, pos);
      break;
    case COUNT_FALSES:
      appendCountFalses(rewrittenQueryBuilder, conf, columnName, pos);
      break;
    case COUNT_NULLS:
      appendCountNulls(rewrittenQueryBuilder, conf, columnName, pos);
      break;
    case MIN:
      appendMin(rewrittenQueryBuilder, conf, columnStatsType, columnName, pos);
      break;
    case MAX:
      appendMax(rewrittenQueryBuilder, conf, columnStatsType, columnName, pos);
      break;
    case NDV:
      appendNDV(rewrittenQueryBuilder, conf, columnName, pos);
      break;
    case BITVECTOR:
      appendBitVector(rewrittenQueryBuilder, conf, columnName, pos);
      break;
    case KLL_SKETCH:
      appendKllSketch(rewrittenQueryBuilder, conf, columnName, columnStatsType, pos);
      break;
    case MAX_LENGTH:
      appendMaxLength(rewrittenQueryBuilder, conf, columnName, pos);
      break;
    case AVG_LENGTH:
      appendAvgLength(rewrittenQueryBuilder, conf, columnName, pos);
      break;
    default:
      throw new SemanticException("Not supported field " + columnStatsField);
    }
  }

  private static void appendColumnType(StringBuilder rewrittenQueryBuilder, HiveConf conf,
      ColumnStatsType columnStatsType, int pos) {
    rewrittenQueryBuilder.append("'")
        .append(columnStatsType.toString())
        .append("' AS ")
        .append(unparseIdentifier(ColumnStatsField.COLUMN_STATS_TYPE.getFieldName() + pos, conf));
  }

  private static void appendMin(StringBuilder rewrittenQueryBuilder, HiveConf conf,
      ColumnStatsType columnStatsType, String columnName, int pos) {
    switch (columnStatsType) {
    case LONG:
      rewrittenQueryBuilder.append("CAST(min(")
          .append(columnName)
          .append(") AS bigint) AS ");
      break;
    case DOUBLE:
      rewrittenQueryBuilder.append("CAST(min(")
          .append(columnName)
          .append(") AS double) AS ");
      break;
    default:
      rewrittenQueryBuilder.append("min(")
          .append(columnName)
          .append(") AS ");
      break;
    }
    rewrittenQueryBuilder.append(
        unparseIdentifier(ColumnStatsField.MIN.getFieldName() + pos, conf));
  }

  private static void appendMax(StringBuilder rewrittenQueryBuilder, HiveConf conf,
      ColumnStatsType columnStatsType, String columnName, int pos) {
    switch (columnStatsType) {
    case LONG:
      rewrittenQueryBuilder.append("CAST(max(")
          .append(columnName)
          .append(") AS bigint) AS ");
      break;
    case DOUBLE:
      rewrittenQueryBuilder.append("CAST(max(")
          .append(columnName)
          .append(") AS double) AS ");
      break;
    default:
      rewrittenQueryBuilder.append("max(")
          .append(columnName)
          .append(") AS ");
      break;
    }
    rewrittenQueryBuilder.append(
        unparseIdentifier(ColumnStatsField.MAX.getFieldName() + pos, conf));
  }

  private static void appendMaxLength(StringBuilder rewrittenQueryBuilder, HiveConf conf,
      String columnName, int pos) {
    rewrittenQueryBuilder.append("CAST(COALESCE(max(LENGTH(")
        .append(columnName)
        .append(")), 0) AS bigint) AS ")
        .append(unparseIdentifier(ColumnStatsField.MAX_LENGTH.getFieldName() + pos, conf));
  }

  private static void appendAvgLength(StringBuilder rewrittenQueryBuilder, HiveConf conf,
      String columnName, int pos) {
    rewrittenQueryBuilder.append("CAST(COALESCE(avg(COALESCE(LENGTH(")
        .append(columnName)
        .append("), 0)), 0) AS double) AS ")
        .append(unparseIdentifier(ColumnStatsField.AVG_LENGTH.getFieldName() + pos, conf));
  }

  private static void appendCountNulls(StringBuilder rewrittenQueryBuilder, HiveConf conf,
      String columnName, int pos) {
    rewrittenQueryBuilder.append("CAST(count(1) - count(")
        .append(columnName)
        .append(") AS bigint) AS ")
        .append(unparseIdentifier(ColumnStatsField.COUNT_NULLS.getFieldName() + pos, conf));
  }

  private static void appendNDV(StringBuilder rewrittenQueryBuilder, HiveConf conf,
      String columnName, int pos) throws SemanticException {
    rewrittenQueryBuilder.append("COALESCE(NDV_COMPUTE_BIT_VECTOR(");
    appendBitVector(rewrittenQueryBuilder, conf, columnName);
    rewrittenQueryBuilder.append("), 0) AS ")
        .append(unparseIdentifier(ColumnStatsField.NDV.getFieldName() + pos, conf));
  }

  private static void appendBitVector(StringBuilder rewrittenQueryBuilder, HiveConf conf,
      String columnName, int pos) throws SemanticException {
    appendBitVector(rewrittenQueryBuilder, conf, columnName);
    rewrittenQueryBuilder.append(" AS ")
        .append(unparseIdentifier(ColumnStatsField.BITVECTOR.getFieldName() + pos, conf));
  }

  private static void appendKllSketch(StringBuilder rewrittenQueryBuilder, HiveConf conf,
      String columnName, ColumnStatsType columnStatsType, int pos) throws SemanticException {
    appendKllSketch(rewrittenQueryBuilder, conf, columnName, columnStatsType);
    rewrittenQueryBuilder.append(" AS ")
        .append(unparseIdentifier(ColumnStatsField.KLL_SKETCH.getFieldName() + pos, conf));
  }

  private static void appendBitVector(StringBuilder rewrittenQueryBuilder, HiveConf conf,
      String columnName) throws SemanticException {
    String func = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_STATS_NDV_ALGO).toLowerCase();
    if ("hll".equals(func)) {
      rewrittenQueryBuilder
          .append("compute_bit_vector_hll(")
          .append(columnName)
          .append(")");
    } else if ("fm".equals(func)) {
      int numBitVectors;
      try {
        numBitVectors = HiveStatsUtils.getNumBitVectorsForNDVEstimation(conf);
      } catch (Exception e) {
        throw new SemanticException(e.getMessage());
      }
      rewrittenQueryBuilder.append("compute_bit_vector_fm(")
          .append(columnName)
          .append(", ")
          .append(numBitVectors)
          .append(")");
    } else {
      throw new UDFArgumentException("available ndv computation options are hll and fm. Got: " + func);
    }
  }

  private static void appendKllSketch(StringBuilder rewrittenQueryBuilder, HiveConf conf, String columnName,
      ColumnStatsType columnStatsType) throws SemanticException {
    int k;
    try {
      k = HiveStatsUtils.getKParamForKllSketch(conf);
    } catch (Exception e) {
      throw new SemanticException(e.getMessage());
    }

    boolean isDateFamily = columnStatsType.equals(ColumnStatsType.DATE) ||
        columnStatsType.equals(ColumnStatsType.TIMESTAMP);

    if (isDateFamily) {
      rewrittenQueryBuilder.append("ds_kll_sketch(cast(unix_timestamp(")
          .append(columnName)
          .append(") as float)");
    } else {
      // add cast to float to make sure it works for other numeric types
      rewrittenQueryBuilder.append("ds_kll_sketch(cast(")
          .append(columnName)
          .append(" as float)");
    }
    rewrittenQueryBuilder.append(", ")
        .append(k)
        .append(")");
  }

  private static void appendCountTrues(StringBuilder rewrittenQueryBuilder, HiveConf conf,
      String columnName, int pos) {
    rewrittenQueryBuilder.append("CAST(count(CASE WHEN ")
        .append(columnName)
        .append(" IS TRUE THEN 1 ELSE null END) AS bigint) AS ")
        .append(unparseIdentifier(ColumnStatsField.COUNT_TRUES.getFieldName() + pos, conf));
  }

  private static void appendCountFalses(StringBuilder rewrittenQueryBuilder, HiveConf conf,
      String columnName, int pos) {
    rewrittenQueryBuilder.append("CAST(count(CASE WHEN ")
        .append(columnName)
        .append(" IS FALSE THEN 1 ELSE null END) AS bigint) AS ")
        .append(unparseIdentifier(ColumnStatsField.COUNT_FALSES.getFieldName() + pos, conf));
  }

  private ASTNode genRewrittenTree(String rewrittenQuery) throws SemanticException {
    // Parse the rewritten query string
    ctx = new Context(conf);
    ctx.setCmd(rewrittenQuery);
    ctx.setHDFSCleanup(true);

    try {
      return ParseUtils.parse(rewrittenQuery, ctx);
    } catch (ParseException e) {
      throw new SemanticException(ErrorMsg.COLUMNSTATSCOLLECTOR_PARSE_ERROR.getMsg());
    }
  }

  private static void logTypeWarning(String colName, String colType) {
    String warning = "Only primitive type arguments are accepted but " + colType
        + " is passed for " + colName + ".";
    warning = "WARNING: " + warning;
    CONSOLE.printInfo(warning);
  }

  @Override
  public void analyze(ASTNode ast, Context origCtx) throws SemanticException {
    QB qb;
    QBParseInfo qbp;

    // initialize QB
    init(true);

    // check if it is no scan. grammar prevents coexit noscan/columns
    super.processNoScanCommand(ast);
    /* Rewrite only analyze table <> column <> compute statistics; Don't rewrite analyze table
     * command - table stats are collected by the table scan operator and is not rewritten to
     * an aggregation.
     */
    if (shouldRewrite(ast)) {
      tbl = AnalyzeCommandUtils.getTable(ast, this);
      originalTree = ast;
      boolean isPartitionStats = AnalyzeCommandUtils.isPartitionLevelStats(ast) 
          || StatsUtils.isPartitionStats(tbl, conf);
      // a partition-scoped column statistics ANALYZE cannot be honored for non-native tables: the stats
      // of all partitions are rewritten as a whole, so it would drop every other partition's statistics
      // (table-level ANALYZE of a partitioned table still computes all partitions - only the explicit
      // partition spec is rejected; the auto-gather path merges instead and stays unaffected)
      validateUnsupportedPartitionClause(tbl, AnalyzeCommandUtils.isPartitionLevelStats(ast));
      
      Map<String, String> partSpec = (isPartitionStats) ?
          AnalyzeCommandUtils.getPartKeyValuePairsFromAST(tbl, ast, conf) : null;

      List<FieldSchema> columnSchemas = getColumnsFromAst(ast);

      if (isPartitionStats) {
        handlePartialPartitionSpec(partSpec, null);
      }
      Map<Integer, List<TransformSpec>> partTransformSpecs =
          isPartitionStats && tbl.hasNonNativePartitionSupport() ?
              tbl.getStorageHandler().getPartitionTransformSpecs(tbl) : null;
      rewrittenColumnSchemas = new FieldSchemas(columnSchemas);
      isTableLevel = !isPartitionStats;

      // a single scan groups every row by the unified partition tuple: the spec that wrote it and
      // that spec's transform values; the rows written under a void spec (unpartitioned, or all of
      // its transforms void) belong to no partition and form one synthetic group
      boolean hasPartitionedSpec = partTransformSpecs == null ||
          !partTransformSpecs.values().stream().allMatch(List::isEmpty);
      if (hasPartitionedSpec) {
        PartitionTupleSql partitionTuple = genPartitionTupleSql(partTransformSpecs, conf);
        // no tuple means the map holds exactly one partitioned spec and no void specs: the
        // master-shaped single-spec query projects its transforms
        List<TransformSpec> partTransformSpec = partitionTuple != null || partTransformSpecs == null ?
            null : Iterables.getOnlyElement(partTransformSpecs.values());
        rewrittenQuery = genRewrittenQuery(rewrittenColumnSchemas, conf, partTransformSpec, partSpec,
            isPartitionStats, partitionTuple);
      } else {
        // no partitioned spec at all: table-level statistics cover every row
        isTableLevel = true;
        rewrittenQuery = genRewrittenQuery(rewrittenColumnSchemas, conf, null, partSpec, false, null);
      }

      rewrittenTree = genRewrittenTree(rewrittenQuery);
    } else {
      // Not an analyze table column compute statistics statement - don't do any rewrites
      originalTree = rewrittenTree = ast;
      rewrittenQuery = null;
      isRewritten = false;
    }

    // Setup the necessary metadata if originating from analyze rewrite
    if (isRewritten) {
      qb = getQB();
      qb.setAnalyzeRewrite(true);
      qbp = qb.getParseInfo();
      analyzeRewrite = new AnalyzeRewriteContext();
      analyzeRewrite.setTableName(tbl.getFullyQualifiedName());
      analyzeRewrite.setTblLvl(isTableLevel);
      analyzeRewrite.setFieldSchemas(rewrittenColumnSchemas);
      qbp.setAnalyzeRewrite(analyzeRewrite);
      origCtx.addSubContext(ctx);
      initCtx(ctx);
      ctx.setExplainConfig(origCtx.getExplainConfig());
      LOG.info("Invoking analyze on rewritten query");
      analyzeInternal(rewrittenTree);
      // After analyzeInternal() Hiveop get set as Query
      // since we are passing in AST for select query, so reset it.
      this.queryState.setCommandType(HiveOperation.ANALYZE_TABLE);
    } else {
      initCtx(origCtx);
      LOG.info("Invoking analyze on original query");
      analyzeInternal(originalTree);
    }
  }

  /**
   * @param ast
   *          is the original analyze ast
   * @param context
   *          the column stats auto gather context
   * @return the rewritten AST
   */
  public ASTNode rewriteAST(ASTNode ast, ColumnStatsAutoGatherContext context)
      throws SemanticException {
    // Save away the original AST
    originalTree = ast;

    tbl = AnalyzeCommandUtils.getTable(ast, this);

    boolean isPartitionStats = AnalyzeCommandUtils.isPartitionLevelStats(ast)
        || StatsUtils.isPartitionStats(tbl, conf);
    
    List<TransformSpec> partTransformSpec = null;
    Map<String, String> partSpec = null;

    List<FieldSchema> columnSchemas = getColumnsFromAst(ast);

    if (isPartitionStats) {
      partSpec = AnalyzeCommandUtils.getPartKeyValuePairsFromAST(tbl, ast, conf);
      handlePartialPartitionSpec(partSpec, context);
      if (tbl.hasNonNativePartitionSupport()) {
        partTransformSpec = tbl.getStorageHandler().getPartitionTransformSpec(tbl);
      }
    }
    rewrittenColumnSchemas = new FieldSchemas(columnSchemas);
    isTableLevel = !isPartitionStats;

    rewrittenQuery = genRewrittenQuery(rewrittenColumnSchemas, conf,
        partTransformSpec, partSpec, isPartitionStats, null);
    rewrittenTree = genRewrittenTree(rewrittenQuery);

    return rewrittenTree;
  }

  protected List<FieldSchema> getColumnsFromAst(ASTNode ast) throws SemanticException {
    List<FieldSchema> statsEligibleFS = null;
    List<String> columnNames;
    if (ast.getChildCount() == 2) {
      FieldSchemas eligibleFS = getStatsEligibleFieldSchemas(tbl);
      statsEligibleFS = eligibleFS.getSchemas();
      columnNames = eligibleFS.getColName();
    } else {
      columnNames = getExplicitColumnNamesFromAst(ast);
    }

    return statsEligibleFS != null ? statsEligibleFS : getFieldSchemasByColName(tbl, columnNames);
  }

  AnalyzeRewriteContext getAnalyzeRewriteContext() {
    AnalyzeRewriteContext analyzeRewrite = new AnalyzeRewriteContext();
    analyzeRewrite.setTableName(tbl.getFullyQualifiedName());
    analyzeRewrite.setTblLvl(isTableLevel);
    analyzeRewrite.setFieldSchemas(rewrittenColumnSchemas);
    return analyzeRewrite;
  }

  static AnalyzeRewriteContext genAnalyzeRewriteContext(HiveConf conf, Table tbl) {
    AnalyzeRewriteContext analyzeRewrite = new AnalyzeRewriteContext();
    analyzeRewrite.setTableName(tbl.getFullyQualifiedName());
    analyzeRewrite.setTblLvl(!(conf.getBoolVar(ConfVars.HIVE_STATS_COLLECT_PART_LEVEL_STATS) && tbl.isPartitioned()));
    analyzeRewrite.setFieldSchemas(getStatsEligibleFieldSchemas(tbl));
    return analyzeRewrite;
  }

  @Override
  public void setQueryType(ASTNode tree) {
    queryProperties.setQueryType(QueryProperties.QueryType.STATS);
  }
}
