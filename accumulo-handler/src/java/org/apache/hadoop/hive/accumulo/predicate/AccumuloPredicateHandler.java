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

package org.apache.hadoop.hive.accumulo.predicate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.accumulo.columns.ColumnEncoding;
import org.apache.hadoop.hive.accumulo.columns.ColumnMapper;
import org.apache.hadoop.hive.accumulo.columns.HiveAccumuloColumnMapping;
import org.apache.hadoop.hive.accumulo.predicate.compare.CompareOp;
import org.apache.hadoop.hive.accumulo.predicate.compare.DoubleCompare;
import org.apache.hadoop.hive.accumulo.predicate.compare.Equal;
import org.apache.hadoop.hive.accumulo.predicate.compare.GreaterThan;
import org.apache.hadoop.hive.accumulo.predicate.compare.GreaterThanOrEqual;
import org.apache.hadoop.hive.accumulo.predicate.compare.IntCompare;
import org.apache.hadoop.hive.accumulo.predicate.compare.LessThan;
import org.apache.hadoop.hive.accumulo.predicate.compare.LessThanOrEqual;
import org.apache.hadoop.hive.accumulo.predicate.compare.Like;
import org.apache.hadoop.hive.accumulo.predicate.compare.LongCompare;
import org.apache.hadoop.hive.accumulo.predicate.compare.NotEqual;
import org.apache.hadoop.hive.accumulo.predicate.compare.PrimitiveComparison;
import org.apache.hadoop.hive.accumulo.predicate.compare.StringCompare;
import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDeParameters;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler.DecomposedPredicate;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 *
 * Supporting operations dealing with Hive Predicate pushdown to iterators and ranges.
 *
 * See {@link PrimitiveComparisonFilter}
 *
 */
public class AccumuloPredicateHandler {
  private static final List<Range> TOTAL_RANGE = Collections.singletonList(new Range());

  private static AccumuloPredicateHandler handler = new AccumuloPredicateHandler();
  private static Map<String, Class<? extends CompareOp>> compareOps = Maps.newHashMap();
  private static Map<String, Class<? extends PrimitiveComparison>> pComparisons = Maps.newHashMap();

  // Want to start sufficiently "high" enough in the iterator stack
  private static int iteratorCount = 50;

  private static final Logger LOG = LoggerFactory.getLogger(AccumuloPredicateHandler.class);
  static {
    compareOps.put(GenericUDFOPEqual.class.getName(), Equal.class);
    compareOps.put(GenericUDFOPNotEqual.class.getName(), NotEqual.class);
    compareOps.put(GenericUDFOPGreaterThan.class.getName(), GreaterThan.class);
    compareOps.put(GenericUDFOPEqualOrGreaterThan.class.getName(), GreaterThanOrEqual.class);
    compareOps.put(GenericUDFOPEqualOrLessThan.class.getName(), LessThanOrEqual.class);
    compareOps.put(GenericUDFOPLessThan.class.getName(), LessThan.class);
    compareOps.put(UDFLike.class.getName(), Like.class);

    pComparisons.put("bigint", LongCompare.class);
    pComparisons.put("int", IntCompare.class);
    pComparisons.put("double", DoubleCompare.class);
    pComparisons.put("string", StringCompare.class);
  }

  public static AccumuloPredicateHandler getInstance() {
    return handler;
  }

  /**
   *
   * @return set of all UDF class names with matching CompareOpt implementations.
   */
  public Set<String> cOpKeyset() {
    return compareOps.keySet();
  }

  /**
   *
   * @return set of all hive data types with matching PrimitiveCompare implementations.
   */
  public Set<String> pComparisonKeyset() {
    return pComparisons.keySet();
  }

  /**
   *
   * @param udfType
   *          GenericUDF classname to lookup matching CompareOpt
   * @return Class<? extends CompareOpt/>
   */
  public Class<? extends CompareOp> getCompareOpClass(String udfType)
      throws NoSuchCompareOpException {
    if (!compareOps.containsKey(udfType)) {
      throw new NoSuchCompareOpException("Null compare op for specified key: " + udfType);
    }
    return compareOps.get(udfType);
  }

  public CompareOp getCompareOp(String udfType, IndexSearchCondition sc)
      throws NoSuchCompareOpException, SerDeException {
    Class<? extends CompareOp> clz = getCompareOpClass(udfType);

    try {
      return clz.newInstance();
    } catch (ClassCastException e) {
      throw new SerDeException("Column type mismatch in WHERE clause "
          + sc.getIndexExpr().getExprString() + " found type "
          + sc.getConstantDesc().getTypeString() + " instead of "
          + sc.getColumnDesc().getTypeString());
    } catch (IllegalAccessException e) {
      throw new SerDeException("Could not instantiate class for WHERE clause", e);
    } catch (InstantiationException e) {
      throw new SerDeException("Could not instantiate class for WHERE clause", e);
    }
  }

  /**
   *
   * @param type
   *          String hive column lookup matching PrimitiveCompare
   * @return Class<? extends ></?>
   */
  public Class<? extends PrimitiveComparison> getPrimitiveComparisonClass(String type)
      throws NoSuchPrimitiveComparisonException {
    if (!pComparisons.containsKey(type)) {
      throw new NoSuchPrimitiveComparisonException("Null primitive comparison for specified key: "
          + type);
    }
    return pComparisons.get(type);
  }

  public PrimitiveComparison getPrimitiveComparison(String type, IndexSearchCondition sc)
      throws NoSuchPrimitiveComparisonException, SerDeException {
    Class<? extends PrimitiveComparison> clz = getPrimitiveComparisonClass(type);

    try {
      return clz.newInstance();
    } catch (ClassCastException e) {
      throw new SerDeException("Column type mismatch in WHERE clause "
          + sc.getIndexExpr().getExprString() + " found type "
          + sc.getConstantDesc().getTypeString() + " instead of "
          + sc.getColumnDesc().getTypeString());
    } catch (IllegalAccessException e) {
      throw new SerDeException("Could not instantiate class for WHERE clause", e);
    } catch (InstantiationException e) {
      throw new SerDeException("Could not instantiate class for WHERE clause", e);
    }
  }

  private AccumuloPredicateHandler() {}

  /**
   * Loop through search conditions and build ranges for predicates involving rowID column, if any.
   */
  public List<Range> getRanges(Configuration conf, ColumnMapper columnMapper)
      throws SerDeException {
    if (!columnMapper.hasRowIdMapping()) {
      return TOTAL_RANGE;
    }

    int rowIdOffset = columnMapper.getRowIdOffset();
    String[] hiveColumnNamesArr = conf.getStrings(serdeConstants.LIST_COLUMNS);

    if (null == hiveColumnNamesArr) {
      throw new IllegalArgumentException("Could not find Hive columns in configuration");
    }

    // Already verified that we should have the rowId mapping
    String hiveRowIdColumnName = hiveColumnNamesArr[rowIdOffset];

    ExprNodeDesc root = this.getExpression(conf);

    // No expression, therefore scan the whole table
    if (null == root) {
      return TOTAL_RANGE;
    }

    Object result = generateRanges(conf, columnMapper, hiveRowIdColumnName, root);

    if (null == result) {
      LOG.info("Calculated null set of ranges, scanning full table");
      return TOTAL_RANGE;
    } else if (result instanceof Range) {
      LOG.info("Computed a single Range for the query: " + result);
      return Collections.singletonList((Range) result);
    } else if (result instanceof List) {
      LOG.info("Computed a collection of Ranges for the query: " + result);
      @SuppressWarnings("unchecked")
      List<Range> ranges = (List<Range>) result;
      return ranges;
    } else {
      throw new IllegalArgumentException("Unhandled return from Range generation: " + result);
    }
  }

  /**
   * Encapsulates the traversal over some {@link ExprNodeDesc} tree for the generation of Accumuluo.
   * Ranges using expressions involving the Accumulo rowid-mapped Hive column.
   *
   * @param conf
   *          Hadoop configuration
   * @param columnMapper
   *          Mapping of Hive to Accumulo columns for the query
   * @param hiveRowIdColumnName
   *          Name of the hive column mapped to the Accumulo rowid
   * @param root
   *          Root of some ExprNodeDesc tree to traverse, the WHERE clause
   * @return An object representing the result from the ExprNodeDesc tree traversal using the
   *         AccumuloRangeGenerator
   */
  protected Object generateRanges(Configuration conf, ColumnMapper columnMapper,
                                  String hiveRowIdColumnName, ExprNodeDesc root) {
    AccumuloRangeGenerator rangeGenerator = new AccumuloRangeGenerator(conf, handler,
        columnMapper.getRowIdMapping(), hiveRowIdColumnName);
    Dispatcher disp = new DefaultRuleDispatcher(rangeGenerator,
        Collections.<Rule, NodeProcessor> emptyMap(), null);
    GraphWalker ogw = new DefaultGraphWalker(disp);
    List<Node> roots = new ArrayList<Node>();
    roots.add(root);
    HashMap<Node, Object> nodeOutput = new HashMap<Node, Object>();

    try {
      ogw.startWalking(roots, nodeOutput);
    } catch (SemanticException ex) {
      throw new RuntimeException(ex);
    }

    return nodeOutput.get(root);
  }

  /**
   * Loop through search conditions and build iterator settings for predicates involving columns
   * other than rowID, if any.
   *
   * @param conf
   *          Configuration
   * @throws SerDeException
   */
  public List<IteratorSetting> getIterators(Configuration conf, ColumnMapper columnMapper)
      throws SerDeException {
    List<IteratorSetting> itrs = Lists.newArrayList();
    boolean shouldPushdown = conf.getBoolean(AccumuloSerDeParameters.ITERATOR_PUSHDOWN_KEY,
        AccumuloSerDeParameters.ITERATOR_PUSHDOWN_DEFAULT);
    if (!shouldPushdown) {
      LOG.info("Iterator pushdown is disabled for this table");
      return itrs;
    }

    boolean binaryEncodedRow = ColumnEncoding.BINARY.getName().
        equalsIgnoreCase(conf.get(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE));

    int rowIdOffset = columnMapper.getRowIdOffset();
    String[] hiveColumnNamesArr = conf.getStrings(serdeConstants.LIST_COLUMNS);

    if (null == hiveColumnNamesArr) {
      throw new IllegalArgumentException("Could not find Hive columns in configuration");
    }

    String hiveRowIdColumnName = null;

    if (rowIdOffset >= 0 && rowIdOffset < hiveColumnNamesArr.length) {
      hiveRowIdColumnName = hiveColumnNamesArr[rowIdOffset];
    }

    List<String> hiveColumnNames = Arrays.asList(hiveColumnNamesArr);

    for (IndexSearchCondition sc : getSearchConditions(conf)) {
      String col = sc.getColumnDesc().getColumn();
      if (hiveRowIdColumnName == null || !hiveRowIdColumnName.equals(col)) {
        HiveAccumuloColumnMapping mapping = (HiveAccumuloColumnMapping) columnMapper
            .getColumnMappingForHiveColumn(hiveColumnNames, col);
        itrs.add(toSetting(mapping, sc, binaryEncodedRow));
      }
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("num iterators = " + itrs.size());
    }
    return itrs;
  }

  /**
   * Create an IteratorSetting for the right qualifier, constant, CompareOpt, and PrimitiveCompare
   * type.
   *
   * @param accumuloColumnMapping
   *          ColumnMapping to filter
   * @param sc
   *          IndexSearchCondition
   * @param binaryEncodedValues
   *          flag for binary encodedValues
   * @return IteratorSetting
   * @throws SerDeException
   */
  public IteratorSetting toSetting(HiveAccumuloColumnMapping accumuloColumnMapping,
      IndexSearchCondition sc, boolean binaryEncodedValues) throws SerDeException {
    iteratorCount++;
    final IteratorSetting is = new IteratorSetting(iteratorCount,
        PrimitiveComparisonFilter.FILTER_PREFIX + iteratorCount,
        PrimitiveComparisonFilter.class);
    final String type =  binaryEncodedValues ? sc.getColumnDesc().getTypeString()
                                             : ColumnEncoding.STRING.getName();
    final String comparisonOpStr = sc.getComparisonOp();

    PushdownTuple tuple;
    try {
      tuple = new PushdownTuple(sc, getPrimitiveComparison(type, sc), getCompareOp(comparisonOpStr,
          sc));
    } catch (NoSuchPrimitiveComparisonException e) {
      throw new SerDeException("No configured PrimitiveComparison class for " + type, e);
    } catch (NoSuchCompareOpException e) {
      throw new SerDeException("No configured CompareOp class for " + comparisonOpStr, e);
    }

    is.addOption(PrimitiveComparisonFilter.P_COMPARE_CLASS, tuple.getpCompare().getClass()
        .getName());
    is.addOption(PrimitiveComparisonFilter.COMPARE_OPT_CLASS, tuple.getcOpt().getClass().getName());
    is.addOption(PrimitiveComparisonFilter.CONST_VAL,
        new String(Base64.encodeBase64(tuple.getConstVal())));
    is.addOption(PrimitiveComparisonFilter.COLUMN, accumuloColumnMapping.serialize());

    return is;
  }

  public ExprNodeDesc getExpression(Configuration conf) {
    String filteredExprSerialized = conf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    if (filteredExprSerialized == null) {
      return null;
    }

    return SerializationUtilities.deserializeExpression(filteredExprSerialized);
  }

  /**
   *
   * @param conf
   *          Configuration
   * @return list of IndexSearchConditions from the filter expression.
   */
  public List<IndexSearchCondition> getSearchConditions(Configuration conf) {
    final List<IndexSearchCondition> sConditions = Lists.newArrayList();
    ExprNodeDesc filterExpr = getExpression(conf);
    if (null == filterExpr) {
      return sConditions;
    }
    IndexPredicateAnalyzer analyzer = newAnalyzer(conf);
    ExprNodeDesc residual = analyzer.analyzePredicate(filterExpr, sConditions);
    if (residual != null) {
      throw new RuntimeException("Unexpected residual predicate: " + residual.getExprString());
    }
    return sConditions;
  }

  /**
   *
   * @param conf
   *          Configuration
   * @param desc
   *          predicate expression node.
   * @return DecomposedPredicate containing translated search conditions the analyzer can support.
   */
  public DecomposedPredicate decompose(Configuration conf, ExprNodeDesc desc) {
    IndexPredicateAnalyzer analyzer = newAnalyzer(conf);
    List<IndexSearchCondition> sConditions = new ArrayList<IndexSearchCondition>();
    ExprNodeDesc residualPredicate = analyzer.analyzePredicate(desc, sConditions);

    if (sConditions.size() == 0) {
      LOG.info("nothing to decompose. Returning");
      return null;
    }

    DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
    decomposedPredicate.pushedPredicate = analyzer.translateSearchConditions(sConditions);
    decomposedPredicate.residualPredicate = (ExprNodeGenericFuncDesc) residualPredicate;
    return decomposedPredicate;
  }

  /**
   * Build an analyzer that allows comparison opts from compareOpts map, and all columns from table
   * definition.
   */
  private IndexPredicateAnalyzer newAnalyzer(Configuration conf) {
    IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();
    analyzer.clearAllowedColumnNames();
    for (String op : cOpKeyset()) {
      analyzer.addComparisonOp(op);
    }

    String[] hiveColumnNames = conf.getStrings(serdeConstants.LIST_COLUMNS);
    for (String col : hiveColumnNames) {
      analyzer.allowColumnName(col);
    }

    return analyzer;
  }
}
