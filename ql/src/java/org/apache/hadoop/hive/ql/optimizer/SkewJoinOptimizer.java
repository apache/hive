/**
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

package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc.ExprNodeDescEqualityWrapper;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.UnionDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SkewJoinOptimizer.
 *
 */
public class SkewJoinOptimizer extends Transform {

  private static final Logger LOG = LoggerFactory.getLogger(SkewJoinOptimizer.class.getName());

  public static class SkewJoinProc implements NodeProcessor {
    private ParseContext parseContext;

    public SkewJoinProc(ParseContext parseContext) {
      super();
      this.parseContext = parseContext;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
      Object... nodeOutputs) throws SemanticException {
      // We should be having a tree which looks like this
      //  TS -> * -> RS -
      //                  \
      //                   -> JOIN -> ..
      //                  /
      //  TS -> * -> RS -
      //
      // We are in the join operator now.

      SkewJoinOptProcCtx ctx = (SkewJoinOptProcCtx) procCtx;
      parseContext = ctx.getpGraphContext();

      JoinOperator joinOp = (JoinOperator)nd;
      // This join has already been processed
      if (ctx.getDoneJoins().contains(joinOp)) {
        return null;
      }

      ctx.getDoneJoins().add(joinOp);

      Operator<? extends OperatorDesc> currOp = joinOp;
      boolean processSelect = false;

      // Is there a select following
      // Clone the select also. It is useful for a follow-on optimization where the union
      // followed by a select star is completely removed.
      if ((joinOp.getChildOperators().size() == 1) &&
          (joinOp.getChildOperators().get(0) instanceof SelectOperator)) {
        currOp = joinOp.getChildOperators().get(0);
        processSelect = true;
      }

      List<TableScanOperator> tableScanOpsForJoin = new ArrayList<TableScanOperator>();
      if (!getTableScanOpsForJoin(joinOp, tableScanOpsForJoin)) {
        return null;
      }

      if ((tableScanOpsForJoin == null) || (tableScanOpsForJoin.isEmpty())) {
        return null;
      }

      // Get the skewed values in all the tables
      Map<List<ExprNodeDesc>, List<List<String>>> skewedValues =
        getSkewedValues(joinOp, tableScanOpsForJoin);

      // If there are no skewed values, nothing needs to be done
      if (skewedValues == null || skewedValues.size() == 0) {
        return null;
      }

      // After this optimization, the tree should be like:
      //  TS -> (FIL "skewed rows") * -> RS -
      //                                     \
      //                                       ->   JOIN
      //                                     /           \
      //  TS -> (FIL "skewed rows") * -> RS -             \
      //                                                   \
      //                                                     ->  UNION -> ..
      //                                                   /
      //  TS -> (FIL "no skewed rows") * -> RS -          /
      //                                        \        /
      //                                         -> JOIN
      //                                        /
      //  TS -> (FIL "no skewed rows") * -> RS -
      //

      // Create a clone of the operator
      Operator<? extends OperatorDesc> currOpClone;
      try {
        currOpClone = currOp.clone();
        insertRowResolvers(currOp, currOpClone, ctx);
      } catch (CloneNotSupportedException e) {
        LOG.debug("Operator tree could not be cloned");
        return null;
      }

      JoinOperator joinOpClone;
      if (processSelect) {
        joinOpClone = (JoinOperator)(currOpClone.getParentOperators().get(0));
      } else {
        joinOpClone = (JoinOperator)currOpClone;
      }
      joinOpClone.getConf().cloneQBJoinTreeProps(joinOp.getConf());
      parseContext.getJoinOps().add(joinOpClone);

      List<TableScanOperator> tableScanCloneOpsForJoin =
          new ArrayList<TableScanOperator>();
      if (!getTableScanOpsForJoin(joinOpClone, tableScanCloneOpsForJoin)) {
        LOG.debug("Operator tree not properly cloned!");
        return null;
      }

      // Put the filter "skewed column = skewed keys" in op
      // and "skewed columns != skewed keys" in selectOpClone
      insertSkewFilter(tableScanOpsForJoin, skewedValues, true);

      insertSkewFilter(tableScanCloneOpsForJoin, skewedValues, false);

      // Update the topOps appropriately
      Map<String, Operator<? extends OperatorDesc>> topOps = getTopOps(joinOpClone);
      Map<String, TableScanOperator> origTopOps = parseContext.getTopOps();

      for (Entry<String, Operator<? extends OperatorDesc>> topOp : topOps.entrySet()) {
        TableScanOperator tso = (TableScanOperator) topOp.getValue();
        String tabAlias = tso.getConf().getAlias();
        int initCnt = 1;
        String newAlias = "subquery" + initCnt + ":" + tabAlias;
        while (origTopOps.containsKey(newAlias)) {
          initCnt++;
          newAlias = "subquery" + initCnt + ":" + tabAlias;
        }

        parseContext.getTopOps().put(newAlias, tso);
        setUpAlias(joinOp, joinOpClone, tabAlias, newAlias, tso);
      }

      // Now do a union of the select operators: selectOp and selectOpClone
      // Store the operator that follows the select after the join, we will be
      // adding this as a child to the Union later
      List<Operator<? extends OperatorDesc>> finalOps = currOp.getChildOperators();
      currOp.setChildOperators(null);
      currOpClone.setChildOperators(null);

      // Make the union operator
      List<Operator<? extends OperatorDesc>> oplist =
        new ArrayList<Operator<? extends OperatorDesc>>();
      oplist.add(currOp);
      oplist.add(currOpClone);
      Operator<? extends OperatorDesc> unionOp =
        OperatorFactory.getAndMakeChild(currOp.getCompilationOpContext(),
          new UnionDesc(), new RowSchema(currOp.getSchema().getSignature()), oplist);

      // Introduce a select after the union
      List<Operator<? extends OperatorDesc>> unionList =
        new ArrayList<Operator<? extends OperatorDesc>>();
      unionList.add(unionOp);

      Operator<? extends OperatorDesc> selectUnionOp =
        OperatorFactory.getAndMakeChild(currOp.getCompilationOpContext(), new SelectDesc(true),
          new RowSchema(unionOp.getSchema().getSignature()), unionList);

      // add the finalOp after the union
      selectUnionOp.setChildOperators(finalOps);
      // replace the original selectOp in the parents with selectUnionOp
      for (Operator<? extends OperatorDesc> finalOp : finalOps) {
        finalOp.replaceParent(currOp, selectUnionOp);
      }
      return null;
    }

    /*
     * Get the list of table scan operators for this join. A interface supportSkewJoinOptimization
     * has been provided. Currently, it is only enabled for simple filters and selects.
     */
    private boolean getTableScanOpsForJoin(
      JoinOperator op,
      List<TableScanOperator> tsOps) {

      for (Operator<? extends OperatorDesc> parent : op.getParentOperators()) {
        if (!getTableScanOps(parent, tsOps)) {
          return false;
        }
      }
      return true;
    }

    private boolean getTableScanOps(
      Operator<? extends OperatorDesc> op,
      List<TableScanOperator> tsOps) {
      for (Operator<? extends OperatorDesc> parent : op.getParentOperators()) {
        if (!parent.supportSkewJoinOptimization()) {
          return false;
        }

        if (parent instanceof TableScanOperator) {
          tsOps.add((TableScanOperator)parent);
        } else if (!getTableScanOps(parent, tsOps)) {
          return false;
        }
      }
      return true;
    }

    /**
     * Returns the skewed values in all the tables which are going to be scanned.
     * If the join is on columns c1, c2 and c3 on tables T1 and T2,
     * T1 is skewed on c1 and c4 with the skew values ((1,2),(3,4)),
     * whereas T2 is skewed on c1, c2 with skew values ((5,6),(7,8)), the resulting
     * map would be: <(c1) -> ((1), (3)), (c1,c2) -> ((5,6),(7,8))>
     * @param op The join operator being optimized
     * @param tableScanOpsForJoin table scan operators which are parents of the join operator
     * @return map<join keys intersection skewedkeys, list of skewed values>.
     * @throws SemanticException
     */
    private Map<List<ExprNodeDesc>, List<List<String>>>
      getSkewedValues(
        Operator<? extends OperatorDesc> op, List<TableScanOperator> tableScanOpsForJoin) throws SemanticException {

      Map <List<ExprNodeDesc>, List<List<String>>> skewDataReturn =
        new HashMap<List<ExprNodeDesc>, List<List<String>>>();

      Map <List<ExprNodeDescEqualityWrapper>, List<List<String>>> skewData =
          new HashMap<List<ExprNodeDescEqualityWrapper>, List<List<String>>>();

      // The join keys are available in the reduceSinkOperators before join
      for (Operator<? extends OperatorDesc> reduceSinkOp : op.getParentOperators()) {
        ReduceSinkDesc rsDesc = ((ReduceSinkOperator) reduceSinkOp).getConf();

        if (rsDesc.getKeyCols() != null) {
          TableScanOperator tableScanOp = null;
          Table table = null;
          // Find the skew information corresponding to the table
          List<String> skewedColumns = null;
          List<List<String>> skewedValueList = null;

          // The join columns which are also skewed
          List<ExprNodeDescEqualityWrapper> joinKeysSkewedCols =
            new ArrayList<ExprNodeDescEqualityWrapper>();

          // skewed Keys which intersect with join keys
          List<Integer> positionSkewedKeys = new ArrayList<Integer>();

          // Update the joinKeys appropriately.
          for (ExprNodeDesc keyColDesc : rsDesc.getKeyCols()) {
            ExprNodeColumnDesc keyCol = null;

            // If the key column is not a column, then dont apply this optimization.
            // This will be fixed as part of https://issues.apache.org/jira/browse/HIVE-3445
            // for type conversion UDFs.
            if (keyColDesc instanceof ExprNodeColumnDesc) {
              keyCol = (ExprNodeColumnDesc) keyColDesc;
              if (table == null) {
                tableScanOp = getTableScanOperator(parseContext, reduceSinkOp, tableScanOpsForJoin);
                table =
                  tableScanOp == null ? null : tableScanOp.getConf().getTableMetadata();
                skewedColumns =
                  table == null ? null : table.getSkewedColNames();
                // No skew on the table to take care of
                if ((skewedColumns == null) || (skewedColumns.isEmpty())) {
                  continue;
                }

                skewedValueList =
                  table == null ? null : table.getSkewedColValues();
              }
              ExprNodeDesc keyColOrigin = ExprNodeDescUtils.backtrack(keyCol,
                      reduceSinkOp, tableScanOp);
              int pos = keyColOrigin == null || !(keyColOrigin instanceof ExprNodeColumnDesc) ?
                      -1 : skewedColumns.indexOf(((ExprNodeColumnDesc)keyColOrigin).getColumn());
              if ((pos >= 0) && (!positionSkewedKeys.contains(pos))) {
                positionSkewedKeys.add(pos);
                ExprNodeColumnDesc keyColClone = (ExprNodeColumnDesc) keyColOrigin.clone();
                keyColClone.setTabAlias(null);
                joinKeysSkewedCols.add(new ExprNodeDescEqualityWrapper(keyColClone));
              }
            }
          }

          // If the skew keys match the join keys, then add it to the list
          if ((skewedColumns != null) && (!skewedColumns.isEmpty())) {
            if (!joinKeysSkewedCols.isEmpty()) {
              // If the join keys matches the skewed keys, use the table skewed keys
              List<List<String>> skewedJoinValues;
              if (skewedColumns.size() == positionSkewedKeys.size()) {
                skewedJoinValues = skewedValueList;
              }
              else {
                skewedJoinValues =
                  getSkewedJoinValues(skewedValueList, positionSkewedKeys);
              }

              List<List<String>> oldSkewedJoinValues =
                skewData.get(joinKeysSkewedCols);
              if (oldSkewedJoinValues == null) {
                oldSkewedJoinValues = new ArrayList<List<String>>();
              }
              for (List<String> skewValue : skewedJoinValues) {
                if (!oldSkewedJoinValues.contains(skewValue)) {
                  oldSkewedJoinValues.add(skewValue);
                }
              }

              skewData.put(joinKeysSkewedCols, oldSkewedJoinValues);
            }
          }
        }
      }

      // convert skewData to contain ExprNodeDesc in the keys
      for (Map.Entry<List<ExprNodeDescEqualityWrapper>, List<List<String>>> mapEntry :
        skewData.entrySet()) {
          List<ExprNodeDesc> skewedKeyJoinCols = new ArrayList<ExprNodeDesc>();
          for (ExprNodeDescEqualityWrapper key : mapEntry.getKey()) {
            skewedKeyJoinCols.add(key.getExprNodeDesc());
          }
          skewDataReturn.put(skewedKeyJoinCols, mapEntry.getValue());
      }

      return skewDataReturn;
    }

    /**
     * Get the table scan.
     */
    private TableScanOperator getTableScanOperator(
      ParseContext parseContext,
      Operator<? extends OperatorDesc> op,
      List<TableScanOperator> tableScanOpsForJoin) {
      while (true) {
        if (op instanceof TableScanOperator) {
          TableScanOperator tsOp = (TableScanOperator)op;
          if (tableScanOpsForJoin.contains(tsOp)) {
            return tsOp;
          }
        }
        if ((op.getParentOperators() == null) || (op.getParentOperators().isEmpty()) ||
            (op.getParentOperators().size() > 1)) {
          return null;
        }
        op = op.getParentOperators().get(0);
      }
    }

    /*
     * If the skewedValues contains ((1,2,3),(4,5,6)), and the user is looking for
     * positions (0,2), the result would be ((1,3),(4,6))
     * Get the skewed key values that are part of the join key.
     * @param skewedValuesList List of all the skewed values
     * @param positionSkewedKeys the requested positions
     * @return sub-list of skewed values with the positions present
     */
    private List<List<String>> getSkewedJoinValues(
      List<List<String>> skewedValueList, List<Integer> positionSkewedKeys) {
      List<List<String>> skewedJoinValues = new ArrayList<List<String>>();
      for (List<String> skewedValuesAllColumns : skewedValueList) {
        List<String> skewedValuesSpecifiedColumns = new ArrayList<String>();
        for (int pos : positionSkewedKeys) {
          skewedValuesSpecifiedColumns.add(skewedValuesAllColumns.get(pos));
        }
        skewedJoinValues.add(skewedValuesSpecifiedColumns);
      }
      return skewedJoinValues;
    }

    /**
     * Inserts a filter comparing the join keys with the skewed keys. If the table
     * is skewed with values (k1, v1) and (k2, v2) on columns (key, value), then
     * filter ((key=k1 AND value=v1) OR (key=k2 AND value=v2)) is inserted. If @skewed
     * is false, a NOT is inserted before it.
     * @param tableScanOpsForJoin table scans for which the filter will be inserted
     * @param skewedValuesList the map of <expressions, list of skewed values>
     * @param skewed True if we want skewedCol = skewedValue, false if we want
     * not (skewedCol = skewedValue)
     */
    private void insertSkewFilter(
      List<TableScanOperator> tableScanOpsForJoin,
      Map<List<ExprNodeDesc>, List<List<String>>> skewedValuesList,
      boolean skewed) {

      ExprNodeDesc filterExpr = constructFilterExpr(skewedValuesList, skewed);
      for (TableScanOperator tableScanOp : tableScanOpsForJoin) {
        insertFilterOnTop(tableScanOp, filterExpr);
      }
    }

    /**
     * Inserts a filter below the table scan operator. Construct the filter
     * from the filter expression provided.
     * @param tableScanOp the table scan operators
     * @param filterExpr the filter expression
     */
    private void insertFilterOnTop(
      TableScanOperator tableScanOp,
      ExprNodeDesc filterExpr) {

      // Get the top operator and it's child, all operators have a single parent
      Operator<? extends OperatorDesc> currChild = tableScanOp.getChildOperators().get(0);

      // Create the filter Operator and update the parents and children appropriately
      tableScanOp.setChildOperators(null);
      currChild.setParentOperators(null);

      Operator<FilterDesc> filter = OperatorFactory.getAndMakeChild(
        new FilterDesc(filterExpr, false),
        new RowSchema(tableScanOp.getSchema().getSignature()), tableScanOp);
      OperatorFactory.makeChild(filter, currChild);
    }

    /**
     * Construct the filter expression from the skewed keys and skewed values.
     * If the skewed join keys are (k1), and (k1,k3) with the skewed values
     * (1,2) and ((2,3),(4,5)) respectively, the filter expression would be:
     * (k1=1) or (k1=2) or ((k1=2) and (k3=3)) or ((k1=4) and (k3=5)).
     */
    private ExprNodeDesc constructFilterExpr(
      Map<List<ExprNodeDesc>, List<List<String>>> skewedValuesMap,
      boolean skewed) {

      ExprNodeDesc finalExprNodeDesc = null;
      try {
        for (Map.Entry<List<ExprNodeDesc>, List<List<String>>> mapEntry :
          skewedValuesMap.entrySet()) {
          List<ExprNodeDesc> keyCols = mapEntry.getKey();
          List<List<String>> skewedValuesList = mapEntry.getValue();

          for (List<String> skewedValues : skewedValuesList) {
            int keyPos = 0;
            ExprNodeDesc currExprNodeDesc = null;

            // Make the following condition: all the values match for all the columns
            for (String skewedValue : skewedValues) {
              List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();

              // We have ensured that the keys are columns
              ExprNodeColumnDesc keyCol = (ExprNodeColumnDesc) keyCols.get(keyPos).clone();
              keyPos++;
              children.add(keyCol);

              // Convert the constants available as strings to the corresponding objects
              children.add(createConstDesc(skewedValue, keyCol));

              ExprNodeGenericFuncDesc expr = null;
              // Create the equality condition
              expr = ExprNodeGenericFuncDesc.newInstance(new GenericUDFOPEqual(), children);
              if (currExprNodeDesc == null) {
                currExprNodeDesc = expr;
              } else {
                // If there are previous nodes, then AND the current node with the previous one
                List<ExprNodeDesc> childrenAND = new ArrayList<ExprNodeDesc>();
                childrenAND.add(currExprNodeDesc);
                childrenAND.add(expr);
                currExprNodeDesc =
                  ExprNodeGenericFuncDesc.newInstance(new GenericUDFOPAnd(), childrenAND);
              }
            }

            // If there are more than one skewed values,
            // then OR the current node with the previous one
            if (finalExprNodeDesc == null) {
              finalExprNodeDesc = currExprNodeDesc;
            } else {
              List<ExprNodeDesc> childrenOR = new ArrayList<ExprNodeDesc>();
              childrenOR.add(finalExprNodeDesc);
              childrenOR.add(currExprNodeDesc);

              finalExprNodeDesc =
                ExprNodeGenericFuncDesc.newInstance(new GenericUDFOPOr(), childrenOR);
            }
          }
        }

        // Add a NOT operator in the beginning (this is for the cloned operator because we
        // want the values which are not skewed
        if (skewed == false) {
          List<ExprNodeDesc> childrenNOT = new ArrayList<ExprNodeDesc>();
          childrenNOT.add(finalExprNodeDesc);
          finalExprNodeDesc =
            ExprNodeGenericFuncDesc.newInstance(new GenericUDFOPNot(), childrenNOT);
        }
      } catch (UDFArgumentException e) {
        // Ignore the exception because we are not comparing Long vs. String here.
        // There should never be an exception
        assert false;
      }
      return finalExprNodeDesc;
    }

    /**
     * Converts the skewedValue available as a string in the metadata to the appropriate object
     * by using the type of the column from the join key.
     * @param skewedValue
     * @param keyCol
     * @return an expression node descriptor of the appropriate constant
     */
    private ExprNodeConstantDesc createConstDesc(
      String skewedValue, ExprNodeColumnDesc keyCol) {
      ObjectInspector inputOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(
        TypeInfoFactory.stringTypeInfo);
      ObjectInspector outputOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(
        keyCol.getTypeInfo());
      Converter converter = ObjectInspectorConverters.getConverter(inputOI, outputOI);
      Object skewedValueObject = converter.convert(skewedValue);
      return new ExprNodeConstantDesc(keyCol.getTypeInfo(), skewedValueObject);
    }

    private Map<String, Operator<? extends OperatorDesc>> getTopOps(
      Operator<? extends OperatorDesc> op) {
      // Must be deterministic order map for consistent q-test output across
      // Java versions
      Map<String, Operator<? extends OperatorDesc>> topOps =
        new LinkedHashMap<String, Operator<? extends OperatorDesc>>();
      if (op.getParentOperators() == null || op.getParentOperators().size() == 0) {
        topOps.put(((TableScanOperator)op).getConf().getAlias(), op);
      } else {
        for (Operator<? extends OperatorDesc> parent : op.getParentOperators()) {
          if (parent != null) {
            topOps.putAll(getTopOps(parent));
          }
        }
      }
      return topOps;
    }

    private void insertRowResolvers(
      Operator<? extends OperatorDesc> op,
      Operator<? extends OperatorDesc> opClone,
      SkewJoinOptProcCtx ctx) {

      if (op instanceof TableScanOperator) {
        ctx.getCloneTSOpMap().put((TableScanOperator)opClone, (TableScanOperator)op);
      }

      List<Operator<? extends OperatorDesc>> parents = op.getParentOperators();
      List<Operator<? extends OperatorDesc>> parentClones = opClone.getParentOperators();
      if ((parents != null) && (!parents.isEmpty()) &&
        (parentClones != null) && (!parentClones.isEmpty())) {
        for (int pos = 0; pos < parents.size(); pos++) {
          insertRowResolvers(parents.get(pos), parentClones.get(pos), ctx);
        }
      }
    }

    /**
     * Set alias in the cloned join tree
     */
    private static void setUpAlias(JoinOperator origin, JoinOperator cloned, String origAlias,
        String newAlias, Operator<? extends OperatorDesc> topOp) {
      cloned.getConf().getAliasToOpInfo().remove(origAlias);
      cloned.getConf().getAliasToOpInfo().put(newAlias, topOp);
      if (origin.getConf().getLeftAlias().equals(origAlias)) {
        cloned.getConf().setLeftAlias(null);
        cloned.getConf().setLeftAlias(newAlias);
      }
      replaceAlias(origin.getConf().getLeftAliases(), cloned.getConf().getLeftAliases(), origAlias, newAlias);
      replaceAlias(origin.getConf().getRightAliases(), cloned.getConf().getRightAliases(), origAlias, newAlias);
      replaceAlias(origin.getConf().getBaseSrc(), cloned.getConf().getBaseSrc(), origAlias, newAlias);
      replaceAlias(origin.getConf().getMapAliases(), cloned.getConf().getMapAliases(), origAlias, newAlias);
      replaceAlias(origin.getConf().getStreamAliases(), cloned.getConf().getStreamAliases(), origAlias, newAlias);
    }

    private static void replaceAlias(String[] origin, String[] cloned,
        String alias, String newAlias) {
      if (origin == null || cloned == null || origin.length != cloned.length) {
        return;
      }
      for (int i = 0; i < origin.length; i++) {
        if (origin[i].equals(alias)) {
          cloned[i] = newAlias;
        }
      }
    }

    private static void replaceAlias(List<String> origin, List<String> cloned,
        String alias, String newAlias) {
      if (origin == null || cloned == null || origin.size() != cloned.size()) {
        return;
      }
      for (int i = 0; i < origin.size(); i++) {
        if (origin.get(i).equals(alias)) {
          cloned.set(i, newAlias);
        }
      }
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.ql.optimizer.Transform#transform
   * (org.apache.hadoop.hive.ql.parse.ParseContext)
   */
  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    opRules.put(new RuleRegExp("R1", "TS%.*RS%JOIN%"), getSkewJoinProc(pctx));
    SkewJoinOptProcCtx skewJoinOptProcCtx = new SkewJoinOptProcCtx(pctx);
    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(
      null, opRules, skewJoinOptProcCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  private NodeProcessor getSkewJoinProc(ParseContext parseContext) {
    return new SkewJoinProc(parseContext);
  }

  /**
   * SkewJoinOptProcCtx.
   *
   */
  public static class SkewJoinOptProcCtx implements NodeProcessorCtx {

    private ParseContext pGraphContext;

    // set of joins already processed
    private Set<JoinOperator> doneJoins;
    private Map<TableScanOperator, TableScanOperator> cloneTSOpMap;

    public SkewJoinOptProcCtx(ParseContext pctx) {
      this.pGraphContext = pctx;
      doneJoins = new HashSet<JoinOperator>();
      cloneTSOpMap = new HashMap<TableScanOperator, TableScanOperator>();
    }

    public ParseContext getpGraphContext() {
      return pGraphContext;
    }

    public void setPGraphContext(ParseContext graphContext) {
      pGraphContext = graphContext;
    }

    public Set<JoinOperator> getDoneJoins() {
      return doneJoins;
    }

    public void setDoneJoins(Set<JoinOperator> doneJoins) {
      this.doneJoins = doneJoins;
    }

    public Map<TableScanOperator, TableScanOperator> getCloneTSOpMap() {
      return cloneTSOpMap;
    }

    public void setCloneTSOpMap(Map<TableScanOperator, TableScanOperator> cloneTSOpMap) {
      this.cloneTSOpMap = cloneTSOpMap;
    }
  }
}
