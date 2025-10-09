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

package org.apache.hadoop.hive.ql.optimizer.calcite.translator.opconventer;

import java.util.Map;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAntiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveMultiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortExchange;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableFunctionScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveValues;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.UnparseTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class HiveOpConverter {
  private static final Logger LOG = LoggerFactory.getLogger(HiveOpConverter.class);

  // TODO: remove this after stashing only rqd pieces from opconverter
  private final SemanticAnalyzer               semanticAnalyzer;
  private final HiveConf                       hiveConf;
  private final UnparseTranslator              unparseTranslator;
  private final Map<String, TableScanOperator> topOps;
  private int                                  uniqueCounter;

  public HiveOpConverter(SemanticAnalyzer semanticAnalyzer, HiveConf hiveConf,
      UnparseTranslator unparseTranslator, Map<String, TableScanOperator> topOps) {
    this.semanticAnalyzer = semanticAnalyzer;
    this.hiveConf = hiveConf;
    this.unparseTranslator = unparseTranslator;
    this.topOps = topOps;
    this.uniqueCounter = 0;
  }

  static class OpAttr {
    final String               tabAlias;
    ImmutableList<Operator<?>> inputs;
    ImmutableSet<Integer>      vcolsInCalcite;

    OpAttr(String tabAlias, Set<Integer> vcols, Operator<?>... inputs) {
      this.tabAlias = tabAlias;
      this.inputs = ImmutableList.copyOf(inputs);
      this.vcolsInCalcite = ImmutableSet.copyOf(vcols);
    }

    OpAttr clone(Operator<?>... inputs) {
      return new OpAttr(tabAlias, vcolsInCalcite, inputs);
    }
  }

  public Operator<?> convert(RelNode root) throws SemanticException {
    OpAttr opAf = dispatch(root);
    Operator<?>rootOp = opAf.inputs.get(0);
    handleTopLimit(rootOp);
    return rootOp;
  }

  OpAttr dispatch(RelNode rn) throws SemanticException {
    if (rn instanceof HiveTableScan) {
      return new HiveTableScanVisitor(this).visit((HiveTableScan) rn);
    } else if (rn instanceof HiveProject) {
      return new HiveProjectVisitor(this).visit((HiveProject) rn);
    } else if (rn instanceof HiveMultiJoin) {
      return new JoinVisitor(this).visit((HiveMultiJoin) rn);
    } else if (rn instanceof HiveJoin) {
      return new JoinVisitor(this).visit((HiveJoin) rn);
    } else if (rn instanceof HiveSemiJoin) {
      return new JoinVisitor(this).visit(rn);
    } else if (rn instanceof HiveAntiJoin) {
      return new JoinVisitor(this).visit(rn);
    } else if (rn instanceof HiveFilter) {
      return new HiveFilterVisitor(this).visit((HiveFilter) rn);
    } else if (rn instanceof HiveSortLimit) {
      return new HiveSortLimitVisitor(this).visit((HiveSortLimit) rn);
    } else if (rn instanceof HiveUnion) {
      return new HiveUnionVisitor(this).visit((HiveUnion) rn);
    } else if (rn instanceof HiveSortExchange) {
      return new HiveSortExchangeVisitor(this).visit((HiveSortExchange) rn);
    } else if (rn instanceof HiveAggregate) {
      return new HiveAggregateVisitor(this).visit((HiveAggregate) rn);
    } else if (rn instanceof HiveTableFunctionScan) {
      return new HiveTableFunctionScanVisitor(this).visit((HiveTableFunctionScan) rn);
    } else if (rn instanceof HiveValues) {
      return new HiveValuesVisitor(this).visit((HiveValues) rn);
    }
    LOG.error(rn.getClass().getCanonicalName() + "operator translation not supported yet in return path.");
    return null;
  }

  private void handleTopLimit(Operator<?> rootOp) {
    if (rootOp instanceof LimitOperator) {
      // this can happen only on top most limit, not while visiting Limit Operator
      // since that can be within subquery.
      this.semanticAnalyzer.getQB().getParseInfo().setOuterQueryLimit(((LimitOperator) rootOp).getConf().getLimit());
    }
  }

  String getHiveDerivedTableAlias() {
    return "$hdt$_" + (this.uniqueCounter++);
  }

  SemanticAnalyzer getSemanticAnalyzer() {
    return semanticAnalyzer;
  }

  HiveConf getHiveConf() {
    return hiveConf;
  }

  UnparseTranslator getUnparseTranslator() {
    return unparseTranslator;
  }

  Map<String, TableScanOperator> getTopOps() {
    return topOps;
  }

  int getUniqueCounter() {
    return uniqueCounter;
  }
}
