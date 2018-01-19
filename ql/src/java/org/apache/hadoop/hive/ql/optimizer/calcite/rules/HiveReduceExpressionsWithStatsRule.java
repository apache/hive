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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.math.BigDecimal;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveIn;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * This rule simplifies the condition in Filter operators using the
 * column statistics (if available).
 *
 * For instance, given the following predicate:
 *   a > 5
 * we can infer that the predicate will evaluate to false if the max
 * value for column a is 4.
 *
 * Currently we support the simplification of:
 *  - =, >=, <=, >, <
 *  - IN
 *  - IS_NULL / IS_NOT_NULL
 */
public class HiveReduceExpressionsWithStatsRule extends RelOptRule {

  protected static final Logger LOG = LoggerFactory.getLogger(
          HiveReduceExpressionsWithStatsRule.class);

  public static final HiveReduceExpressionsWithStatsRule INSTANCE =
          new HiveReduceExpressionsWithStatsRule();

  private static final Set<SqlKind> COMPARISON = EnumSet.of(SqlKind.EQUALS,
                                                          SqlKind.GREATER_THAN_OR_EQUAL,
                                                          SqlKind.LESS_THAN_OR_EQUAL,
                                                          SqlKind.GREATER_THAN,
                                                          SqlKind.LESS_THAN);

  private HiveReduceExpressionsWithStatsRule() {
    super(operand(Filter.class, operand(RelNode.class, any())));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);

    final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    final RelMetadataQuery metadataProvider = call.getMetadataQuery();

    // 1. Recompose filter possibly by pulling out common elements from DNF
    // expressions
    RexNode newFilterCondition = RexUtil.pullFactors(rexBuilder, filter.getCondition());

    // 2. Reduce filter with stats information
    RexReplacer replacer = new RexReplacer(filter, rexBuilder, metadataProvider);
    newFilterCondition = replacer.apply(newFilterCondition);

    // 3. Transform if we have created a new filter operator
    if (!filter.getCondition().toString().equals(newFilterCondition.toString())) {
      Filter newFilter = filter.copy(filter.getTraitSet(), filter.getInput(), newFilterCondition);
      call.transformTo(newFilter);
    }

  }

  /**
   * Replaces expressions with their reductions. Note that we only have to
   * look for RexCall, since nothing else is reducible in the first place.
   */
  protected static class RexReplacer extends RexShuttle {
    private final Filter filterOp;
    private final RexBuilder rexBuilder;
    private final RelMetadataQuery metadataProvider;

    RexReplacer(Filter filterOp, RexBuilder rexBuilder, RelMetadataQuery metadataProvider) {
      this.filterOp = filterOp;
      this.rexBuilder = rexBuilder;
      this.metadataProvider = metadataProvider;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      if (COMPARISON.contains(call.getOperator().getKind())) {
        RexInputRef ref = null;
        RexLiteral literal = null;
        SqlKind kind = null;
        if (call.operands.get(0) instanceof RexInputRef
            && call.operands.get(1) instanceof RexLiteral) {
          ref = (RexInputRef) call.operands.get(0);
          literal = (RexLiteral) call.operands.get(1);
          kind = call.getOperator().getKind();
        } else if (call.operands.get(1) instanceof RexInputRef
            && call.operands.get(0) instanceof RexLiteral) {
          ref = (RexInputRef) call.operands.get(1);
          literal = (RexLiteral) call.operands.get(0);
          kind = call.getOperator().getKind().reverse();
        }

        // Found an expression that we can try to reduce
        Number max = null;
        Number min = null;
        if (ref != null && literal != null && kind != null) {
          Pair<Number,Number> maxMin = extractMaxMin(ref);
          max = maxMin.left;
          min = maxMin.right;
        }

        if (max != null && min != null) {
          // Stats were available, try to reduce
          RexNode reduced = reduceCall(literal, kind, max, min);
          if (reduced != null) {
            return reduced;
          }
        }

        // We cannot apply the reduction
        return call;
      } else if (call.getOperator().getKind() == SqlKind.IN) {
        if (call.getOperands().get(0) instanceof RexInputRef) {
          // Ref
          RexInputRef ref = (RexInputRef) call.getOperands().get(0);
          // Found an expression that we can try to reduce
          Number max = null;
          Number min = null;
          if (ref != null) {
            Pair<Number,Number> maxMin = extractMaxMin(ref);
            max = maxMin.left;
            min = maxMin.right;
          }

          if (max != null && min != null) {
            // Stats were available, try to reduce
            List<RexNode> newOperands = Lists.newArrayList();
            newOperands.add(ref);
            for (int i = 1; i < call.getOperands().size(); i++) {
              RexNode operand = call.getOperands().get(i);
              if (operand instanceof RexLiteral) {
                RexLiteral literal = (RexLiteral) operand;
                RexNode reduced = reduceCall(literal, SqlKind.EQUALS, max, min);
                if (reduced != null) {
                  if (reduced.isAlwaysTrue()) {
                    return rexBuilder.makeLiteral(true);
                  }
                } else {
                  newOperands.add(literal);
                }
              } else {
                newOperands.add(operand);
              }
            }
            if (newOperands.size() == 1) {
              return rexBuilder.makeLiteral(false);
            }
            return rexBuilder.makeCall(HiveIn.INSTANCE, newOperands);
          }
        } else if (call.getOperands().get(0).getKind() == SqlKind.ROW) {
          // Struct
          RexCall struct = (RexCall) call.getOperands().get(0);
          List<RexInputRef> refs = Lists.newArrayList();
          List<Pair<Number,Number>> maxMinStats = Lists.newArrayList();
          for (RexNode operand: struct.getOperands()) {
            if (!(operand instanceof RexInputRef)) {
              // Cannot simplify, we bail out
              return call;
            }
            RexInputRef ref = (RexInputRef) operand;
            refs.add(ref);
            maxMinStats.add(extractMaxMin(ref));
          }

          // Try to reduce
          List<RexNode> newOperands = Lists.newArrayList();
          newOperands.add(struct);
          for (int i = 1; i < call.getOperands().size(); i++) {
            RexCall constStruct = (RexCall) call.getOperands().get(i);
            boolean allTrue = true;
            boolean addOperand = true;
            for (int j = 0; i < constStruct.getOperands().size(); j++) {
              RexNode operand = constStruct.getOperands().get(j);
              if (operand instanceof RexLiteral) {
                RexLiteral literal = (RexLiteral) operand;
                RexNode reduced = reduceCall(literal, SqlKind.EQUALS,
                        maxMinStats.get(j).left, maxMinStats.get(j).right);
                if (reduced != null) {
                  if (reduced.isAlwaysFalse()) {
                    allTrue = false;
                    addOperand = false;
                    break;
                  }
                } else {
                  allTrue = false;
                }
              } else {
                allTrue = false;
              }
            }
            if (allTrue) {
              return rexBuilder.makeLiteral(true);
            }
            if (addOperand) {
              newOperands.add(constStruct);
            }
          }
          if (newOperands.size() == 1) {
            return rexBuilder.makeLiteral(false);
          }
          return rexBuilder.makeCall(HiveIn.INSTANCE, newOperands);
        }
        // We cannot apply the reduction
        return call;
      } else if (call.getOperator().getKind() == SqlKind.IS_NULL || call.getOperator().getKind() == SqlKind.IS_NOT_NULL) {
        SqlKind kind = call.getOperator().getKind();

        if (call.operands.get(0) instanceof RexInputRef) {
          RexInputRef ref = (RexInputRef) call.operands.get(0);

          ColStatistics stat = extractColStats(ref);
          Long rowCount = extractRowCount(ref);
          if (stat != null && rowCount != null) {
            if (stat.getNumNulls() == 0 || stat.getNumNulls() == rowCount) {
              boolean allNulls = (stat.getNumNulls() == rowCount);

              if (kind == SqlKind.IS_NULL) {
                return rexBuilder.makeLiteral(allNulls);
              } else {
                return rexBuilder.makeLiteral(!allNulls);
              }
            }
          }
        }
      }

      // If we did not reduce, check the children nodes
      RexNode node = super.visitCall(call);
      if (node != call) {
        node = RexUtil.simplify(rexBuilder, node);
      }
      return node;
    }

    private Pair<Number,Number> extractMaxMin(RexInputRef ref) {

      ColStatistics cs = extractColStats(ref);
      Number max = null;
      Number min = null;
      if (cs != null && cs.getRange()!=null) {
        max = cs.getRange().maxValue;
        min = cs.getRange().minValue;
      }
      return Pair.<Number, Number> of(max, min);
    }

    private ColStatistics extractColStats(RexInputRef ref) {
      RelColumnOrigin columnOrigin = this.metadataProvider.getColumnOrigin(filterOp, ref.getIndex());
      if (columnOrigin != null) {
        RelOptHiveTable table = (RelOptHiveTable) columnOrigin.getOriginTable();
        if (table != null) {
          ColStatistics colStats =
                  table.getColStat(Lists.newArrayList(columnOrigin.getOriginColumnOrdinal())).get(0);
          if (colStats != null && StatsSetupConst.areColumnStatsUptoDate(
                  table.getHiveTableMD().getParameters(), colStats.getColumnName())) {
            return colStats;
          }
        }
      }
      return null;
    }

    private Long extractRowCount(RexInputRef ref) {
      RelColumnOrigin columnOrigin = this.metadataProvider.getColumnOrigin(filterOp, ref.getIndex());
      if (columnOrigin != null) {
        RelOptHiveTable table = (RelOptHiveTable) columnOrigin.getOriginTable();
        if (table != null) {
          if (StatsSetupConst.areBasicStatsUptoDate(table.getHiveTableMD().getParameters())) {
            return StatsUtils.getNumRows(table.getHiveTableMD());
          }
        }
      }
      return null;
    }

    @SuppressWarnings("unchecked")
    private RexNode reduceCall(RexLiteral literal, SqlKind kind, Number max, Number min) {
      // Stats were available, try to reduce
      if (max != null && min != null) {
        BigDecimal maxVal = new BigDecimal(max.floatValue());
        BigDecimal minVal = new BigDecimal(min.floatValue());
        RexLiteral maxLiteral = rexBuilder.makeExactLiteral(maxVal, literal.getType());
        RexLiteral minLiteral = rexBuilder.makeExactLiteral(minVal, literal.getType());

        // Equals
        if (kind == SqlKind.EQUALS) {
          if (minLiteral.getValue().compareTo(literal.getValue()) > 0 ||
                  maxLiteral.getValue().compareTo(literal.getValue()) < 0) {
            return rexBuilder.makeLiteral(false);
          }
        }

        // Greater than (or equal), and less than (or equal)
        if (kind == SqlKind.GREATER_THAN) {
          if (minLiteral.getValue().compareTo(literal.getValue()) > 0) {
            return rexBuilder.makeLiteral(true);
          } else if (maxLiteral.getValue().compareTo(literal.getValue()) <= 0) {
            return rexBuilder.makeLiteral(false);
          }
        } else if (kind == SqlKind.GREATER_THAN_OR_EQUAL) {
          if (minLiteral.getValue().compareTo(literal.getValue()) >= 0) {
            return rexBuilder.makeLiteral(true);
          } else if (maxLiteral.getValue().compareTo(literal.getValue()) < 0) {
            return rexBuilder.makeLiteral(false);
          }
        } else if (kind == SqlKind.LESS_THAN) {
          if (minLiteral.getValue().compareTo(literal.getValue()) >= 0) {
            return rexBuilder.makeLiteral(false);
          } else if (maxLiteral.getValue().compareTo(literal.getValue()) < 0) {
            return rexBuilder.makeLiteral(true);
          }
        } else if (kind == SqlKind.LESS_THAN_OR_EQUAL) {
          if (minLiteral.getValue().compareTo(literal.getValue()) > 0) {
            return rexBuilder.makeLiteral(false);
          } else if (maxLiteral.getValue().compareTo(literal.getValue()) <= 0) {
            return rexBuilder.makeLiteral(true);
          }
        }
      }
      return null;
    }
  }

}
