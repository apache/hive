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

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.GlobalLimitCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.QBParseInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.SplitSample;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * This optimizer is used to reduce the input size for the query for queries which are
 * specifying a limit.
 * <p/>
 * For eg. for a query of type:
 * <p/>
 * select expr from T where <filter> limit 100;
 * <p/>
 * Most probably, the whole table T need not be scanned.
 * Chances are that even if we scan the first file of T, we would get the 100 rows
 * needed by this query.
 * This optimizer step populates the GlobalLimitCtx which is used later on to prune the inputs.
 */
public class GlobalLimitOptimizer implements Transform {

  private final Log LOG = LogFactory.getLog(GlobalLimitOptimizer.class.getName());

  public ParseContext transform(ParseContext pctx) throws SemanticException {
    Context ctx = pctx.getContext();
    Map<String, Operator<? extends OperatorDesc>> topOps = pctx.getTopOps();
    GlobalLimitCtx globalLimitCtx = pctx.getGlobalLimitCtx();
    Map<TableScanOperator, ExprNodeDesc> opToPartPruner = pctx.getOpToPartPruner();
    Map<String, SplitSample> nameToSplitSample = pctx.getNameToSplitSample();
    Map<TableScanOperator, Table> topToTable = pctx.getTopToTable();

    QB qb = pctx.getQB();
    HiveConf conf = pctx.getConf();
    QBParseInfo qbParseInfo = qb.getParseInfo();

    // determine the query qualifies reduce input size for LIMIT
    // The query only qualifies when there are only one top operator
    // and there is no transformer or UDTF and no block sampling
    // is used.
    if (ctx.getTryCount() == 0 && topOps.size() == 1
        && !globalLimitCtx.ifHasTransformOrUDTF() &&
        nameToSplitSample.isEmpty()) {

      // Here we recursively check:
      // 1. whether there are exact one LIMIT in the query
      // 2. whether there is no aggregation, group-by, distinct, sort by,
      //    distributed by, or table sampling in any of the sub-query.
      // The query only qualifies if both conditions are satisfied.
      //
      // Example qualified queries:
      //    CREATE TABLE ... AS SELECT col1, col2 FROM tbl LIMIT ..
      //    INSERT OVERWRITE TABLE ... SELECT col1, hash(col2), split(col1)
      //                               FROM ... LIMIT...
      //    SELECT * FROM (SELECT col1 as col2 (SELECT * FROM ...) t1 LIMIT ...) t2);
      //
      Integer tempGlobalLimit = checkQbpForGlobalLimit(qb);

      // query qualify for the optimization
      if (tempGlobalLimit != null && tempGlobalLimit != 0) {
        TableScanOperator ts = (TableScanOperator) topOps.values().toArray()[0];
        Table tab = topToTable.get(ts);

        if (!tab.isPartitioned()) {
          if (qbParseInfo.getDestToWhereExpr().isEmpty()) {
            globalLimitCtx.enableOpt(tempGlobalLimit);
          }
        } else {
          // check if the pruner only contains partition columns
          if (PartitionPruner.onlyContainsPartnCols(tab,
              opToPartPruner.get(ts))) {

            PrunedPartitionList partsList;
            try {
              String alias = (String) topOps.keySet().toArray()[0];
              partsList = PartitionPruner.prune(ts, pctx, alias);
            } catch (HiveException e) {
              // Has to use full name to make sure it does not conflict with
              // org.apache.commons.lang.StringUtils
              LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
              throw new SemanticException(e.getMessage(), e);
            }

            // If there is any unknown partition, create a map-reduce job for
            // the filter to prune correctly
            if (!partsList.hasUnknownPartitions()) {
              globalLimitCtx.enableOpt(tempGlobalLimit);
            }
          }
        }
        if (globalLimitCtx.isEnable()) {
          LOG.info("Qualify the optimize that reduces input size for 'limit' for limit "
              + globalLimitCtx.getGlobalLimit());
        }
      }
    }
    return pctx;
  }

  /**
   * Recursively check the limit number in all sub queries
   *
   * @param qbParseInfo
   * @return if there is one and only one limit for all subqueries, return the limit
   *         if there is no limit, return 0
   *         otherwise, return null
   */
  private Integer checkQbpForGlobalLimit(QB localQb) {
    QBParseInfo qbParseInfo = localQb.getParseInfo();
    if (localQb.getNumSelDi() == 0 && qbParseInfo.getDestToClusterBy().isEmpty()
        && qbParseInfo.getDestToDistributeBy().isEmpty()
        && qbParseInfo.getDestToOrderBy().isEmpty()
        && qbParseInfo.getDestToSortBy().isEmpty()
        && qbParseInfo.getDestToAggregationExprs().size() <= 1
        && qbParseInfo.getDestToDistinctFuncExprs().size() <= 1
        && qbParseInfo.getNameToSample().isEmpty()) {
      if ((qbParseInfo.getDestToAggregationExprs().size() < 1 ||
          qbParseInfo.getDestToAggregationExprs().values().iterator().next().isEmpty()) &&
          (qbParseInfo.getDestToDistinctFuncExprs().size() < 1 ||
              qbParseInfo.getDestToDistinctFuncExprs().values().iterator().next().isEmpty())
          && qbParseInfo.getDestToLimit().size() <= 1) {
        Integer retValue;
        if (qbParseInfo.getDestToLimit().size() == 0) {
          retValue = 0;
        } else {
          retValue = qbParseInfo.getDestToLimit().values().iterator().next();
        }

        for (String alias : localQb.getSubqAliases()) {
          Integer limit = checkQbpForGlobalLimit(localQb.getSubqForAlias(alias).getQB());
          if (limit == null) {
            return null;
          } else if (retValue > 0 && limit > 0) {
            // Any query has more than one LIMITs shown in the query is not
            // qualified to this optimization
            return null;
          } else if (limit > 0) {
            retValue = limit;
          }
        }
        return retValue;
      }
    }
    return null;
  }
}
