/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.JobConf;

public class ProjectionPusher {

  private static final Logger LOG = LoggerFactory.getLogger(ProjectionPusher.class);

  private final Map<Path, PartitionDesc> pathToPartitionInfo = new LinkedHashMap<>();
  /**
   * MapWork is the Hive object which describes input files,
   * columns projections, and filters.
   */
  private MapWork mapWork;

  /**
   * Sets the mapWork variable based on the current JobConf in order to get all partitions.
   *
   * @param job
   */
  private void updateMrWork(final JobConf job) {
    final String plan = HiveConf.getVar(job, HiveConf.ConfVars.PLAN);
    if (mapWork == null && plan != null && plan.length() > 0) {
      mapWork = Utilities.getMapWork(job);
      pathToPartitionInfo.clear();
      for (final Map.Entry<Path, PartitionDesc> entry : mapWork.getPathToPartitionInfo().entrySet()) {
        // key contains scheme (such as pfile://) and we want only the path portion fix in HIVE-6366
        pathToPartitionInfo.put(Path.getPathWithoutSchemeAndAuthority(entry.getKey()), entry.getValue());
      }
    }
  }

  private void pushProjectionsAndFilters(final JobConf jobConf,
      final String splitPath,
      final String splitPathWithNoSchema) {

    if (mapWork == null) {
      return;
    } else if (mapWork.getPathToAliases() == null) {
      return;
    }

    final Set<String> aliases = new HashSet<String>();
    try {
      ArrayList<String> a = HiveFileFormatUtils.getFromPathRecursively(
          mapWork.getPathToAliases(), new Path(splitPath), null, false, true);
      if (a != null) {
        aliases.addAll(a);
      }
      if (a == null || a.isEmpty()) {
        // TODO: not having aliases for path usually means some bug. Should it give up?
        LOG.warn("Couldn't find aliases for " + splitPath);
      }
    } catch (IllegalArgumentException | IOException e) {
      throw new RuntimeException(e);
    }
    // Collect the needed columns from all the aliases and create ORed filter
    // expression for the table.
    boolean allColumnsNeeded = false;
    boolean noFilters = false;
    Set<Integer> neededColumnIDs = new HashSet<Integer>();
    // To support nested column pruning, we need to track the path from the top to the nested
    // fields
    Set<String> neededNestedColumnPaths = new HashSet<String>();
    List<ExprNodeGenericFuncDesc> filterExprs = new ArrayList<ExprNodeGenericFuncDesc>();
    RowSchema rowSchema = null;

    for(String alias : aliases) {
      final Operator<? extends Serializable> op =
          mapWork.getAliasToWork().get(alias);
      if (op != null && op instanceof TableScanOperator) {
        final TableScanOperator ts = (TableScanOperator) op;

        if (ts.getNeededColumnIDs() == null) {
          allColumnsNeeded = true;
        } else {
          neededColumnIDs.addAll(ts.getNeededColumnIDs());
          if (ts.getNeededNestedColumnPaths() != null) {
            neededNestedColumnPaths.addAll(ts.getNeededNestedColumnPaths());
          }
        }

        rowSchema = ts.getSchema();
        ExprNodeGenericFuncDesc filterExpr =
            ts.getConf() == null ? null : ts.getConf().getFilterExpr();
        noFilters = filterExpr == null; // No filter if any TS has no filter expression
        filterExprs.add(filterExpr);
      }
    }

    ExprNodeGenericFuncDesc tableFilterExpr = null;
    if (!noFilters) {
      try {
        for (ExprNodeGenericFuncDesc filterExpr : filterExprs) {
          if (tableFilterExpr == null ) {
            tableFilterExpr = filterExpr;
          } else {
            tableFilterExpr = ExprNodeGenericFuncDesc.newInstance(new GenericUDFOPOr(),
                Arrays.<ExprNodeDesc>asList(tableFilterExpr, filterExpr));
          }
        }
      } catch(UDFArgumentException ex) {
        LOG.debug("Turn off filtering due to " + ex);
        tableFilterExpr = null;
      }
    }

    // push down projections
    if (!allColumnsNeeded) {
      if (!neededColumnIDs.isEmpty()) {
        ColumnProjectionUtils.appendReadColumns(jobConf, new ArrayList<Integer>(neededColumnIDs));
        ColumnProjectionUtils.appendNestedColumnPaths(jobConf,
          new ArrayList<String>(neededNestedColumnPaths));
      }
    } else {
      ColumnProjectionUtils.setReadAllColumns(jobConf);
    }

    pushFilters(jobConf, rowSchema, tableFilterExpr);
  }

  private void pushFilters(final JobConf jobConf, RowSchema rowSchema, ExprNodeGenericFuncDesc filterExpr) {
    // construct column name list for reference by filter push down
    Utilities.setColumnNameList(jobConf, rowSchema);

    // push down filters
    if (filterExpr == null) {
      LOG.debug("Not pushing filters because FilterExpr is null");
      return;
    }

    final String filterText = filterExpr.getExprString();
    final String filterExprSerialized = SerializationUtilities.serializeExpression(filterExpr);
    jobConf.set(
        TableScanDesc.FILTER_TEXT_CONF_STR,
        filterText);
    jobConf.set(
        TableScanDesc.FILTER_EXPR_CONF_STR,
        filterExprSerialized);
  }

  public JobConf pushProjectionsAndFilters(JobConf jobConf, Path path)
      throws IOException {
    updateMrWork(jobConf);  // TODO: refactor this in HIVE-6366
    final JobConf cloneJobConf = new JobConf(jobConf);
    final PartitionDesc part = HiveFileFormatUtils.getFromPathRecursively(
        pathToPartitionInfo, path, null, false, true);

    try {
      if ((part != null) && (part.getTableDesc() != null)) {
        Utilities.copyTableJobPropertiesToConf(part.getTableDesc(), cloneJobConf);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }

    pushProjectionsAndFilters(cloneJobConf, path.toString(), path.toUri().getPath());
    return cloneJobConf;
  }
}
