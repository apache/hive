/**
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.JobConf;

public class ProjectionPusher {

  private static final Log LOG = LogFactory.getLog(ProjectionPusher.class);

  private final Map<String, PartitionDesc> pathToPartitionInfo =
      new LinkedHashMap<String, PartitionDesc>();
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
      for (final Map.Entry<String, PartitionDesc> entry : mapWork.getPathToPartitionInfo().entrySet()) {
        // key contains scheme (such as pfile://) and we want only the path portion fix in HIVE-6366
        pathToPartitionInfo.put(new Path(entry.getKey()).toUri().getPath(), entry.getValue());
      }
    }
  }

  private void pushProjectionsAndFilters(final JobConf jobConf,
      final String splitPath, final String splitPathWithNoSchema) {

    if (mapWork == null) {
      return;
    } else if (mapWork.getPathToAliases() == null) {
      return;
    }

    final ArrayList<String> aliases = new ArrayList<String>();
    final Iterator<Entry<String, ArrayList<String>>> iterator = mapWork.getPathToAliases().entrySet().iterator();

    while (iterator.hasNext()) {
      final Entry<String, ArrayList<String>> entry = iterator.next();
      final String key = new Path(entry.getKey()).toUri().getPath();
      if (splitPath.equals(key) || splitPathWithNoSchema.equals(key)) {
        final ArrayList<String> list = entry.getValue();
        for (final String val : list) {
          aliases.add(val);
        }
      }
    }

    for (final String alias : aliases) {
      final Operator<? extends Serializable> op = mapWork.getAliasToWork().get(
          alias);
      if (op != null && op instanceof TableScanOperator) {
        final TableScanOperator tableScan = (TableScanOperator) op;

        // push down projections
        final List<Integer> list = tableScan.getNeededColumnIDs();

        if (list != null) {
          ColumnProjectionUtils.appendReadColumnIDs(jobConf, list);
        } else {
          ColumnProjectionUtils.setFullyReadColumns(jobConf);
        }

        pushFilters(jobConf, tableScan);
      }
    }
  }

  private void pushFilters(final JobConf jobConf, final TableScanOperator tableScan) {

    final TableScanDesc scanDesc = tableScan.getConf();
    if (scanDesc == null) {
      LOG.debug("Not pushing filters because TableScanDesc is null");
      return;
    }

    // construct column name list for reference by filter push down
    Utilities.setColumnNameList(jobConf, tableScan);

    // push down filters
    final ExprNodeGenericFuncDesc filterExpr = scanDesc.getFilterExpr();
    if (filterExpr == null) {
      LOG.debug("Not pushing filters because FilterExpr is null");
      return;
    }

    final String filterText = filterExpr.getExprString();
    final String filterExprSerialized = Utilities.serializeExpression(filterExpr);
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
    final PartitionDesc part = pathToPartitionInfo.get(path.toString());

    if ((part != null) && (part.getTableDesc() != null)) {
      Utilities.copyTableJobPropertiesToConf(part.getTableDesc(), cloneJobConf);
    }
    pushProjectionsAndFilters(cloneJobConf, path.toString(), path.toUri().toString());
    return cloneJobConf;
  }
}
