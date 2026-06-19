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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.opconventer.HiveOpConverter.OpAttr;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;

class HiveTableScanVisitor extends HiveRelNodeVisitor<HiveTableScan> {
  HiveTableScanVisitor(HiveOpConverter hiveOpConverter) {
    super(hiveOpConverter);
  }

  /**
   * TODO: 1. PPD needs to get pushed in to TS.
   */
  @Override
  OpAttr visit(HiveTableScan scanRel) {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Translating operator rel#" + scanRel.getId() + ":" + scanRel.getRelTypeName()
          + " with row type: [" + scanRel.getRowType() + "]");
    }

    RelOptHiveTable ht = (RelOptHiveTable) scanRel.getTable();

    // 1. Setup TableScan Desc
    // 1.1 Build col details used by scan
    ArrayList<ColumnInfo> colInfos = new ArrayList<ColumnInfo>();
    List<VirtualColumn> virtualCols = new ArrayList<VirtualColumn>();
    List<Integer> neededColumnIDs = new ArrayList<Integer>();
    List<String> neededColumnNames = new ArrayList<String>();
    Set<Integer> vcolsInCalcite = new HashSet<Integer>();

    List<String> partColNames = new ArrayList<String>();
    Map<Integer, VirtualColumn> vColsMap = HiveCalciteUtil.getVColsMap(ht.getVirtualCols(),
        ht.getNoOfNonVirtualCols());
    Map<Integer, ColumnInfo> posToPartColInfo = ht.getPartColInfoMap();
    Map<Integer, ColumnInfo> posToNonPartColInfo = ht.getNonPartColInfoMap();
    List<Integer> neededColIndxsFrmReloptHT = scanRel.getNeededColIndxsFrmReloptHT();
    List<String> scanColNames = scanRel.getRowType().getFieldNames();
    String tableAlias = scanRel.getConcatQbIDAlias();

    String colName;
    ColumnInfo colInfo;
    VirtualColumn vc;

    for (int index = 0; index < scanRel.getRowType().getFieldList().size(); index++) {
      colName = scanColNames.get(index);
      if (vColsMap.containsKey(index)) {
        vc = vColsMap.get(index);
        virtualCols.add(vc);
        colInfo = new ColumnInfo(vc.getName(), vc.getTypeInfo(), tableAlias, true, vc.getIsHidden());
        vcolsInCalcite.add(index);
      } else if (posToPartColInfo.containsKey(index)) {
        partColNames.add(colName);
        colInfo = posToPartColInfo.get(index);
        vcolsInCalcite.add(index);
      } else {
        colInfo = posToNonPartColInfo.get(index);
      }
      colInfos.add(colInfo);
      if (neededColIndxsFrmReloptHT.contains(index)) {
        neededColumnIDs.add(index);
        neededColumnNames.add(colName);
      }
    }

    // 1.2 Create TableScanDesc
    TableScanDesc tsd = new TableScanDesc(tableAlias, virtualCols, ht.getHiveTableMD());

    // 1.3. Set Partition cols in TSDesc
    tsd.setPartColumns(partColNames);

    // 1.4. Set needed cols in TSDesc
    tsd.setNeededColumnIDs(neededColumnIDs);
    tsd.setNeededColumns(neededColumnNames);

    // 2. Setup TableScan
    TableScanOperator ts = (TableScanOperator) OperatorFactory.get(
        hiveOpConverter.getSemanticAnalyzer().getOpContext(), tsd, new RowSchema(colInfos));

    // now that we let Calcite process sub-queries we might have more than one
    // tableScan with same alias.
    if (hiveOpConverter.getTopOps().get(tableAlias) != null) {
      tableAlias = tableAlias + hiveOpConverter.getUniqueCounter();
    }
    hiveOpConverter.getTopOps().put(tableAlias, ts);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated " + ts + " with row schema: [" + ts.getSchema() + "]");
    }

    return new OpAttr(tableAlias, vcolsInCalcite, ts);
  }
}
