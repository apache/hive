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

package org.apache.hadoop.hive.ql.io.orc;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import static org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil.createColumnVector;

/**
 * Class enables row-level filtering for ORC readers.
 * Mainly converts traditional filter expressions of ExprNode type
 * to VectorExpressions and exposes the appropriate callback.
 */
public class ORCRowFilter {

    private static final Logger LOG = LoggerFactory.getLogger(ORCRowFilter.class);

    private final VectorExpression filterExpression;
    private final VectorizationContext filterVc;
    private final int filterExprScratchCols;
    private int filterExprMaxColdId;

    private final Map<Integer, TypeInfo> filterExprScratchColTypeMap;
    private boolean useDecimal64ColumnVectors;

    public ORCRowFilter(VectorExpression expression, VectorizationContext vc, boolean useDecimal64ColumnVectors) {
        this.filterExprScratchColTypeMap = new HashMap<>();
        this.filterExpression = expression;
        this.filterVc = vc;
        this.useDecimal64ColumnVectors = useDecimal64ColumnVectors;

        filterExprMaxColdId = vc.getInitialColumnNames().size() -1;
        Stack<VectorExpression> exprStack = new Stack<>();
        if (filterExpression.getChildExpressions() != null && filterExpression.getChildExpressions().length > 0) {
            exprStack.addAll(Arrays.asList(filterExpression.getChildExpressions()));
        }
        while (!exprStack.isEmpty()) {
            VectorExpression currExpr = exprStack.pop();
            if (currExpr.getOutputColumnNum() != -1)
                filterExprScratchColTypeMap.put(currExpr.getOutputColumnNum(), currExpr.getOutputTypeInfo());
            if (currExpr.getOutputColumnNum() > filterExprMaxColdId) filterExprMaxColdId = currExpr.getOutputColumnNum();
            if (currExpr.getChildExpressions() != null && currExpr.getChildExpressions().length > 0) {
                exprStack.addAll(Arrays.asList(currExpr.getChildExpressions()));
            }
        }
        filterExprScratchCols = filterExprMaxColdId - (vc.getInitialColumnNames().size() -1);
        LOG.info("ProbeDecode RowFilerExpression {} with extraScratchCols {}", filterExpression, filterExprScratchCols);
    }

    /**
     * Properly handle conversion from ExprNodeDesc to VectorExpression
     */
    @VisibleForTesting
    public static ORCRowFilter getRowFilter(ExprNodeDesc filterExpr, List<String> columns, boolean useDecimal64ColumnVectors) {
        ORCRowFilter createdFilter = null;
        try {
            VectorizationContext vc = new VectorizationContext("row_filter", columns);
            VectorExpression currFilterExpr = vc.getVectorExpression(filterExpr, VectorExpressionDescriptor.Mode.FILTER);
            LOG.info("ProbeDecode converted {} filter to {} VectorExpression useDecimal64 {}",
                    filterExpr, currFilterExpr, useDecimal64ColumnVectors);
            createdFilter = new ORCRowFilter(currFilterExpr, vc, useDecimal64ColumnVectors);
        } catch (HiveException e) {
            LOG.error("ProbeDecode could not covert filter {}, {}",filterExpr, e.getMessage());
            throw new RuntimeException("ProbeDecode could not covert filter " + e);
        }
        return createdFilter;
    }

    /**
     *
     * Applies the filter-callback to a VectorizedRowBatch given as a parameter.
     * It also handles the case where the additional scratch columns needed
     * by the filter are not either allocated OR initialized!
     * This means that the resulting VRB might end up with more columns! (needed for filtering)
     * @param cvb VectorizedRowBatch
     */
    public void rowFilterCallback(VectorizedRowBatch cvb) {
        try {
            // Column expansion -- should happen only once per Reader
            if (cvb.cols.length -1 < filterExprMaxColdId) {
                ColumnVector[] expandedColVec = new ColumnVector[filterExprMaxColdId + 1];
                System.arraycopy(cvb.cols, 0, expandedColVec, 0, cvb.numCols);
                cvb.cols = expandedColVec;
                // Initialize any missing scratch columns
                for (Map.Entry<Integer, TypeInfo>  entry: filterExprScratchColTypeMap.entrySet()) {
                    if (cvb.cols[entry.getKey()] == null) {
                        cvb.cols[entry.getKey()] = createColumnVector(entry.getValue(), useDecimal64ColumnVectors ?
                                DataTypePhysicalVariation.DECIMAL_64 : DataTypePhysicalVariation.NONE);
                    }
                }
            }
            filterExpression.evaluate(cvb);
            // VectorExpressions just set size to 0 when there is NO match (or repeating No Match)
            // In that case, make sure selected is set to True as it will be used for row-filtering!
            if (cvb.size == 0) cvb.selectedInUse = true;
        } catch (HiveException e) {
            e.printStackTrace();
        }
    }

    /**
     * Return the Scratch Columnid-Type mapping needed for this row-filter.
     * @return a Columnid-Type Map
     */
    public Map<Integer, TypeInfo> getFilterExprScratchColTypeMap() {
        return filterExprScratchColTypeMap;
    }

    /**
     * Return an BitSet of colIds that are used in this row-filter.
     * Every index that is set to true represents a colId that is needed by this filter.
     * @param allIncludedColNames
     * @return
     */
    public boolean[] getFilterColIndex(List<String> allIncludedColNames) {
        boolean[] probeStaticColidIndex = new boolean[this.filterExprMaxColdId + 1];
        for (String filColName : allIncludedColNames) {
            try {
                probeStaticColidIndex[this.filterVc.getInputColumnIndex(filColName)] = true;
            } catch (HiveException e) {
                LOG.error("ProbeDecode could not create filterColIndex for {}, {}", filterExpression, e.getMessage());
            }
        }
        return probeStaticColidIndex;
    }
}
