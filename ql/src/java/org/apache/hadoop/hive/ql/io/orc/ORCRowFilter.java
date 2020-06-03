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
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Class enables row-level filtering for ORC readers.
 * Mainly converts traditional filter expressions of ExprNode type
 * to VectorExpressions and exposes the appropriate callback.
 */
public class ORCRowFilter {

    private static final Logger LOG = LoggerFactory.getLogger(ORCRowFilter.class);

    private VectorExpression predicateExpression = null;

    public ORCRowFilter(VectorExpression expression) {
        this.predicateExpression = expression;
    }


    /**
     * Need to properly handle conversion from ExprNodeDesc to VectorExpression
     */
    @VisibleForTesting
    public ORCRowFilter(ExprNodeDesc filterExpr, List<String> columns) {
        VectorizationContext vc = new VectorizationContext("row-filter", columns);
        try {
            predicateExpression = vc.getVectorExpression(filterExpr, VectorExpressionDescriptor.Mode.FILTER);
        } catch (HiveException e) {
            e.printStackTrace();
        }
        LOG.info("ProbeDecode converted {} filter to {} VectorExpression", filterExpr, predicateExpression);
    }

    public void rowFilterCallback(VectorizedRowBatch batch) {
        try {
            predicateExpression.evaluate(batch);
            // VectorExpressions just set size to 0 when there is NO match (or repeating No Match)
            // In that case, make sure selected is set to True as it will be used for row-filtering!
            if (batch.size == 0) batch.selectedInUse = true;
        } catch (HiveException e) {
            e.printStackTrace();
        }
    }
}
