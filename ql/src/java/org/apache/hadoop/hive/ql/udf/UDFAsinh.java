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
package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncAsinhDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncAsinhLongToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.MathExpr;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

/**
 * UDFAsinh.
 */
@Description(name = "asinh",
        value = "_FUNC_(x) - returns inverse hyperbolic sine of x",
        extended = "Example:\n "
                + "  > SELECT _FUNC_(0.98), _FUNC_(1.57), _FUNC_(-0.5) FROM src LIMIT 1;\n"
                + "  0.8671605068296652     1.2329753895597169      -0.48121182505960336"
)
@VectorizedExpressions({FuncAsinhDoubleToDouble.class, FuncAsinhLongToDouble.class})
public class UDFAsinh  extends UDFMath{
    private final DoubleWritable result = new DoubleWritable();

    @Override
    protected DoubleWritable doEvaluate(DoubleWritable a) {
        double res = MathExpr.asinh(a.get());
        if(Double.isNaN(res)) {
          return null;
        }
        else {
          result.set(res);
          return result;
        }
    }
}
