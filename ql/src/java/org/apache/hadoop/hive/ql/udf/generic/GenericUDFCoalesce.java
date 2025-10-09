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

package org.apache.hadoop.hive.ql.udf.generic;

import java.util.List;
import java.util.Optional;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressionsSupportDecimal64;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.stats.estimator.StatEstimator;
import org.apache.hadoop.hive.ql.stats.estimator.StatEstimatorProvider;
import org.apache.hadoop.hive.ql.stats.estimator.PessimisticStatCombiner;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * GenericUDF Class for SQL construct "COALESCE(a, b, c)".
 *
 * NOTES: 1. a, b and c should have the same TypeInfo, or an exception will be
 * thrown.
 */
@Description(name = "coalesce",
    value = "_FUNC_(a1, a2, ...) - Returns the first non-null argument",
    extended = "Example:\n"
    + "  > SELECT _FUNC_(NULL, 1, NULL) FROM src LIMIT 1;\n" + "  1")
@VectorizedExpressionsSupportDecimal64()
public class GenericUDFCoalesce extends GenericUDF implements StatEstimatorProvider {
  private transient ObjectInspector[] argumentOIs;
  private transient GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentTypeException {

    argumentOIs = arguments;

    returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
    for (int i = 0; i < arguments.length; i++) {
      if (!returnOIResolver.update(arguments[i])) {
        throw new UDFArgumentTypeException(i,
            "The expressions after COALESCE should all have the same type: \""
            + returnOIResolver.get().getTypeName()
            + "\" is expected but \"" + arguments[i].getTypeName()
            + "\" is found");
      }
    }
    return returnOIResolver.get();
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    for (int i = 0; i < arguments.length; i++) {
      Object ai = arguments[i].get();
      if (ai == null) {
        continue;
      }
      return returnOIResolver.convertIfNecessary(ai, argumentOIs[i]);
    }
    return null;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("COALESCE", children, ",");
  }

  @Override
  public StatEstimator getStatEstimator() {
    return new CoalesceStatEstimator();
  }

  static class CoalesceStatEstimator implements StatEstimator {

    @Override
    public Optional<ColStatistics> estimate(List<ColStatistics> argStats) {
      PessimisticStatCombiner combiner = new PessimisticStatCombiner();
      for (int i = 0; i < argStats.size(); i++) {
        combiner.add(argStats.get(i));
      }
      return combiner.getResult();
    }
  }
}
