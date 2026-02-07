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
import org.apache.hadoop.hive.ql.stats.estimator.PessimisticStatCombiner;
import org.apache.hadoop.hive.ql.stats.estimator.StatEstimator;
import org.apache.hadoop.hive.ql.stats.estimator.StatEstimatorProvider;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
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
  private transient int firstConstantIndex = -1;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentTypeException {

    argumentOIs = arguments;

    returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
    firstConstantIndex = -1;

    for (int i = 0; i < arguments.length; i++) {
      if (!returnOIResolver.update(arguments[i])) {
        throw new UDFArgumentTypeException(i,
            "The expressions after COALESCE should all have the same type: \""
            + returnOIResolver.get().getTypeName()
            + "\" is expected but \"" + arguments[i].getTypeName()
            + "\" is found");
      }
      if (firstConstantIndex < 0 && arguments[i] instanceof ConstantObjectInspector) {
        firstConstantIndex = i;
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
    return new CoalesceStatEstimator(firstConstantIndex);
  }

  /**
   * COALESCE returns the first non-null argument, so only values before (and including)
   * the first constant are reachable. Constants after the first one can never be returned.
   */
  static class CoalesceStatEstimator implements StatEstimator {
    private final int firstConstantIndex;

    CoalesceStatEstimator(int firstConstantIndex) {
      this.firstConstantIndex = firstConstantIndex;
    }

    @Override
    public Optional<ColStatistics> estimate(List<ColStatistics> argStats) {
      PessimisticStatCombiner combiner = new PessimisticStatCombiner();

      if (firstConstantIndex == 0) {
        // First arg is constant - always returns that constant, NDV = 1
        combiner.add(argStats.get(0));
        return combiner.getResult();
      }

      // Combine stats of columns before the first constant (or all if no constant)
      int limit = firstConstantIndex > 0 ? firstConstantIndex : argStats.size();
      for (int i = 0; i < limit; i++) {
        combiner.add(argStats.get(i));
      }

      Optional<ColStatistics> result = combiner.getResult();

      // If there's a constant after columns, add 1 to NDV for that constant
      if (result.isPresent() && firstConstantIndex > 0) {
        ColStatistics stat = result.get();
        if (stat.getCountDistint() > 0) {
          stat.setCountDistint(stat.getCountDistint() + 1);
        }
      }

      return result;
    }
  }
}
