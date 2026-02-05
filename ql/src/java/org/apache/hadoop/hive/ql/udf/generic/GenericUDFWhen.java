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

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.stats.estimator.PessimisticStatCombiner;
import org.apache.hadoop.hive.ql.stats.estimator.StatEstimator;
import org.apache.hadoop.hive.ql.stats.estimator.StatEstimatorProvider;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;

/**
 * GenericUDF Class for SQL construct
 * "CASE WHEN a THEN b WHEN c THEN d [ELSE f] END".
 *
 * NOTES: 1. a and c should be boolean, or an exception will be thrown. 2. b, d
 * and f should be common types, or an exception will be thrown.
 */
@Description(
    name = "when",
    value = "CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END - "
        + "When a = true, returns b; when c = true, return d; else return e",
    extended = "Example:\n "
    + "SELECT\n"
    + " CASE\n"
    + "   WHEN deptno=1 THEN Engineering\n"
    + "   WHEN deptno=2 THEN Finance\n"
    + "   ELSE admin\n"
    + " END,\n"
    + " CASE\n"
    + "   WHEN zone=7 THEN Americas\n"
    + "   ELSE Asia-Pac\n"
    + " END\n"
    + " FROM emp_details")

public class GenericUDFWhen extends GenericUDF implements StatEstimatorProvider {
  private transient ObjectInspector[] argumentOIs;
  private transient GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
  private transient Integer numberOfDistinctConstants;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentTypeException {

    argumentOIs = arguments;
    returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);

    Set<Object> distinctConstants = new HashSet<>();
    boolean allBranchesConstant = true;

    for (int i = 0; i + 1 < arguments.length; i += 2) {
      if (!arguments[i].getTypeName().equals(serdeConstants.BOOLEAN_TYPE_NAME)) {
        throw new UDFArgumentTypeException(i, "\""
            + serdeConstants.BOOLEAN_TYPE_NAME + "\" is expected after WHEN, "
            + "but \"" + arguments[i].getTypeName() + "\" is found");
      }
      if (!returnOIResolver.update(arguments[i + 1])) {
        throw new UDFArgumentTypeException(i + 1,
            "The expressions after THEN should have the same type: \""
            + returnOIResolver.get().getTypeName()
            + "\" is expected but \"" + arguments[i + 1].getTypeName()
            + "\" is found");
      }
      if (allBranchesConstant) {
        if (arguments[i + 1] instanceof ConstantObjectInspector) {
          distinctConstants.add(((ConstantObjectInspector) arguments[i + 1]).getWritableConstantValue());
        } else {
          allBranchesConstant = false;
        }
      }
    }
    if (arguments.length % 2 == 1) {
      int i = arguments.length - 2;
      if (!returnOIResolver.update(arguments[i + 1])) {
        throw new UDFArgumentTypeException(i + 1,
            "The expression after ELSE should have the same type as those after THEN: \""
            + returnOIResolver.get().getTypeName()
            + "\" is expected but \"" + arguments[i + 1].getTypeName()
            + "\" is found");
      }
      if (allBranchesConstant) {
        if (arguments[i + 1] instanceof ConstantObjectInspector) {
          distinctConstants.add(((ConstantObjectInspector) arguments[i + 1]).getWritableConstantValue());
        } else {
          allBranchesConstant = false;
        }
      }
    }

    numberOfDistinctConstants = allBranchesConstant && !distinctConstants.isEmpty()
        ? distinctConstants.size() : null;

    return returnOIResolver.get();
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    for (int i = 0; i + 1 < arguments.length; i += 2) {
      Object caseKey = arguments[i].get();
      if (caseKey != null
          && ((BooleanObjectInspector) argumentOIs[i]).get(caseKey)) {
        Object caseValue = arguments[i + 1].get();
        return returnOIResolver.convertIfNecessary(caseValue,
            argumentOIs[i + 1]);
      }
    }
    // Process else statement
    if (arguments.length % 2 == 1) {
      int i = arguments.length - 2;
      Object elseValue = arguments[i + 1].get();
      return returnOIResolver.convertIfNecessary(elseValue, argumentOIs[i + 1]);
    }
    return null;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length >= 2);
    StringBuilder sb = new StringBuilder();
    sb.append("CASE");
    for (int i = 0; i + 1 < children.length; i += 2) {
      sb.append(" WHEN (");
      sb.append(children[i]);
      sb.append(") THEN (");
      sb.append(children[i + 1]);
      sb.append(")");
    }
    if (children.length % 2 == 1) {
      sb.append(" ELSE (");
      sb.append(children[children.length - 1]);
      sb.append(")");
    }
    sb.append(" END");
    return sb.toString();
  }

  @Override
  public StatEstimator getStatEstimator() {
    return new WhenStatEstimator(numberOfDistinctConstants);
  }

  static class WhenStatEstimator implements StatEstimator {
    private final Integer numberOfDistinctConstants;

    WhenStatEstimator(Integer numberOfDistinctConstants) {
      this.numberOfDistinctConstants = numberOfDistinctConstants;
    }

    @Override
    public Optional<ColStatistics> estimate(List<ColStatistics> argStats) {
      if (numberOfDistinctConstants != null) {
        ColStatistics result = argStats.get(1).clone();
        result.setCountDistint(numberOfDistinctConstants);
        for (int i = 3; i < argStats.size(); i += 2) {
          if (argStats.get(i).getAvgColLen() > result.getAvgColLen()) {
            result.setAvgColLen(argStats.get(i).getAvgColLen());
          }
        }
        if (argStats.size() % 2 == 1) {
          ColStatistics elseStat = argStats.get(argStats.size() - 1);
          if (elseStat.getAvgColLen() > result.getAvgColLen()) {
            result.setAvgColLen(elseStat.getAvgColLen());
          }
        }
        return Optional.of(result);
      }

      // Fall back to pessimistic combining
      PessimisticStatCombiner combiner = new PessimisticStatCombiner();
      for (int i = 1; i < argStats.size(); i += 2) {
        combiner.add(argStats.get(i));
      }
      if (argStats.size() % 2 == 1) {
        combiner.add(argStats.get(argStats.size() - 1));
      }
      return combiner.getResult();
    }
  }
}
