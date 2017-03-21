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
package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage.GenericUDAFAverageEvaluatorDouble;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage.GenericUDAFAverageEvaluatorDecimal;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCorrelation.GenericUDAFCorrelationEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCount.GenericUDAFCountEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFVariance.GenericUDAFVarianceEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class GenericUDAFBinarySetFunctions extends AbstractGenericUDAFResolver {

  @Description(name = "regr_count", value = "_FUNC_(y,x) - returns the number of non-null pairs", extended = "The function takes as arguments any pair of numeric types and returns a long.\n"
      + "Any pair with a NULL is ignored.")
  public static class RegrCount extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
      checkArgumentTypes(parameters);
      return new Evaluator();
    }

    private static class Evaluator extends GenericUDAFCountEvaluator {

      @Override
      public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
        switch (m) {
        case COMPLETE:
        case PARTIAL1:
          return super.init(m, new ObjectInspector[] { parameters[0] });
        default:
          return super.init(m, parameters);
        }
      }

      @Override
      public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
        if (parameters[0] == null || parameters[1] == null)
          return;
        super.iterate(agg, new Object[] { parameters[0] });
      }
    }
  }

  @Description(name = "regr_sxx", value = "_FUNC_(y,x) - auxiliary analytic function", extended = "The function takes as arguments any pair of numeric types and returns a double.\n"
      + "Any pair with a NULL is ignored.\n"
      + "If applied to an empty set: NULL is returned.\n"
      + "Otherwise, it computes the following:\n"
      + "   SUM(x*x)-SUM(x)*SUM(x)/N\n")
  public static class RegrSXX extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
      checkArgumentTypes(parameters);
      return new Evaluator();
    }

    private static class Evaluator extends GenericUDAFVarianceEvaluator {

      @Override
      public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
        switch (m) {
        case COMPLETE:
        case PARTIAL1:
          return super.init(m, new ObjectInspector[] { parameters[1] });
        default:
          return super.init(m, parameters);
        }
      }

      @Override
      public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
        if (parameters[0] == null || parameters[1] == null)
          return;
        super.iterate(agg, new Object[] { parameters[1] });
      }

      @Override
      public Object terminate(AggregationBuffer agg) throws HiveException {
        StdAgg myagg = (StdAgg) agg;
        if (myagg.count == 0) {
          return null;
        } else {
          DoubleWritable result = getResult();
          result.set(myagg.variance);
          return result;
        }
      }
    }
  }

  @Description(name = "regr_syy", value = "_FUNC_(y,x) - auxiliary analytic function", extended = "The function takes as arguments any pair of numeric types and returns a double.\n"
      + "Any pair with a NULL is ignored.\n"
      + "If applied to an empty set: NULL is returned.\n"
      + "Otherwise, it computes the following:\n"
      + "   SUM(y*y)-SUM(y)*SUM(y)/N\n")
  public static class RegrSYY extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
      checkArgumentTypes(parameters);
      return new Evaluator();
    }

    private static class Evaluator extends GenericUDAFVarianceEvaluator {

      @Override
      public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
        switch (m) {
        case COMPLETE:
        case PARTIAL1:
          return super.init(m, new ObjectInspector[] { parameters[0] });
        default:
          return super.init(m, parameters);
        }
      }

      @Override
      public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
        if (parameters[0] == null || parameters[1] == null)
          return;
        super.iterate(agg, new Object[] { parameters[0] });
      }

      @Override
      public Object terminate(AggregationBuffer agg) throws HiveException {
        StdAgg myagg = (StdAgg) agg;
        if (myagg.count == 0) {
          return null;
        } else {
          DoubleWritable result = getResult();
          result.set(myagg.variance);
          return result;
        }
      }
    }
  }

  @Description(name = "regr_avgx", value = "_FUNC_(y,x) - evaluates the average of the independent variable", extended = "The function takes as arguments any pair of numeric types and returns a double.\n"
      + "Any pair with a NULL is ignored.\n"
      + "If applied to an empty set: NULL is returned.\n"
      + "Otherwise, it computes the following:\n"
      + "   AVG(X)")
  public static class RegrAvgX extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
      checkArgumentTypes(parameters);
      if (((PrimitiveTypeInfo) parameters[1]).getPrimitiveCategory() == PrimitiveCategory.DECIMAL) {
        return new EvaluatorDecimal();
      } else {
        return new EvaluatorDouble();
      }
    }

    private static class EvaluatorDouble extends GenericUDAFAverageEvaluatorDouble {

      @Override
      public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
        switch (m) {
        case COMPLETE:
        case PARTIAL1:
          return super.init(m, new ObjectInspector[] { parameters[1] });
        default:
          return super.init(m, parameters);
        }
      }

      @Override
      public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
        if (parameters[0] == null || parameters[1] == null)
          return;
        super.iterate(agg, new Object[] { parameters[1] });
      }
    }

    private static class EvaluatorDecimal extends GenericUDAFAverageEvaluatorDecimal {

      @Override
      public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
        switch (m) {
        case COMPLETE:
        case PARTIAL1:
          return super.init(m, new ObjectInspector[] { parameters[1] });
        default:
          return super.init(m, parameters);
        }
      }

      @Override
      public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
        if (parameters[0] == null || parameters[1] == null)
          return;
        super.iterate(agg, new Object[] { parameters[1] });
      }
    }
  }

  @Description(name = "regr_avgy", value = "_FUNC_(y,x) - evaluates the average of the dependent variable", extended = "The function takes as arguments any pair of numeric types and returns a double.\n"
      + "Any pair with a NULL is ignored.\n"
      + "If applied to an empty set: NULL is returned.\n"
      + "Otherwise, it computes the following:\n"
      + "   AVG(Y)")
  public static class RegrAvgY extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
      checkArgumentTypes(parameters);
      if (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory() == PrimitiveCategory.DECIMAL) {
        return new EvaluatorDecimal();
      } else {
        return new EvaluatorDouble();
      }
    }

    private static class EvaluatorDouble extends GenericUDAFAverageEvaluatorDouble {

      @Override
      public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
        switch (m) {
        case COMPLETE:
        case PARTIAL1:
          return super.init(m, new ObjectInspector[] { parameters[0] });
        default:
          return super.init(m, parameters);
        }
      }

      @Override
      public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
        if (parameters[0] == null || parameters[1] == null)
          return;
        super.iterate(agg, new Object[] { parameters[0] });
      }
    }

    private static class EvaluatorDecimal extends GenericUDAFAverageEvaluatorDecimal {

      @Override
      public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
        switch (m) {
        case COMPLETE:
        case PARTIAL1:
          return super.init(m, new ObjectInspector[] { parameters[0] });
        default:
          return super.init(m, parameters);
        }
      }

      @Override
      public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
        if (parameters[0] == null || parameters[1] == null)
          return;
        super.iterate(agg, new Object[] { parameters[0] });
      }
    }
  }

  @Description(name = "regr_slope", value = "_FUNC_(y,x) - returns the slope of the linear regression line", extended = "The function takes as arguments any pair of numeric types and returns a double.\n"
      + "Any pair with a NULL is ignored.\n"
      + "If applied to an empty set: NULL is returned.\n"
      + "If N*SUM(x*x) = SUM(x)*SUM(x): NULL is returned (the fit would be a vertical).\n"
      + "Otherwise, it computes the following:\n"
      + "   (N*SUM(x*y)-SUM(x)*SUM(y)) / (N*SUM(x*x)-SUM(x)*SUM(x))")
  public static class RegrSlope extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
      checkArgumentTypes(parameters);
      return new Evaluator();
    }

    private static class Evaluator extends GenericUDAFCorrelationEvaluator {

      @Override
      public Object terminate(AggregationBuffer agg) throws HiveException {
        StdAgg myagg = (StdAgg) agg;

        if (myagg.count < 2 || myagg.xvar == 0.0d) {
          return null;
        } else {
          getResult().set(myagg.covar / myagg.xvar);
          return getResult();
        }
      }
    }
  }

  @Description(name = "regr_r2", value = "_FUNC_(y,x) - returns the coefficient of determination (also called R-squared or goodness of fit) for the regression line.", extended = "The function takes as arguments any pair of numeric types and returns a double.\n"
      + "Any pair with a NULL is ignored.\n"
      + "If applied to an empty set: NULL is returned.\n"
      + "If N*SUM(x*x) = SUM(x)*SUM(x): NULL is returned.\n"
      + "If N*SUM(y*y) = SUM(y)*SUM(y): 1 is returned.\n"
      + "Otherwise, it computes the following:\n"
      + "   POWER( N*SUM(x*y)-SUM(x)*SUM(y) ,2)  /  ( (N*SUM(x*x)-SUM(x)*SUM(x)) * (N*SUM(y*y)-SUM(y)*SUM(y)) )")
  public static class RegrR2 extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
      checkArgumentTypes(parameters);
      return new Evaluator();
    }

    private static class Evaluator extends GenericUDAFCorrelationEvaluator {

      @Override
      public Object terminate(AggregationBuffer agg) throws HiveException {
        StdAgg myagg = (StdAgg) agg;

        if (myagg.count < 2 || myagg.xvar == 0.0d) {
          return null;
        }
        DoubleWritable result = getResult();
        if (myagg.yvar == 0.0d) {
          result.set(1.0d);
        } else {
          result.set(myagg.covar * myagg.covar / myagg.xvar / myagg.yvar);
        }
        return result;
      }
    }
  }

  @Description(name = "regr_sxy", value = "_FUNC_(y,x) - return a value that can be used to evaluate the statistical validity of a regression model.", extended = "The function takes as arguments any pair of numeric types and returns a double.\n"
      + "Any pair with a NULL is ignored.\n"
      + "If applied to an empty set: NULL is returned.\n"
      + "If N*SUM(x*x) = SUM(x)*SUM(x): NULL is returned.\n"
      + "Otherwise, it computes the following:\n"
      + "   SUM(x*y)-SUM(x)*SUM(y)/N")
  public static class RegrSXY extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
      checkArgumentTypes(parameters);
      return new Evaluator();
    }

    private static class Evaluator extends GenericUDAFCorrelationEvaluator {

      @Override
      public Object terminate(AggregationBuffer agg) throws HiveException {
        StdAgg myagg = (StdAgg) agg;

        if (myagg.count == 0) {
          return null;
        }
        DoubleWritable result = getResult();
        result.set(myagg.covar);
        return result;
      }
    }
  }

  @Description(name = "regr_intercept", value = "_FUNC_(y,x) - returns the y-intercept of the regression line.", extended = "The function takes as arguments any pair of numeric types and returns a double.\n"
      + "Any pair with a NULL is ignored.\n"
      + "If applied to an empty set: NULL is returned.\n"
      + "If N*SUM(x*x) = SUM(x)*SUM(x): NULL is returned.\n"
      + "Otherwise, it computes the following:\n"
      + "   ( SUM(y)*SUM(x*x)-SUM(X)*SUM(x*y) )  /  ( N*SUM(x*x)-SUM(x)*SUM(x) )")
  public static class RegrIntercept extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
      checkArgumentTypes(parameters);
      return new Evaluator();
    }

    private static class Evaluator extends GenericUDAFCorrelationEvaluator {

      @Override
      public Object terminate(AggregationBuffer agg) throws HiveException {
        StdAgg myagg = (StdAgg) agg;

        if (myagg.count == 0 || myagg.xvar == 0.0d) {
          return null;
        }
        DoubleWritable result = getResult();
        double slope = myagg.covar / myagg.xvar;
        result.set(myagg.yavg - slope * myagg.xavg);
        return result;
      }
    }
  }

  private static void checkArgumentTypes(TypeInfo[] parameters) throws UDFArgumentTypeException {
    if (parameters.length != 2) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly two arguments are expected.");
    }

    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted but "
          + parameters[0].getTypeName() + " is passed.");
    }

    if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(1, "Only primitive type arguments are accepted but "
          + parameters[1].getTypeName() + " is passed.");
    }

    if (!acceptedPrimitiveCategory(((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory())) {
      throw new UDFArgumentTypeException(0, "Only numeric type arguments are accepted but "
          + parameters[0].getTypeName() + " is passed.");

    }
    if (!acceptedPrimitiveCategory(((PrimitiveTypeInfo) parameters[1]).getPrimitiveCategory())) {
      throw new UDFArgumentTypeException(1, "Only numeric type arguments are accepted but "
          + parameters[1].getTypeName() + " is passed.");
    }
  }

  private static boolean acceptedPrimitiveCategory(PrimitiveCategory primitiveCategory) {
    switch (primitiveCategory) {
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE:
    case TIMESTAMP:
    case DECIMAL:
      return true;
    default:
      return false;
    }
  }

}
