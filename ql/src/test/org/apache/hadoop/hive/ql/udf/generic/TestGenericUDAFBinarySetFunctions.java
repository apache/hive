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

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import jersey.repackaged.com.google.common.collect.Lists;

@RunWith(Parameterized.class)
public class TestGenericUDAFBinarySetFunctions {

  private List<Object[]> rowSet;

  @Parameters(name = "{0}")
  public static List<Object[]> getParameters() {
    List<Object[]> ret = new ArrayList<>();
    ret.add(new Object[] { "seq/seq", RowSetGenerator.generate(10,
        new RowSetGenerator.DoubleSequence(0), new RowSetGenerator.DoubleSequence(0)) });
    ret.add(new Object[] { "seq/ones", RowSetGenerator.generate(10,
        new RowSetGenerator.DoubleSequence(0), new RowSetGenerator.ConstantSequence(1.0)) });
    ret.add(new Object[] { "ones/seq", RowSetGenerator.generate(10,
        new RowSetGenerator.ConstantSequence(1.0), new RowSetGenerator.DoubleSequence(0)) });
    ret.add(new Object[] { "empty", RowSetGenerator.generate(0,
        new RowSetGenerator.DoubleSequence(0), new RowSetGenerator.DoubleSequence(0)) });
    ret.add(new Object[] { "lonely", RowSetGenerator.generate(1,
        new RowSetGenerator.DoubleSequence(10), new RowSetGenerator.DoubleSequence(10)) });
    ret.add(new Object[] { "seq/seq+10", RowSetGenerator.generate(10,
        new RowSetGenerator.DoubleSequence(0), new RowSetGenerator.DoubleSequence(10)) });
    ret.add(new Object[] { "seq/null", RowSetGenerator.generate(10,
        new RowSetGenerator.DoubleSequence(0), new RowSetGenerator.ConstantSequence(null)) });
    ret.add(new Object[] { "null/seq0", RowSetGenerator.generate(10,
        new RowSetGenerator.ConstantSequence(null), new RowSetGenerator.DoubleSequence(0)) });
    return ret;
  }

  public static class GenericUDAFExecutor {

    private GenericUDAFResolver2 evaluatorFactory;
    private GenericUDAFParameterInfo info;
    private ObjectInspector[] partialOIs;

    public GenericUDAFExecutor(GenericUDAFResolver2 evaluatorFactory, GenericUDAFParameterInfo info)
        throws Exception {
      this.evaluatorFactory = evaluatorFactory;
      this.info = info;

      GenericUDAFEvaluator eval0 = evaluatorFactory.getEvaluator(info);
      partialOIs = new ObjectInspector[] {
          eval0.init(GenericUDAFEvaluator.Mode.PARTIAL1, info.getParameterObjectInspectors()) };

    }

    List<Object> run(List<Object[]> values) throws Exception {
      Object r1 = runComplete(values);
      Object r2 = runPartialFinal(values);
      Object r3 = runPartial2Final(values);
      return Lists.newArrayList(r1, r2, r3);
    }

    private Object runComplete(List<Object[]> values) throws SemanticException, HiveException {
      GenericUDAFEvaluator eval = evaluatorFactory.getEvaluator(info);
      eval.init(GenericUDAFEvaluator.Mode.COMPLETE, info.getParameterObjectInspectors());
      AggregationBuffer agg = eval.getNewAggregationBuffer();
      for (Object[] parameters : values) {
        eval.iterate(agg, parameters);
      }
      return eval.terminate(agg);
    }

    private Object runPartialFinal(List<Object[]> values) throws Exception {
      GenericUDAFEvaluator eval = evaluatorFactory.getEvaluator(info);
      eval.init(GenericUDAFEvaluator.Mode.FINAL, partialOIs);
      AggregationBuffer buf = eval.getNewAggregationBuffer();
      for (Object partialResult : runPartial1(values)) {
        eval.merge(buf, partialResult);
      }
      return eval.terminate(buf);
    }

    private Object runPartial2Final(List<Object[]> values) throws Exception {
      GenericUDAFEvaluator eval = evaluatorFactory.getEvaluator(info);
      eval.init(GenericUDAFEvaluator.Mode.FINAL, partialOIs);
      AggregationBuffer buf = eval.getNewAggregationBuffer();
      for (Object partialResult : runPartial2(runPartial1(values))) {
        eval.merge(buf, partialResult);
      }
      return eval.terminate(buf);
    }

    private List<Object> runPartial1(List<Object[]> values) throws Exception {
      List<Object> ret = new ArrayList<>();
      int batchSize = 1;
      Iterator<Object[]> iter = values.iterator();
      do {
        GenericUDAFEvaluator eval = evaluatorFactory.getEvaluator(info);
        eval.init(GenericUDAFEvaluator.Mode.PARTIAL1, info.getParameterObjectInspectors());
        AggregationBuffer buf = eval.getNewAggregationBuffer();
        for (int i = 0; i < batchSize - 1 && iter.hasNext(); i++) {
          eval.iterate(buf, iter.next());
        }
        batchSize <<= 1;
        ret.add(eval.terminatePartial(buf));

        // back-check to force at least 1 output; and this should have a partial which is empty
      } while (iter.hasNext());
      return ret;
    }

    private List<Object> runPartial2(List<Object> values) throws Exception {
      List<Object> ret = new ArrayList<>();
      int batchSize = 1;
      Iterator<Object> iter = values.iterator();
      do {
        GenericUDAFEvaluator eval = evaluatorFactory.getEvaluator(info);
        eval.init(GenericUDAFEvaluator.Mode.PARTIAL2, partialOIs);
        AggregationBuffer buf = eval.getNewAggregationBuffer();
        for (int i = 0; i < batchSize - 1 && iter.hasNext(); i++) {
          eval.merge(buf, iter.next());
        }
        batchSize <<= 1;
        ret.add(eval.terminatePartial(buf));

        // back-check to force at least 1 output; and this should have a partial which is empty
      } while (iter.hasNext());
      return ret;
    }
  }

  public static class RowSetGenerator {
    public static interface FieldGenerator {
      public Object apply(int rowIndex);
    }

    public static class ConstantSequence implements FieldGenerator {
      private Object constant;

      public ConstantSequence(Object constant) {
        this.constant = constant;
      }

      @Override
      public Object apply(int rowIndex) {
        return constant;
      }
    }

    public static class DoubleSequence implements FieldGenerator {

      private int offset;

      public DoubleSequence(int offset) {
        this.offset = offset;
      }

      @Override
      public Object apply(int rowIndex) {
        double d = rowIndex + offset;
        return d;
      }
    }

    public static List<Object[]> generate(int numRows, FieldGenerator... generators) {
      ArrayList<Object[]> ret = new ArrayList<>(numRows);
      for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
        ArrayList<Object> row = new ArrayList<>();
        for (FieldGenerator g : generators) {
          row.add(g.apply(rowIdx));
        }
        ret.add(row.toArray());
      }
      return ret;
    }
  }

  public TestGenericUDAFBinarySetFunctions(String label, List<Object[]> rowSet) {
    this.rowSet = rowSet;
  }

  @Test
  public void regr_count() throws Exception {
    RegrIntermediate expected = RegrIntermediate.computeFor(rowSet);
    validateUDAF(expected.count(), new GenericUDAFBinarySetFunctions.RegrCount());
  }

  @Test
  public void regr_sxx() throws Exception {
    RegrIntermediate expected = RegrIntermediate.computeFor(rowSet);
    validateUDAF(expected.sxx(), new GenericUDAFBinarySetFunctions.RegrSXX());
  }

  @Test
  public void regr_syy() throws Exception {
    RegrIntermediate expected = RegrIntermediate.computeFor(rowSet);
    validateUDAF(expected.syy(), new GenericUDAFBinarySetFunctions.RegrSYY());
  }

  @Test
  public void regr_sxy() throws Exception {
    RegrIntermediate expected = RegrIntermediate.computeFor(rowSet);
    validateUDAF(expected.sxy(), new GenericUDAFBinarySetFunctions.RegrSXY());
  }

  @Test
  public void regr_avgx() throws Exception {
    RegrIntermediate expected = RegrIntermediate.computeFor(rowSet);
    validateUDAF(expected.avgx(), new GenericUDAFBinarySetFunctions.RegrAvgX());
  }

  @Test
  public void regr_avgy() throws Exception {
    RegrIntermediate expected = RegrIntermediate.computeFor(rowSet);
    validateUDAF(expected.avgy(), new GenericUDAFBinarySetFunctions.RegrAvgY());
  }

  @Test
  public void regr_slope() throws Exception {
    RegrIntermediate expected = RegrIntermediate.computeFor(rowSet);
    validateUDAF(expected.slope(), new GenericUDAFBinarySetFunctions.RegrSlope());
  }

  @Test
  public void regr_r2() throws Exception {
    RegrIntermediate expected = RegrIntermediate.computeFor(rowSet);
    validateUDAF(expected.r2(), new GenericUDAFBinarySetFunctions.RegrR2());
  }

  @Test
  public void regr_intercept() throws Exception {
    RegrIntermediate expected = RegrIntermediate.computeFor(rowSet);
    validateUDAF(expected.intercept(), new GenericUDAFBinarySetFunctions.RegrIntercept());
  }

  @Test
  public void corr() throws Exception {
    RegrIntermediate expected = RegrIntermediate.computeFor(rowSet);
    validateUDAF(expected.corr(), new GenericUDAFCorrelation());
  }

  @Test
  public void covar_pop() throws Exception {
    RegrIntermediate expected = RegrIntermediate.computeFor(rowSet);
    validateUDAF(expected.covar_pop(), new GenericUDAFCovariance());
  }

  @Test
  public void covar_samp() throws Exception {
    RegrIntermediate expected = RegrIntermediate.computeFor(rowSet);
    validateUDAF(expected.covar_samp(), new GenericUDAFCovarianceSample());
  }

  private void validateUDAF(Double expectedResult, GenericUDAFResolver2 udaf) throws Exception {
    ObjectInspector[] params =
        new ObjectInspector[] { javaDoubleObjectInspector, javaDoubleObjectInspector };
    GenericUDAFParameterInfo gpi = new SimpleGenericUDAFParameterInfo(params, false, false, false);
    GenericUDAFExecutor executor = new GenericUDAFExecutor(udaf, gpi);

    List<Object> values = executor.run(rowSet);

    if (expectedResult == null) {
      for (Object v : values) {
        assertNull(v);
      }
    } else {
      for (Object v : values) {
        if (v instanceof DoubleWritable) {
          assertEquals(expectedResult, ((DoubleWritable) v).get(), 1e-10);
        } else {
          assertEquals(expectedResult, ((LongWritable) v).get(), 1e-10);
        }
      }
    }
  }

  static class RegrIntermediate {
    public double sum_x2, sum_y2;
    public double sum_x, sum_y;
    public double sum_xy;
    public double n;

    public void add(Double y, Double x) {
      if (x == null || y == null) {
        return;
      }
      sum_x2 += x * x;
      sum_y2 += y * y;
      sum_x += x;
      sum_y += y;
      sum_xy += x * y;
      n++;
    }

    public Double intercept() {
      double xx = n * sum_x2 - sum_x * sum_x;
      if (n == 0 || xx == 0.0d)
        return null;
      return (sum_y * sum_x2 - sum_x * sum_xy) / xx;
    }

    public Double sxy() {
      if (n == 0)
        return null;
      return sum_xy - sum_x * sum_y / n;
    }

    public Double covar_pop() {
      if (n == 0)
        return null;
      return (sum_xy - sum_x * sum_y / n) / n;
    }

    public Double covar_samp() {
      if (n <= 1)
        return null;
      return (sum_xy - sum_x * sum_y / n) / (n - 1);
    }

    public Double corr() {
      double xx = n * sum_x2 - sum_x * sum_x;
      double yy = n * sum_y2 - sum_y * sum_y;
      if (n == 0 || xx == 0.0d || yy == 0.0d)
        return null;
      double c = n * sum_xy - sum_x * sum_y;
      return Math.sqrt(c * c / xx / yy);
    }

    public Double r2() {
      double xx = n * sum_x2 - sum_x * sum_x;
      double yy = n * sum_y2 - sum_y * sum_y;
      if (n == 0 || xx == 0.0d)
        return null;
      if (yy == 0.0d)
        return 1.0d;
      double c = n * sum_xy - sum_x * sum_y;
      return c * c / xx / yy;
    }

    public Double slope() {
      if (n == 0 || n * sum_x2 == sum_x * sum_x)
        return null;
      return (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x);
    }

    public Double avgx() {
      if (n == 0)
        return null;
      return sum_x / n;
    }

    public Double avgy() {
      if (n == 0)
        return null;
      return sum_y / n;
    }

    public Double count() {
      return n;
    }

    public Double sxx() {
      if (n == 0)
        return null;
      return sum_x2 - sum_x * sum_x / n;
    }

    public Double syy() {
      if (n == 0)
        return null;
      return sum_y2 - sum_y * sum_y / n;
    }

    public static RegrIntermediate computeFor(List<Object[]> rows) {
      RegrIntermediate ri = new RegrIntermediate();
      for (Object[] objects : rows) {
        ri.add((Double) objects[0], (Double) objects[1]);
      }
      return ri;
    }

  }
}
