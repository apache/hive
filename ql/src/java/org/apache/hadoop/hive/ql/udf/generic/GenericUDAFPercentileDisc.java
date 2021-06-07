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
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.WindowFunctionDescription;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

/**
 * GenericUDAFPercentileDisc.
 */
@Description(
        name = "percentile_disc",
        value = "_FUNC_(input, pc) - "
                + "Returns the percentile of expr at pc (range: [0,1]) without interpolation.")
@WindowFunctionDescription(
        supportsWindow = false,
        pivotResult = true,
        orderedAggregate = true)
public class GenericUDAFPercentileDisc extends GenericUDAFPercentileCont {

  @Override
  protected GenericUDAFEvaluator createLongEvaluator(TypeInfo percentile) {
    return percentile.getCategory() == ObjectInspector.Category.LIST ?
            new PercentileDiscLongArrayEvaluator() : new PercentileDiscLongEvaluator();
  }

  @Override
  protected GenericUDAFEvaluator createDoubleEvaluator(TypeInfo percentile) {
    return percentile.getCategory() == ObjectInspector.Category.LIST ?
            new PercentileDiscDoubleArrayEvaluator() : new PercentileDiscDoubleEvaluator();
  }

  /**
   * The evaluator for discrete percentile computation based on long.
   */
  public static class PercentileDiscLongEvaluator extends PercentileContLongEvaluator {
    public PercentileDiscLongEvaluator() {
    }

    @Override
    protected PercentileCalculator<LongWritable> getCalculator() {
      return new PercentileDiscLongCalculator();
    }
  }

  /**
   * The evaluator for discrete percentile computation based on array of longs.
   */
  public static class PercentileDiscLongArrayEvaluator extends PercentileContLongArrayEvaluator {
    public PercentileDiscLongArrayEvaluator() {
    }

    @Override
    protected PercentileCalculator<LongWritable> getCalculator() {
      return new PercentileDiscLongCalculator();
    }
  }

  /**
   * The evaluator for discrete percentile computation based on double.
   */
  public static class PercentileDiscDoubleEvaluator extends PercentileContDoubleEvaluator {
    public PercentileDiscDoubleEvaluator() {
      super();
    }

    @Override
    protected PercentileCalculator<DoubleWritable> getCalculator() {
      return new PercentileDiscDoubleCalculator();
    }
  }

  /**
   * The evaluator for discrete percentile computation based on array of doubles.
   */
  public static class PercentileDiscDoubleArrayEvaluator extends PercentileContDoubleArrayEvaluator {
    public PercentileDiscDoubleArrayEvaluator() {
      super();
    }

    @Override
    protected PercentileCalculator<DoubleWritable> getCalculator() {
      return new PercentileDiscDoubleCalculator();
    }
  }

  public static class PercentileDiscLongCalculator implements PercentileCalculator<LongWritable> {
    public double getPercentile(List<Map.Entry<LongWritable, LongWritable>> entriesList,
        double position) {
      long lower = (long) Math.floor(position);
      long higher = (long) Math.ceil(position);

      int i = 0;
      while (entriesList.get(i).getValue().get() < lower + 1) {
        i++;
      }

      long lowerKey = entriesList.get(i).getKey().get();
      if (higher == lower) {
        return lowerKey;
      }

      if (entriesList.get(i).getValue().get() < higher + 1) {
        i++;
      }
      return entriesList.get(i).getKey().get();
    }
  }

  public static class PercentileDiscDoubleCalculator
      implements PercentileCalculator<DoubleWritable> {
    public double getPercentile(List<Map.Entry<DoubleWritable, LongWritable>> entriesList,
        double position) {
      long lower = (long) Math.floor(position);
      long higher = (long) Math.ceil(position);

      int i = 0;
      while (entriesList.get(i).getValue().get() < lower + 1) {
        i++;
      }

      double lowerKey = entriesList.get(i).getKey().get();
      if (higher == lower) {
        return lowerKey;
      }

      if (entriesList.get(i).getValue().get() < higher + 1) {
        i++;
      }
      return entriesList.get(i).getKey().get();
    }
  }
}
