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

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFPercentileCont.PercentileContEvaluator.PercentileAgg;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFPercentileCont.PercentileContLongEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFPercentileDisc.PercentileDiscDoubleEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFPercentileDisc.PercentileDiscLongCalculator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for GenericUDAFPercentileDisc.
 */
public class TestGenericUDAFPercentileDisc {
  PercentileDiscLongCalculator calc = new PercentileDiscLongCalculator();

  // Long type tests
  @Test
  public void testNoInterpolation() throws Exception {
    Long[] items = new Long[] {1L, 2L, 3L, 4L, 5L };
    checkPercentile(items, 0.5, 3);
  }

  @Test
  public void testInterpolateLower() throws Exception {
    Long[] items = new Long[] {1L, 2L, 3L, 4L, 5L };
    checkPercentile(items, 0.49, 3.0);
  }

  @Test
  public void testInterpolateHigher() throws Exception {
    Long[] items = new Long[] {1L, 2L, 3L, 4L, 5L };
    checkPercentile(items, 0.51, 4.0);
  }

  @Test
  public void testSingleItem50() throws Exception {
    Long[] items = new Long[] {1L };
    checkPercentile(items, 0.5, 1);
  }

  @Test
  public void testSingleItem100() throws Exception {
    Long[] items = new Long[] {1L };
    checkPercentile(items, 1, 1);
  }

  /*
   * POSTGRES check: WITH vals (k) AS (VALUES (54), (35), (15), (15), (76), (87), (78)) SELECT *
   * INTO table percentile_src FROM vals; select percentile_disc(.50) within group (order by k) as
   * perc from percentile_src;
   */
  @Test
  public void testPostgresRefExample() throws Exception {
    Long[] items = new Long[] {54L, 35L, 15L, 15L, 76L, 87L, 78L };
    checkPercentile(items, 0.5, 54);
  }

  /*
   * POSTGRES check: WITH vals (k) AS (VALUES (54), (35), (15), (15), (76), (87), (78)) SELECT *
   * INTO table percentile_src FROM vals; select percentile_disc(.72) within group (order by k) as
   * perc from percentile_src;
   */
  @Test
  public void testPostgresRefExample2() throws Exception {
    Long[] items = new Long[] {54L, 35L, 15L, 15L, 76L, 87L, 78L };
    checkPercentile(items, 0.72, 78);
  }

  // Long type tests
  @Test
  public void testDoubleNoInterpolation() throws Exception {
    Double[] items = new Double[] {1.0, 2.0, 3.0, 4.0, 5.0 };
    checkPercentile(items, 0.5, 3);
  }

  @Test
  public void testDoubleInterpolateLower() throws Exception {
    Double[] items = new Double[] {1.0, 2.0, 3.0, 4.0, 5.0 };
    checkPercentile(items, 0.49, 3.0);
  }

  @Test
  public void testDoubleInterpolateHigher() throws Exception {
    Double[] items = new Double[] {1.0, 2.0, 3.0, 4.0, 5.0 };
    checkPercentile(items, 0.51, 4.0);
  }

  @Test
  public void testDoubleSingleItem50() throws Exception {
    Double[] items = new Double[] {1.0 };
    checkPercentile(items, 0.5, 1);
  }

  @Test
  public void testDoubleSingleItem100() throws Exception {
    Double[] items = new Double[] {1.0 };
    checkPercentile(items, 1, 1);
  }

  /*
   * POSTGRES check: WITH vals (k) AS (VALUES (54.0), (35.0), (15.0), (15.0), (76.0), (87.0),
   * (78.0)) SELECT * INTO table percentile_src FROM vals; select percentile_disc(.50) within group
   * (order by k) as perc from percentile_src;
   */
  @Test
  public void testDoublePostgresRefExample() throws Exception {
    Double[] items = new Double[] {54.0, 35.0, 15.0, 15.0, 76.0, 87.0, 78.0 };
    checkPercentile(items, 0.5, 54);
  }

  /*
   * POSTGRES check: WITH vals (k) AS (VALUES (54.5), (35.3), (15.7), (15.7), (76.8), (87.34),
   * (78.0)) SELECT * INTO table percentile_src FROM vals; select percentile_disc(.72) within group
   * (order by k) as perc from percentile_src;
   */
  @Test
  public void testDoublePostgresRefExample2() throws Exception {
    Double[] items = new Double[] {54.5, 35.3, 15.7, 15.7, 76.8, 87.34, 78.0 };
    checkPercentile(items, 0.72, 78.0);
  }

  private void checkPercentile(Long[] items, double percentile, double expected) throws Exception {
    PercentileContLongEvaluator eval = new GenericUDAFPercentileDisc.PercentileDiscLongEvaluator();

    PercentileAgg agg = new PercentileContLongEvaluator().new PercentileAgg();

    agg.percentiles = new ArrayList<DoubleWritable>();
    agg.percentiles.add(new DoubleWritable(percentile));
    agg.isAscending = true;

    for (int i = 0; i < items.length; i++) {
      eval.increment(agg, new LongWritable(items[i]), 1);
    }

    DoubleWritable result = (DoubleWritable) eval.terminate(agg);

    Assert.assertEquals(expected, result.get(), 0.01);
    eval.close();
  }

  @SuppressWarnings({ "unchecked", "resource" })
  private void checkPercentile(Double[] items, double percentile, double expected)
      throws Exception {
    PercentileDiscDoubleEvaluator eval =
        new GenericUDAFPercentileDisc.PercentileDiscDoubleEvaluator();

    PercentileAgg agg = new PercentileDiscDoubleEvaluator().new PercentileAgg();

    agg.percentiles = new ArrayList<DoubleWritable>();
    agg.percentiles.add(new DoubleWritable(percentile));
    agg.isAscending = true;

    for (int i = 0; i < items.length; i++) {
      eval.increment(agg, new DoubleWritable(items[i]), 1);
    }

    DoubleWritable result = (DoubleWritable) eval.terminate(agg);

    Assert.assertEquals(expected, result.get(), 0.01);
    eval.close();
  }
}
