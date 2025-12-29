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
package org.apache.hadoop.hive.ql.udf.esri;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestStDistanceSphere {

  private BytesWritable point(double lon, double lat) {
    ST_Point stPt = new ST_Point();
    return stPt.evaluate(new DoubleWritable(lon), new DoubleWritable(lat));
  }

  @Test
  public void testStDistanceSphere() {
    // Comparing distances to results from st_distancesphere() in PostGis 3.2.1.
    // The results are not exact matches, but very close.
    ST_DistanceSphere ds = new ST_DistanceSphere();
    BytesWritable pParlaimentDome = point(19.04572641312447, 47.50745742600276);
    BytesWritable pParlaimentFlag = point(19.04773610387992, 47.507062805452804);
    BytesWritable pBudaCastle = point(19.04125284394088, 47.49660323441422);
    BytesWritable pSzeged = point(20.145, 46.255);
    BytesWritable pBangalore = point(77.59167, 12.97889);
    DoubleWritable res;
    res = ds.evaluate(pParlaimentFlag, pParlaimentDome);
    assertEquals(157.20008441, res.get(), 0.01);

    res = ds.evaluate(pBudaCastle, pParlaimentDome);
    assertEquals(1252.84371679, res.get(), 0.05);

    res = ds.evaluate(pBudaCastle, pSzeged);
    assertEquals(161548.64536448, res.get(), 0.1);

    res = ds.evaluate(pBudaCastle, pBangalore);
    assertEquals(6604682.18138413, res.get(), 1.0);
  }

  @Test
  public void testHaversine() {
    // based on Apache Sedona's tests:
    // https://github.com/apache/sedona/blob/0523fbd/common/src/test/java/org/apache/sedona/common/FunctionsTest.java#L1489
    // Basic check
    assertEquals(1.00075559643809E7, Haversine.distanceMeters(90, 0, 0, 0), 0.1);
    assertEquals(543796.9506134904, Haversine.distanceMeters(-0.56, 51.3168, -3.1883, 55.9533), 0.1);
    assertEquals(299073.03416817175, Haversine.distanceMeters(11.786111, 48.353889, 8.570556, 50.033333), 0.1);
    assertEquals(479569.4558072244, Haversine.distanceMeters(11.786111, 48.353889, 13.287778, 52.559722), 0.1);

    // HK to Sydney
    assertEquals(7393893.072901942, Haversine.distanceMeters(113.914603, 22.308919, 151.177222, -33.946111), 0.1);

    // HK to Toronto
    assertEquals(1.2548548944238186E7, Haversine.distanceMeters(113.914603, 22.308919, -79.630556, 43.677223), 0.1);

    // Crossing the anti-meridian
    assertTrue(Haversine.distanceMeters(179.999, 0, -179.999, 0) < 300);
    assertTrue(Haversine.distanceMeters(-179.999, 0, 179.999, 0) < 300);
    assertTrue(Haversine.distanceMeters(179.999, 60, -179.999, 60) < 300);
    assertTrue(Haversine.distanceMeters(-179.999, 60, 179.999, 60) < 300);
    assertTrue(Haversine.distanceMeters(179.999, -60, -179.999, -60) < 300);
    assertTrue(Haversine.distanceMeters(-179.999, -60, 179.999, -60) < 300);

    // Crossing the North Pole
    assertTrue(Haversine.distanceMeters(-60, 89.999, 120, 89.999) < 300);
    assertTrue(Haversine.distanceMeters(120, 89.999, -60, 89.999) < 300);

    // Crossing the South Pole
    assertTrue(Haversine.distanceMeters(-60, -89.999, 120, -89.999) < 300);
    assertTrue(Haversine.distanceMeters(120, -89.999, -60, -89.999) < 300);
  }

}

