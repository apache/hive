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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.udf.esri;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestStGeodesicLengthWGS84 {

  private final ST_GeodesicLengthWGS84 stLength = new ST_GeodesicLengthWGS84();
  private final ST_GeomFromText stGeomFromText = new ST_GeomFromText();
  private final ST_SetSRID stSetSRID = new ST_SetSRID();

  private BytesWritable wgs84(String wkt) throws Exception {
    BytesWritable geom = stGeomFromText.evaluate(new Text(wkt));
    return stSetSRID.evaluate(geom, new IntWritable(4326));
  }

  // Reference values from Apache Impala's geospatial-esri.test, which were generated from the
  // original ESRI (Vincenty, ellipsoidal WGS84) implementation. A spherical (Haversine)
  // approximation would not reproduce them.

  @Test
  public void testLineStringLengthMatchesEsriVincenty() throws Exception {
    DoubleWritable result = stLength.evaluate(wgs84("linestring (0 0, 0.03 0.04)"));
    assertNotNull(result);
    assertEquals(5542.156362735362, result.get(), 1e-6);
  }

  @Test
  public void testMultiLineStringLengthMatchesEsriVincenty() throws Exception {
    DoubleWritable result = stLength.evaluate(wgs84("multilinestring ((0 0, 0.03 0.04))"));
    assertNotNull(result);
    assertEquals(5542.156362735362, result.get(), 1e-6);
  }

  @Test
  public void testLengthAtHighLatitudeMatchesEsriVincenty() throws Exception {
    // At 80 degrees latitude the longitudinal degrees are strongly compressed.
    DoubleWritable result = stLength.evaluate(wgs84("multilinestring ((0 80, 0.03 80.04))"));
    assertNotNull(result);
    assertEquals(4503.988488226892, result.get(), 1e-6);
  }

  @Test
  public void testMultiLineStringSumsParts() throws Exception {
    // evaluate() reuses a shared DoubleWritable, so read the value before the next call.
    double single = stLength.evaluate(wgs84("linestring (0 0, 0.03 0.04)")).get();
    double multi =
        stLength.evaluate(wgs84("multilinestring ((0 0, 0.03 0.04), (0 0, 0.03 0.04))")).get();
    assertEquals(2.0 * single, multi, 1e-6);
  }

  @Test
  public void testPointIsZeroLength() throws Exception {
    DoubleWritable result = stLength.evaluate(wgs84("point (5 5)"));
    assertNotNull(result);
    assertEquals(0.0, result.get(), 0.0);
  }

  @Test
  public void testNonWgs84ReturnsNull() throws Exception {
    BytesWritable geom = stGeomFromText.evaluate(new Text("linestring (0 0, 0.03 0.04)"));
    BytesWritable webMercator = stSetSRID.evaluate(geom, new IntWritable(3857));
    assertNull(stLength.evaluate(webMercator));
  }

  @Test
  public void testNullInput() {
    assertNull(stLength.evaluate(null));
  }

  /**
   * Opposite poles are perfectly antipodal: the distance is the full pole-to-pole meridian
   * (2 * quarter-meridian). Exercises ESRI's dedicated antipodal branch, which the earlier
   * plain-Vincenty reimplementation did not handle.
   */
  @Test
  public void testOppositePolesAntipodal() throws Exception {
    DoubleWritable result = stLength.evaluate(wgs84("linestring (0 -90, 0 90)"));
    assertNotNull(result);
    assertEquals(20003931.458625443, result.get(), 1e-6);
  }

  /**
   * A near-antipodal line: the ellipsoidal inverse solution is notoriously slow/failing to
   * converge here for naive Vincenty. The ESRI NGS algorithm switches to its antipodal loop
   * and returns a finite distance near half the Earth's circumference.
   */
  @Test
  public void testNearAntipodalConverges() throws Exception {
    DoubleWritable result = stLength.evaluate(wgs84("linestring (0 0, 179.5 0.5)"));
    assertNotNull(result);
    assertTrue("expected a finite length, got " + result.get(), Double.isFinite(result.get()));
    assertEquals(19936288.578978203, result.get(), 1e-3);
  }
}
