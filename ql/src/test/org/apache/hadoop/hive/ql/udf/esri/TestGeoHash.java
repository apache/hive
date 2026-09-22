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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Unit tests for geohash UDFs; golden encode/decode output is covered by geospatial_geohash.q. */
public class TestGeoHash {

  @Test
  public void testStGeoHashFromPoint() {
    ST_GeoHash gh = new ST_GeoHash();
    ST_Point pt = new ST_Point();
    BytesWritable geom2d =
        pt.evaluate(new DoubleWritable(-126.965375), new DoubleWritable(43.234528));
    assertEquals("9pttyydekk4t", gh.evaluate(geom2d, new IntWritable(12)).toString());
    assertEquals("9ptty", gh.evaluate(geom2d, new IntWritable(5)).toString());

    BytesWritable geom3d = pt.evaluate(new DoubleWritable(-126.965375), new DoubleWritable(43.234528),
        new DoubleWritable(999.0));
    assertEquals("9pttyydekk4t", gh.evaluate(geom3d, new IntWritable(12)).toString());
  }

  @Test
  public void testStGeoHashRejectsNonPoint() throws Exception {
    ST_GeoHash gh = new ST_GeoHash();
    ST_LineString line = new ST_LineString();
    BytesWritable lineGeom = line.evaluate(new DoubleWritable(0), new DoubleWritable(0),
        new DoubleWritable(1), new DoubleWritable(1));
    assertNull(gh.evaluate(lineGeom, new IntWritable(5)));
    assertNull(gh.evaluate((BytesWritable) null));
  }

  @Test
  public void testStGeoHashInvalidInput() {
    ST_GeoHash gh = new ST_GeoHash();
    ST_Point pt = new ST_Point();
    BytesWritable point = pt.evaluate(new DoubleWritable(0), new DoubleWritable(0));

    assertNull(gh.evaluate((BytesWritable) null));
    assertNull(gh.evaluate(point, new IntWritable(0)));
    assertNull(gh.evaluate(point, new IntWritable(13)));
  }

  @Test
  public void testStGeomFromGeoHashInvalidInput() {
    ST_GeomFromGeoHash fromHash = new ST_GeomFromGeoHash();
    assertNull(fromHash.evaluate(null));
    assertNull(fromHash.evaluate(new Text("")));
    assertNull(fromHash.evaluate(new Text("9ptty"), new IntWritable(0)));
    assertNull(fromHash.evaluate(new Text("9ptty"), new IntWritable(6)));
    assertNull(fromHash.evaluate(new Text("9ptty"), new IntWritable(13)));
  }

  @Test
  public void testGeoHashUtilsEncode() {
    assertEquals(5, GeoHashUtils.geohashForPoint(0, 0, 5).length());
    assertEquals(GeoHashUtils.DEFAULT_CHARACTER_PRECISION,
        GeoHashUtils.geohashForPoint(0, 0, GeoHashUtils.DEFAULT_CHARACTER_PRECISION).length());
  }

  @Test
  public void testGeoHashUtilsCellPolygon() {
    assertTrue(GeoHashUtils.geohashCellPolygon("9ptty", 5).isValid());
  }
}
