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
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

// select ST_GeometryType(ST_Point(0, 0)) from onerow;
// select ST_GeometryType(ST_Point('point (10.02 20.01)')) from onerow;
// select ST_GeometryType(ST_Point('point z (10.02 20.01 2)')) from onerow;
// select ST_GeometryType(ST_MultiPoint('multipoint ((1 2))')) from onerow;
// select ST_GeometryType(ST_Linestring(10,10, 20,20)) from onerow;
// select ST_GeometryType(ST_Linestring('linestring (10 10, 20 20)')) from onerow;
// select ST_GeometryType(ST_Linestring('linestring z (10 10 2, 20 20 4)')) from onerow;
// select ST_GeometryType(ST_GeomFromText('polygon ((0 0, 0 10, 10 0, 0 0))')) from onerow;
// select ST_GeometryType(ST_Polygon('polygon ((0 0, 0 10, 10 0, 0 0))')) from onerow;
// select ST_GeometryType(ST_Polygon(1,1, 1,4, 4,1)) from onerow;
// select ST_GeometryType(ST_Polygon(1,1, 4,1, 1,4)) from onerow;
// select ST_GeometryType(ST_Polygon(1,1, 1,4, 4,1, 1,1)) from onerow;
// select ST_GeometryType(ST_Polygon(1,1, 4,1, 1,4, 1,1)) from onerow;
// select ST_GeometryType(ST_GeomFromGeoJson('{"type":"Point", "coordinates":[1.2, 2.4]}')) from onerow;

public class TestStGeometryType {

  @Test
  public void TestStGeometryType() throws Exception {
    ST_GeometryType typer = new ST_GeometryType();
    ST_Point stPt = new ST_Point();
    ST_MultiPoint stMp = new ST_MultiPoint();
    ST_LineString stLn = new ST_LineString();
    ST_Polygon stPoly = new ST_Polygon();
    BytesWritable bwGeom = stPt.evaluate(new DoubleWritable(0), new DoubleWritable(0));
    Text gty = typer.evaluate(bwGeom);
    assertEquals("ST_POINT", gty.toString());
    bwGeom = stPt.evaluate(new Text("point z (10.02 20.01 2)"));
    gty = typer.evaluate(bwGeom);
    assertEquals("ST_POINT", gty.toString());
    bwGeom = stLn.evaluate(new Text("linestring (10 10, 20 20)"));
    gty = typer.evaluate(bwGeom);
    assertEquals("ST_LINESTRING", gty.toString());
    bwGeom = stPoly.evaluate(new Text("polygon ((0 0, 0 10, 10 0, 0 0))"));
    gty = typer.evaluate(bwGeom);
    assertEquals("ST_POLYGON", gty.toString());
  }

}

