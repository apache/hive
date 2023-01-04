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

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Geometry.Type;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestStAsShape {

  private final static double Epsilon = 0.0001;

  @Test
  public void testPointAsShape() {
    ST_Point point = new ST_Point();
    final double longitude = 12.224;
    final double latitude = 51.829;
    BytesWritable pointAsWritable = point.evaluate(new DoubleWritable(longitude), new DoubleWritable(latitude));
    assertNotNull("The point writable must not be null!", pointAsWritable);

    ST_AsShape asShape = new ST_AsShape();
    BytesWritable shapeAsWritable = asShape.evaluate(pointAsWritable);
    assertNotNull("The shape writable must not be null!", pointAsWritable);

    byte[] esriShapeBuffer = shapeAsWritable.getBytes();
    Geometry esriGeometry = GeometryEngine.geometryFromEsriShape(esriShapeBuffer, Type.Point);
    assertNotNull("The geometry must not be null!", esriGeometry);
    assertTrue("Geometry type point expected!", esriGeometry instanceof Point);

    Point esriPoint = (Point) esriGeometry;
    assertEquals("Longitude is different!", longitude, esriPoint.getX(), Epsilon);
    assertEquals("Latitude is different!", latitude, esriPoint.getY(), Epsilon);
  }
}
