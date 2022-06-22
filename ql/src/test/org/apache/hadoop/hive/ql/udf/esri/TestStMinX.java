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

// select ST_MinX(ST_Point(1,2)) from onerow;
// select ST_MinX(ST_LineString(1.5,2.5, 3.0,2.2)) from onerow;
// select ST_MinX(ST_Polygon(1, 1, 1, 4, 4, 4, 4, 1)) from onerow;
// select ST_MinX(ST_MultiPoint(0,0, 2,2)) from onerow;
// select ST_MinX(ST_MultiLineString(array(1, 1, 2, 2), array(10, 10, 20, 20))) from onerow;
// select ST_MinX(ST_MultiPolygon(array(1,1, 1,2, 2,2, 2,1), array(3,3, 3,4, 4,4, 4,3))) from onerow;

public class TestStMinX {

  @Test
  public void TestStMinX() {
    ST_MinX stMinX = new ST_MinX();
    ST_Point stPt = new ST_Point();
    BytesWritable bwGeom = stPt.evaluate(new DoubleWritable(1.2), new DoubleWritable(3.4));
    DoubleWritable dwx = stMinX.evaluate(bwGeom);
    assertEquals(1.2, dwx.get(), .000001);
    bwGeom = stPt.evaluate(new DoubleWritable(6.5), new DoubleWritable(4.3), new DoubleWritable(2.1));
    dwx = stMinX.evaluate(bwGeom);
    assertEquals(6.5, dwx.get(), 0.0);
  }

}
