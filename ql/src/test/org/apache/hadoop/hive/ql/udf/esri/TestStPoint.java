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

public class TestStPoint {

  @Test
  public void TestStPoint() throws Exception {
    ST_GeometryType typer = new ST_GeometryType();
    ST_X stX = new ST_X();
    ST_Y stY = new ST_Y();
    ST_Point stPt = new ST_Point();
    BytesWritable bwGeom = stPt.evaluate(new DoubleWritable(1.2), new DoubleWritable(3.4));
    DoubleWritable dwx = stX.evaluate(bwGeom);
    DoubleWritable dwy = stY.evaluate(bwGeom);
    assertEquals(1.2, dwx.get(), .000001);
    assertEquals(3.4, dwy.get(), .000001);
    Text gty = typer.evaluate(bwGeom);
    assertEquals("ST_POINT", gty.toString());
    bwGeom = stPt.evaluate(new DoubleWritable(6.5), new DoubleWritable(4.3), new DoubleWritable(2.1));
    gty = typer.evaluate(bwGeom);
    assertEquals("ST_POINT", gty.toString());
    bwGeom = stPt.evaluate(new Text("point (10.02 20.01)"));
    gty = typer.evaluate(bwGeom);
    assertEquals("ST_POINT", gty.toString());
    bwGeom = stPt.evaluate(new Text("point z (10.02 20.01 2)"));
    gty = typer.evaluate(bwGeom);
    assertEquals("ST_POINT", gty.toString());
  }

}

