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

public class TestStY {

  @Test
  public void TestStY() {
    ST_Y stY = new ST_Y();
    ST_Point stPt = new ST_Point();
    BytesWritable bwGeom = stPt.evaluate(new DoubleWritable(1.2), new DoubleWritable(3.4));
    DoubleWritable dwy = stY.evaluate(bwGeom);
    assertEquals(3.4, dwy.get(), .000001);
    bwGeom = stPt.evaluate(new DoubleWritable(6.5), new DoubleWritable(4.3), new DoubleWritable(2.1));
    dwy = stY.evaluate(bwGeom);
    assertEquals(4.3, dwy.get(), 0.0);
  }

}

