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

import org.junit.Test;

// select ST_MinY(ST_GeomFromGeoJson('{"type":"LineString", "coordinates":[[1,2], [3,4]]}')) from onerow;
// select ST_MinY(ST_Point(1,2)) from onerow;
// select ST_MinY(ST_LineString(1.5,2.5, 3.0,2.2)) from onerow;
// select ST_MinY(ST_Polygon(1, 1, 1, 4, 4, 4, 4, 1)) from onerow;
// select ST_MinY(ST_MultiPoint(0,0, 2,2)) from onerow;
// select ST_MinY(ST_MultiLineString(array(1, 1, 2, 2), array(10, 10, 20, 20))) from onerow;
// select ST_MinY(ST_MultiPolygon(array(1,1, 1,2, 2,2, 2,1), array(3,3, 3,4, 4,4, 4,3))) from onerow;

public class TestStMinY {

  @Test
  public void TestStMinY() {
    ST_MinY stMinY = new ST_MinY();
    ST_Point stPt = new ST_Point();
  }

}
