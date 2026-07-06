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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometry;

@UDFType(deterministic = true) @Description(name = "ST_Contains",
    value = "_FUNC_(geometry1, geometry2) - return true if geometry1 contains geometry2",
    extended = "Example:\n"
        + "SELECT _FUNC_(st_polygon(1,1, 1,4, 4,4, 4,1), st_point(2, 3) from src LIMIT 1;  -- return true\n"
        + "SELECT _FUNC_(st_polygon(1,1, 1,4, 4,4, 4,1), st_point(8, 8) from src LIMIT 1;  -- return false") public class ST_Contains
    extends ST_GeometryRelational {

  @Override
  protected boolean executeRelation(Geometry geom1, Geometry geom2) {
    return geom1.contains(geom2);
  }

  @Override
  protected boolean executeRelationPrepared(PreparedGeometry prepGeom1, Geometry geom2) {
    return prepGeom1.contains(geom2);
  }

  @Override
  public String getDisplayString(String[] args) {
    return String.format("returns true if %s contains %s", args[0], args[1]);
  }
}
