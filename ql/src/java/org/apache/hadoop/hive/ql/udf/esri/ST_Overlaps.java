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

import com.esri.core.geometry.OperatorOverlaps;
import com.esri.core.geometry.OperatorSimpleRelation;
import org.apache.hadoop.hive.ql.exec.Description;

@Description(name = "ST_Overlaps",
    value = "_FUNC_(geometry1, geometry2) - return true if geometry1 overlaps geometry2",
    extended = "Example:\n"
        + "SELECT _FUNC_(st_polygon(2,0, 2,3, 3,0), st_polygon(1,1, 1,4, 4,4, 4,1)) from src LIMIT 1;  -- return true\n"
        + "SELECT _FUNC_(st_polygon(2,0, 2,1, 3,1), ST_Polygon(1,1, 1,4, 4,4, 4,1)) from src LIMIT 1;  -- return false")

public class ST_Overlaps extends ST_GeometryRelational {

  @Override
  protected OperatorSimpleRelation getRelationOperator() {
    return OperatorOverlaps.local();
  }

  @Override
  public String getDisplayString(String[] args) {
    return String.format("returns true if %s overlaps %s", args[0], args[1]);
  }
}
