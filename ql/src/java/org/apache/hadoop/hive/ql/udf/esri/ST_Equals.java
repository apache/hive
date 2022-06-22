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

import com.esri.core.geometry.OperatorEquals;
import com.esri.core.geometry.OperatorSimpleRelation;
import org.apache.hadoop.hive.ql.exec.Description;

@Description(name = "ST_Equals",
    value = "_FUNC_(geometry1, geometry2) - return true if geometry1 equals geometry2",
    extended = "Example:\n"
        + "SELECT _FUNC_(st_linestring(0,0, 1,1), st_linestring(1,1, 0,0)) from src LIMIT 1;  -- return true\n"
        + "SELECT _FUNC_(st_linestring(0,0, 1,1), st_linestring(1,0, 0,1)) from src LIMIT 1;  -- return false\n") public class ST_Equals
    extends ST_GeometryRelational {

  @Override
  protected OperatorSimpleRelation getRelationOperator() {
    return OperatorEquals.local();
  }

  @Override
  public String getDisplayString(String[] args) {
    return String.format("returns true if %s equals %s", args[0], args[1]);
  }
}
