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
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.union.UnaryUnionOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

@Description(name = "ST_Aggr_Union",
    value = "_FUNC_(ST_Geometry) - aggregate union of all geometries passed",
    extended = "Example:\n" + "  SELECT _FUNC_(geometry) FROM source; -- return union of all geometries in source")

public class ST_Aggr_Union extends UDAF {
  static final Logger LOG = LoggerFactory.getLogger(ST_Aggr_Union.class.getName());

  public static class AggrUnionBinaryEvaluator implements UDAFEvaluator {

    int firstWKID = -2;
    List<Geometry> geomList = new ArrayList<>();

    /*
     * Initialize evaluator
     */
    @Override
    public void init() {  // no-op
    }

    /*
     * Iterate is called once per row in a table
     */
    public boolean iterate(BytesWritable geomref) throws HiveException {

      if (geomref == null) {
        LogUtils.Log_ArgumentsNull(LOG);
        return false;
      }

      if (firstWKID == -2) {
        firstWKID = GeometryUtils.getWKID(geomref);
      } else if (firstWKID != GeometryUtils.getWKID(geomref)) {
        LogUtils.Log_SRIDMismatch(LOG, geomref, firstWKID);
        return false;
      }

      try {
        Geometry geom = GeometryUtils.geometryFromEsriShape(geomref);
        if (geom != null) {
          geomList.add(geom);
        }
        return true;
      } catch (Exception e) {
        LogUtils.Log_InternalError(LOG, "ST_Aggr_Union: " + e);
        return false;
      }
    }

    /*
     * Merge the current state of this evaluator with the result of another evaluator's terminatePartial()
     */
    public boolean merge(BytesWritable other) throws HiveException {
      // for our purposes, merge is the same as iterate
      return iterate(other);
    }

    /*
     * Return a geometry that is the union of all geometries added up until this point
     */
    public BytesWritable terminatePartial() throws HiveException {
      if (geomList.isEmpty()) {
        return null;
      }
      try {
        Geometry result = UnaryUnionOp.union(geomList);
        if (result == null) {
          return null;
        }
        int wkid = (firstWKID == -2) ? GeometryUtils.WKID_UNKNOWN : firstWKID;
        return GeometryUtils.geometryToEsriShapeBytesWritable(result, wkid);
      } catch (Exception e) {
        LogUtils.Log_InternalError(LOG, "ST_Aggr_Union: " + e);
      } finally {
        geomList.clear();
        firstWKID = -2;
      }
      return null;
    }

    /*
     * Return a geometry that is the union of all geometries added up until this point
     */
    public BytesWritable terminate() throws HiveException {
      // for our purposes, terminate is the same as terminatePartial
      return terminatePartial();
    }

  }
}
