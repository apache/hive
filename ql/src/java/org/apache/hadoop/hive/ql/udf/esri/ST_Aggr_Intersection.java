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

import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "ST_Aggr_Intersection",
    value = "_FUNC_(ST_Geometry) - aggregate intersection of all geometries passed",
    extended = "Example:\n"
        + "  SELECT _FUNC_(geometry) FROM source; -- return intersection of all geometries in source")

public class ST_Aggr_Intersection extends UDAF {
  static final Logger LOG = LoggerFactory.getLogger(ST_Aggr_Intersection.class.getName());

  public static class AggrIntersectionBinaryEvaluator implements UDAFEvaluator {

    private OGCGeometry isectGeom = null;
    SpatialReference spatialRef = null;
    int firstWKID = -2;

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
        if (firstWKID != GeometryUtils.WKID_UNKNOWN) {
          spatialRef = SpatialReference.create(firstWKID);
        }
      } else if (firstWKID != GeometryUtils.getWKID(geomref)) {
        LogUtils.Log_SRIDMismatch(LOG, geomref, firstWKID);
        return false;
      }

      try {
        OGCGeometry rowGeom = GeometryUtils.geometryFromEsriShape(geomref);
        rowGeom.setSpatialReference(spatialRef);
        if (isectGeom == null)
          isectGeom = rowGeom;
        else
          isectGeom = isectGeom.intersection(rowGeom);
        return true;
      } catch (Exception e) {
        LogUtils.Log_InternalError(LOG, "ST_Aggr_Intersection: " + e);
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
     * Return a geometry that is the intersection of all geometries added up until this point
     */
    public BytesWritable terminatePartial() throws HiveException {
      if (isectGeom == null) {
        return null;
      } else {
        return GeometryUtils.geometryToEsriShapeBytesWritable(isectGeom);
      }
    }

    public BytesWritable terminate() throws HiveException {
      // for our purposes, terminate is the same as terminatePartial
      return terminatePartial();
    }

  }
}
