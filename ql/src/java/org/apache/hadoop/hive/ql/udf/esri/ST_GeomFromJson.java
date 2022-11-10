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

import com.esri.core.geometry.ogc.OGCGeometry;
import com.fasterxml.jackson.core.JsonFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

@Description(name = "ST_GeomFromJSON",
    value = "_FUNC_(json) - construct an ST_Geometry from Esri JSON",
    extended = "Example:\n" + "  SELECT _FUNC_('{\"x\":0.0,\"y\":0.0}') FROM src LIMIT 1;  -- constructs ST_Point\n")

public class ST_GeomFromJson extends GenericUDF {

  static final JsonFactory jsonFactory = new JsonFactory();
  ObjectInspector jsonOI;

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    DeferredObject jsonDeferredObject = arguments[0];

    String json = null;

    if (jsonOI.getCategory() == Category.STRUCT) {
      //StructObjectInspector structOI = (StructObjectInspector)jsonOI;

      // TODO support structs
    } else {
      PrimitiveObjectInspector primOI = (PrimitiveObjectInspector) jsonOI;
      json = (String) primOI.getPrimitiveJavaObject(jsonDeferredObject.get());
    }

    try {
      OGCGeometry ogcGeom = OGCGeometry.fromJson(json);
      return GeometryUtils.geometryToEsriShapeBytesWritable(ogcGeom);
    } catch (Exception e) {

    }

    return null;
  }

  @Override
  public String getDisplayString(String[] args) {
    StringBuilder sb = new StringBuilder();
    sb.append(this.getClass().getName());
    String delim = "(";
    for (String arg : args) {
      sb.append(delim).append(arg);
      delim = ", ";
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    if (arguments.length != 1) {
      throw new UDFArgumentLengthException("ST_GeomFromJson takes only one argument");
    }

    ObjectInspector argJsonOI = arguments[0];

    if (argJsonOI.getCategory() == Category.PRIMITIVE) {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) argJsonOI;

      if (poi.getPrimitiveCategory() != PrimitiveCategory.STRING) {
        throw new UDFArgumentTypeException(0,
            "ST_GeomFromJson argument category must be either a string primitive or struct");
      }
    } else if (argJsonOI.getCategory() != Category.STRUCT) {

    } else {
      throw new UDFArgumentTypeException(0,
          "ST_GeomFromJson argument category must be either a string primitive or struct");
    }

    jsonOI = argJsonOI;

    return GeometryUtils.geometryTransportObjectInspector;
  }

}
