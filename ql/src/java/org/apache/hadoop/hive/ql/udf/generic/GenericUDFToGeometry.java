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

package org.apache.hadoop.hive.ql.udf.generic;

import java.io.Serializable;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.SettableUDF;
import org.apache.hadoop.hive.serde2.geospatial.WkbValidator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.BinaryConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.GeometryTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;

@Description(name = "geometry",
    value = "CAST(<value> AS GEOMETRY[('<crs>')]) - Converts WKB binary to a geometry value.",
    extended = "Only binary values in the well-known binary (WKB) format, geometry values or NULL can be cast "
        + "into geometry. Binary values must be valid ISO WKB; extended WKB (EWKB) is rejected. Strings are not "
        + "converted implicitly: use unhex() for hex-encoded WKB and ST_AsBinary(ST_GeomFromText(...)) for WKT. "
        + "The coordinate reference system defaults to OGC:CRS84. "
        + "A geometry value can only be cast to a geometry type with the same coordinate reference system.\n"
        + "Example:\n"
        + "  > SELECT CAST(ST_AsBinary(ST_Point(1, 2)) AS GEOMETRY('EPSG:4326'));")
public class GenericUDFToGeometry extends GenericUDF implements SettableUDF, Serializable {
  private static final long serialVersionUID = 1L;

  private transient BinaryConverter converter;
  private transient boolean validateWkb;

  // The geometry type info needs to be set prior to initialization,
  // and must be preserved when the plan is serialized to other processes.
  private GeometryTypeInfo typeInfo = TypeInfoFactory.geometryTypeInfo;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentException("GEOMETRY cast requires a value argument");
    }
    if (!(arguments[0] instanceof PrimitiveObjectInspector)) {
      throw new UDFArgumentException("The function GEOMETRY takes only primitive types");
    }
    PrimitiveObjectInspector argumentOI = (PrimitiveObjectInspector) arguments[0];
    switch (argumentOI.getPrimitiveCategory()) {
    case GEOMETRY:
      if (!typeInfo.isCompatibleWith(argumentOI.getTypeInfo())) {
        throw new UDFArgumentException("Cannot cast " + argumentOI.getTypeName() + " to "
            + typeInfo.getQualifiedName() + ": the coordinate reference systems differ. Casting does not "
            + "transform coordinates; to relabel the CRS use CAST(CAST(value AS BINARY) AS "
            + typeInfo.getQualifiedName() + ")");
      }
      break;
    case BINARY:
      validateWkb = true;
      break;
    case VOID:
      break;
    default:
      throw new UDFArgumentException("Only binary data in the WKB format can be cast into geometry, but got "
          + argumentOI.getTypeName() + ". Convert hex-encoded WKB with unhex() and WKT with "
          + "ST_AsBinary(ST_GeomFromText(...)) first");
    }

    SettableBinaryObjectInspector outputOI = (SettableBinaryObjectInspector)
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(typeInfo);
    converter = new BinaryConverter(argumentOI, outputOI);
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object o0 = arguments[0].get();
    if (o0 == null) {
      return null;
    }
    BytesWritable value = (BytesWritable) converter.convert(o0);
    if (validateWkb) {
      try {
        WkbValidator.validate(value.getBytes(), 0, value.getLength());
      } catch (IllegalArgumentException e) {
        throw new HiveException("Cannot cast binary to " + typeInfo.getQualifiedName() + ": " + e.getMessage(), e);
      }
    }
    return value;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 1);
    return "CAST( " + children[0] + " AS " + typeInfo.getQualifiedName() + ")";
  }

  @Override
  public void setTypeInfo(TypeInfo typeInfo) throws UDFArgumentException {
    this.typeInfo = (GeometryTypeInfo) typeInfo;
  }

  @Override
  public TypeInfo getTypeInfo() {
    return typeInfo;
  }
}
