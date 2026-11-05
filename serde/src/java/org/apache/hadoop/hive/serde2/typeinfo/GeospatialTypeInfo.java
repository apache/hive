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

package org.apache.hadoop.hive.serde2.typeinfo;

/**
 * Common base of the geospatial types. Values are WKB-encoded byte arrays, and the type carries
 * the coordinate reference system (CRS) of the values.
 *
 * The qualified name omits parameters that have their default values, e.g. {@code geometry} for
 * the default CRS and {@code geometry('EPSG:3857')} otherwise. The CRS is quoted because it may
 * contain characters, such as ':', that are separators in type strings.
 *
 * Types are equal only if their parameters are spelled the same, so that the spelling of a CRS does not depend on
 * which equal type was cached first. Values can still be converted between types whose parameters differ only in
 * case, see {@link #isCompatibleWith}.
 */
public abstract class GeospatialTypeInfo extends PrimitiveTypeInfo {
  private static final long serialVersionUID = 1L;

  public static final String DEFAULT_CRS = "OGC:CRS84";

  private String crs;

  // no-arg constructor to make kyro happy.
  protected GeospatialTypeInfo(String typeName) {
    this(typeName, DEFAULT_CRS);
  }

  protected GeospatialTypeInfo(String typeName, String crs) {
    super(typeName);
    this.crs = validateCrs(crs);
  }

  private static String validateCrs(String crs) {
    if (crs == null) {
      return DEFAULT_CRS;
    }
    if (crs.isBlank() || crs.indexOf('\'') >= 0) {
      throw new IllegalArgumentException("Invalid CRS: '" + crs + "'");
    }
    return DEFAULT_CRS.equalsIgnoreCase(crs) ? DEFAULT_CRS : crs;
  }

  public String getCrs() {
    return crs;
  }

  public void setCrs(String crs) {
    this.crs = validateCrs(crs);
  }

  /**
   * Whether all type parameters have their default values.
   */
  protected boolean hasDefaultParameters() {
    return DEFAULT_CRS.equals(crs);
  }

  /**
   * Appends the type parameters that follow the CRS, each preceded by a ','.
   */
  protected void appendAdditionalParameters(StringBuilder sb) {
  }

  @Override
  public String getQualifiedName() {
    if (hasDefaultParameters()) {
      return typeName;
    }
    StringBuilder sb = new StringBuilder(typeName).append("('").append(crs).append('\'');
    appendAdditionalParameters(sb);
    return sb.append(')').toString();
  }

  @Override
  public String getTypeName() {
    return getQualifiedName();
  }

  @Override
  public void setTypeName(String typeName) {
    // type name should already be set by subclass
  }

  @Override
  public String toString() {
    return getQualifiedName();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    return getQualifiedName().equals(((GeospatialTypeInfo) obj).getQualifiedName());
  }

  @Override
  public int hashCode() {
    return getQualifiedName().hashCode();
  }

  /**
   * Whether values of the other type can be used as values of this type: both are the same kind of geospatial
   * type and their parameters match case-insensitively, as Iceberg compares them.
   */
  public boolean isCompatibleWith(TypeInfo other) {
    return other != null && getClass() == other.getClass()
        && getQualifiedName().equalsIgnoreCase(((GeospatialTypeInfo) other).getQualifiedName());
  }
}
