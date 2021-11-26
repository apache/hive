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

import org.apache.hadoop.hive.serde.serdeConstants;

public class DecimalTypeInfo extends PrimitiveTypeInfo {
  private static final long serialVersionUID = 1L;

  private int precision;
  private int scale;

  // no-arg constructor to make kyro happy.
  public DecimalTypeInfo() {
    super(serdeConstants.DECIMAL_TYPE_NAME);
  }

  public DecimalTypeInfo(int precision, int scale) {
    super(serdeConstants.DECIMAL_TYPE_NAME);
    HiveDecimalUtils.validateParameter(precision, scale);
    this.precision = precision;
    this.scale = scale;
  }

  @Override
  public String getTypeName() {
    return getQualifiedName();
  }

  @Override
  public void setTypeName(String typeName) {
    // No need to set type name, it should always be decimal
    return;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    DecimalTypeInfo dti = (DecimalTypeInfo)other;

    return this.precision() == dti.precision() && this.scale() == dti.scale();

  }

  /**
   * Generate the hashCode for this TypeInfo.
   */
  @Override
  public int hashCode() {
    return 31 * (17 + precision) + scale;
  }

  @Override
  public String toString() {
    return getQualifiedName();
  }

  @Override
  public String getQualifiedName() {
    return getQualifiedName(precision, scale);
  }

  public static String getQualifiedName(int precision, int scale) {
    StringBuilder sb = new StringBuilder(serdeConstants.DECIMAL_TYPE_NAME);
    sb.append("(");
    sb.append(precision);
    sb.append(",");
    sb.append(scale);
    sb.append(")");
    return sb.toString();
  }

  public int precision() {
    return precision;
  }

  public int scale() {
    return scale;
  }

  @Override
  public boolean accept(TypeInfo other) {
    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    DecimalTypeInfo dti = (DecimalTypeInfo)other;
    // Make sure "this" has enough integer room to accommodate other's integer digits.
    return this.precision() - this.scale() >= dti.precision() - dti.scale();
  }

  public int getPrecision() {
    return precision;
  }

  public void setPrecision(int precision) {
    this.precision = precision;
  }

  public int getScale() {
    return scale;
  }

  public void setScale(int scale) {
    this.scale = scale;
  }

}
