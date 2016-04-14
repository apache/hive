/**
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

package org.apache.hadoop.hive.llap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class TypeDesc implements Writable {
  public static enum Type {
    BOOLEAN,
    TINYINT,
    SMALLINT,
    INT,
    BIGINT,
    FLOAT,
    DOUBLE,
    STRING,
    CHAR,
    VARCHAR,
    DATE,
    TIMESTAMP,
    BINARY,
    DECIMAL,
  }

  private TypeDesc.Type type;
  private int precision;
  private int scale;

  // For types with no type qualifiers
  public TypeDesc(TypeDesc.Type type) {
    this(type, 0, 0);
  }

  // For decimal types
  public TypeDesc(TypeDesc.Type type, int precision, int scale) {
    this.type = type;
    this.precision = precision;
    this.scale = scale;
  }

  // For char/varchar types
  public TypeDesc(TypeDesc.Type type, int precision) {
    this(type, precision, 0);
  }

  // Should be used for serialization only
  public TypeDesc() {
    this(TypeDesc.Type.INT, 0, 0);
  }

  public TypeDesc.Type getType() {
    return type;
  }

  public int getPrecision() {
    return precision;
  }

  public int getScale() {
    return scale;
  }

  @Override
  public String toString() {
    switch (type) {
      case DECIMAL:
        return type.name().toLowerCase() + "(" + precision + "," + scale + ")";
      case CHAR:
      case VARCHAR:
        return type.name().toLowerCase() + "(" + precision + ")";
      default:
        return type.name().toLowerCase();
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(type.name());
    out.writeInt(precision);
    out.writeInt(scale);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    type = TypeDesc.Type.valueOf(in.readUTF());
    precision = in.readInt();
    scale = in.readInt();
  }
}