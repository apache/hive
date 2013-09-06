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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.data.schema;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hive.hcatalog.common.HCatException;

public class HCatFieldSchema implements Serializable {

  public enum Type {
    INT,
    TINYINT,
    SMALLINT,
    BIGINT,
    BOOLEAN,
    FLOAT,
    DOUBLE,
    STRING,
    ARRAY,
    MAP,
    STRUCT,
    BINARY,
  }

  public enum Category {
    PRIMITIVE,
    ARRAY,
    MAP,
    STRUCT;

    public static Category fromType(Type type) {
      if (Type.ARRAY == type) {
        return ARRAY;
      } else if (Type.STRUCT == type) {
        return STRUCT;
      } else if (Type.MAP == type) {
        return MAP;
      } else {
        return PRIMITIVE;
      }
    }
  }

  ;

  public boolean isComplex() {
    return (category == Category.PRIMITIVE) ? false : true;
  }

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  String fieldName = null;
  String comment = null;
  Type type = null;
  Category category = null;

  // Populated if column is struct, array or map types.
  // If struct type, contains schema of the struct.
  // If array type, contains schema of one of the elements.
  // If map type, contains schema of the value element.
  HCatSchema subSchema = null;

  // populated if column is Map type
  Type mapKeyType = null;

  private String typeString = null;

  @SuppressWarnings("unused")
  private HCatFieldSchema() {
    // preventing empty ctor from being callable
  }

  /**
   * Returns type of the field
   * @return type of the field
   */
  public Type getType() {
    return type;
  }

  /**
   * Returns category of the field
   * @return category of the field
   */
  public Category getCategory() {
    return category;
  }

  /**
   * Returns name of the field
   * @return name of the field
   */
  public String getName() {
    return fieldName;
  }

  public String getComment() {
    return comment;
  }

  /**
   * Constructor constructing a primitive datatype HCatFieldSchema
   * @param fieldName Name of the primitive field
   * @param type Type of the primitive field
   * @throws HCatException if call made on non-primitive types
   */
  public HCatFieldSchema(String fieldName, Type type, String comment) throws HCatException {
    assertTypeInCategory(type, Category.PRIMITIVE, fieldName);
    this.fieldName = fieldName;
    this.type = type;
    this.category = Category.PRIMITIVE;
    this.comment = comment;
  }

  /**
   * Constructor for constructing a ARRAY type or STRUCT type HCatFieldSchema, passing type and subschema
   * @param fieldName Name of the array or struct field
   * @param type Type of the field - either Type.ARRAY or Type.STRUCT
   * @param subSchema - subschema of the struct, or element schema of the elements in the array
   * @throws HCatException if call made on Primitive or Map types
   */
  public HCatFieldSchema(String fieldName, Type type, HCatSchema subSchema, String comment) throws HCatException {
    assertTypeNotInCategory(type, Category.PRIMITIVE);
    assertTypeNotInCategory(type, Category.MAP);
    this.fieldName = fieldName;
    this.type = type;
    this.category = Category.fromType(type);
    this.subSchema = subSchema;
    if (type == Type.ARRAY) {
      this.subSchema.get(0).setName(null);
    }
    this.comment = comment;
  }

  private void setName(String name) {
    this.fieldName = name;
  }

  /**
   * Constructor for constructing a MAP type HCatFieldSchema, passing type of key and value
   * @param fieldName Name of the array or struct field
   * @param type Type of the field - must be Type.MAP
   * @param mapKeyType - key type of the Map
   * @param mapValueSchema - subschema of the value of the Map
   * @throws HCatException if call made on non-Map types
   */
  public HCatFieldSchema(String fieldName, Type type, Type mapKeyType, HCatSchema mapValueSchema, String comment) throws HCatException {
    assertTypeInCategory(type, Category.MAP, fieldName);
    assertTypeInCategory(mapKeyType, Category.PRIMITIVE, fieldName);
    this.fieldName = fieldName;
    this.type = Type.MAP;
    this.category = Category.MAP;
    this.mapKeyType = mapKeyType;
    this.subSchema = mapValueSchema;
    this.subSchema.get(0).setName(null);
    this.comment = comment;
  }

  public HCatSchema getStructSubSchema() throws HCatException {
    assertTypeInCategory(this.type, Category.STRUCT, this.fieldName);
    return subSchema;
  }

  public HCatSchema getArrayElementSchema() throws HCatException {
    assertTypeInCategory(this.type, Category.ARRAY, this.fieldName);
    return subSchema;
  }

  public Type getMapKeyType() throws HCatException {
    assertTypeInCategory(this.type, Category.MAP, this.fieldName);
    return mapKeyType;
  }

  public HCatSchema getMapValueSchema() throws HCatException {
    assertTypeInCategory(this.type, Category.MAP, this.fieldName);
    return subSchema;
  }

  private static void assertTypeInCategory(Type type, Category category, String fieldName) throws HCatException {
    Category typeCategory = Category.fromType(type);
    if (typeCategory != category) {
      throw new HCatException("Type category mismatch. Expected " + category + " but type " + type + " in category " + typeCategory + " (field " + fieldName + ")");
    }
  }

  private static void assertTypeNotInCategory(Type type, Category category) throws HCatException {
    Category typeCategory = Category.fromType(type);
    if (typeCategory == category) {
      throw new HCatException("Type category mismatch. Expected type " + type + " not in category " + category + " but was so.");
    }
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
      .append("fieldName", fieldName)
      .append("comment", comment)
      .append("type", getTypeString())
      .append("category", category)
      .toString();
  }

  public String getTypeString() {
    if (typeString != null) {
      return typeString;
    }

    StringBuilder sb = new StringBuilder();
    if (Category.PRIMITIVE == category) {
      sb.append(type);
    } else if (Category.STRUCT == category) {
      sb.append("struct<");
      sb.append(subSchema.getSchemaAsTypeString());
      sb.append(">");
    } else if (Category.ARRAY == category) {
      sb.append("array<");
      sb.append(subSchema.getSchemaAsTypeString());
      sb.append(">");
    } else if (Category.MAP == category) {
      sb.append("map<");
      sb.append(mapKeyType);
      sb.append(",");
      sb.append(subSchema.getSchemaAsTypeString());
      sb.append(">");
    }
    return (typeString = sb.toString().toLowerCase());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof HCatFieldSchema)) {
      return false;
    }
    HCatFieldSchema other = (HCatFieldSchema) obj;
    if (category != other.category) {
      return false;
    }
    if (fieldName == null) {
      if (other.fieldName != null) {
        return false;
      }
    } else if (!fieldName.equals(other.fieldName)) {
      return false;
    }
    if (this.getTypeString() == null) {
      if (other.getTypeString() != null) {
        return false;
      }
    } else if (!this.getTypeString().equals(other.getTypeString())) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    //result could be cached if this object were to be made immutable... 
    int result = 17;
    result = 31 * result + (category == null ? 0 : category.hashCode());
    result = 31 * result + (fieldName == null ? 0 : fieldName.hashCode());
    result = 31 * result + (getTypeString() == null ? 0 : 
        getTypeString().hashCode());
    return result;
  }
}
