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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.data.schema;

import java.io.Serializable;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.common.HCatUtil;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class HCatFieldSchema implements Serializable {
/*the implementation of HCatFieldSchema is a bit messy since with the addition of parametrized 
types (e.g. char(7)) we need to represent something richer than an enum but for backwards 
compatibility (and effort required to do full refactoring) this class has both 'type' and 'typeInfo';
similarly for mapKeyType/mapKeyTypeInfo */
  
  public enum Type {
    /*this captures mapping of Hive type names to HCat type names; in the long run
    * we should just use Hive types directly but that is a larger refactoring effort
    * For HCat->Pig mapping see PigHCatUtil.getPigType(Type)
    * For Pig->HCat mapping see HCatBaseStorer#validateSchema(...)*/
    BOOLEAN(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN),
    TINYINT(PrimitiveObjectInspector.PrimitiveCategory.BYTE),
    SMALLINT(PrimitiveObjectInspector.PrimitiveCategory.SHORT),
    INT(PrimitiveObjectInspector.PrimitiveCategory.INT),
    BIGINT(PrimitiveObjectInspector.PrimitiveCategory.LONG),
    FLOAT(PrimitiveObjectInspector.PrimitiveCategory.FLOAT),
    DOUBLE(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE),
    DECIMAL(PrimitiveObjectInspector.PrimitiveCategory.DECIMAL),
    STRING(PrimitiveObjectInspector.PrimitiveCategory.STRING),
    CHAR(PrimitiveObjectInspector.PrimitiveCategory.CHAR),
    VARCHAR(PrimitiveObjectInspector.PrimitiveCategory.VARCHAR),
    BINARY(PrimitiveObjectInspector.PrimitiveCategory.BINARY),
    DATE(PrimitiveObjectInspector.PrimitiveCategory.DATE), 
    TIMESTAMP(PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP), 

    ARRAY(ObjectInspector.Category.LIST),
    MAP(ObjectInspector.Category.MAP),
    STRUCT(ObjectInspector.Category.STRUCT);

    
    private final ObjectInspector.Category category;
    private final PrimitiveObjectInspector.PrimitiveCategory primitiveCategory;
    private Type(ObjectInspector.Category cat) {
      category = cat;
      primitiveCategory = null;
      assert category != ObjectInspector.Category.PRIMITIVE : 
              "This c'tor should be used for complex category types";
    }
    private Type(PrimitiveObjectInspector.PrimitiveCategory primCat) {
      category = ObjectInspector.Category.PRIMITIVE;
      primitiveCategory = primCat;
    }
    public ObjectInspector.Category getCategory() {
      return category;
    }
    /**
     * May return {@code null}
     */
    public PrimitiveObjectInspector.PrimitiveCategory getPrimitiveCategory() {
      return primitiveCategory;
    }
    public static Type getPrimitiveHType(PrimitiveTypeInfo basePrimitiveTypeInfo) {
      for(Type t : values()) {
        if(t.getPrimitiveCategory() == basePrimitiveTypeInfo.getPrimitiveCategory()) {
          return t;
        }
      }
      throw new TypeNotPresentException(basePrimitiveTypeInfo.getTypeName(), null);
    }
    //aid in testing
    public static int numPrimitiveTypes() {
      int numPrimitives = 0;
      for(Type t : values()) {
        if(t.category == ObjectInspector.Category.PRIMITIVE) {
          numPrimitives++;
        }
      }
      return numPrimitives;
    }
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

  public boolean isComplex() {
    return category != Category.PRIMITIVE;
  }

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  String fieldName = null;
  String comment = null;
  /**
   * @deprecated as of 0.13, slated for removal with 0.15
   * use {@link #typeInfo} instead
   */
  Type type = null;
  Category category = null;

  // Populated if column is struct, array or map types.
  // If struct type, contains schema of the struct.
  // If array type, contains schema of one of the elements.
  // If map type, contains schema of the value element.
  HCatSchema subSchema = null;

  // populated if column is Map type
  @Deprecated // @deprecated as of 0.13, slated for removal with 0.15
  Type mapKeyType = null;

  private String typeString = null;
  /**
   * This is needed for parametrized types such as decimal(8,9), char(7), varchar(6)
   */
  private PrimitiveTypeInfo typeInfo;
  /**
   * represents key type for a Map; currently Hive only supports primitive keys
   */
  private PrimitiveTypeInfo mapKeyTypeInfo;

  @SuppressWarnings("unused")
  private HCatFieldSchema() {
    // preventing empty ctor from being callable
  }

  /**
   * Returns type of the field
   * @return type of the field
   * @deprecated as of 0.13, slated for removal with 0.15
   * use {@link #getTypeInfo()} instead
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
   * May return {@code null}
   */
  public PrimitiveTypeInfo getTypeInfo() {
    return typeInfo;
  }
  /**
   * Constructor constructing a primitive datatype HCatFieldSchema
   * @param fieldName Name of the primitive field
   * @param type Type of the primitive field
   * @throws HCatException if call made on non-primitive types
   * @deprecated as of 0.13, slated for removal with 0.15
   * use {@link #HCatFieldSchema(String, org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo, String)}
   */
  public HCatFieldSchema(String fieldName, Type type, String comment) throws HCatException {
    assertTypeInCategory(type, Category.PRIMITIVE, fieldName);
    this.fieldName = fieldName;
    this.type = type;
    this.category = Category.PRIMITIVE;
    this.comment = comment;
  }
  public HCatFieldSchema(String fieldName, PrimitiveTypeInfo typeInfo, String comment)
          throws HCatException {
    this.fieldName = fieldName;
    this.category = Category.PRIMITIVE;
    this.typeInfo = typeInfo;
    if (typeInfo == null) {
      throw new IllegalArgumentException("typeInfo cannot be null; fieldName=" + fieldName);
    }
    type = Type.getPrimitiveHType(typeInfo);
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
   * @deprecated as of 0.13, slated for removal with 0.15
   * use {@link #createMapTypeFieldSchema(String, org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo, HCatSchema, String)}
   */
  public HCatFieldSchema(String fieldName, Type type, Type mapKeyType, HCatSchema mapValueSchema, String comment) throws HCatException {
    assertTypeInCategory(type, Category.MAP, fieldName);
    //Hive only supports primitive map keys: 
    //https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-ComplexTypes
    assertTypeInCategory(mapKeyType, Category.PRIMITIVE, fieldName);
    this.fieldName = fieldName;
    this.type = Type.MAP;
    this.category = Category.MAP;
    this.mapKeyType = mapKeyType;
    this.subSchema = mapValueSchema;
    this.subSchema.get(0).setName(null);
    this.comment = comment;
  }
  public static HCatFieldSchema createMapTypeFieldSchema(String fieldName, PrimitiveTypeInfo mapKeyType, 
                                                         HCatSchema mapValueSchema, 
                                                         String comment) throws HCatException {
    HCatFieldSchema mapSchema = new HCatFieldSchema(fieldName, Type.MAP,  
            Type.getPrimitiveHType(mapKeyType), 
            mapValueSchema, comment);
    mapSchema.mapKeyTypeInfo = mapKeyType;
    return mapSchema;
  }
  

  public HCatSchema getStructSubSchema() throws HCatException {
    assertTypeInCategory(this.type, Category.STRUCT, this.fieldName);
    return subSchema;
  }

  public HCatSchema getArrayElementSchema() throws HCatException {
    assertTypeInCategory(this.type, Category.ARRAY, this.fieldName);
    return subSchema;
  }
  /**
   * @deprecated as of 0.13, slated for removal with 0.15
   * use {@link #getMapKeyTypeInfo()} instead
   */
  public Type getMapKeyType() throws HCatException {
    assertTypeInCategory(this.type, Category.MAP, this.fieldName);
    return mapKeyType;
  }
  public PrimitiveTypeInfo getMapKeyTypeInfo() throws HCatException {
    assertTypeInCategory(this.type, Category.MAP, this.fieldName);
    return mapKeyTypeInfo;
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
    if (!isComplex()) {
      sb.append(typeInfo == null ? type : typeInfo.getTypeName());
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
      sb.append(mapKeyTypeInfo == null ? mapKeyType : mapKeyTypeInfo.getTypeName());
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
