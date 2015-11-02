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

package org.apache.orc;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This is the description of the types in an ORC file.
 */
public class TypeDescription {
  private static final int MAX_PRECISION = 38;
  private static final int MAX_SCALE = 38;
  private static final int DEFAULT_PRECISION = 38;
  private static final int DEFAULT_SCALE = 10;
  private static final int DEFAULT_LENGTH = 256;
  public enum Category {
    BOOLEAN("boolean", true),
    BYTE("tinyint", true),
    SHORT("smallint", true),
    INT("int", true),
    LONG("bigint", true),
    FLOAT("float", true),
    DOUBLE("double", true),
    STRING("string", true),
    DATE("date", true),
    TIMESTAMP("timestamp", true),
    BINARY("binary", true),
    DECIMAL("decimal", true),
    VARCHAR("varchar", true),
    CHAR("char", true),
    LIST("array", false),
    MAP("map", false),
    STRUCT("struct", false),
    UNION("union", false);

    Category(String name, boolean isPrimitive) {
      this.name = name;
      this.isPrimitive = isPrimitive;
    }

    final boolean isPrimitive;
    final String name;

    public boolean isPrimitive() {
      return isPrimitive;
    }

    public String getName() {
      return name;
    }
  }

  public static TypeDescription createBoolean() {
    return new TypeDescription(Category.BOOLEAN);
  }

  public static TypeDescription createByte() {
    return new TypeDescription(Category.BYTE);
  }

  public static TypeDescription createShort() {
    return new TypeDescription(Category.SHORT);
  }

  public static TypeDescription createInt() {
    return new TypeDescription(Category.INT);
  }

  public static TypeDescription createLong() {
    return new TypeDescription(Category.LONG);
  }

  public static TypeDescription createFloat() {
    return new TypeDescription(Category.FLOAT);
  }

  public static TypeDescription createDouble() {
    return new TypeDescription(Category.DOUBLE);
  }

  public static TypeDescription createString() {
    return new TypeDescription(Category.STRING);
  }

  public static TypeDescription createDate() {
    return new TypeDescription(Category.DATE);
  }

  public static TypeDescription createTimestamp() {
    return new TypeDescription(Category.TIMESTAMP);
  }

  public static TypeDescription createBinary() {
    return new TypeDescription(Category.BINARY);
  }

  public static TypeDescription createDecimal() {
    return new TypeDescription(Category.DECIMAL);
  }

  /**
   * For decimal types, set the precision.
   * @param precision the new precision
   * @return this
   */
  public TypeDescription withPrecision(int precision) {
    if (category != Category.DECIMAL) {
      throw new IllegalArgumentException("precision is only allowed on decimal"+
         " and not " + category.name);
    } else if (precision < 1 || precision > MAX_PRECISION || scale > precision){
      throw new IllegalArgumentException("precision " + precision +
          " is out of range 1 .. " + scale);
    }
    this.precision = precision;
    return this;
  }

  /**
   * For decimal types, set the scale.
   * @param scale the new scale
   * @return this
   */
  public TypeDescription withScale(int scale) {
    if (category != Category.DECIMAL) {
      throw new IllegalArgumentException("scale is only allowed on decimal"+
          " and not " + category.name);
    } else if (scale < 0 || scale > MAX_SCALE || scale > precision) {
      throw new IllegalArgumentException("scale is out of range at " + scale);
    }
    this.scale = scale;
    return this;
  }

  public static TypeDescription createVarchar() {
    return new TypeDescription(Category.VARCHAR);
  }

  public static TypeDescription createChar() {
    return new TypeDescription(Category.CHAR);
  }

  /**
   * Set the maximum length for char and varchar types.
   * @param maxLength the maximum value
   * @return this
   */
  public TypeDescription withMaxLength(int maxLength) {
    if (category != Category.VARCHAR && category != Category.CHAR) {
      throw new IllegalArgumentException("maxLength is only allowed on char" +
                   " and varchar and not " + category.name);
    }
    this.maxLength = maxLength;
    return this;
  }

  public static TypeDescription createList(TypeDescription childType) {
    TypeDescription result = new TypeDescription(Category.LIST);
    result.children.add(childType);
    childType.parent = result;
    return result;
  }

  public static TypeDescription createMap(TypeDescription keyType,
                                          TypeDescription valueType) {
    TypeDescription result = new TypeDescription(Category.MAP);
    result.children.add(keyType);
    result.children.add(valueType);
    keyType.parent = result;
    valueType.parent = result;
    return result;
  }

  public static TypeDescription createUnion() {
    return new TypeDescription(Category.UNION);
  }

  public static TypeDescription createStruct() {
    return new TypeDescription(Category.STRUCT);
  }

  /**
   * Add a child to a union type.
   * @param child a new child type to add
   * @return the union type.
   */
  public TypeDescription addUnionChild(TypeDescription child) {
    if (category != Category.UNION) {
      throw new IllegalArgumentException("Can only add types to union type" +
          " and not " + category);
    }
    children.add(child);
    child.parent = this;
    return this;
  }

  /**
   * Add a field to a struct type as it is built.
   * @param field the field name
   * @param fieldType the type of the field
   * @return the struct type
   */
  public TypeDescription addField(String field, TypeDescription fieldType) {
    if (category != Category.STRUCT) {
      throw new IllegalArgumentException("Can only add fields to struct type" +
          " and not " + category);
    }
    fieldNames.add(field);
    children.add(fieldType);
    fieldType.parent = this;
    return this;
  }

  /**
   * Get the id for this type.
   * The first call will cause all of the the ids in tree to be assigned, so
   * it should not be called before the type is completely built.
   * @return the sequential id
   */
  public int getId() {
    // if the id hasn't been assigned, assign all of the ids from the root
    if (id == -1) {
      TypeDescription root = this;
      while (root.parent != null) {
        root = root.parent;
      }
      root.assignIds(0);
    }
    return id;
  }

  /**
   * Get the maximum id assigned to this type or its children.
   * The first call will cause all of the the ids in tree to be assigned, so
   * it should not be called before the type is completely built.
   * @return the maximum id assigned under this type
   */
  public int getMaximumId() {
    // if the id hasn't been assigned, assign all of the ids from the root
    if (maxId == -1) {
      TypeDescription root = this;
      while (root.parent != null) {
        root = root.parent;
      }
      root.assignIds(0);
    }
    return maxId;
  }

  private ColumnVector createColumn() {
    switch (category) {
      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case TIMESTAMP:
      case DATE:
        return new LongColumnVector();
      case FLOAT:
      case DOUBLE:
        return new DoubleColumnVector();
      case DECIMAL:
        return new DecimalColumnVector(precision, scale);
      case STRING:
      case BINARY:
      case CHAR:
      case VARCHAR:
        return new BytesColumnVector();
      case STRUCT: {
        ColumnVector[] fieldVector = new ColumnVector[children.size()];
        for(int i=0; i < fieldVector.length; ++i) {
          fieldVector[i] = children.get(i).createColumn();
        }
        return new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
                fieldVector);
      }
      case UNION: {
        ColumnVector[] fieldVector = new ColumnVector[children.size()];
        for(int i=0; i < fieldVector.length; ++i) {
          fieldVector[i] = children.get(i).createColumn();
        }
        return new UnionColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
            fieldVector);
      }
      case LIST:
        return new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
            children.get(0).createColumn());
      case MAP:
        return new MapColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
            children.get(0).createColumn(), children.get(1).createColumn());
      default:
        throw new IllegalArgumentException("Unknown type " + category);
    }
  }

  public VectorizedRowBatch createRowBatch() {
    VectorizedRowBatch result;
    if (category == Category.STRUCT) {
      result = new VectorizedRowBatch(children.size(),
          VectorizedRowBatch.DEFAULT_SIZE);
      for(int i=0; i < result.cols.length; ++i) {
        result.cols[i] = children.get(i).createColumn();
      }
    } else {
      result = new VectorizedRowBatch(1, VectorizedRowBatch.DEFAULT_SIZE);
      result.cols[0] = createColumn();
    }
    result.reset();
    return result;
  }

  /**
   * Get the kind of this type.
   * @return get the category for this type.
   */
  public Category getCategory() {
    return category;
  }

  /**
   * Get the maximum length of the type. Only used for char and varchar types.
   * @return the maximum length of the string type
   */
  public int getMaxLength() {
    return maxLength;
  }

  /**
   * Get the precision of the decimal type.
   * @return the number of digits for the precision.
   */
  public int getPrecision() {
    return precision;
  }

  /**
   * Get the scale of the decimal type.
   * @return the number of digits for the scale.
   */
  public int getScale() {
    return scale;
  }

  /**
   * For struct types, get the list of field names.
   * @return the list of field names.
   */
  public List<String> getFieldNames() {
    return Collections.unmodifiableList(fieldNames);
  }

  /**
   * Get the subtypes of this type.
   * @return the list of children types
   */
  public List<TypeDescription> getChildren() {
    return children == null ? null : Collections.unmodifiableList(children);
  }

  /**
   * Assign ids to all of the nodes under this one.
   * @param startId the lowest id to assign
   * @return the next available id
   */
  private int assignIds(int startId) {
    id = startId++;
    if (children != null) {
      for (TypeDescription child : children) {
        startId = child.assignIds(startId);
      }
    }
    maxId = startId - 1;
    return startId;
  }

  private TypeDescription(Category category) {
    this.category = category;
    if (category.isPrimitive) {
      children = null;
    } else {
      children = new ArrayList<>();
    }
    if (category == Category.STRUCT) {
      fieldNames = new ArrayList<>();
    } else {
      fieldNames = null;
    }
  }

  private int id = -1;
  private int maxId = -1;
  private TypeDescription parent;
  private final Category category;
  private final List<TypeDescription> children;
  private final List<String> fieldNames;
  private int maxLength = DEFAULT_LENGTH;
  private int precision = DEFAULT_PRECISION;
  private int scale = DEFAULT_SCALE;

  public void printToBuffer(StringBuilder buffer) {
    buffer.append(category.name);
    switch (category) {
      case DECIMAL:
        buffer.append('(');
        buffer.append(precision);
        buffer.append(',');
        buffer.append(scale);
        buffer.append(')');
        break;
      case CHAR:
      case VARCHAR:
        buffer.append('(');
        buffer.append(maxLength);
        buffer.append(')');
        break;
      case LIST:
      case MAP:
      case UNION:
        buffer.append('<');
        for(int i=0; i < children.size(); ++i) {
          if (i != 0) {
            buffer.append(',');
          }
          children.get(i).printToBuffer(buffer);
        }
        buffer.append('>');
        break;
      case STRUCT:
        buffer.append('<');
        for(int i=0; i < children.size(); ++i) {
          if (i != 0) {
            buffer.append(',');
          }
          buffer.append(fieldNames.get(i));
          buffer.append(':');
          children.get(i).printToBuffer(buffer);
        }
        buffer.append('>');
        break;
      default:
        break;
    }
  }

  public String toString() {
    StringBuilder buffer = new StringBuilder();
    printToBuffer(buffer);
    return buffer.toString();
  }

  private void printJsonToBuffer(String prefix, StringBuilder buffer,
                                 int indent) {
    for(int i=0; i < indent; ++i) {
      buffer.append(' ');
    }
    buffer.append(prefix);
    buffer.append("{\"category\": \"");
    buffer.append(category.name);
    buffer.append("\", \"id\": ");
    buffer.append(getId());
    buffer.append(", \"max\": ");
    buffer.append(maxId);
    switch (category) {
      case DECIMAL:
        buffer.append(", \"precision\": ");
        buffer.append(precision);
        buffer.append(", \"scale\": ");
        buffer.append(scale);
        break;
      case CHAR:
      case VARCHAR:
        buffer.append(", \"length\": ");
        buffer.append(maxLength);
        break;
      case LIST:
      case MAP:
      case UNION:
        buffer.append(", \"children\": [");
        for(int i=0; i < children.size(); ++i) {
          buffer.append('\n');
          children.get(i).printJsonToBuffer("", buffer, indent + 2);
          if (i != children.size() - 1) {
            buffer.append(',');
          }
        }
        buffer.append("]");
        break;
      case STRUCT:
        buffer.append(", \"fields\": [");
        for(int i=0; i < children.size(); ++i) {
          buffer.append('\n');
          children.get(i).printJsonToBuffer("\"" + fieldNames.get(i) + "\": ",
              buffer, indent + 2);
          if (i != children.size() - 1) {
            buffer.append(',');
          }
        }
        buffer.append(']');
        break;
      default:
        break;
    }
    buffer.append('}');
  }

  public String toJson() {
    StringBuilder buffer = new StringBuilder();
    printJsonToBuffer("", buffer, 0);
    return buffer.toString();
  }
}
