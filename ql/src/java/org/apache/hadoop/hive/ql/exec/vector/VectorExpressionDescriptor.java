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

package org.apache.hadoop.hive.ql.exec.vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hive.common.util.AnnotationUtils;

/**
 * Describes a vector expression and encapsulates the {@link Mode}, number of arguments,
 * argument types {@link ArgumentType} and expression types {@link InputExpressionType}.
 */
public class VectorExpressionDescriptor {

  private static final Logger LOG = LoggerFactory.getLogger(
            VectorExpressionDescriptor.class.getName());

  final static int MAX_NUM_ARGUMENTS = 3;

  //
  // Special handling is needed at times for DATE, TIMESTAMP, (STRING), CHAR, and VARCHAR so they can
  // be named specifically as argument types.
  //
  // LongColumnVector -->
  //    INT_FAMILY
  //    DATE
  //    INTERVAL_FAMILY
  //
  // DoubleColumnVector -->
  //    FLOAT_FAMILY
  //
  // DecimalColumnVector -->
  //    DECIMAL
  //
  // BytesColumnVector -->
  //    STRING
  //    CHAR
  //    VARCHAR
  //
  // TimestampColumnVector -->
  //    TIMESTAMP
  //
  // IntervalDayTimeColumnVector -->
  //    INTERVAL_DAY_TIME
  //
  public enum ArgumentType {
    NONE                    (0x000),
    INT_FAMILY              (0x001),
    FLOAT_FAMILY            (0x002),
    DECIMAL                 (0x004),
    STRING                  (0x008),
    CHAR                    (0x010),
    VARCHAR                 (0x020),
    STRING_FAMILY           (STRING.value | CHAR.value | VARCHAR.value),
    DATE                    (0x040),
    TIMESTAMP               (0x080),
    INTERVAL_YEAR_MONTH     (0x100),
    INTERVAL_DAY_TIME       (0x200),
    BINARY                  (0x400),
    STRUCT                  (0x800),
    DECIMAL_64              (0x1000),
    LIST                    (0x2000),
    MAP                     (0x4000),
    INT_DECIMAL_64_FAMILY   (INT_FAMILY.value | DECIMAL_64.value),
    DATETIME_FAMILY         (DATE.value | TIMESTAMP.value),
    INTERVAL_FAMILY         (INTERVAL_YEAR_MONTH.value | INTERVAL_DAY_TIME.value),
    INT_INTERVAL_YEAR_MONTH     (INT_FAMILY.value | INTERVAL_YEAR_MONTH.value),
    INT_DATE_INTERVAL_YEAR_MONTH  (INT_FAMILY.value | DATE.value | INTERVAL_YEAR_MONTH.value),
    STRING_DATETIME_FAMILY  (STRING_FAMILY.value | DATETIME_FAMILY.value),
    ALL_FAMILY              (0xFFFF);

    private final int value;

    ArgumentType(int val) {
      this.value = val;
    }

    public int getValue() {
      return value;
    }

    public static ArgumentType fromHiveTypeName(String hiveTypeName) {
      String lower = hiveTypeName.toLowerCase();
      if (lower.equals("tinyint") ||
          lower.equals("smallint") ||
          lower.equals("int") ||
          lower.equals("bigint") ||
          lower.equals("boolean") ||
          lower.equals("long")) {
        return INT_FAMILY;
      } else if (lower.equals("double") || lower.equals("float")) {
        return FLOAT_FAMILY;
      } else if (lower.equals("string")) {
        return STRING;
      } else if (VectorizationContext.charTypePattern.matcher(lower).matches()) {
        return CHAR;
      } else if (VectorizationContext.varcharTypePattern.matcher(lower).matches()) {
        return VARCHAR;
      } else if (lower.equals("binary")) {
        return BINARY;
      } else if (VectorizationContext.decimalTypePattern.matcher(lower).matches()) {
        return DECIMAL;
      } else if (lower.equals("timestamp")) {
        return TIMESTAMP;
      } else if (lower.equals("date")) {
        return DATE;
      } else if (lower.equals(serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME)) {
        return INTERVAL_YEAR_MONTH;
      } else if (lower.equals(serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME)) {
        return INTERVAL_DAY_TIME;
      } else if (VectorizationContext.structTypePattern.matcher(lower).matches()) {
        return STRUCT;
      } else if (VectorizationContext.listTypePattern.matcher(lower).matches()) {
        return LIST;
      } else if (VectorizationContext.mapTypePattern.matcher(lower).matches()) {
        return MAP;
      } else if (lower.equals("void")) {
        // The old code let void through...
        return INT_FAMILY;
      } else {
        return NONE;
      }
    }

    public static ArgumentType getType(String inType) {
      if (inType.equalsIgnoreCase("long")) {
        // A synonym in some places in the code...
        return INT_FAMILY;
      } else if (inType.equalsIgnoreCase("double")) {
        // A synonym in some places in the code...
        return FLOAT_FAMILY;
      } else if (VectorizationContext.decimalTypePattern.matcher(inType).matches()) {
        return DECIMAL;
      } else if (VectorizationContext.charTypePattern.matcher(inType).matches()) {
        return CHAR;
      } else if (VectorizationContext.varcharTypePattern.matcher(inType).matches()) {
        return VARCHAR;
      }
      return valueOf(inType.toUpperCase());
    }

    public boolean isSameTypeOrFamily(ArgumentType other) {
      return ((value & other.value) != 0);
    }

    public static String getVectorColumnSimpleName(ArgumentType argType) {
      if (argType == INT_FAMILY ||
          argType == DATE ||
          argType == INTERVAL_YEAR_MONTH
          ) {
        return "Long";
      } else if (argType == TIMESTAMP ||
                 argType == INTERVAL_DAY_TIME) {
        return "Timestamp";
      } else if (argType == FLOAT_FAMILY) {
        return "Double";
      } else if (argType == DECIMAL) {
        return "Decimal";
      } else if (argType == STRING ||
                 argType == CHAR ||
                 argType == VARCHAR ||
                 argType == BINARY) {
        return "String";
      } else {
        return "None";
      }
    }

    public static String getVectorColumnSimpleName(String hiveTypeName) {
      ArgumentType argType = fromHiveTypeName(hiveTypeName);
      return getVectorColumnSimpleName(argType);
    }
  }

  public enum InputExpressionType {
    NONE(0),
    COLUMN(1),
    SCALAR(2),
    DYNAMICVALUE(3);

    private final int value;

    InputExpressionType(int val) {
      this.value = val;
    }

    public int getValue() {
      return value;
    }
  }

  public enum Mode {
    PROJECTION(0),
    FILTER(1);

    private final int value;

    Mode(int val) {
      this.value = val;
    }

    public int getValue() {
      return value;
    }
  }

  /**
   * Builder builds a {@link Descriptor} object. Setter methods are provided to set the {@link Mode}, number
   * of arguments, argument types and expression types for each argument.
   */
  public static class Builder {
    private Mode mode = Mode.PROJECTION;
    ArgumentType [] argTypes = new ArgumentType[MAX_NUM_ARGUMENTS];
    InputExpressionType [] exprTypes = new InputExpressionType[MAX_NUM_ARGUMENTS];
    private int argCount = 0;

    public Builder() {
      for (int i = 0 ; i < MAX_NUM_ARGUMENTS; i++) {
        argTypes[i] = ArgumentType.NONE;
        exprTypes[i] = InputExpressionType.NONE;
      }
    }

    public Builder setMode(Mode m) {
      this.mode = m;
      return this;
    }

    public Builder setNumArguments(int argCount) {
      this.argCount = argCount;
      return this;
    }

    public Builder setArgumentTypes(ArgumentType ... types) {
      for (int i = 0; i < types.length; i++) {
        argTypes[i] = types[i];
      }
      return this;
    }

    public Builder setArgumentTypes(String ... types) {
      for (int i = 0; i < types.length; i++) {
        argTypes[i] = ArgumentType.getType(types[i]);
      }
      return this;
    }

    public Builder setArgumentType(int index, ArgumentType type) {
      argTypes[index] = type;
      return this;
    }

    public Builder setArgumentType(int index, String type) {
      argTypes[index] = ArgumentType.getType(type);
      return this;
    }

    public Builder setInputExpressionTypes(InputExpressionType ... types) {
      for (int i = 0; i < types.length; i++) {
        exprTypes[i] = types[i];
      }
      return this;
    }

    public Builder setInputExpressionType(int index, InputExpressionType type) {
      exprTypes[index] = type;
      return this;
    }

    public Descriptor build() {
      return new Descriptor(mode, argCount, argTypes, exprTypes);
    }
  }

  /**
   * Descriptor is immutable and is constructed by the {@link Builder} only. {@link #equals(Object)} is the only
   * publicly exposed member which can be used to compare two descriptors.
   */
  public static final class Descriptor {

    public boolean matches(Descriptor other) {
      if (!mode.equals(other.mode) || (argCount != other.argCount) ) {
        return false;
      }
      for (int i = 0; i < argCount; i++) {
        if (!argTypes[i].isSameTypeOrFamily(other.argTypes[i])) {
          return false;
        }
        if (!exprTypes[i].equals(other.exprTypes[i])) {
          return false;
        }
      }
      return true;
    }

    private final Mode mode;
    private final ArgumentType [] argTypes;
    private final InputExpressionType [] exprTypes;
    private final int argCount;

    private Descriptor(Mode mode, int argCount, ArgumentType[] argTypes, InputExpressionType[] exprTypes) {
      this.mode = mode;
      this.argTypes = argTypes.clone();
      this.exprTypes = exprTypes.clone();
      this.argCount = argCount;
    }

    @Override
    public String toString() {
      StringBuilder b = new StringBuilder("Argument Count = ");
      b.append(argCount);
      b.append(", mode = ");
      b.append(mode);
      b.append(", Argument Types = {");
      for (int i = 0; i < argCount; i++) {
        if (i != 0) {
          b.append(",");
        }
        b.append(argTypes[i]);
      }
      b.append("}");

      b.append(", Input Expression Types = {");
      for (int i = 0; i < argCount; i++) {
        if (i != 0) {
          b.append(",");
        }
        b.append(exprTypes[i]);
      }
      b.append("}");
      return b.toString();
    }
  }

  public Class<?> getVectorExpressionClass(Class<?> udf, Descriptor descriptor,
      boolean useCheckedExpressionIfAvailable) throws HiveException {
    VectorizedExpressions annotation =
        AnnotationUtils.getAnnotation(udf, VectorizedExpressions.class);
    if (annotation == null || annotation.value() == null) {
      return null;
    }
    Class<? extends VectorExpression>[] list = annotation.value();
    Class<? extends VectorExpression> matchedVe = null;
    for (Class<? extends VectorExpression> ve : list) {
      try {
        VectorExpression candidateVe = ve.newInstance();
        if (candidateVe.getDescriptor().matches(descriptor)) {
          if (!useCheckedExpressionIfAvailable) {
            // no need to look further for a checked variant of this expression
            return ve;
          } else if (candidateVe.supportsCheckedExecution()) {
            return ve;
          } else {
            // vector expression doesn't support checked execution
            // hold on to it in case there is no available checked variant
            matchedVe = ve;
          }
        }
      } catch (Exception ex) {
        throw new HiveException("Could not instantiate VectorExpression class " + ve.getSimpleName(), ex);
      }
    }
    if (matchedVe != null) {
      return matchedVe;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("getVectorExpressionClass udf " + udf.getSimpleName() + " descriptor: " + descriptor.toString());
      for (Class<? extends VectorExpression> ve : list) {
        try {
          LOG.debug("getVectorExpressionClass doesn't match " + ve.getSimpleName() + " " + ve.newInstance().getDescriptor().toString());
        } catch (Exception ex) {
          throw new HiveException(ex);
        }
      }
    }
    return null;
  }
}
