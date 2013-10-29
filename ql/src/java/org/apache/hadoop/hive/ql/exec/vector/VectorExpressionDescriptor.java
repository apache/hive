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

package org.apache.hadoop.hive.ql.exec.vector;

import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Describes a vector expression and encapsulates the {@link Mode}, number of arguments,
 * argument types {@link ArgumentType} and expression types {@link InputExpressionType}.
 */
public class VectorExpressionDescriptor {

  final static int MAX_NUM_ARGUMENTS = 3;

  public enum ArgumentType {
    NONE(0),
    LONG(1),
    DOUBLE(2),
    STRING(3),
    ANY(7);

    private final int value;

    ArgumentType(int val) {
      this.value = val;
    }

    public int getValue() {
      return value;
    }

    public static ArgumentType getType(String inType) {
      return valueOf(VectorizationContext.getNormalizedTypeName(inType).toUpperCase());
    }
  }

  public enum InputExpressionType {
    NONE(0),
    COLUMN(1),
    SCALAR(2);

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

    @Override
    public boolean equals(Object o) {
      Descriptor other = (Descriptor) o;
      if (!mode.equals(other.mode) || (argCount != other.argCount) ) {
        return false;
      }
      for (int i = 0; i < argCount; i++) {
        if (!argTypes[i].equals(other.argTypes[i]) && (!argTypes[i].equals(ArgumentType.ANY) &&
            !other.argTypes[i].equals(ArgumentType.ANY))) {
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
  }

  public Class<?> getVectorExpressionClass(Class<?> udf, Descriptor descriptor) throws HiveException {
    VectorizedExpressions annotation = udf.getAnnotation(VectorizedExpressions.class);
    if (annotation == null || annotation.value() == null) {
      return null;
    }
    Class<? extends VectorExpression>[] list = annotation.value();
    for (Class<? extends VectorExpression> ve : list) {
      try {
        if (ve.newInstance().getDescriptor().equals(descriptor)) {
          return ve;
        }
      } catch (Exception ex) {
        throw new HiveException(ex);
      }
    }
    return null;
  }
}
