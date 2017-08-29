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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * Base class for expressions.
 */
public abstract class VectorExpression implements Serializable {
  public enum Type {
    STRING, CHAR, VARCHAR, TIMESTAMP, DATE, LONG, DOUBLE, DECIMAL,
    INTERVAL_YEAR_MONTH, INTERVAL_DAY_TIME, BINARY, OTHER;
    private static Map<String, Type> types = ImmutableMap.<String, Type>builder()
        .put("string", STRING)
        .put("char", CHAR)
        .put("varchar", VARCHAR)
        .put("timestamp", TIMESTAMP)
        .put("date", DATE)
        .put("long", LONG)
        .put("double", DOUBLE)
        .put("decimal", DECIMAL)
        .put("interval_year_month", INTERVAL_YEAR_MONTH)
        .put("interval_day_time", INTERVAL_DAY_TIME)
        .put("binary", BINARY)
        .build();

    public static Type getValue(String name) {
      String nameLower = name.toLowerCase();
      if (types.containsKey(nameLower)) {
        return types.get(nameLower);
      }
      return OTHER;
    }
  }

  private static final long serialVersionUID = 1L;
  /**
   * Child expressions are evaluated post order.
   */
  protected VectorExpression [] childExpressions = null;

  /**
   * More detailed input types, such as date and timestamp.
   */
  protected Type [] inputTypes;

  /**
   * Output type of the expression.
   */
  protected String outputType;

  /**
   * This is the primary method to implement expression logic.
   * @param batch
   */
  public abstract void evaluate(VectorizedRowBatch batch);

  public void init(Configuration conf) {
    if (childExpressions != null) {
      for (VectorExpression child : childExpressions) {
        child.init(conf);
      }
    }
  }

  /**
   * Returns the index of the output column in the array
   * of column vectors. If not applicable, -1 is returned.
   * @return Index of the output column
   */
  public abstract int getOutputColumn();

  /**
   * Returns type of the output column.
   */
  public String getOutputType() {
    return outputType;
  }

  /**
   * Set type of the output column.
   */
  public void setOutputType(String type) {
    this.outputType = type;
  }

  /**
   * Initialize the child expressions.
   */
  public void setChildExpressions(VectorExpression [] ve) {

    childExpressions = ve;
  }

  public VectorExpression[] getChildExpressions() {
    return childExpressions;
  }

  public abstract VectorExpressionDescriptor.Descriptor getDescriptor();

  /**
   * Evaluate the child expressions on the given input batch.
   * @param vrg {@link VectorizedRowBatch}
   */
  final protected void evaluateChildren(VectorizedRowBatch vrg) {
    if (childExpressions != null) {
      for (VectorExpression ve : childExpressions) {
        ve.evaluate(vrg);
      }
    }
  }

  /**
   * Set more detailed types to distinguish certain types that is represented in same
   * {@link org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.ArgumentType}s. For example, date and
   * timestamp will be in {@link org.apache.hadoop.hive.ql.exec.vector.LongColumnVector} but they need to be
   * distinguished.
   * @param inputTypes
   */
  public void setInputTypes(Type ... inputTypes) {
    this.inputTypes = inputTypes;
  }

  public Type [] getInputTypes() {
    return inputTypes;
  }

  public String vectorExpressionParameters() {
    return null;
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    if (this instanceof IdentityExpression) {
      b.append(vectorExpressionParameters());
    } else {
      b.append(this.getClass().getSimpleName());
      String vectorExpressionParameters = vectorExpressionParameters();
      if (vectorExpressionParameters != null) {
        b.append("(");
        b.append(vectorExpressionParameters);
        b.append(")");
      }
      if (childExpressions != null) {
        b.append("(children: ");
        for (int i = 0; i < childExpressions.length; i++) {
          b.append(childExpressions[i].toString());
          if (i < childExpressions.length-1) {
            b.append(", ");
          }
        }
        b.append(")");
      }
      b.append(" -> ");
      int outputColumn = getOutputColumn();
      if (outputColumn != -1) {
        b.append(outputColumn);
        b.append(":");
      }
      b.append(getOutputType());
    }

    return b.toString();
  }

  public static String displayUtf8Bytes(byte[] bytes) {
    if (bytes == null) {
      return "NULL";
    } else {
      return new String(bytes, StandardCharsets.UTF_8);
    }
  }

  public static String displayArrayOfUtf8ByteArrays(byte[][] arrayOfByteArrays) {
    StringBuilder sb = new StringBuilder();
    boolean isFirst = true;
    for (byte[] bytes : arrayOfByteArrays) {
      if (isFirst) {
        isFirst = false;
      } else {
        sb.append(", ");
      }
      sb.append(displayUtf8Bytes(bytes));
    }
    return sb.toString();
  }
}