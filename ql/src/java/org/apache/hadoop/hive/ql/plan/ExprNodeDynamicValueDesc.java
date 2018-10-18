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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;


/**
 * This expression represents a value that will be available at runtime.
 *
 */
public class ExprNodeDynamicValueDesc extends ExprNodeDesc implements Serializable {

  private static final long serialVersionUID = 1L;

  protected DynamicValue dynamicValue;

  public ExprNodeDynamicValueDesc() {
  }

  public ExprNodeDynamicValueDesc(DynamicValue value) {
    super(value.getTypeInfo());
    this.dynamicValue = value;
  }

  @Override
  public ExprNodeDesc clone() {
    return new ExprNodeDynamicValueDesc(dynamicValue);
  }

  @Override
  public boolean isSame(Object o) {
    if (o instanceof ExprNodeDynamicValueDesc) {
      Object otherValue = ((ExprNodeDynamicValueDesc) o).getDynamicValue();
      if (dynamicValue == null) {
        return otherValue == null;
      }
      return dynamicValue.equals(otherValue);
    }
    return false;
  }

  public DynamicValue getDynamicValue() {
    return dynamicValue;
  }

  public void setValue(DynamicValue value) {
    this.dynamicValue = value;
  }

  @Override
  public String getExprString() {
    return dynamicValue != null ? dynamicValue.toString() : "null dynamic literal";
  }

  @Override
  public String toString() {
    return dynamicValue != null ? dynamicValue.toString() : "null dynamic literal";
  }
}
