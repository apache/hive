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

import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * This expression represents a list that will be available at runtime.
 */
@SuppressWarnings("serial")
public class ExprNodeDynamicListDesc extends ExprNodeDesc {

  Operator<? extends OperatorDesc> source;
  int keyIndex;

  public ExprNodeDynamicListDesc() {
  }

  public ExprNodeDynamicListDesc(TypeInfo typeInfo, Operator<? extends OperatorDesc> source,
      int keyIndex) {
    super(typeInfo);
    this.source = source;
    this.keyIndex = keyIndex;
  }

  public void setSource(Operator<? extends OperatorDesc> source) {
    this.source = source;
  }

  public Operator<? extends OperatorDesc> getSource() {
    return source;
  }

  public void setKeyIndex(int keyIndex) {
    this.keyIndex = keyIndex;
  }

  public int getKeyIndex() {
    return this.keyIndex;
  }

  @Override
  public ExprNodeDesc clone() {
    return new ExprNodeDynamicListDesc(typeInfo, source, keyIndex);
  }

  @Override
  public boolean isSame(Object o) {
    if (o instanceof ExprNodeDynamicListDesc) {
      return source.equals(((ExprNodeDynamicListDesc)o).getSource());
    }
    return false;
  }

  @Override
  public String getExprString() {
    return source.toString();
  }

  @Override
  public String toString() {
    return source.toString();
  }

}
