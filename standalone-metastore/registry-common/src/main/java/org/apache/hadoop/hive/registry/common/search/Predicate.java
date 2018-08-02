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

package org.apache.hadoop.hive.registry.common.search;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Predicate implements Serializable {
  private static final long serialVersionUID = 3928533466168563000L;

  public enum Operation {EQ, LT, GT, LTE, GTE, CONTAINS}

  private String field;
  private Object value;
  private Operation operation;

  private Predicate() {
  }

  public Predicate(String field, Object value, Operation operation) {
    this.field = field;
    this.value = value;
    this.operation = operation;
  }

  public String getField() {
    return field;
  }

  public Object getValue() {
    return value;
  }

  public Operation getOperation() {
    return operation;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Predicate predicate = (Predicate) o;

    if (field != null ? !field.equals(predicate.field) : predicate.field != null) return false;
    if (value != null ? !value.equals(predicate.value) : predicate.value != null) return false;
    return operation == predicate.operation;
  }

  @Override
  public int hashCode() {
    int result = field != null ? field.hashCode() : 0;
    result = 31 * result + (value != null ? value.hashCode() : 0);
    result = 31 * result + (operation != null ? operation.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "Predicate{" +
            "field='" + field + '\'' +
            ", value=" + value +
            ", operation=" + operation +
            '}';
  }
}
