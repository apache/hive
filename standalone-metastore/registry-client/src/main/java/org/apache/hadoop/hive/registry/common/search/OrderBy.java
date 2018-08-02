/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

/**
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderBy implements Serializable {
  private static final long serialVersionUID = 8220102056152801315L;

  private String fieldName;
  private boolean asc;

  private OrderBy() {
  }

  private OrderBy(String fieldName, boolean asc) {
    this.fieldName = fieldName;
    this.asc = asc;
  }

  public String getFieldName() {
    return fieldName;
  }

  public boolean isAsc() {
    return asc;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OrderBy orderBy = (OrderBy) o;

    if (asc != orderBy.asc) return false;
    return fieldName != null ? fieldName.equals(orderBy.fieldName) : orderBy.fieldName == null;
  }

  @Override
  public int hashCode() {
    int result = fieldName != null ? fieldName.hashCode() : 0;
    result = 31 * result + (asc ? 1 : 0);
    return result;
  }

  public static OrderBy asc(String fieldName) {
    return new OrderBy(fieldName, true);
  }

  public static OrderBy desc(String fieldName) {
    return new OrderBy(fieldName, false);
  }
}
