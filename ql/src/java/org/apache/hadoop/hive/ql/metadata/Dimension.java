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

package org.apache.hadoop.hive.ql.metadata;

/**
 * Hive consists of a fixed, well defined set of Dimensions. Each dimension has
 * a type and id. Dimensions link columns in different tables
 * 
 */
public class Dimension {

  protected Class<?> dimensionType;
  protected String dimensionId;

  public Dimension(Class<?> t, String id) {
    dimensionType = t;
    dimensionId = id;
  }

  public Class<?> getDimensionType() {
    return dimensionType;
  }

  public String getDimensionId() {
    return dimensionId;
  }

  @Override
  public boolean equals(Object o) {
    if (super.equals(o)) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (o instanceof Dimension) {
      Dimension d = (Dimension) o;
      return (dimensionId.equals(d.dimensionId) && (dimensionType == d.dimensionType));
    }
    return false;
  }

  @Override
  @SuppressWarnings("nls")
  public String toString() {
    return "Type=" + dimensionType.getName() + "," + "Id=" + dimensionId;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((dimensionId == null) ? 0 : dimensionId.hashCode());
    result = prime * result
        + ((dimensionType == null) ? 0 : dimensionType.hashCode());
    return result;
  }

  public int hashCode(Object o) {
    return dimensionType.hashCode() ^ dimensionId.hashCode();
  }
}
