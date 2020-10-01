/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hadoop.hive.metastore.model;

public class MPosParam {
  private String name;
  private String type;
  private Integer length;
  private Integer scale;
  private boolean isOut;

  public MPosParam(String name, String type, boolean isOut, Integer length, Integer scale) {
    this.name = name;
    this.type = type;
    this.length = length;
    this.scale = scale;
    this.isOut = isOut;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public boolean isOut() {
    return isOut;
  }

  public Integer getLength() {
    return length;
  }

  public Integer getScale() {
    return scale;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("PosParam{");
    sb.append("name='").append(name).append('\'');
    sb.append(", type='").append(type).append('\'');
    sb.append(", isOut=").append(isOut);
    sb.append(", length=").append(length);
    sb.append(", scale=").append(scale);
    sb.append('}');
    return sb.toString();
  }
}
