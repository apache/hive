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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * A description of the column to create.
 */
@XmlRootElement
public class ColumnDesc extends GroupPermissionsDesc {
  public String name;
  public String type;
  public String comment;

  public ColumnDesc() {}

  /**
   * Create a new ColumnDesc
   */
  public ColumnDesc(String name, String type, String comment) {
    this.name = name;
    this.type = type;
    this.comment = comment;
  }

  public String toString() {
    return String.format("ColumnDesc(name=%s, type=%s, comment=%s)",
               name, type, comment);
  }

  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (! (o instanceof ColumnDesc))
      return false;
    ColumnDesc that = (ColumnDesc) o;
    return xequals(this.name,       that.name)
      && xequals(this.type,       that.type)
      && xequals(this.comment,    that.comment)
      && super.equals(that)
      ;
  }
}
