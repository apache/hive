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

package org.apache.hadoop.hive.ql.hooks;

import java.io.Serializable;

import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * This class encapsulates the information on the partition and tables that are
 * read by the query.
 */
public class ReadEntity extends Entity implements Serializable {

  /**
   * For serialization only.
   */
  public ReadEntity() {
    super();
  }

  /**
   * Constructor.
   *
   * @param t
   *          The Table that the query reads from.
   */
  public ReadEntity(Table t) {
    super(t);
  }

  /**
   * Constructor given a partiton.
   *
   * @param p
   *          The partition that the query reads from.
   */
  public ReadEntity(Partition p) {
    super(p);
  }

  /**
   * Equals function.
   */
  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }

    if (o instanceof ReadEntity) {
      ReadEntity ore = (ReadEntity) o;
      return (toString().equalsIgnoreCase(ore.toString()));
    } else {
      return false;
    }
  }
}
