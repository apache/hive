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

package org.apache.hadoop.hive.metastore.model;

import java.io.Serializable;
import java.util.Objects;

import org.datanucleus.identity.LongId;

public class MColumn {
  private MColumnDescriptor cd;
  private String name;
  private String type;
  private String comment;

  @SuppressWarnings("serial")
  public static class PK implements Serializable {
    public LongId cd;
    public String name;

    public PK() {}

    public PK(LongId id, String name) {
      this.cd = id;
      this.name = name;
    }

    public String toString() {
      return String.format("%s-%s", cd.toString(), name);
    }

    public int hashCode() {
      return Objects.hash(cd.getKey(), name);
    }

    public boolean equals(Object other) {
      if (other instanceof PK otherPK) {
        return Objects.equals(otherPK.name, name)
                && Objects.equals(otherPK.cd.getKey(), cd.getKey());
      }
      return false;
    }
  }

  public MColumn() {
  }

  public MColumn(String name, String type, String comment) {
    this.name = name;
    this.type = type;
    this.comment = comment;
  }

  public MColumnDescriptor getCd() {
    return cd;
  }

  public void setCd(MColumnDescriptor cd) {
    this.cd = cd;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }
}
