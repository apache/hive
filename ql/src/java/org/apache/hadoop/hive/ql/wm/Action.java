/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.wm;

import java.util.Objects;

/**
 * Action that gets invoked for trigger violations.
 */
public class Action {

  public enum Type {
    KILL_QUERY("KILL"),
    MOVE_TO_POOL("MOVE TO");

    String displayName;

    Type(final String displayName) {
      this.displayName = displayName;
    }

    public String getDisplayName() {
      return displayName;
    }

    @Override
    public String toString() {
      return displayName;
    }
  }

  private final Type type;
  private final String poolName;

  public static Action fromMetastoreExpression(String metastoreActionExpression) {
    if (metastoreActionExpression.equalsIgnoreCase(Type.KILL_QUERY.getDisplayName())) {
      return new Action(Type.KILL_QUERY);
    } else {
      final String poolName = metastoreActionExpression.substring(Type.MOVE_TO_POOL.getDisplayName().length()).trim();
      if (poolName.isEmpty()) {
        throw new IllegalArgumentException("Invalid move action expression (" + metastoreActionExpression + "). Pool " +
          "name is empty");
      } else {
        return new Action(Type.MOVE_TO_POOL, poolName);
      }
    }
  }

  public Action(Type type) {
    this(type, null);
  }

  public Action(Type type, String poolName) {
    this.type = type;
    if (type == Type.MOVE_TO_POOL && (poolName == null || poolName.trim().isEmpty())) {
      throw new IllegalArgumentException("Pool name cannot be null or empty for action type " + type);
    }
    this.poolName = poolName;
  }

  public Type getType() {
    return type;
  }

  public String getPoolName() {
    return poolName;
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof Action)) {
      return false;
    }

    if (other == this) {
      return true;
    }

    Action otherAction = (Action) other;
    return type == otherAction.type && Objects.equals(poolName, otherAction.poolName);
  }

  @Override
  public int hashCode() {
    int hash = poolName == null ? 31 : 31 * poolName.hashCode();
    hash += type == null ? 31 * hash : 31 * hash * type.hashCode();
    return hash;
  }

  @Override
  public String toString() {
    return type.getDisplayName() + (poolName == null ? "" : " " + poolName);
  }
}
