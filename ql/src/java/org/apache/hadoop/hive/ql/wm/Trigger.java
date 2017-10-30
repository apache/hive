/**
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

/**
 * Trigger interface which gets mapped to CREATE TRIGGER .. queries. A trigger can have a name, expression and action.
 * Trigger is a simple expression which gets evaluated during the lifecycle of query and executes an action
 * if the expression defined in trigger evaluates to true.
 */
public interface Trigger {

  enum Action {
    KILL_QUERY(""),
    MOVE_TO_POOL("");

    String poolName;
    String msg;

    Action(final String poolName) {
      this.poolName = poolName;
    }

    public Action setPoolName(final String poolName) {
      this.poolName = poolName;
      return this;
    }

    public String getPoolName() {
      return poolName;
    }

    public String getMsg() {
      return msg;
    }

    public void setMsg(final String msg) {
      this.msg = msg;
    }
  }

  /**
   * Based on current value, returns true if trigger is applied else false.
   *
   * @param current - current value
   * @return true if trigger got applied false otherwise
   */
  boolean apply(long current);

  /**
   * Get trigger expression
   *
   * @return expression
   */
  Expression getExpression();

  /**
   * Return the name of the trigger
   *
   * @return trigger name
   */
  String getName();

  /**
   * Return the action that will get executed when trigger expression evaluates to true
   *
   * @return action
   */
  Action getAction();

  /**
   * Return cloned copy of this trigger
   *
   * @return clone copy
   */
  Trigger clone();
}
