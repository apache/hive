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

import java.io.Serializable;
import java.util.LinkedHashMap;

import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;

/**
 * Join conditions Descriptor implementation.
 *
 */
public class JoinCondDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  private int left;
  private int right;
  private int type;
  private boolean preserved;
  protected final static Logger LOG = LoggerFactory.getLogger(JoinCondDesc.class.getName());

  public JoinCondDesc() {
  }

  public JoinCondDesc(int left, int right, int type) {
    this.left = left;
    this.right = right;
    this.type = type;
  }

  public JoinCondDesc(org.apache.hadoop.hive.ql.parse.JoinCond condn) {
    left = condn.getLeft();
    right = condn.getRight();
    preserved = condn.getPreserved();
    switch (condn.getJoinType()) {
    case INNER:
      type = JoinDesc.INNER_JOIN;
      break;
    case LEFTOUTER:
      type = JoinDesc.LEFT_OUTER_JOIN;
      break;
    case RIGHTOUTER:
      type = JoinDesc.RIGHT_OUTER_JOIN;
      break;
    case FULLOUTER:
      type = JoinDesc.FULL_OUTER_JOIN;
      break;
    case UNIQUE:
      type = JoinDesc.UNIQUE_JOIN;
      break;
    case LEFTSEMI:
      type = JoinDesc.LEFT_SEMI_JOIN;
      break;
    case ANTI:
      type = JoinDesc.ANTI_JOIN;
      break;
    default:
      assert false;
    }
  }

  /**
   * @return true if table is preserved, false otherwise
   */
  public boolean getPreserved() {
    return preserved;
  }

  /**
   * @param preserved
   *          if table is preserved, false otherwise
   */
  public void setPreserved(final boolean preserved) {
    this.preserved = preserved;
  }

  public int getLeft() {
    return left;
  }

  public void setLeft(final int left) {
    this.left = left;
  }

  public int getRight() {
    return right;
  }

  public void setRight(final int right) {
    this.right = right;
  }

  public int getType() {
    return type;
  }

  public void setType(final int type) {
    this.type = type;
  }

  @Explain(explainLevels = { Level.DEFAULT, Level.EXTENDED })
  public String getJoinCondString() {
    StringBuilder sb = new StringBuilder();

    switch (type) {
    case JoinDesc.INNER_JOIN:
      sb.append("Inner Join ");
      break;
    case JoinDesc.FULL_OUTER_JOIN:
      sb.append("Full Outer Join ");
      break;
    case JoinDesc.LEFT_OUTER_JOIN:
      sb.append("Left Outer Join ");
      break;
    case JoinDesc.RIGHT_OUTER_JOIN:
      sb.append("Right Outer Join ");
      break;
    case JoinDesc.UNIQUE_JOIN:
      sb.append("Unique Join ");
      break;
    case JoinDesc.LEFT_SEMI_JOIN:
      sb.append("Left Semi Join ");
      break;
    case JoinDesc.ANTI_JOIN:
      sb.append("Anti Join ");
      break;
    default:
      sb.append("Unknown Join ");
      break;
    }

    sb.append(left);
    sb.append(" to ");
    sb.append(right);

    return sb.toString();
  }

  @Explain(explainLevels = { Level.USER })
  public String getUserLevelJoinCondString() {
    JSONObject join = new JSONObject(new LinkedHashMap<>());
    try {
      switch (type) {
      case JoinDesc.INNER_JOIN:
        join.put("type", "Inner");
        break;
      case JoinDesc.FULL_OUTER_JOIN:
        join.put("type", "Outer");
        break;
      case JoinDesc.LEFT_OUTER_JOIN:
        join.put("type", "Left Outer");
        break;
      case JoinDesc.RIGHT_OUTER_JOIN:
        join.put("type", "Right Outer");
        break;
      case JoinDesc.UNIQUE_JOIN:
        join.put("type", "Unique");
        break;
      case JoinDesc.LEFT_SEMI_JOIN:
        join.put("type", "Left Semi");
        break;
      case JoinDesc.ANTI_JOIN:
        join.put("type", "Anti");
        break;
      default:
        join.put("type", "Unknown Join");
        break;
      }
      join.put("left", left);
      join.put("right", right);
    } catch (JSONException e) {
      // impossible to throw any json exceptions.
      LOG.trace(e.getMessage());
    }
    return join.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof JoinCondDesc)) {
      return false;
    }

    JoinCondDesc other = (JoinCondDesc) obj;
    if (this.type != other.type || this.left != other.left ||
      this.right != other.right || this.preserved != other.preserved) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(type, left, right, preserved);
  }
}
