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

package org.apache.hadoop.hive.ql.optimizer.signature;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.google.common.annotations.VisibleForTesting;

/**
 * Signature of the operator(non-recursive).
 */
@JsonIdentityInfo(generator = ObjectIdGenerators.IntSequenceGenerator.class, property = "@id")
public final class OpSignature {

  /**
   * Holds the signature of the operator; the keys are are the methods name marked by {@link Signature}.
   */
  @JsonProperty
  private Map<String, Object> sigMap;

  // need this for Jackson to work
  @SuppressWarnings("unused")
  private OpSignature() {
  }

  private OpSignature(Operator<? extends OperatorDesc> op) {
    sigMap = new HashMap<>();
    // FIXME: consider other operator info as well..not just conf?
    SignatureUtils.write(sigMap, op.getConf());
  }

  public static OpSignature of(Operator<? extends OperatorDesc> op) {
    return new OpSignature(op);
  }

  @Override
  public int hashCode() {
    return sigMap.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof OpSignature)) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    OpSignature o = (OpSignature) obj;
    return signatureCompare(o);
  }

  public boolean signatureCompare(OpSignature other) {
    return sigMap.equals(other.sigMap);
  }

  @VisibleForTesting
  public void proveEquals(OpSignature other) {
    proveEquals(sigMap, other.sigMap);
  }

  private static void proveEquals(Map<String, Object> m1, Map<String, Object> m2) {
    for (Entry<String, Object> e : m1.entrySet()) {
      String key = e.getKey();
      Object v1 = e.getValue();
      Object v2 = m2.get(key);
      if (v1 == v2) {
        continue;
      }
      if (v1 == null || v2 == null || !v1.equals(v2)) {
        throw new RuntimeException(String.format("equals fails: %s (%s!=%s)", key, v1, v2));
      }
    }
  }

  @Override
  public String toString() {
    return toString("");
  }

  public String toString(String pad) {
    StringBuffer sb = new StringBuffer();
    for (Entry<String, Object> e : sigMap.entrySet()) {
      sb.append(pad);
      sb.append(e.getKey());
      sb.append(" = ");
      sb.append(e.getValue());
      sb.append('\n');
    }
    return sb.toString();
  }

  public Map<String, Object> getSigMap() {
    return sigMap;
  }

}
