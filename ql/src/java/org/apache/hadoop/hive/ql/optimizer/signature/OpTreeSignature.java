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

import java.util.ArrayList;
import java.util.Objects;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * Operator tree signature.
 */
public class OpTreeSignature {
  private int hashCode;
  private OpSignature sig;
  private ArrayList<OpTreeSignature> parentSig;

  OpTreeSignature(Operator<?> op, OpTreeSignatureFactory osf) {
    sig = OpSignature.of(op);
    parentSig = new ArrayList<>();
    for (Operator<? extends OperatorDesc> parentOp : op.getParentOperators()) {
      parentSig.add(osf.getSignature(parentOp));
    }
    hashCode = Objects.hash(sig, parentSig);
  }

  public static OpTreeSignature of(Operator<?> root) {
    return of(root, OpTreeSignatureFactory.DIRECT);
  }

  public static OpTreeSignature of(Operator<? extends OperatorDesc> op, OpTreeSignatureFactory osf) {
    return new OpTreeSignature(op, osf);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof OpTreeSignature)) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    OpTreeSignature o = (OpTreeSignature) obj;

    return sig.equals(o.sig) && parentSig.equals(o.parentSig);
  }

  @Override
  public String toString() {
    return toString("");
  }

  public String toString(String pad) {
    StringBuffer sb = new StringBuffer();
    sb.append(pad + "hashcode:" + hashCode + "\n");
    sb.append(sig.toString(pad));
    for (OpTreeSignature p : parentSig) {
      sb.append(p.toString(pad + " "));
    }
    return sb.toString();
  }

}
