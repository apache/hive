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

import java.util.IdentityHashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * A simple cache backend to prevent repeated signature computations.
 */
public interface OpTreeSignatureFactory {

  OpTreeSignature getSignature(Operator<? extends OperatorDesc> op);

  void clear();

  OpTreeSignatureFactory DIRECT = new Direct();

  static OpTreeSignatureFactory direct() {
    return DIRECT;
  }

  static OpTreeSignatureFactory newCache() {
    return new CachedFactory();
  }

  // FIXME: possible alternative: move both OpSignature/OpTreeSignature into
  // under some class as nested ones; and that way this factory level caching can be made "transparent"

  class Direct implements OpTreeSignatureFactory {

    @Override
    public OpTreeSignature getSignature(Operator<? extends OperatorDesc> op) {
      return OpTreeSignature.of(op, this);
    }

    @Override
    public void clear() {
    }

  }

  class CachedFactory implements OpTreeSignatureFactory {

    Map<Operator<? extends OperatorDesc>, OpTreeSignature> cache = new IdentityHashMap<>();

    @Override
    public OpTreeSignature getSignature(Operator<? extends OperatorDesc> op) {
      return cache.computeIfAbsent(op, k -> OpTreeSignature.of(op, this));
    }

    @Override
    public void clear() {
      cache.clear();
    }

  }


}
