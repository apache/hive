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

package org.apache.hadoop.hive.ql.hooks;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.ql.optimizer.signature.OpTreeSignature;
import org.apache.hadoop.hive.ql.optimizer.signature.RuntimeStatsPersister;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This hook adds a persistence loop-back ensure that runtime statistics could be used.
 */
public class RuntimeStatsPersistenceCheckerHook implements ExecuteWithHookContext {

  private static final Logger LOG = LoggerFactory.getLogger(RuntimeStatsPersistenceCheckerHook.class);

  @Override
  public void run(HookContext hookContext) throws Exception {

    PlanMapper pm = ((PrivateHookContext) hookContext).getContext().getPlanMapper();

    List<OpTreeSignature> sigs = pm.getAll(OpTreeSignature.class);

    for (OpTreeSignature sig : sigs) {
      try {
        OpTreeSignature sig2 = persistenceLoop(sig, OpTreeSignature.class);
        sig.getSig().proveEquals(sig2.getSig());
      } catch (Exception e) {
        throw new RuntimeException("while checking the signature of: " + sig.getSig(), e);
      }
    }
    for (OpTreeSignature sig : sigs) {
      try {
        OpTreeSignature sig2 = persistenceLoop(sig, OpTreeSignature.class);
        if (!sig.equals(sig2)) {
          throw new RuntimeException("signature mismatch");
        }
      } catch (Exception e) {
        throw new RuntimeException("while checking the signature of: " + sig.getSig(), e);
      }
    }
    LOG.debug("signature checked: " + sigs.size());
  }

  private <T> T persistenceLoop(T sig, Class<T> clazz) throws IOException {
    RuntimeStatsPersister sp = RuntimeStatsPersister.INSTANCE;
    String stored = sp.encode(sig);
    return sp.decode(stored, clazz);
  }

}
