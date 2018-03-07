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

package org.apache.hadoop.hive.ql.plan.mapper;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.optimizer.signature.OpTreeSignature;
import org.apache.hadoop.hive.ql.optimizer.signature.OpTreeSignatureFactory;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper.LinkGroup;

public class PlanMapperProcess {

  private static class OpTreeSignatureMapper implements GroupTransformer {

    private OpTreeSignatureFactory cache = OpTreeSignatureFactory.newCache();

    @Override
    public void map(LinkGroup group) {
      List<Operator> operators= group.getAll(Operator.class);
      for (Operator op : operators) {
        group.add(OpTreeSignature.of(op,cache));
      }
    }
  }

  public static void runPostProcess(PlanMapper planMapper) {
    planMapper.runMapper(new OpTreeSignatureMapper());
  }

}
