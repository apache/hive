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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.AbstractOperatorDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;


public class TestOpSigFactory {
  CompilationOpContext cCtx = new CompilationOpContext();

  @Rule
  public MockitoRule a = MockitoJUnit.rule();

  @Spy
  OpTreeSignatureFactory f = OpTreeSignatureFactory.newCache();

  public static class SampleDesc extends AbstractOperatorDesc {
    private static final long serialVersionUID = 1L;
    private int desc_invocations;

    @Signature
    public int asd() {
      desc_invocations++;
      return 8;
    }

    public int getDesc_invocations() {
      return desc_invocations;
    }
  }

  static class SampleOperator extends Operator<SampleDesc> {
    private static final long serialVersionUID = 1L;

    @Override
    public void process(Object row, int tag) throws HiveException {
    }

    @Override
    public String getName() {
      return "A1";
    }

    @Override
    public OperatorType getType() {
      return OperatorType.FILTER;
    }
  }

  @Test
  public void checkExplicit() {
    SampleOperator so = new SampleOperator();
    SampleDesc sd = new SampleDesc();
    so.setConf(sd);

    f.getSignature(so);
    f.getSignature(so);

    verify(f, times(2)).getSignature(Mockito.any());
    assertEquals(1, sd.getDesc_invocations());
  }

  @Test
  public void checkImplicit() {
    SampleOperator so = new SampleOperator();
    SampleDesc sd = new SampleDesc();
    so.setConf(sd);

    SampleOperator so2 = new SampleOperator();
    SampleDesc sd2 = new SampleDesc();
    so2.setConf(sd2);

    so.getParentOperators().add(so2);
    so2.getChildOperators().add(so);

    f.getSignature(so);
    // computes the sig of every object
    verify(f, times(2)).getSignature(Mockito.any());
    assertEquals(1, sd.getDesc_invocations());
    assertEquals(1, sd2.getDesc_invocations());

    f.getSignature(so);
    f.getSignature(so2);

    verify(f, times(4)).getSignature(Mockito.any());
    assertEquals(1, sd.getDesc_invocations());
    assertEquals(1, sd2.getDesc_invocations());
  }

}
