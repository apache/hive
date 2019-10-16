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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelWriterImplCopy;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.google.common.base.Objects;

/**
 * Operator tree signature.
 */
@JsonIdentityInfo(generator = ObjectIdGenerators.IntSequenceGenerator.class, property = "@id")
public final class RelTreeSignature {

  @JsonProperty
  private int hashCode;
  @JsonProperty
  private String sig;
  @JsonProperty
  private ArrayList<RelTreeSignature> childSig;

  // need this for Jackson to work
  @SuppressWarnings("unused")
  private RelTreeSignature() {
  }

  public RelTreeSignature(RelNode node) {
    sig = relSignature(node);
    childSig = new ArrayList<RelTreeSignature>();
    for (RelNode relNode : node.getInputs()) {
      childSig.add(RelTreeSignature.of(relNode));
    }
    hashCode = Objects.hashCode(sig, childSig);
  }

  public static RelTreeSignature of(RelNode node) {
    return new RelTreeSignature(node);
  }

  @Override
  public boolean equals(Object obj) {

    if (!(obj instanceof RelTreeSignature)) {
      return false;
    }
    RelTreeSignature other = (RelTreeSignature) obj;
    return sig.equals(other.sig) && childSig.equals(other.childSig);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  private String relSignature(RelNode rel) {
    if (rel == null) {
      return null;
    }
    final StringWriter sw = new StringWriter();
    final RelWriter planWriter =
        new NonRecursiveRelWriterImpl(
            new PrintWriter(sw), SqlExplainLevel.EXPPLAN_ATTRIBUTES, false);
    rel.explain(planWriter);
    return sw.toString();
  }

  static class NonRecursiveRelWriterImpl extends RelWriterImplCopy {

    public NonRecursiveRelWriterImpl(PrintWriter pw, SqlExplainLevel detailLevel, boolean withIdPrefix) {
      super(pw, detailLevel, withIdPrefix);
    }

    @Override
    protected void explainInputs(List<RelNode> inputs) {
      // no-op
    }
  }
}
