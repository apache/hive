/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.registry.storage.core.search;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;


@JsonIgnoreProperties(ignoreUnknown = true)
public class PredicateCombinerPair implements Serializable {
    private static final long serialVersionUID = 8860616526693470116L;
    private Predicate predicate;
    private WhereClauseCombiner.Operation combinerOperation;

    private PredicateCombinerPair() {
    }

    PredicateCombinerPair(Predicate predicate) {
        this(predicate, null);
    }

    PredicateCombinerPair(Predicate predicate, WhereClauseCombiner.Operation combinerOperation) {
        this.predicate = predicate;
        this.combinerOperation = combinerOperation;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public WhereClauseCombiner.Operation getCombinerOperation() {
        return combinerOperation;
    }

    @Override
    public String toString() {
        return "PredicateCombinerPair{" +
                "predicate=" + predicate +
                ", combinerOperation=" + combinerOperation +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PredicateCombinerPair that = (PredicateCombinerPair) o;

        if (predicate != null ? !predicate.equals(that.predicate) : that.predicate != null) return false;
        return combinerOperation == that.combinerOperation;
    }

    @Override
    public int hashCode() {
        int result = predicate != null ? predicate.hashCode() : 0;
        result = 31 * result + (combinerOperation != null ? combinerOperation.hashCode() : 0);
        return result;
    }
}
