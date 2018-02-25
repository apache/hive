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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Combines the clause in a chained from like below.
 * <pre>@code {

 * }</pre>
 *
 * More complex clauses can be written like
 * <pre>@code{

 * }</pre>
 *
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WhereClause implements Serializable {
    private static final long serialVersionUID = 901279529557954101L;

    protected List<PredicateCombinerPair> predicateCombinerPairs;

    private WhereClause() {
    }

    protected WhereClause(List<PredicateCombinerPair> predicateCombinerPairs) {
        this.predicateCombinerPairs = Collections.unmodifiableList(predicateCombinerPairs);
    }

    public static WhereClause.Builder begin() {
        return new WhereClause.Builder();
    }

    public List<PredicateCombinerPair> getPredicateCombinerPairs() {
        return predicateCombinerPairs;
    }

    @Override
    public String toString() {
        return "Clause{" +
                "predicateCombinerPairs=" + predicateCombinerPairs +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WhereClause whereClause = (WhereClause) o;

        return predicateCombinerPairs != null ? predicateCombinerPairs.equals(whereClause.predicateCombinerPairs) : whereClause.predicateCombinerPairs == null;
    }

    @Override
    public int hashCode() {
        return predicateCombinerPairs != null ? predicateCombinerPairs.hashCode() : 0;
    }

    public static class Builder {
        protected List<PredicateCombinerPair> predicateCombinerPairs = new ArrayList<>();

        private Builder() {
        }

        void addPredeicateCombiner(PredicateCombinerPair predicateCombinerPair) {
            predicateCombinerPairs.add(predicateCombinerPair);
        }

        public WhereClauseCombiner enclose(WhereClauseCombiner whereClauseCombiner) {
            return new WhereClauseCombiner(this, whereClauseCombiner);
        }

        public WhereClauseCombiner eq(String fieldName, Object value) {
            return new WhereClauseCombiner(this, new Predicate(fieldName, value, Predicate.Operation.EQ));
        }

        public WhereClauseCombiner contains(String fieldName, Object value) {
            return new WhereClauseCombiner(this, new Predicate(fieldName, value, Predicate.Operation.CONTAINS));
        }

        public WhereClauseCombiner lt(String fieldName, Object value) {
            return new WhereClauseCombiner(this, new Predicate(fieldName, value, Predicate.Operation.LT));
        }

        public WhereClauseCombiner gt(String fieldName, Object value) {
            return new WhereClauseCombiner(this, new Predicate(fieldName, value, Predicate.Operation.GT));
        }

        public WhereClause build() {
            return new WhereClause(predicateCombinerPairs);
        }
    }

}
