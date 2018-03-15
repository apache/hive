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


public class WhereClauseCombiner {
    public enum Operation {AND, OR, ENCL_START, ENCL_FINISH}

    private WhereClause.Builder builder;
    private Predicate predicate;

    public WhereClauseCombiner(WhereClause.Builder builder, WhereClauseCombiner whereClauseCombiner) {
        this.builder = builder;
        builder.addPredeicateCombiner(new PredicateCombinerPair(null, Operation.ENCL_START));
        for (PredicateCombinerPair predicateCombinerPair : whereClauseCombiner.combine().getPredicateCombinerPairs()) {
            builder.addPredeicateCombiner(predicateCombinerPair);
        }
        builder.addPredeicateCombiner(new PredicateCombinerPair(null, Operation.ENCL_FINISH));
    }

    WhereClauseCombiner(WhereClause.Builder builder, Predicate predicate) {
        this.builder = builder;
        this.predicate = predicate;
    }

    public WhereClause.Builder and() {
        builder.addPredeicateCombiner(new PredicateCombinerPair(predicate, Operation.AND));
        return builder;
    }

    public WhereClause.Builder or() {
        builder.addPredeicateCombiner(new PredicateCombinerPair(predicate, Operation.OR));
        return builder;
    }

    public WhereClause combine() {
        builder.addPredeicateCombiner(new PredicateCombinerPair(predicate));
        return builder.build();
    }

}
