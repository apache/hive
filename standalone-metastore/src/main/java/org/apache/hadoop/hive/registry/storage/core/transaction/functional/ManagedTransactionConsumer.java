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

package org.apache.hadoop.hive.registry.storage.core.transaction.functional;

public interface ManagedTransactionConsumer {

    @FunctionalInterface
    interface Arg0 {
        void apply() throws Exception;
    }

    @FunctionalInterface
    interface Arg1<T> {
        void apply(T t) throws Exception;
    }

    @FunctionalInterface
    interface Arg2<T1, T2> {
        void apply(T1 t1, T2 t2) throws Exception;
    }

    @FunctionalInterface
    interface Arg3<T1, T2, T3> {
        void apply(T1 t1, T2 t2, T3 t3) throws Exception;
    }

    @FunctionalInterface
    interface Arg4<T1, T2, T3, T4> {
        void apply(T1 t1, T2 t2, T3 t3, T4 t4) throws Exception;
    }

    @FunctionalInterface
    interface Arg5<T1, T2, T3, T4, T5> {
        void apply(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) throws Exception;
    }

}
