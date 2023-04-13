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
package org.apache.hadoop.hive.metastore.utils;

import java.util.function.Function;

/**
 * This functional interface represents a function and very similar to {@link Function}, the only
 * difference is this represents a function that throws Unchecked Exceptions.
 * <br>Currently, the standard functional interfaces do not deal with checked exceptions well. This representation of function
 * has a utility method to suppress checked exception and in turn throw {@link RuntimeException}. This could be very useful
 * while dealing with exceptions in functional programming without losing the brevity and simplicity.
 *
 * @param <T> the parameterized type of the input to the function
 * @param <R> the type of result of the function
 * @param <E> the type of exception that could be thrown while executing the function
 *
 * @see Function
 *
 */
@FunctionalInterface public interface ThrowingFunction<T, R, E extends Exception> {
  R apply(T t) throws E;

  static <T, R, E extends Exception> Function<T, R> unchecked(ThrowingFunction<T, R, E> throwingFunction) {
    return T -> {
      try {
        return throwingFunction.apply(T);
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    };
  }
}
