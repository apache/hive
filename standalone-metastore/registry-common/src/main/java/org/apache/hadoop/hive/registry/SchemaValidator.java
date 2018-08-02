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
package org.apache.hadoop.hive.registry;

/**
 * This is used to implement the compatibility of given reader and writer schemas.
 */
public interface SchemaValidator<T> {

    /**
     * Validates whether the payloads written with {@code writerSchema} can be projected or read according to the given
     * {@code readerSchema} and returns respective {@link CompatibilityResult} instance.
     *
     * @param readerSchema reader schema
     * @param writerSchema writer schema
     * @return CompatibilityResult after validating the given reader and writer schemas.
     */
    CompatibilityResult validate(T readerSchema, T writerSchema);
}
