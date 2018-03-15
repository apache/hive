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
package org.apache.hadoop.hive.registry.serde;

import org.apache.hadoop.hive.registry.Resourceable;

/**
 * Deserializer interface for deserializing input {@code I} into output {@code O} according to the Schema {@code S}.
 * <p>Common way to use this deserializer implementation is like below. </p>
 * <pre>{@code
 *     SnapshotDeserializer deserializer = ...
 *     // initialize with given configuration
 *     deserializer.init(config);
 *
 *     // this instance can be used for multiple serialization invocations
 *     deserializer.deserialize(input, readerSchemaInfo);
 *
 *     // close it to release any resources held
 *     deserializer.close();
 * }</pre>
 *
 * @param <I>  Input type of the payload
 * @param <O>  Output type of the deserialized content.
 * @param <RS> Reader schema information.
 */
public interface SnapshotDeserializer<I, O, RS> extends Resourceable {

    /**
     * Returns output {@code O} after deserializing the given {@code input} according to the writer schema {@code WS} and
     * it may be projected if reader schema {@code RS} is given.
     *
     * @param input input payload to be deserialized
     * @param readerSchemaInfo schema information about reading/projection
     * @return O output to be deserialized into
     */
    O deserialize(I input, RS readerSchemaInfo) throws SerDesException;

}
