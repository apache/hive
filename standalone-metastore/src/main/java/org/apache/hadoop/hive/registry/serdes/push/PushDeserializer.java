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
package org.apache.hadoop.hive.registry.serdes.push;

import org.apache.hadoop.hive.registry.Resourceable;

import java.io.InputStream;

/**
 * This deserializer gives callbacks to the given {@link PushDeserializerHandler} whenever a respective event or field is encountered.
 * It pushes the parser contents to the given {@link PushDeserializerHandler}
 *
 * @param <S> Schema representation class
 * @param <F> Field representation class
 */
public interface PushDeserializer<S, F> extends Resourceable {

    /**
     * Deserializes the given input stream and invokes respective callbacks to the given {@code handler} whenever a respective
     * event or field is encountered.
     *
     * @param inputStream payload input stream
     * @param schema current schema
     * @param handler handler to which events are pushed.
     */
    void deserialize(InputStream inputStream, S schema, PushDeserializerHandler<F> handler);

}
