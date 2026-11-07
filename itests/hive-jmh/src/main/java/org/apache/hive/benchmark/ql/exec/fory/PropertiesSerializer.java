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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.benchmark.ql.exec.fory;

import java.util.Properties;

import org.apache.fory.resolver.TypeResolver;
import org.apache.fory.serializer.collection.MapSerializer;

/**
 * Evaluation-only: compact Fory replacement for java.util.Properties, which Fory has no built-in
 * serializer for (unlike Kryo's bundled MapSerializer, which already covers Hashtable/Properties)
 * and therefore falls back to slow/bloated JDK serialization for. Not wired into any production code path.
 *
 * <p>Must extend MapSerializer (not plain Serializer) because Fory's ClassResolver rejects
 * registering a non-MapLikeSerializer for any Map-implementing type, and Properties implements
 * Map via its Hashtable superclass. The inherited write/read/onMapWrite from MapSerializer
 * already do varint-size-prefixed chunked entry encoding, so no overrides are needed beyond the
 * constructor.
 */
public class PropertiesSerializer extends MapSerializer<Properties> {
  public PropertiesSerializer(TypeResolver typeResolver) {
    super(typeResolver, Properties.class);
  }
}
