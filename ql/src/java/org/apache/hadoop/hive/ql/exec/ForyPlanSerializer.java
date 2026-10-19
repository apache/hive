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
package org.apache.hadoop.hive.ql.exec;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.fory.Fory;
import org.apache.fory.ThreadSafeFory;
import org.apache.fory.config.CompatibleMode;
import org.apache.fory.config.Language;
import org.apache.fory.context.ReadContext;
import org.apache.fory.context.WriteContext;
import org.apache.fory.io.ForyInputStream;
import org.apache.fory.resolver.DisallowedList;
import org.apache.fory.resolver.TypeResolver;
import org.apache.fory.serializer.collection.MapSerializer;
import org.apache.hadoop.hive.common.CopyOnFirstWriteProperties;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;

/**
 * Experimental Apache Fory engine for plan (de)serialization, selected by
 * {@code hive.plan.serialization.engine=fory}.
 *
 * Fory binds its class loader at build time, so one instance is kept per class loader (the session
 * class loader changes with ADD JAR). Soft values keep a stale loader collectable under memory pressure.
 *
 * Security: this runs with class registration disabled, like Kryo does, guarded only by Fory's built-in
 * deny list. It must not be used for payloads from untrusted sources (e.g. the metastore Thrift path).
 */
final class ForyPlanSerializer {
  private static final Cache<ClassLoader, ThreadSafeFory> FORY_BY_LOADER =
      CacheBuilder.newBuilder().weakKeys().softValues().build();

  private ForyPlanSerializer() {
  }

  static void serialize(ClassLoader loader, Object plan, OutputStream out) {
    get(loader).serialize(out, plan);
  }

  static <T> T deserialize(ClassLoader loader, InputStream in, Class<T> clazz) {
    return get(loader).deserialize(new ForyInputStream(in), clazz);
  }

  private static ThreadSafeFory get(ClassLoader loader) {
    try {
      return FORY_BY_LOADER.get(loader, () -> create(loader));
    } catch (ExecutionException | UncheckedExecutionException e) {
      throw new RuntimeException("Failed to create Fory instance", e.getCause());
    }
  }

  private static ThreadSafeFory create(ClassLoader loader) {
    ThreadSafeFory fory = Fory.builder()
        .withLanguage(Language.JAVA)
        // Kryo equivalent: setReferences(true). Plans are object graphs with shared and cyclic references.
        .withRefTracking(true)
        // Without this Fory writes every repeated String in full, even with ref tracking on.
        .ignoreStringRef(false)
        .requireClassRegistration(false)
        // Same-version writer and reader only; no per-object schema metadata.
        .withCompatibleMode(CompatibleMode.SCHEMA_CONSISTENT)
        .withTypeChecker((resolver, className) -> !DisallowedList.contains(className))
        .withClassLoader(loader)
        // Experiment switch (-Dhive.plan.fory.codegen=false): JIT serializers cost ~0.8s on first use in a cold JVM, and
        // Fory 1.7.6 cannot generate them when plan classes and fory live in different class loaders.
        .withCodegen(Boolean.parseBoolean(System.getProperty("hive.plan.fory.codegen", "true")))
        .buildThreadSafeFory();
    fory.registerSerializer(Properties.class, resolver -> new MapSerializer<Properties>(resolver, Properties.class));
    fory.registerSerializer(CopyOnFirstWriteProperties.class, CopyOnFirstWritePropertiesSerializer::new);
    return fory;
  }

  /**
   * CopyOnFirstWriteProperties has no no-arg constructor and keeps its shared content in an 'interned' instance that
   * the entry views delegate to. Like the Kryo serializer, the entries are written as a plain map and the interned
   * instance is written afterwards by reference, so the sharing across thousands of PartitionDescs survives.
   */
  private static final class CopyOnFirstWritePropertiesSerializer extends MapSerializer<CopyOnFirstWriteProperties> {
    CopyOnFirstWritePropertiesSerializer(TypeResolver resolver) {
      super(resolver, CopyOnFirstWriteProperties.class);
    }

    @Override
    public Map newMap(ReadContext ctx, boolean entryReadAlwaysAdvances) {
      setNumElements(readMapSize(ctx, ctx.getBuffer(), entryReadAlwaysAdvances));
      CopyOnFirstWriteProperties props = new CopyOnFirstWriteProperties(new Properties());
      ctx.reference(props);
      return props;
    }

    @Override
    public void write(WriteContext ctx, CopyOnFirstWriteProperties props) {
      super.write(ctx, props);
      ctx.writeRef(props.getInterned());
    }

    @Override
    public CopyOnFirstWriteProperties read(ReadContext ctx) {
      CopyOnFirstWriteProperties props = super.read(ctx);
      props.setInterned((Properties) ctx.readRef());
      return props;
    }
  }
}
