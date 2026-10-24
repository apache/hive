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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
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
import org.apache.fory.serializer.ObjectSerializer;
import org.apache.fory.serializer.Serializer;
import org.apache.fory.serializer.collection.MapSerializer;
import org.apache.hadoop.hive.common.CopyOnFirstWriteProperties;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;

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
  // Readers used while a SerializationUtilities global hook (the LLAP UDF whitelist) is installed. Fory's type checker
  // also runs on write and caches accepted class names per instance, so the hooked checker must never serve writes.
  private static final Cache<ClassLoader, ThreadSafeFory> HOOKED_READER_BY_LOADER =
      CacheBuilder.newBuilder().weakKeys().softValues().build();

  private ForyPlanSerializer() {
  }

  static void serialize(ClassLoader loader, Object plan, OutputStream out) {
    get(FORY_BY_LOADER, loader, false).serialize(out, plan);
  }

  static <T> T deserialize(ClassLoader loader, InputStream in, Class<T> clazz) {
    ThreadSafeFory fory = SerializationUtilities.globalHook() == null
        ? get(FORY_BY_LOADER, loader, false) : get(HOOKED_READER_BY_LOADER, loader, true);
    return fory.deserialize(new ForyInputStream(in), clazz);
  }

  private static ThreadSafeFory get(Cache<ClassLoader, ThreadSafeFory> cache, ClassLoader loader, boolean hooked) {
    try {
      return cache.get(loader, () -> create(loader, hooked));
    } catch (ExecutionException | UncheckedExecutionException e) {
      throw new RuntimeException("Failed to create Fory instance", e.getCause());
    }
  }

  private static ThreadSafeFory create(ClassLoader loader, boolean hooked) {
    ThreadSafeFory fory = Fory.builder()
        .withLanguage(Language.JAVA)
        // Kryo equivalent: setReferences(true). Plans are object graphs with shared and cyclic references.
        .withRefTracking(true)
        // Without this Fory writes every repeated String in full, even with ref tracking on.
        .ignoreStringRef(false)
        .requireClassRegistration(false)
        // Same-version writer and reader only; no per-object schema metadata.
        .withCompatibleMode(CompatibleMode.SCHEMA_CONSISTENT)
        .withTypeChecker((resolver, className) ->
            !DisallowedList.contains(className) && (!hooked || hookAllows(loader, className)))
        .withClassLoader(loader)
        // Experiment switch (-Dhive.plan.fory.codegen=false): JIT serializers cost ~0.8s on first use in a cold JVM, and
        // Fory 1.7.6 cannot generate them when plan classes and fory live in different class loaders.
        .withCodegen(Boolean.parseBoolean(System.getProperty("hive.plan.fory.codegen", "true")))
        .buildThreadSafeFory();
    fory.registerSerializer(Properties.class, resolver -> new MapSerializer<Properties>(resolver, Properties.class));
    fory.registerSerializer(CopyOnFirstWriteProperties.class, CopyOnFirstWritePropertiesSerializer::new);
    fory.registerSerializer(Random.class, resolver -> new RandomSerializer(resolver));
    if (hooked) {
      fory.registerSerializer(GenericUDFBridge.class, resolver -> new HookedBridgeSerializer(resolver));
    }
    return fory;
  }

  /**
   * Plans can carry a java.util.Random (FuncRand holds one, built from the rand(seed) argument). Fory's generic
   * handling of Random fails its own field schema check, so use the JDK's serialized form, which keeps the seed state.
   */
  private static final class RandomSerializer extends Serializer<Random> {
    RandomSerializer(TypeResolver resolver) {
      super(resolver.getConfig(), Random.class);
    }

    @Override
    public void write(WriteContext ctx, Random random) {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (ObjectOutputStream oos = new ObjectOutputStream(bytes)) {
        oos.writeObject(random);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      ctx.getBuffer().writeInt32(bytes.size());
      ctx.getBuffer().writeBytes(bytes.toByteArray());
    }

    @Override
    public Random read(ReadContext ctx) {
      byte[] bytes = ctx.getBuffer().readBytes(ctx.getBuffer().readInt32());
      try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
        Random random = (Random) ois.readObject();
        ctx.reference(random);
        return random;
      } catch (IOException | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** Ports Hook.preRead of the kryo engine: a hook that throws SecurityException rejects the class. */
  private static boolean hookAllows(ClassLoader loader, String className) {
    SerializationUtilities.Hook hook = SerializationUtilities.globalHook();
    if (hook == null) {
      return true;
    }
    Class<?> type;
    try {
      type = Class.forName(className, false, loader);
    } catch (ClassNotFoundException | LinkageError e) {
      return true; // Fory fails on its own when it cannot load the class
    }
    try {
      hook.preRead(type);
      return true;
    } catch (SecurityException e) {
      return false;
    }
  }

  /** Ports Hook.postRead of the kryo engine, which injects the whitelist checker into GenericUDFBridge instances. */
  private static final class HookedBridgeSerializer extends Serializer<GenericUDFBridge> {
    private final ObjectSerializer<GenericUDFBridge> delegate;

    HookedBridgeSerializer(TypeResolver resolver) {
      super(resolver.getConfig(), GenericUDFBridge.class);
      this.delegate = new ObjectSerializer<>(resolver, GenericUDFBridge.class);
    }

    @Override
    public void write(WriteContext ctx, GenericUDFBridge bridge) {
      delegate.write(ctx, bridge);
    }

    @Override
    public GenericUDFBridge read(ReadContext ctx) {
      GenericUDFBridge bridge = delegate.read(ctx);
      SerializationUtilities.Hook hook = SerializationUtilities.globalHook();
      return hook == null ? bridge : (GenericUDFBridge) hook.postRead(bridge);
    }
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
