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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.CopyOnFirstWriteProperties;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.plan.AbstractOperatorDesc;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.util.DefaultInstantiatorStrategy;
import com.esotericsoftware.kryo.kryo5.objenesis.strategy.StdInstantiatorStrategy;
import com.esotericsoftware.kryo.kryo5.Registration;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.esotericsoftware.kryo.kryo5.util.Pool;
import com.esotericsoftware.kryo.kryo5.serializers.FieldSerializer;

import com.google.common.annotations.VisibleForTesting;

/**
 * Utilities related to serialization and deserialization.
 */
public class SerializationUtilities {
  private static final String CLASS_NAME = SerializationUtilities.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  public static class Hook {
    public boolean preRead(Class<?> type) {
      return true;
    }

    public Object postRead(Object o) {
      return o;
    }
  }
  private static final Map<Class<?>, Hook> kryoTypeHooks = new HashMap<>();
  private static Hook globalHook = null;

  /**
   * Must be called before any serialization takes place (e.g. in some static/service init)!
   * Not thread safe.
   *
   * Will not work if different classes are added in different order on two sides of the
   * communication, due to explicit class registration that we use causing class ID mismatch.
   * Some processing might be added for this later (e.g. sorting the overrides here if the order
   * is hard to enforce, and making sure they are added symmetrically everywhere, or just
   * reverting back to hardcoding stuff if all else fails).
   * For now, this seems to work, but Kryo seems pretty brittle. Seems to be ok to add class on
   * read side but not write side, the other way doesn't work. Kryo needs a proper event system,
   * otherwise this is all rather brittle.
   */
  public static void addKryoTypeHook(Class<?> clazz, Hook hook) {
    kryoTypeHooks.put(clazz, hook);
  }

  /**
   * Must be called before any serialization takes place (e.g. in some static/service init)!
   * Not thread safe.
   *
   * This is somewhat brittle because there's no way to add proper superclass hook in Kryo.
   * On the other hand, it doesn't suffer from the mismatch problems that register() causes!
   */
  public static void setGlobalHook(Hook hook) {
    globalHook = hook;
  }

  /**
   * Provides general-purpose hooks for specific types, as well as a global hook.
   */
  private static class KryoWithHooks extends Kryo implements Configurable {
    private Hook globalHook;
    // this should be set on-the-fly after borrowing this instance and needs to be reset on release
    private Configuration configuration;
    // when non-null, the first class read must be compatible with this type
    private Class<?> rootType = null;
    // total classes we have met during (de)serialization, should be reset on release
    private long classCounter = 0;

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static final class SerializerWithHook extends com.esotericsoftware.kryo.kryo5.Serializer {
      private final com.esotericsoftware.kryo.kryo5.Serializer old;
      private final Hook hook;

      private SerializerWithHook(com.esotericsoftware.kryo.kryo5.Serializer old, Hook hook) {
        this.old = old;
        this.hook = hook;
      }

      @Override
      public Object read(Kryo kryo, Input input, Class type) {
        return hook.preRead(type)
            ? hook.postRead(old.read(kryo, input, type)) : old.read(kryo, input, type);
      }

      @Override
      public void write(Kryo kryo, Output output, Object object) {
        // Add write hooks if needed.
        old.write(kryo, output, object);
      }
    }

    public Kryo processHooks(Map<Class<?>, Hook> hooks, Hook globalHook) {
      for (Map.Entry<Class<?>, Hook> e : hooks.entrySet()) {
        register(e.getKey(), new SerializerWithHook(
            newDefaultSerializer(e.getKey()), e.getValue()));
      }
      this.globalHook = globalHook;
      return this; // To make it more explicit below that processHooks needs to be called last.
    }

    // The globalHook stuff. There's no proper way to insert this, so we add it everywhere.

    private Hook ponderGlobalPreReadHook(Class<?> clazz) {
      Hook globalHook = this.globalHook;
      return (globalHook != null && globalHook.preRead(clazz)) ? globalHook : null;
    }

    @SuppressWarnings("unchecked")
    private <T> T ponderGlobalPostReadHook(Hook hook, T result) {
      return (hook == null) ? result : (T)hook.postRead(result);
    }

    private Object ponderGlobalPostHook(Object result) {
      Hook globalHook = this.globalHook;
      return (globalHook != null) ? globalHook.postRead(result) : result;
    }

    @Override
    public Object readClassAndObject(Input input) {
      return ponderGlobalPostHook(super.readClassAndObject(input));
    }

    @Override
    public Registration readClass(Input input) {
      Registration reg = super.readClass(input);
      if (reg != null) {
        ponderGlobalPreReadHook(reg.getType()); // Needed to intercept readClassAndObject.
      }
      return reg;
    }

    @Override
    public <T> T readObjectOrNull(Input input, Class<T> type) {
      Hook hook = ponderGlobalPreReadHook(type);
      T result = super.readObjectOrNull(input, type);
      return ponderGlobalPostReadHook(hook, result);
    }

    @Override
    public <T> T readObjectOrNull(Input input, Class<T> type,
        @SuppressWarnings("rawtypes") com.esotericsoftware.kryo.kryo5.Serializer serializer) {
      Hook hook = ponderGlobalPreReadHook(type);
      T result = super.readObjectOrNull(input, type, serializer);
      return ponderGlobalPostReadHook(hook, result);
    }

    @Override
    public <T> T readObject(Input input, Class<T> type) {
      Hook hook = ponderGlobalPreReadHook(type);
      T result = super.readObject(input, type);
      return ponderGlobalPostReadHook(hook, result);
    }

    @Override
    public <T> T readObject(Input input, Class<T> type,
        @SuppressWarnings("rawtypes") com.esotericsoftware.kryo.kryo5.Serializer serializer) {
      Hook hook = ponderGlobalPreReadHook(type);
      T result = super.readObject(input, type, serializer);
      return ponderGlobalPostReadHook(hook, result);
    }

    @Override
    public void setConf(Configuration conf) {
      this.configuration = conf;

    }

    @Override
    public Configuration getConf() {
      return configuration;
    }

    @Override
    public com.esotericsoftware.kryo.kryo5.Registration getRegistration(Class type) {
      // If this instance deserializes a payload that a remote client controls (e.g. PartitionExpressionForMetastore at
      // a remote HMS) or that a client can persist (e.g. a table property copied into the job conf), the first class
      // encountered during deserialization must be compatible with the expected root type.
      if (rootType != null && classCounter == 0 && !rootType.isAssignableFrom(type)) {
        throw new UnsupportedOperationException("The object to be deserialized must be a "
            + rootType.getName() + ", but encountered: " + type);
      }
      classCounter++;
      return super.getRegistration(type);
    }

    void setRootType(Class<?> rootType) {
      this.rootType = rootType;
      this.classCounter = 0;
    }

    // reset the fields on release
    public void restore() {
      setConf(null);
      rootType = null;
      classCounter = 0;
    }
  }

  private static final Object FAKE_REFERENCE = new Object();

  // Bounded queue could be specified here but that will lead to blocking.
  // ConcurrentLinkedQueue is unbounded and will release soft referenced kryo instances under
  // memory pressure.
  private static Pool<Kryo> kryoPool = new Pool<Kryo>(true, true, 32) {
    protected Kryo create() {
      return createNewKryo();
    }
  };

  public static Kryo createNewKryo() {
    KryoWithHooks kryo = new KryoWithHooks();

    // references was true by default in kryo4, so we need to set this here for unchanged behavior
    kryo.setReferences(true);
    // registrationRequired=false lets kryo users skip the kryo.register call for all serialized classes
    kryo.setRegistrationRequired(false);

    kryo.register(java.sql.Date.class, new SqlDateSerializer());
    kryo.register(java.sql.Timestamp.class, new TimestampSerializer());
    kryo.register(TimestampTZ.class, new TimestampTZSerializer());
    kryo.register(Path.class, new PathSerializer());
    kryo.register(Arrays.asList("").getClass(), new ArraysAsListSerializer());
    kryo.register(new java.util.ArrayList().subList(0,0).getClass(), new ArrayListSubListSerializer());
    kryo.register(CopyOnFirstWriteProperties.class, new CopyOnFirstWritePropertiesSerializer());
    kryo.register(MapWork.class, new MapWorkSerializer(kryo, MapWork.class));
    kryo.register(PartitionDesc.class, new PartitionDescSerializer(kryo, PartitionDesc.class));

    ((DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy())
        .setFallbackInstantiatorStrategy(
            new StdInstantiatorStrategy());
    removeField(kryo, AbstractOperatorDesc.class, "colExprMap");
    removeField(kryo, AbstractOperatorDesc.class, "statistics");

    // This must be called after all the explicit register calls.
    return kryo.processHooks(kryoTypeHooks, globalHook);
  }

  /**
   * By default, kryo pool uses ConcurrentLinkedQueue which is unbounded. To facilitate reuse of
   * kryo object call releaseKryo() after done using the kryo instance. The class loader for the
   * kryo instance will be set to current thread's context class loader.
   *
   * @return kryo instance
   */
  public static Kryo borrowKryo() {
    return borrowKryo(null);
  }

  public static Kryo borrowKryo(Configuration configuration) {
    Kryo kryo = kryoPool.obtain();
    kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
    ((KryoWithHooks) kryo).setConf(configuration);
    return kryo;
  }

  /**
   * Release kryo instance back to the pool.
   *
   * @param kryo - kryo instance to be released
   */
  public static void releaseKryo(Kryo kryo) {
    if (kryo != null){
      ((KryoWithHooks) kryo).restore();
    }
    kryoPool.free(kryo);
  }

  private static void removeField(Kryo kryo, Class type, String fieldName) {
    FieldSerializer fld = new FieldSerializer(kryo, type);
    fld.removeField(fieldName);
    kryo.register(type, fld);
  }

  /**
   * Kryo serializer for timestamp.
   */
  private static class TimestampSerializer extends
      com.esotericsoftware.kryo.kryo5.Serializer<Timestamp> {

    @Override
    public Timestamp read(Kryo kryo, Input input, Class<? extends Timestamp> clazz) {
      Timestamp ts = new Timestamp(input.readLong());
      ts.setNanos(input.readInt());
      return ts;
    }

    @Override
    public void write(Kryo kryo, Output output, Timestamp ts) {
      output.writeLong(ts.getTime());
      output.writeInt(ts.getNanos());
    }
  }

  private static class TimestampTZSerializer extends com.esotericsoftware.kryo.kryo5.Serializer<TimestampTZ> {

    @Override
    public void write(Kryo kryo, Output output, TimestampTZ object) {
      output.writeLong(object.getEpochSecond());
      output.writeInt(object.getNanos());
      output.writeString(object.getZonedDateTime().getZone().getId());
    }

    @Override
    public TimestampTZ read(Kryo kryo, Input input, Class<? extends TimestampTZ> type) {
      long seconds = input.readLong();
      int nanos = input.readInt();
      String zoneId = input.readString();
      return new TimestampTZ(seconds, nanos, ZoneId.of(zoneId));
    }
  }

  /**
   * Custom Kryo serializer for sql date, otherwise Kryo gets confused between
   * java.sql.Date and java.util.Date while deserializing
   */
  private static class SqlDateSerializer extends
      com.esotericsoftware.kryo.kryo5.Serializer<java.sql.Date> {

    @Override
    public java.sql.Date read(Kryo kryo, Input input, Class<? extends java.sql.Date> clazz) {
      return new java.sql.Date(input.readLong());
    }

    @Override
    public void write(Kryo kryo, Output output, java.sql.Date sqlDate) {
      output.writeLong(sqlDate.getTime());
    }
  }

  private static class PathSerializer extends com.esotericsoftware.kryo.kryo5.Serializer<Path> {

    @Override
    public void write(Kryo kryo, Output output, Path path) {
      output.writeString(path.toUri().toString());
    }

    @Override
    public Path read(Kryo kryo, Input input, Class<? extends Path> type) {
      return new Path(URI.create(input.readString()));
    }
  }

  /**
   * Supports sublists created via {@link ArrayList#subList(int, int)} since java7 and {@link LinkedList#subList(int, int)} since java9 (openjdk).
   * This is from kryo-serializers package.
   */
  private static class ArrayListSubListSerializer extends com.esotericsoftware.kryo.kryo5.Serializer<List<?>> {

    private Field _parentField;
    private Field _parentOffsetField;
    private Field _sizeField;

    public ArrayListSubListSerializer() {
      try {
        final Class<?> clazz = Class.forName("java.util.ArrayList$SubList");
        _parentField = getParentField(clazz);
        _parentOffsetField = getOffsetField(clazz);
        _sizeField = clazz.getDeclaredField( "size" );
        _parentField.setAccessible( true );
        _parentOffsetField.setAccessible( true );
        _sizeField.setAccessible( true );
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    private static Field getParentField(Class clazz) throws NoSuchFieldException {
      try {
        // java 9+
        return clazz.getDeclaredField("root");
      } catch(NoSuchFieldException e) {
        return clazz.getDeclaredField("parent");
      }
    }

    private static Field getOffsetField(Class<?> clazz) throws NoSuchFieldException {
      try {
        // up to jdk8 (which also has an "offset" field (we don't need) - therefore we check "parentOffset" first
        return clazz.getDeclaredField( "parentOffset" );
      } catch (NoSuchFieldException e) {
        // jdk9+ only has "offset" which is the parent offset
        return clazz.getDeclaredField( "offset" );
      }
    }

    @Override
    public List<?> read(final Kryo kryo, final Input input, final Class<? extends List<?>> clazz) {
      kryo.reference(FAKE_REFERENCE);
      final List<?> list = (List<?>) kryo.readClassAndObject(input);
      final int fromIndex = input.readInt(true);
      final int toIndex = input.readInt(true);
      return list.subList(fromIndex, toIndex);
    }

    @Override
    public void write(final Kryo kryo, final Output output, final List<?> obj) {
      try {
        kryo.writeClassAndObject(output, _parentField.get(obj));
        final int parentOffset = _parentOffsetField.getInt( obj );
        final int fromIndex = parentOffset;
        output.writeInt(fromIndex, true);
        final int toIndex = fromIndex + _sizeField.getInt( obj );
        output.writeInt(toIndex, true);
      } catch (final RuntimeException e) {
        // Don't eat and wrap RuntimeExceptions because the ObjectBuffer.write...
        // handles SerializationException specifically (resizing the buffer)...
        throw e;
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public List<?> copy(final Kryo kryo, final List<?> original) {
      kryo.reference(FAKE_REFERENCE);
      try {
        final List<?> list = (List<?>) _parentField.get(original);
        final int parentOffset = _parentOffsetField.getInt( original );
        final int fromIndex = parentOffset;
        final int toIndex = fromIndex + _sizeField.getInt( original );
        return kryo.copy(list).subList(fromIndex, toIndex);
      } catch(final Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * A kryo {@link Serializer} for lists created via {@link Arrays#asList(Object...)}.
   * <p>
   * Note: This serializer does not support cyclic references, so if one of the objects
   * gets set the list as attribute this might cause an error during deserialization.
   * </p>
   * <p/>
   * This is from kryo-serializers package. Added explicitly to avoid classpath issues.
   */
  private static class ArraysAsListSerializer
      extends com.esotericsoftware.kryo.kryo5.Serializer<List<?>> {

    private Field _arrayField;

    public ArraysAsListSerializer() {
      try {
        _arrayField = Class.forName("java.util.Arrays$ArrayList").getDeclaredField("a");
        _arrayField.setAccessible(true);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
      // Immutable causes #copy(obj) to return the original object
      setImmutable(true);
    }

    @Override
    public List<?> read(final Kryo kryo, final Input input, final Class<? extends List<?>> type) {
      final int length = input.readInt(true);
      Class<?> componentType = kryo.readClass(input).getType();
      if (componentType.isPrimitive()) {
        componentType = getPrimitiveWrapperClass(componentType);
      }
      try {
        final Object items = Array.newInstance(componentType, length);
        for (int i = 0; i < length; i++) {
          Array.set(items, i, kryo.readClassAndObject(input));
        }
        return Arrays.asList((Object[]) items);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void write(final Kryo kryo, final Output output, final List<?> obj) {
      try {
        final Object[] array = (Object[]) _arrayField.get(obj);
        output.writeInt(array.length, true);
        final Class<?> componentType = array.getClass().getComponentType();
        kryo.writeClass(output, componentType);
        for (final Object item : array) {
          kryo.writeClassAndObject(output, item);
        }
      } catch (final RuntimeException e) {
        // Don't eat and wrap RuntimeExceptions because the ObjectBuffer.write...
        // handles SerializationException specifically (resizing the buffer)...
        throw e;
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    private Class<?> getPrimitiveWrapperClass(final Class<?> c) {
      if (c.isPrimitive()) {
        if (c.equals(Long.TYPE)) {
          return Long.class;
        } else if (c.equals(Integer.TYPE)) {
          return Integer.class;
        } else if (c.equals(Double.TYPE)) {
          return Double.class;
        } else if (c.equals(Float.TYPE)) {
          return Float.class;
        } else if (c.equals(Boolean.TYPE)) {
          return Boolean.class;
        } else if (c.equals(Character.TYPE)) {
          return Character.class;
        } else if (c.equals(Short.TYPE)) {
          return Short.class;
        } else if (c.equals(Byte.TYPE)) {
          return Byte.class;
        }
      }
      return c;
    }
  }

  /**
   * CopyOnFirstWriteProperties needs a special serializer, since it extends Properties,
   * which implements Map, so MapSerializer would be used for it by default. Yet it has
   * the additional 'interned' field that the standard MapSerializer doesn't handle
   * properly. But FieldSerializer doesn't work for it as well, because the Hashtable
   * superclass declares most of its fields transient.
   */
  private static class CopyOnFirstWritePropertiesSerializer extends
      com.esotericsoftware.kryo.kryo5.serializers.MapSerializer<Map> {

    @Override
    public void write(Kryo kryo, Output output, Map map) {
      super.write(kryo, output, map);
      CopyOnFirstWriteProperties p = (CopyOnFirstWriteProperties) map;
      Properties ip = p.getInterned();
      kryo.writeObjectOrNull(output, ip, Properties.class);
    }

    @Override
    public Map read(Kryo kryo, Input input, Class<? extends Map> type) {
      Map map = super.read(kryo, input, type);
      Properties ip = kryo.readObjectOrNull(input, Properties.class);
      ((CopyOnFirstWriteProperties) map).setInterned(ip);
      return map;
    }
  }

  /**
   * We use a custom {@link com.esotericsoftware.kryo.Serializer} for {@link MapWork} objects e.g. in
   * order to remove useless properties in execution time.
   */
  private static class MapWorkSerializer extends FieldSerializer<MapWork> {

    public MapWorkSerializer(Kryo kryo, Class type) {
      super(kryo, type);
    }

    @Override
    public void write(Kryo kryo, Output output, MapWork mapWork) {
      filterMapworkProperties(((KryoWithHooks) kryo).getConf(), mapWork);
      super.write(kryo, output, mapWork);
    }

    private static void filterMapworkProperties(Configuration configuration, MapWork mapWork) {
      if (configuration == null || HiveConf
          .getVar(configuration, HiveConf.ConfVars.HIVE_PLAN_MAPWORK_SERIALIZATION_SKIP_PROPERTIES).isEmpty()) {
        return;
      }
      String[] filterProps =
          HiveConf.getVar(configuration, HiveConf.ConfVars.HIVE_PLAN_MAPWORK_SERIALIZATION_SKIP_PROPERTIES).split(",");
      for (String prop : filterProps) {
        boolean isRegex = isRegex(prop);
        Pattern pattern = Pattern.compile(prop);

        LOG.debug("Trying to filter MapWork properties (regex: " + isRegex + "): " + prop);

        for (Entry<Path, PartitionDesc> partDescEntry : mapWork.getPathToPartitionInfo().entrySet()) {
          /*
           * remove by regex, could be a bit more expensive because of iterating and matching regexes
           * e.g.: in case of impala_intermediate_stats_chunk1, impala_intermediate_stats_chunk2, user only needs to
           * configure impala_intermediate_stats_chunk.*
           */
          if (isRegex) {
            Iterator<Entry<Object, Object>> itProps =
                partDescEntry.getValue().getProperties().entrySet().iterator();
            while (itProps.hasNext()) {
              Map.Entry<Object, Object> entry = itProps.next();
              String actualProp = (String) entry.getKey();
              Matcher matcher = pattern.matcher(actualProp);

              if (matcher.find()) {
                LOG.debug("Removed '{}' from MapWork (partition: {})", actualProp, partDescEntry.getKey());
                itProps.remove();
              }
            }
          } else {
            Object objRemoved = partDescEntry.getValue().getProperties().remove(prop);
            if (objRemoved != null) {
              LOG.debug("Removed '{}' from MapWork (partition: {})", prop, partDescEntry.getKey());
            }
          }
        }
      }
    }

    private static boolean isRegex(String prop) {
      return prop.contains("*");
    }
  }

  /** Re-runs the interning setters of a PartitionDesc after it was materialized by a serializer. */
  private static void internPartitionDesc(PartitionDesc partitionDesc) {
    partitionDesc.setBaseFileName(partitionDesc.getBaseFileName());
    partitionDesc.setPartSpec(partitionDesc.getPartSpec());
    partitionDesc.setInputFileFormatClass(partitionDesc.getInputFileFormatClass());
    partitionDesc.setOutputFileFormatClass(partitionDesc.getOutputFileFormatClass());
  }

  /**
   * We use a custom {@link com.esotericsoftware.kryo.Serializer} for {@link PartitionDesc} objects
   * in order to invoke any string interning code present in the "setter" methods. {@link
   * PartitionDesc} objects are usually stored by {@link MapWork} objects and contain duplicate info
   * like input format class names, partition specs, etc.
   */
  private static class PartitionDescSerializer extends FieldSerializer<PartitionDesc> {

    PartitionDescSerializer(Kryo kryo, Class type) {
      super(kryo, type);
    }

    @Override
    public PartitionDesc read(Kryo kryo, Input input, Class<? extends PartitionDesc> type) {
      PartitionDesc partitionDesc = super.read(kryo, input, type);
      // The set methods in PartitionDesc intern the any duplicate strings which is why we call them
      // during de-serialization
      internPartitionDesc(partitionDesc);
      return partitionDesc;
    }
  }

  /**
   * Serializes the plan.
   *
   * @param plan The plan, such as QueryPlan, MapredWork, etc.
   * @param out  The stream to write to.
   */
  public static void serializePlan(Object plan, OutputStream out) {
    serializePlan(plan, out, null);
  }

  @VisibleForTesting
  static void serializePlan(Object plan, OutputStream out, Configuration configuration) {
    Kryo kryo = borrowKryo(configuration);
    try {
      serializePlan(kryo, plan, out);
    } finally {
      releaseKryo(kryo);
    }
  }

  public static void serializePlan(Kryo kryo, Object plan, OutputStream out) {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.SERIALIZE_PLAN);
    LOG.info("Serializing " + plan.getClass().getSimpleName() + " using kryo");
    serializeObjectByKryo(kryo, plan, out);
    perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.SERIALIZE_PLAN);
  }

  /**
   * Deserializes the plan.
   *
   * @param in        The stream to read from.
   * @param planClass class of plan
   * @return The plan, such as QueryPlan, MapredWork, etc.
   */
  public static <T> T deserializePlan(InputStream in, Class<T> planClass) {
    Kryo kryo = borrowKryo();
    T result = null;
    try {
      result = deserializePlan(kryo, in, planClass);
    } finally {
      releaseKryo(kryo);
    }
    return result;
  }

  public static <T> T deserializePlan(Kryo kryo, InputStream in, Class<T> planClass) {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.DESERIALIZE_PLAN);
    T plan;
    LOG.info("Deserializing " + planClass.getSimpleName() + " using kryo");
    plan = deserializeObjectByKryo(kryo, in, planClass);
    perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.DESERIALIZE_PLAN);
    return plan;
  }

  // Prefix of every plan written by the Fory engine, so readers pick the engine from the data and not from the
  // configuration. A Kryo plan starts with a small reference-marker byte, not 'F'.
  private static final byte[] FORY_MAGIC = {'F', 'O', 'R', 'Y'};

  public static boolean isForyEngine(Configuration conf) {
    return conf != null
        && "fory".equals(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_PLAN_SERIALIZATION_ENGINE));
  }

  /**
   * Serializes a plan with the engine selected by hive.plan.serialization.engine in the given configuration.
   * Only for same-version, trusted plan data: never for metastore-bound or other untrusted payloads.
   */
  public static void serializePlanWithEngine(Configuration conf, Object plan, OutputStream out) {
    if (!isForyEngine(conf)) {
      serializePlan(plan, out, conf);
      return;
    }
    filterPlanProperties(conf, plan);
    serializeWithFory(plan, out);
  }

  private static void serializeWithFory(Object plan, OutputStream out) {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.SERIALIZE_PLAN);
    LOG.info("Serializing " + plan.getClass().getSimpleName() + " using fory");
    try {
      out.write(FORY_MAGIC);
      ForyPlanSerializer.serialize(Utilities.getSessionSpecifiedClassLoader(), plan, out);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.SERIALIZE_PLAN);
  }

  // In-memory clones pick the engine from the session but, like the kryo clones, never filter properties: the
  // filter mutates the plan it is given, which here is the live plan.
  private static void serializeForClone(Object plan, OutputStream out) {
    if (isForyEngine(sessionConf())) {
      serializeWithFory(plan, out);
    } else {
      serializePlan(plan, out);
    }
  }

  private static <T> T deserializeForClone(byte[] bytes, Class<T> planClass) {
    Configuration conf = sessionConf();
    InputStream in = new ByteArrayInputStream(bytes);
    return isForyEngine(conf) ? deserializePlanWithEngine(conf, in, planClass) : deserializePlan(in, planClass);
  }

  /**
   * Deserializes a plan written by {@link #serializePlanWithEngine}. The engine is detected from the data, so a
   * plan stays readable if the configuration changed (e.g. a rollback to kryo) after it was written.
   */
  public static <T> T deserializePlanWithEngine(Configuration conf, InputStream in, Class<T> planClass) {
    BufferedInputStream bin = new BufferedInputStream(in);
    boolean fory;
    try {
      bin.mark(FORY_MAGIC.length);
      byte[] head = new byte[FORY_MAGIC.length];
      fory = bin.readNBytes(head, 0, head.length) == head.length && Arrays.equals(head, FORY_MAGIC);
      if (!fory) {
        bin.reset();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (!fory) {
      return deserializePlan(bin, planClass);
    }
    if (globalHook != null || !kryoTypeHooks.isEmpty()) {
      // Fail closed: the hooks (e.g. the LLAP UDF whitelist) are only implemented for kryo.
      throw new UnsupportedOperationException("A fory serialized plan cannot be read where deserialization hooks "
          + "are installed (LLAP). Set " + HiveConf.ConfVars.HIVE_PLAN_SERIALIZATION_ENGINE.varname + "=kryo.");
    }
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.DESERIALIZE_PLAN);
    LOG.info("Deserializing " + planClass.getSimpleName() + " using fory");
    T plan = ForyPlanSerializer.deserialize(Utilities.getSessionSpecifiedClassLoader(), bin, planClass);
    internPartitionDescs(plan);
    perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.DESERIALIZE_PLAN);
    return plan;
  }

  /** Counterpart of PartitionDescSerializer.read, which Fory cannot hook per type. */
  private static void internPartitionDescs(Object plan) {
    MapWork mapWork = null;
    if (plan instanceof MapredWork) {
      mapWork = ((MapredWork) plan).getMapWork();
    } else if (plan instanceof MapWork) {
      mapWork = (MapWork) plan;
    }
    if (mapWork == null) {
      return;
    }
    Map<Path, PartitionDesc> byPath = mapWork.getPathToPartitionInfo();
    if (byPath != null) {
      byPath.values().forEach(SerializationUtilities::internPartitionDesc);
    }
    mapWork.getPartitionDescs().forEachRemaining(SerializationUtilities::internPartitionDesc);
  }

  /** Mirrors MapWorkSerializer, which Kryo registers for the exact MapWork class only. */
  private static void filterPlanProperties(Configuration conf, Object plan) {
    MapWork mapWork = null;
    if (plan instanceof MapredWork) {
      mapWork = ((MapredWork) plan).getMapWork();
    } else if (plan instanceof MapWork) {
      mapWork = (MapWork) plan;
    }
    if (mapWork != null && mapWork.getClass() == MapWork.class) {
      MapWorkSerializer.filterMapworkProperties(conf, mapWork);
    }
  }

  /** The configuration used to choose the engine for in-memory clones, which have no configuration of their own. */
  private static Configuration sessionConf() {
    SessionState ss = SessionState.get();
    return ss == null ? null : ss.getConf();
  }

  /**
   * Clones using the powers of XML. Do not use unless necessary.
   * @param plan The plan.
   * @return The clone.
   */
  public static MapredWork clonePlan(MapredWork plan) {
    // TODO: need proper clone. Meanwhile, let's at least keep this horror in one place
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.CLONE_PLAN);
    Operator<?> op = plan.getAnyOperator();
    CompilationOpContext ctx = (op == null) ? null : op.getCompilationOpContext();
    ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
    serializeForClone(plan, baos);
    MapredWork newPlan = deserializeForClone(baos.toByteArray(), MapredWork.class);
    // Restore the context.
    for (Operator<?> newOp : newPlan.getAllOperators()) {
      newOp.setCompilationOpContext(ctx);
    }
    perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.CLONE_PLAN);
    return newPlan;
  }

  /**
   * Clones using the powers of XML. Do not use unless necessary.
   * @param roots The roots.
   * @return The clone.
   */
  public static List<Operator<?>> cloneOperatorTree(List<Operator<?>> roots) {
    if (roots.isEmpty()) {
      return Collections.emptyList();
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
    CompilationOpContext ctx = roots.get(0).getCompilationOpContext();
    serializeForClone(roots, baos);
    @SuppressWarnings("unchecked")
    List<Operator<?>> result = deserializeForClone(baos.toByteArray(), roots.getClass());
    // Restore the context.
    LinkedList<Operator<?>> newOps = new LinkedList<>(result);
    while (!newOps.isEmpty()) {
      Operator<?> newOp = newOps.poll();
      newOp.setCompilationOpContext(ctx);
      List<Operator<?>> children = newOp.getChildOperators();
      if (children != null) {
        newOps.addAll(children);
      }
    }
    return result;
  }

  /**
   * Clones using the powers of XML. Do not use unless necessary.
   * @param plan The plan.
   * @return The clone.
   */
  public static BaseWork cloneBaseWork(BaseWork plan) {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.CLONE_PLAN);
    Operator<?> op = plan.getAnyRootOperator();
    CompilationOpContext ctx = (op == null) ? null : op.getCompilationOpContext();
    ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
    serializeForClone(plan, baos);
    BaseWork newPlan = deserializeForClone(baos.toByteArray(), plan.getClass());
    // Restore the context.
    for (Operator<?> newOp : newPlan.getAllOperators()) {
      newOp.setCompilationOpContext(ctx);
    }
    perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.CLONE_PLAN);
    return newPlan;
  }

  /**
   * @param plan Usually of type MapredWork, MapredLocalWork etc.
   * @param out stream in which serialized plan is written into
   */
  private static void serializeObjectByKryo(Kryo kryo, Object plan, OutputStream out) {
    Output output = new Output(out);
    kryo.setClassLoader(Utilities.getSessionSpecifiedClassLoader());
    kryo.writeObject(output, plan);
    output.close();
  }

  private static <T> T deserializeObjectByKryo(Kryo kryo, InputStream in, Class<T> clazz ) {
    Input inp = new Input(in);
    kryo.setClassLoader(Utilities.getSessionSpecifiedClassLoader());
    T t = kryo.readObject(inp,clazz);
    inp.close();
    return t;
  }

  /**
   * Serializes any object via Kryo. Type information will be serialized as well, allowing dynamic deserialization
   * without the need to pass the class.
   * @param object The object to serialize.
   * @return Bytes.
   */
  public static byte[] serializeObjectWithTypeInformation(Serializable object) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Kryo kryo = borrowKryo();
    try (Output output = new Output(baos)) {
      kryo.writeClassAndObject(output, object);
    } finally {
      releaseKryo(kryo);
    }
    return baos.toByteArray();
  }

  /**
   * Deserializes expression from Kryo.
   * @param bytes Bytes containing the expression.
   * @param isPartFilter ture if it is a partition filter
   * @return Expression; null if deserialization succeeded, but the result type is incorrect.
   */
  public static <T> T deserializeObjectWithTypeInformation(byte[] bytes,
      boolean isPartFilter) {
    KryoWithHooks kryo = (KryoWithHooks) borrowKryo();
    kryo.setRootType(isPartFilter ? ExprNodeDesc.class : null);
    try (Input inp = new Input(new ByteArrayInputStream(bytes))) {
      return (T) kryo.readClassAndObject(inp);
    } finally {
      releaseKryo(kryo);
    }
  }

  public static String serializeExpression(ExprNodeGenericFuncDesc expr) {
    return new String(Base64.encodeBase64(serializeObjectToKryo(expr)),
        StandardCharsets.UTF_8);
  }

  public static ExprNodeGenericFuncDesc deserializeExpression(String s) {
    byte[] bytes = Base64.decodeBase64(s.getBytes(StandardCharsets.UTF_8));
    return deserializeObjectFromKryo(bytes, ExprNodeGenericFuncDesc.class);
  }

  public static byte[] serializeObjectToKryo(Serializable object) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output output = new Output(baos);
    Kryo kryo = borrowKryo();
    try {
      kryo.writeObject(output, object);
    } finally {
      releaseKryo(kryo);
    }
    output.close();
    return baos.toByteArray();
  }

  public static <T> T deserializeObjectFromKryo(byte[] bytes, Class<T> clazz) {
    KryoWithHooks kryo = (KryoWithHooks) borrowKryo();
    kryo.setRootType(clazz);
    try (Input inp = new Input(new ByteArrayInputStream(bytes))) {
      return kryo.readObject(inp, clazz);
    } finally {
      releaseKryo(kryo);
    }
  }

  public static String serializeObject(Serializable expr) {
    return new String(Base64.encodeBase64(serializeObjectToKryo(expr)),
        StandardCharsets.UTF_8);
  }

  public static <T extends Serializable> T deserializeObject(String s,
                                                             Class<T> clazz) {
    return deserializeObjectFromKryo(
            Base64.decodeBase64(s.getBytes(StandardCharsets.UTF_8)), clazz);
  }

}
