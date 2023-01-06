/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.Pool;
import com.google.common.annotations.VisibleForTesting;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

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
    // default false, should be reset on release
    private boolean isExprNodeFirst = false;
    // total classes we have met during (de)serialization, should be reset on release
    private long classCounter = 0;

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static final class SerializerWithHook extends com.esotericsoftware.kryo.Serializer {
      private final com.esotericsoftware.kryo.Serializer old;
      private final Hook hook;

      private SerializerWithHook(com.esotericsoftware.kryo.Serializer old, Hook hook) {
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
        @SuppressWarnings("rawtypes") com.esotericsoftware.kryo.Serializer serializer) {
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
        @SuppressWarnings("rawtypes") com.esotericsoftware.kryo.Serializer serializer) {
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
    public com.esotericsoftware.kryo.Registration getRegistration(Class type) {
      // If PartitionExpressionForMetastore performs deserialization at remote HMS,
      // the first class encountered during deserialization must be an ExprNodeDesc,
      // throw exception to avoid potential security problem if it is not.
      if (isExprNodeFirst && classCounter == 0) {
        if (!ExprNodeDesc.class.isAssignableFrom(type)) {
          throw new UnsupportedOperationException(
              "The object to be deserialized must be an ExprNodeDesc, but encountered: " + type);
        }
      }
      classCounter++;
      return super.getRegistration(type);
    }

    public void setExprNodeFirst(boolean isPartFilter) {
      this.isExprNodeFirst = isPartFilter;
    }

    // reset the fields on release
    public void restore() {
      setConf(null);
      isExprNodeFirst = false;
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
      com.esotericsoftware.kryo.Serializer<Timestamp> {

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

  private static class TimestampTZSerializer extends com.esotericsoftware.kryo.Serializer<TimestampTZ> {

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
      com.esotericsoftware.kryo.Serializer<java.sql.Date> {

    @Override
    public java.sql.Date read(Kryo kryo, Input input, Class<? extends java.sql.Date> clazz) {
      return new java.sql.Date(input.readLong());
    }

    @Override
    public void write(Kryo kryo, Output output, java.sql.Date sqlDate) {
      output.writeLong(sqlDate.getTime());
    }
  }

  private static class PathSerializer extends com.esotericsoftware.kryo.Serializer<Path> {

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
  private static class ArrayListSubListSerializer extends com.esotericsoftware.kryo.Serializer<List<?>> {

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
      extends com.esotericsoftware.kryo.Serializer<List<?>> {

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
      com.esotericsoftware.kryo.serializers.MapSerializer<Map> {

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
      filterMapworkProperties(kryo, mapWork);
      super.write(kryo, output, mapWork);
    }

    private void filterMapworkProperties(Kryo kryo, MapWork mapWork) {
      Configuration configuration = ((KryoWithHooks) kryo).getConf();
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

    private boolean isRegex(String prop) {
      return prop.contains("*");
    }
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
      partitionDesc.setBaseFileName(partitionDesc.getBaseFileName());
      partitionDesc.setPartSpec(partitionDesc.getPartSpec());
      partitionDesc.setInputFileFormatClass(partitionDesc.getInputFileFormatClass());
      partitionDesc.setOutputFileFormatClass(partitionDesc.getOutputFileFormatClass());
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
    serializePlan(plan, baos);
    MapredWork newPlan = deserializePlan(new ByteArrayInputStream(baos.toByteArray()),
        MapredWork.class);
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
    serializePlan(roots, baos);
    @SuppressWarnings("unchecked")
    List<Operator<?>> result =
        deserializePlan(new ByteArrayInputStream(baos.toByteArray()),
            roots.getClass());
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
    serializePlan(plan, baos);
    BaseWork newPlan = deserializePlan(new ByteArrayInputStream(baos.toByteArray()),
        plan.getClass());
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
    kryo.setExprNodeFirst(isPartFilter);
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

  public static <T extends Serializable> T deserializeObjectFromKryo(byte[] bytes, Class<T> clazz) {
    Input inp = new Input(new ByteArrayInputStream(bytes));
    Kryo kryo = borrowKryo();
    T func = null;
    try {
      func = kryo.readObject(inp, clazz);
    } finally {
      releaseKryo(kryo);
    }
    inp.close();
    return func;
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
