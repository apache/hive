/**
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
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.net.URI;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.plan.AbstractOperatorDesc;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SparkEdgeProperty;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantStructObjectInspector;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

/**
 * Utilities related to serialization and deserialization.
 */
public class SerializationUtilities {
  private static final String CLASS_NAME = SerializationUtilities.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  private static KryoFactory factory = new KryoFactory() {
    public Kryo create() {
      Kryo kryo = new Kryo();
      kryo.register(java.sql.Date.class, new SqlDateSerializer());
      kryo.register(java.sql.Timestamp.class, new TimestampSerializer());
      kryo.register(Path.class, new PathSerializer());
      kryo.register(Arrays.asList("").getClass(), new ArraysAsListSerializer());
      ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy())
          .setFallbackInstantiatorStrategy(
              new StdInstantiatorStrategy());
      removeField(kryo, Operator.class, "colExprMap");
      removeField(kryo, AbstractOperatorDesc.class, "statistics");
      kryo.register(MapWork.class);
      kryo.register(ReduceWork.class);
      kryo.register(TableDesc.class);
      kryo.register(UnionOperator.class);
      kryo.register(FileSinkOperator.class);
      kryo.register(HiveIgnoreKeyTextOutputFormat.class);
      kryo.register(StandardConstantListObjectInspector.class);
      kryo.register(StandardConstantMapObjectInspector.class);
      kryo.register(StandardConstantStructObjectInspector.class);
      kryo.register(SequenceFileInputFormat.class);
      kryo.register(RCFileInputFormat.class);
      kryo.register(HiveSequenceFileOutputFormat.class);
      kryo.register(SparkEdgeProperty.class);
      kryo.register(SparkWork.class);
      kryo.register(Pair.class);
      return kryo;
    }
  };

  // Bounded queue could be specified here but that will lead to blocking.
  // ConcurrentLinkedQueue is unbounded and will release soft referenced kryo instances under
  // memory pressure.
  private static KryoPool kryoPool = new KryoPool.Builder(factory).softReferences().build();

  /**
   * By default, kryo pool uses ConcurrentLinkedQueue which is unbounded. To facilitate reuse of
   * kryo object call releaseKryo() after done using the kryo instance. The class loader for the
   * kryo instance will be set to current thread's context class loader.
   *
   * @return kryo instance
   */
  public static Kryo borrowKryo() {
    Kryo kryo = kryoPool.borrow();
    kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
    return kryo;
  }

  /**
   * Release kryo instance back to the pool.
   *
   * @param kryo - kryo instance to be released
   */
  public static void releaseKryo(Kryo kryo) {
    kryoPool.release(kryo);
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
    public Timestamp read(Kryo kryo, Input input, Class<Timestamp> clazz) {
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

  /**
   * Custom Kryo serializer for sql date, otherwise Kryo gets confused between
   * java.sql.Date and java.util.Date while deserializing
   */
  private static class SqlDateSerializer extends
      com.esotericsoftware.kryo.Serializer<java.sql.Date> {

    @Override
    public java.sql.Date read(Kryo kryo, Input input, Class<java.sql.Date> clazz) {
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
    public Path read(Kryo kryo, Input input, Class<Path> type) {
      return new Path(URI.create(input.readString()));
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
    public List<?> read(final Kryo kryo, final Input input, final Class<List<?>> type) {
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
   * Serializes the plan.
   *
   * @param plan The plan, such as QueryPlan, MapredWork, etc.
   * @param out  The stream to write to.
   */
  public static void serializePlan(Object plan, OutputStream out) {
    serializePlan(plan, out, false);
  }

  public static void serializePlan(Kryo kryo, Object plan, OutputStream out) {
    serializePlan(kryo, plan, out, false);
  }

  private static void serializePlan(Object plan, OutputStream out, boolean cloningPlan) {
    Kryo kryo = borrowKryo();
    try {
      serializePlan(kryo, plan, out, cloningPlan);
    } finally {
      releaseKryo(kryo);
    }
  }

  private static void serializePlan(Kryo kryo, Object plan, OutputStream out, boolean cloningPlan) {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.SERIALIZE_PLAN);
    LOG.info("Serializing " + plan.getClass().getSimpleName() + " using kryo");
    if (cloningPlan) {
      serializeObjectByKryo(kryo, plan, out);
    } else {
      serializeObjectByKryo(kryo, plan, out);
    }
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.SERIALIZE_PLAN);
  }

  /**
   * Deserializes the plan.
   *
   * @param in        The stream to read from.
   * @param planClass class of plan
   * @return The plan, such as QueryPlan, MapredWork, etc.
   */
  public static <T> T deserializePlan(InputStream in, Class<T> planClass) {
    return deserializePlan(in, planClass, false);
  }

  public static <T> T deserializePlan(Kryo kryo, InputStream in, Class<T> planClass) {
    return deserializePlan(kryo, in, planClass, false);
  }

  private static <T> T deserializePlan(InputStream in, Class<T> planClass, boolean cloningPlan) {
    Kryo kryo = borrowKryo();
    T result = null;
    try {
      result = deserializePlan(kryo, in, planClass, cloningPlan);
    } finally {
      releaseKryo(kryo);
    }
    return result;
  }

  private static <T> T deserializePlan(Kryo kryo, InputStream in, Class<T> planClass,
      boolean cloningPlan) {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.DESERIALIZE_PLAN);
    T plan;
    LOG.info("Deserializing " + planClass.getSimpleName() + " using kryo");
    if (cloningPlan) {
      plan = deserializeObjectByKryo(kryo, in, planClass);
    } else {
      plan = deserializeObjectByKryo(kryo, in, planClass);
    }
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.DESERIALIZE_PLAN);
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
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.CLONE_PLAN);
    ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
    serializePlan(plan, baos, true);
    MapredWork newPlan = deserializePlan(new ByteArrayInputStream(baos.toByteArray()),
        MapredWork.class, true);
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.CLONE_PLAN);
    return newPlan;
  }

  /**
   * Clones using the powers of XML. Do not use unless necessary.
   * @param plan The plan.
   * @return The clone.
   */
  public static BaseWork cloneBaseWork(BaseWork plan) {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.CLONE_PLAN);
    ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
    serializePlan(plan, baos, true);
    BaseWork newPlan = deserializePlan(new ByteArrayInputStream(baos.toByteArray()),
        plan.getClass(), true);
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.CLONE_PLAN);
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

  public static List<Operator<?>> cloneOperatorTree(List<Operator<?>> roots) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
    serializePlan(roots, baos, true);
    @SuppressWarnings("unchecked")
    List<Operator<?>> result =
        deserializePlan(new ByteArrayInputStream(baos.toByteArray()),
            roots.getClass(), true);
    return result;
  }

  /**
   * Serializes expression via Kryo.
   * @param expr Expression.
   * @return Bytes.
   */
  public static byte[] serializeExpressionToKryo(ExprNodeGenericFuncDesc expr) {
    return serializeObjectToKryo(expr);
  }

  /**
   * Deserializes expression from Kryo.
   * @param bytes Bytes containing the expression.
   * @return Expression; null if deserialization succeeded, but the result type is incorrect.
   */
  public static ExprNodeGenericFuncDesc deserializeExpressionFromKryo(byte[] bytes) {
    return deserializeObjectFromKryo(bytes, ExprNodeGenericFuncDesc.class);
  }

  public static String serializeExpression(ExprNodeGenericFuncDesc expr) {
    try {
      return new String(Base64.encodeBase64(serializeExpressionToKryo(expr)), "UTF-8");
    } catch (UnsupportedEncodingException ex) {
      throw new RuntimeException("UTF-8 support required", ex);
    }
  }

  public static ExprNodeGenericFuncDesc deserializeExpression(String s) {
    byte[] bytes;
    try {
      bytes = Base64.decodeBase64(s.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException ex) {
      throw new RuntimeException("UTF-8 support required", ex);
    }
    return deserializeExpressionFromKryo(bytes);
  }

  private static byte[] serializeObjectToKryo(Serializable object) {
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

  private static <T extends Serializable> T deserializeObjectFromKryo(byte[] bytes, Class<T> clazz) {
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
    try {
      return new String(Base64.encodeBase64(serializeObjectToKryo(expr)), "UTF-8");
    } catch (UnsupportedEncodingException ex) {
      throw new RuntimeException("UTF-8 support required", ex);
    }
  }

  public static <T extends Serializable> T deserializeObject(String s, Class<T> clazz) {
    try {
      return deserializeObjectFromKryo(Base64.decodeBase64(s.getBytes("UTF-8")), clazz);
    } catch (UnsupportedEncodingException ex) {
      throw new RuntimeException("UTF-8 support required", ex);
    }
  }

}
