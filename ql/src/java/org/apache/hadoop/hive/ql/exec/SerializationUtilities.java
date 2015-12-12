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

import java.beans.DefaultPersistenceDelegate;
import java.beans.Encoder;
import java.beans.ExceptionListener;
import java.beans.Expression;
import java.beans.PersistenceDelegate;
import java.beans.Statement;
import java.beans.XMLDecoder;
import java.beans.XMLEncoder;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.runtime.CommonToken;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.plan.AbstractOperatorDesc;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
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
   * @param conf to pick which serialization format is desired.
   */
  public static void serializePlan(Object plan, OutputStream out, Configuration conf) {
    serializePlan(plan, out, conf, false);
  }

  public static void serializePlan(Kryo kryo, Object plan, OutputStream out, Configuration conf) {
    serializePlan(kryo, plan, out, conf, false);
  }

  private static void serializePlan(Object plan, OutputStream out, Configuration conf,
      boolean cloningPlan) {
    Kryo kryo = borrowKryo();
    try {
      serializePlan(kryo, plan, out, conf, cloningPlan);
    } finally {
      releaseKryo(kryo);
    }
  }

  private static void serializePlan(Kryo kryo, Object plan, OutputStream out, Configuration conf,
      boolean cloningPlan) {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.SERIALIZE_PLAN);
    String serializationType = conf.get(HiveConf.ConfVars.PLAN_SERIALIZATION.varname, "kryo");
    LOG.info("Serializing " + plan.getClass().getSimpleName() + " via " + serializationType);
    if ("javaXML".equalsIgnoreCase(serializationType)) {
      serializeObjectByJavaXML(plan, out);
    } else {
      if (cloningPlan) {
        serializeObjectByKryo(kryo, plan, out);
      } else {
        serializeObjectByKryo(kryo, plan, out);
      }
    }
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.SERIALIZE_PLAN);
  }

  /**
   * Deserializes the plan.
   *
   * @param in        The stream to read from.
   * @param planClass class of plan
   * @param conf      configuration
   * @return The plan, such as QueryPlan, MapredWork, etc.
   */
  public static <T> T deserializePlan(InputStream in, Class<T> planClass, Configuration conf) {
    return deserializePlan(in, planClass, conf, false);
  }

  public static <T> T deserializePlan(Kryo kryo, InputStream in, Class<T> planClass,
      Configuration conf) {
    return deserializePlan(kryo, in, planClass, conf, false);
  }

  private static <T> T deserializePlan(InputStream in, Class<T> planClass, Configuration conf,
      boolean cloningPlan) {
    Kryo kryo = borrowKryo();
    T result = null;
    try {
      result = deserializePlan(kryo, in, planClass, conf, cloningPlan);
    } finally {
      releaseKryo(kryo);
    }
    return result;
  }

  private static <T> T deserializePlan(Kryo kryo, InputStream in, Class<T> planClass,
      Configuration conf, boolean cloningPlan) {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.DESERIALIZE_PLAN);
    T plan;
    String serializationType = conf.get(HiveConf.ConfVars.PLAN_SERIALIZATION.varname, "kryo");
    LOG.info("Deserializing " + planClass.getSimpleName() + " via " + serializationType);
    if ("javaXML".equalsIgnoreCase(serializationType)) {
      plan = deserializeObjectByJavaXML(in);
    } else {
      if (cloningPlan) {
        plan = deserializeObjectByKryo(kryo, in, planClass);
      } else {
        plan = deserializeObjectByKryo(kryo, in, planClass);
      }
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
    Configuration conf = new HiveConf();
    serializePlan(plan, baos, conf, true);
    MapredWork newPlan = deserializePlan(new ByteArrayInputStream(baos.toByteArray()),
        MapredWork.class, conf, true);
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
    Configuration conf = new HiveConf();
    serializePlan(plan, baos, conf, true);
    BaseWork newPlan = deserializePlan(new ByteArrayInputStream(baos.toByteArray()),
        plan.getClass(), conf, true);
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.CLONE_PLAN);
    return newPlan;
  }

  /**
   * Serialize the object. This helper function mainly makes sure that enums,
   * counters, etc are handled properly.
   */
  private static void serializeObjectByJavaXML(Object plan, OutputStream out) {
    XMLEncoder e = new XMLEncoder(out);
    e.setExceptionListener(new ExceptionListener() {
      @Override
      public void exceptionThrown(Exception e) {
        LOG.warn(org.apache.hadoop.util.StringUtils.stringifyException(e));
        throw new RuntimeException("Cannot serialize object", e);
      }
    });
    // workaround for java 1.5
    e.setPersistenceDelegate(PlanUtils.ExpressionTypes.class, new EnumDelegate());
    e.setPersistenceDelegate(GroupByDesc.Mode.class, new EnumDelegate());
    e.setPersistenceDelegate(java.sql.Date.class, new DatePersistenceDelegate());
    e.setPersistenceDelegate(Timestamp.class, new TimestampPersistenceDelegate());

    e.setPersistenceDelegate(org.datanucleus.store.types.backed.Map.class, new MapDelegate());
    e.setPersistenceDelegate(org.datanucleus.store.types.backed.List.class, new ListDelegate());
    e.setPersistenceDelegate(CommonToken.class, new CommonTokenDelegate());
    e.setPersistenceDelegate(Path.class, new PathDelegate());

    e.writeObject(plan);
    e.close();
  }


  /**
   * Java 1.5 workaround. From http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=5015403
   */
  public static class EnumDelegate extends DefaultPersistenceDelegate {
    @Override
    protected Expression instantiate(Object oldInstance, Encoder out) {
      return new Expression(Enum.class, "valueOf", new Object[] {oldInstance.getClass(),
          ((Enum<?>) oldInstance).name()});
    }

    @Override
    protected boolean mutatesTo(Object oldInstance, Object newInstance) {
      return oldInstance == newInstance;
    }
  }

  public static class MapDelegate extends DefaultPersistenceDelegate {
    @Override
    protected Expression instantiate(Object oldInstance, Encoder out) {
      Map oldMap = (Map) oldInstance;
      HashMap newMap = new HashMap(oldMap);
      return new Expression(newMap, HashMap.class, "new", new Object[] {});
    }

    @Override
    protected boolean mutatesTo(Object oldInstance, Object newInstance) {
      return false;
    }

    @Override
    protected void initialize(Class<?> type, Object oldInstance, Object newInstance, Encoder out) {
      java.util.Collection oldO = (java.util.Collection) oldInstance;
      java.util.Collection newO = (java.util.Collection) newInstance;

      if (newO.size() != 0) {
        out.writeStatement(new Statement(oldInstance, "clear", new Object[] {}));
      }
      for (Iterator i = oldO.iterator(); i.hasNext();) {
        out.writeStatement(new Statement(oldInstance, "add", new Object[] {i.next()}));
      }
    }
  }

  public static class SetDelegate extends DefaultPersistenceDelegate {
    @Override
    protected Expression instantiate(Object oldInstance, Encoder out) {
      Set oldSet = (Set) oldInstance;
      HashSet newSet = new HashSet(oldSet);
      return new Expression(newSet, HashSet.class, "new", new Object[] {});
    }

    @Override
    protected boolean mutatesTo(Object oldInstance, Object newInstance) {
      return false;
    }

    @Override
    protected void initialize(Class<?> type, Object oldInstance, Object newInstance, Encoder out) {
      java.util.Collection oldO = (java.util.Collection) oldInstance;
      java.util.Collection newO = (java.util.Collection) newInstance;

      if (newO.size() != 0) {
        out.writeStatement(new Statement(oldInstance, "clear", new Object[] {}));
      }
      for (Iterator i = oldO.iterator(); i.hasNext();) {
        out.writeStatement(new Statement(oldInstance, "add", new Object[] {i.next()}));
      }
    }

  }

  public static class ListDelegate extends DefaultPersistenceDelegate {
    @Override
    protected Expression instantiate(Object oldInstance, Encoder out) {
      List oldList = (List) oldInstance;
      ArrayList newList = new ArrayList(oldList);
      return new Expression(newList, ArrayList.class, "new", new Object[] {});
    }

    @Override
    protected boolean mutatesTo(Object oldInstance, Object newInstance) {
      return false;
    }

    @Override
    protected void initialize(Class<?> type, Object oldInstance, Object newInstance, Encoder out) {
      java.util.Collection oldO = (java.util.Collection) oldInstance;
      java.util.Collection newO = (java.util.Collection) newInstance;

      if (newO.size() != 0) {
        out.writeStatement(new Statement(oldInstance, "clear", new Object[] {}));
      }
      for (Iterator i = oldO.iterator(); i.hasNext();) {
        out.writeStatement(new Statement(oldInstance, "add", new Object[] {i.next()}));
      }
    }

  }

  /**
   * DatePersistenceDelegate. Needed to serialize java.util.Date
   * since it is not serialization friendly.
   * Also works for java.sql.Date since it derives from java.util.Date.
   */
  public static class DatePersistenceDelegate extends PersistenceDelegate {

    @Override
    protected Expression instantiate(Object oldInstance, Encoder out) {
      Date dateVal = (Date)oldInstance;
      Object[] args = { dateVal.getTime() };
      return new Expression(dateVal, dateVal.getClass(), "new", args);
    }

    @Override
    protected boolean mutatesTo(Object oldInstance, Object newInstance) {
      if (oldInstance == null || newInstance == null) {
        return false;
      }
      return oldInstance.getClass() == newInstance.getClass();
    }
  }

  /**
   * TimestampPersistenceDelegate. Needed to serialize java.sql.Timestamp since
   * it is not serialization friendly.
   */
  public static class TimestampPersistenceDelegate extends DatePersistenceDelegate {
    @Override
    protected void initialize(Class<?> type, Object oldInstance, Object newInstance, Encoder out) {
      Timestamp ts = (Timestamp)oldInstance;
      Object[] args = { ts.getNanos() };
      Statement stmt = new Statement(oldInstance, "setNanos", args);
      out.writeStatement(stmt);
    }
  }

  /**
   * Need to serialize org.antlr.runtime.CommonToken
   */
  public static class CommonTokenDelegate extends PersistenceDelegate {
    @Override
    protected Expression instantiate(Object oldInstance, Encoder out) {
      CommonToken ct = (CommonToken)oldInstance;
      Object[] args = {ct.getType(), ct.getText()};
      return new Expression(ct, ct.getClass(), "new", args);
    }
  }

  public static class PathDelegate extends PersistenceDelegate {
    @Override
    protected Expression instantiate(Object oldInstance, Encoder out) {
      Path p = (Path)oldInstance;
      Object[] args = {p.toString()};
      return new Expression(p, p.getClass(), "new", args);
    }
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

  /**
   * De-serialize an object. This helper function mainly makes sure that enums,
   * counters, etc are handled properly.
   */
  @SuppressWarnings("unchecked")
  private static <T> T deserializeObjectByJavaXML(InputStream in) {
    XMLDecoder d = null;
    try {
      d = new XMLDecoder(in, null, null);
      return (T) d.readObject();
    } finally {
      if (null != d) {
        d.close();
      }
    }
  }

  private static <T> T deserializeObjectByKryo(Kryo kryo, InputStream in, Class<T> clazz ) {
    Input inp = new Input(in);
    kryo.setClassLoader(Utilities.getSessionSpecifiedClassLoader());
    T t = kryo.readObject(inp,clazz);
    inp.close();
    return t;
  }

  public static List<Operator<?>> cloneOperatorTree(Configuration conf, List<Operator<?>> roots) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
    serializePlan(roots, baos, conf, true);
    @SuppressWarnings("unchecked")
    List<Operator<?>> result =
        deserializePlan(new ByteArrayInputStream(baos.toByteArray()),
            roots.getClass(), conf, true);
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
