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
package org.apache.hadoop.hive.metastore.properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.hadoop.hive.metastore.properties.Serializer.SERIALIZER;

/**
 * The serialization proxy template.
 * <p>
 * This allows a class that defines final members to be made serializable in an easy way.
 * The class <em>must</em> implement:
 * <ul>
 * <li>a constructor that takes a DataInput (or derived class) as parameter</li>
 * <li>a write method that takes a DataOutput (or derived class) as parameter</li>
 * </ul>
 * <p>
 *   One should consider the constructor as being potentially fed with an invalid stream so
 *   all usual checks of a public constructor should apply.
 * </p>
 * Standard usage is to add the Serializable interface implementation through the following 2 methods:
 * <code>
 * private Object writeReplace() throws ObjectStreamException {
 *     return new SerializationProxy&lt;TheClass&gt;(this);
 * }
 * private void readObject(ObjectInputStream in)throws IOException,ClassNotFoundException{
 *     throw new InvalidObjectException("proxy required");
 * }
 * </code>
 * @param <T> the serializable object type
 */
public class SerializationProxy<T extends Serializable> implements Externalizable {
  /** Serial version. */
  private static final long serialVersionUID = 202212281757L;
  /** The logger. */
  public static final Logger LOGGER = LoggerFactory.getLogger(SerializationProxy.class);
  /** The map of class names to types. */
  private static final ConcurrentMap<String, Type<?>> TYPES = new ConcurrentHashMap<>();
  /** The list of registered pre-defined classes. */
  private static final List<Type<?>> REGISTERED = new ArrayList<>();
  /** A thread local context used for arguments passing during serialization/de-serialization. */
  private static final ThreadLocal<Object[]> EXTRA_ARGUMENTS = new ThreadLocal<>();

  /** The type of instance being read or written. */
  private transient Type<T> type = null;
  /** The instance being read or written. */
  private transient T proxied = null;

  /**
   * Wraps any error that may occur whilst using reflective calls.
   */
  public static class ProxyException extends RuntimeException {
    public ProxyException(Throwable cause) {
      super(cause);
    }

    public ProxyException(String msg) {
      super(msg);
    }

    /**
     * Convert an exception to a VDBRuntimeException.
     * @param cause the exception to convert
     * @return the wrapping CubeException
     */
    public static ProxyException convert(Throwable cause) {
      if (cause instanceof ProxyException) {
        return (ProxyException) cause;
      } else {
        return new ProxyException(cause);
      }
    }
  }

  /**
   * Constructor called from proxify.writeReplace().
   * @param proxify the instance to proxy
   */
  @SuppressWarnings("unchecked")
  public SerializationProxy(T proxify) {
    Class<T> clazz = (Class<T>) proxify.getClass();
    type = (Type<T>) TYPES.computeIfAbsent(clazz.getName(), this::createType);
    proxied = proxify;
  }

  /**
   * Default constructor.
   */
  public SerializationProxy() {
    // do nothing
  }

  /**
   * Sets the extra-arguments as a thread local context.
   * <p>Used to pass extra arguments o constructors/write methods.</p>
   * @param o the arguments
   */
  public static void setExtraArguments(Object[] o) {
    if (null == o) {
      EXTRA_ARGUMENTS.remove();
    } else {
      EXTRA_ARGUMENTS.set(o);
    }
  }

  /**
   * Gets the extra-arguments to ctor/write executable stored in a thread local context.
   * @return the arguments
   */
  public static Object[] getExtraArguments() {
    return EXTRA_ARGUMENTS.get();
  }

  /**
   * Swaps the thread local context.
   * <p>This may be used to stack up contexts during cascading calls.</p>
   * @param newArgs the new arguments
   * @return the down-stack caller arguments
   */
  public static Object[] swapExtraArguments(Object[] newArgs) {
    Object[] previous = EXTRA_ARGUMENTS.get();
    setExtraArguments(newArgs);
    return previous;
  }

  /**
   * Unloads the proxy.
   */
  public static void unload() {
    EXTRA_ARGUMENTS.remove();
    TYPES.clear();
  }

  /**
   * Registers a pre-defined class (known to be used throughout the whole application).
   * @param <T> the type
   * @param slot the slot number
   * @param clazz the class
   */
  public static <T extends Serializable> void registerType(final int slot, Class<T> clazz) {
    synchronized (REGISTERED) {
      Type<T> ntype = new Type<>(clazz);
      ntype.slot = slot;
      if (slot >= 255) {
        throw new IllegalArgumentException(ntype + "@" + slot + ": can not register more than 254 types");
      }
      List<Type<?>> types = REGISTERED;
      while (types.size() <= slot) {
        types.add(null);
      }
      if (types.get(slot) != null) {
        throw new IllegalArgumentException(ntype + "@" + slot + ": slot already used by " + types.get(slot));
      }
      types.set(slot, ntype);
      TYPES.put(clazz.getName(), ntype);
    }
  }

  /**
   * Called by serialization after readExternal.
   * @return the proxied instance
   * @throws IOException for signature compliance
   */
  public Object readResolve() throws IOException {
    return proxied;
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    long serial = in.readLong();
    if (serial != serialVersionUID) {
      throw new ProxyException("invalid serial version, got " + serial +", expected " + serialVersionUID);
    }
    type = readType(in);
    proxied = type.proxyNew(in);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeLong(serialVersionUID);
    writeType(type, out);
    type.proxyWrite(proxied, out);
  }

  /**
   * Converts a serializable object to an array of bytes.
   * @param serializable the object to serialize
   * @param args the proxy arguments
   * @return the array of bytes
   * @throws ProxyException on any underlying error
   */
  public static byte[] toBytes(Serializable serializable, Object... args) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(512);
    final Object[] stack = SerializationProxy.swapExtraArguments(args);
    try (ObjectOutput oos = new ObjectOutputStream(bos)) {
      oos.writeObject(serializable);
      oos.flush();
      return bos.toByteArray();
    } catch (IOException xany) {
      throw ProxyException.convert(xany);
    } finally {
      SerializationProxy.swapExtraArguments(stack);
    }
  }

  /**
   * Materialize a serializable object from an array of bytes.
   * @param bytes the bytes
   * @param args the proxy arguments
   * @return the object
   * @throws ProxyException on any underlying error
   */
  @SuppressWarnings("unchecked")
  public static <T extends Serializable> T fromBytes(byte[] bytes, Object... args) {
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    final Object[] stack = SerializationProxy.swapExtraArguments(args);
    try (ObjectInput ois = new ObjectInputStream(bis)) {
      return (T) ois.readObject();
    } catch (IOException | ClassNotFoundException | ClassCastException xany) {
      throw ProxyException.convert(xany);
    } finally {
      SerializationProxy.swapExtraArguments(stack);
    }
  }

  /**
   * Saves an object to persistent storage.
   * @param file the file to write to
   * @param persist the object to serialize
   * @param args the proxy constructor arguments
   * @return true if successful, false if file is null
   * @throws ProxyException in case of low level error
   */
  public static boolean write(File file, Serializable persist, Object... args) {
    return SERIALIZER.write(file, persist, args);
  }

  /**
   * Saves an object to persistent storage.
   * @param out the stream to write to
   * @param persist the object to serialize
   * @param args the proxy write method arguments
   * @return true if successful, false if file is null
   * @throws ProxyException in case of low level error
   */
  public static boolean write(OutputStream out, Serializable persist, Object... args) {
    return SERIALIZER.write(out, persist, args);
  }

  /**
   * Loads an object from the persistent storage.
   * @param file the file to read from
   * @param args the proxy arguments
   * @return the object or null if file is null
   * @throws ProxyException in case of low level error
   */
  public static Serializable read(File file, Object... args) {
    return SERIALIZER.read(file, args);
  }

  /**
   * Loads an object from the persistent storage.
   * @param in the stream to read from
   * @param args the proxy arguments
   * @return the object or null if file is null
   * @throws ProxyException in case of low level error
   */
  public static <T extends Serializable> T read(InputStream in, Object... args) {
    return SERIALIZER.read(in, args);
  }

  /**
   * Creates a Type using a class name.
   * @param cname the class name
   * @return a type instance
   * @throws ProxyException on any underlying error
   */
   protected Type<T> createType(String cname) {
    try {
      @SuppressWarnings("unchecked")
      Class<T> clazz = (Class<T>) Class.forName(cname);
      return new Type<>(clazz);
    } catch (ClassNotFoundException xnotfound) {
      throw ProxyException.convert(xnotfound);
    }
  }

  /**
   * When writing out this instance, write down the canonical class name it proxifies.
   * @param out the output
   * @throws IOException if things go wrong
   */
  protected void writeType(Type<?> type, DataOutput out) throws IOException {
    int slot = type.getSlot();
    out.write(slot);
    if (slot == 255) {
      out.writeUTF(type.getTargetName());
    }
  }

  /**
   * When reading an instance, fetch the type through the canonical class name that was persisted.
   * @param in the input
   * @throws IOException on read error
   * @throws ProxyException if class was expected to be registered but can not be found
   */
  @SuppressWarnings("unchecked")
  protected Type<T> readType(DataInput in) throws IOException {
    final Type<T> type;
    String className = "?";
    int slot = (int) in.readByte() & 0xff;
    if (slot == 255) {
      className = in.readUTF();
      type = (Type<T>) TYPES.computeIfAbsent(className, this::createType);
    } else if (slot < REGISTERED.size()) {
      type = (Type<T>) REGISTERED.get(slot);
    } else {
      type = null;
    }
    if (type == null) {
      throw new ProxyException("can not resolve class @ " + slot +", " + className);
    }
    return type;
  }

  /**
   * Encapsulates the mandatory constructor and write methods for a given proxified class.
   * @param <T> the proxified class
   */
  protected static class Type<T extends Serializable> {
    private final Constructor<T>[] ctors;
    private final Method[] writes;
    private transient int slot = 255;

    /**
     * Creates a new instance of type.
     * @param clazz the proxified class
     */
    public Type(Class<T> clazz) {
        ctors = typeConstructors(clazz);
        writes = typeWrites(clazz);
    }

    /**
     * The slot number if the class is registered.
     * @return the slot number, 255 means not-registered
     */
    public int getSlot() {
      return slot;
    }

    /**
     * @return the target class
     */
    public String getTargetName() {
      // there is always at least one ctor
      return ctors[0].getDeclaringClass().getName();
    }

    /**
     * Compare parameter signatures of executables.
     * @param lhs left-hand side
     * @param rhs right-hand side
     * @return 0 if equal, +/- 1 if left &lt;/&gt; than right
     */
    private static int compareSignatures(Executable lhs, Executable rhs) {
      return compareSignatures(lhs.getParameterTypes(), rhs.getParameterTypes());
    }

    /**
     * Compare executables parameter signatures.
     * @param lhs left-hand side executable
     * @param rhs right-hand side executable
     * @return 0 if equal, +/- 1 if left &lt;/&gt; than right
     */
    private static int compareSignatures(Class<?>[] lhs, Class<?>[] rhs) {
      if (lhs.length < rhs.length) {
        return -1;
      }
      if (lhs.length > rhs.length) {
        return 1;
      }
      int cmp = 0;
      // lhs.length == rhs.length
      final int length = lhs.length;
      for (int p = 0; p < length; ++p) {
        Class<?> actual = lhs[p];
        Class<?> formal = rhs[p];
        if (formal != null && actual != null && !formal.isAssignableFrom(actual)) {
          // if formal parameter is primitive and actual argument is compatible
          int dist;
          if (formal.isPrimitive() && (dist = CONVERTIBLES.get(formal).indexOf(actual)) >= 0) {
            cmp +=  dist;
            continue;
          }
          dist = formal.getName().compareTo(actual.getName());
          if (dist != 0) {
            return dist * (length - p);
          }
        }
      }
      return cmp;
    }

    /**
     * Primitive types to compatible classes.
     */
    private static final Map<Class<?>, List<Class<?>>> CONVERTIBLES;
    static {
      CONVERTIBLES = new HashMap<>(9);
      CONVERTIBLES.put(Void.TYPE, Collections.emptyList());
      CONVERTIBLES.put(Boolean.TYPE, Collections.singletonList(Boolean.class));
      CONVERTIBLES.put(Character.TYPE, Collections.singletonList(Character.class));
      CONVERTIBLES.put(Byte.TYPE, Collections.singletonList(Byte.class));
      CONVERTIBLES.put(Short.TYPE,
          Arrays.asList(Short.class, Byte.class));
      CONVERTIBLES.put(Integer.TYPE,
          Arrays.asList(Integer.class, Short.class, Byte.class));
      CONVERTIBLES.put(Long.TYPE,
          Arrays.asList(Long.class, Integer.class, Short.class, Byte.class));
      CONVERTIBLES.put(Float.TYPE,
          Arrays.asList(Float.class, Long.class, Integer.class, Short.class, Byte.class));
      CONVERTIBLES.put(Double.TYPE,
          Arrays.asList(Double.class, Float.class, Long.class, Integer.class, Short.class, Byte.class));
    }
    /**
     * Determines the constructors that take at least a DataInput (as superclass) as first argument.
     * @param clazz the class whose constructors we seek
     * @return the constructors
     * @param <T> the class type
     */
    private static <T> Constructor<T>[] typeConstructors(Class<T> clazz) {
      // find the constructors that use a derived DataInput instance as first parameter
      @SuppressWarnings("unchecked")
      Constructor<T>[] mctor = (Constructor<T>[]) clazz.getConstructors();
      List<Constructor<T>> keepers = new ArrayList<>(mctor.length);
      for (Constructor<T> kctor : mctor) {
        Class<?>[] parms = kctor.getParameterTypes();
        if (parms.length > 0 && DataInput.class.isAssignableFrom(parms[0])) {
          kctor.setAccessible(true);
          keepers.add(kctor);
        }
      }
      if (keepers.isEmpty()) {
        throw new ProxyException(clazz + ": serialization proxy can not find suitable constructor");
      }
      // order the array of ctors by comparing their number of parameters
      @SuppressWarnings("unchecked")
      Constructor<T>[] kctors = keepers.toArray((Constructor<T>[]) new Constructor<?>[0]);
      Arrays.sort(kctors, Type::compareSignatures);
      return kctors;
    }

    /**
     * Determines the write method that take at least a DataOutput (as superclass) as first argument.
     * @param clazz the class whose write methods we seek
     * @return the methods
     */
    private static Method[] typeWrites(Class<?> clazz) {
      // find the methods that use a derived DataOutput instance as first parameter
      Method[] mwrites = clazz.getDeclaredMethods();
      List<Method> keepers = new ArrayList<>(mwrites.length);
      for (Method kwrite : mwrites) {
        Class<?>[] parms = kwrite.getParameterTypes();
        if ("write".equals(kwrite.getName())) {
          if (parms.length > 0 && DataOutput.class.isAssignableFrom(parms[0])) {
            kwrite.setAccessible(true);
            keepers.add(kwrite);
          }
        }
      }
      if (keepers.isEmpty()) {
        throw new ProxyException(clazz + ": serialization proxy can not find suitable write method");
      }
      // order the array of write methods by comparing their signatures
      Method[] kwrites = keepers.toArray(new Method[0]);
      Arrays.sort(kwrites, Type::compareSignatures);
      return kwrites;
    }

    /**
     * Creates a new instance of the proxified class calling one mandatory ctors(DataInput).
     * @param in the input
     * @return the instance
     * @throws ProxyException if any error occurs
     */
    public T proxyNew(ObjectInput in) {
      final Object[] arguments = SerializationProxy.swapExtraArguments(null);
      try {
        final Object[] ctorArgs = makeArgs(in, arguments);
        final Constructor<T> ctor = findBest(ctors, ctorArgs);
        return ctor.newInstance(ctorArgs);
      } catch (InvocationTargetException xinvoke) {
        Throwable xtarget = xinvoke.getTargetException();
        throw ProxyException.convert(xtarget != null ? xtarget : xinvoke);
      } catch (InstantiationException | IllegalAccessException | IllegalArgumentException xany) {
        throw ProxyException.convert(xany);
      } finally {
        SerializationProxy.swapExtraArguments(arguments);
      }
    }

    /**
     * Writes a proxified instance through an output calling one mandatory method write(DataOutput).
     * @param proxy the proxy
     * @param out the out
     * @throws ProxyException if any error occurs
     */
    public void proxyWrite(T proxy, ObjectOutput out) {
      final Object[] arguments = SerializationProxy.swapExtraArguments(null);
      try {
        final Object[] writeArgs = makeArgs(out, arguments);
        final Method write = findBest(writes, writeArgs);
        write.invoke(proxy, writeArgs);
      } catch (InvocationTargetException xinvoke) {
        Throwable xtarget = xinvoke.getTargetException();
        throw ProxyException.convert(xtarget != null ? xtarget : xinvoke);
      } catch (IllegalAccessException | IllegalArgumentException xany) {
        throw ProxyException.convert(xany);
      } finally {
        SerializationProxy.swapExtraArguments(arguments);
      }
    }

    /**
     * Make an array of arguments.
     * @param stream the 1st argument
     * @param other the other arguments
     * @return all arguments
     */
    static Object[] makeArgs(final Object stream, final Object[] other) {
      if (other == null || other.length == 0) {
        return new Object[]{stream};
      }
      Object[] args = new Object[other.length + 1];
      args[0] = stream;
      System.arraycopy(other, 0, args, 1, other.length);
      return args;
    }

    /**
     * Seeks the best executable with or without extra-arguments besides stream.
     *
     * @param candidates the candidate executors
     * @param args the arguments
     * @return the best executable, never null but may be non-suitable for call
     * @param <X> constructor or method
     */
    private static <X extends Executable> X findBest(final X[] candidates, final Object[] args) {
      X exec = candidates[0];
      // if there is only one candidate, return it
      if (candidates.length > 1) {
        Class<?>[] parameters = new Class<?>[args.length];
        // get parameter classes from arguments
        for (int c = 0; c < args.length; ++c) {
          parameters[c] = args[c] == null ? null : args[c].getClass();
        }
        // find "best" executable
        for (X candidate : candidates) {
          if (compareSignatures(parameters, candidate.getParameterTypes()) == 0) {
            exec = candidate;
            break;
          }
        }
      }
      return exec;
    }
  }

}

