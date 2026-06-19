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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.file.Files;

/**
 * A helper class to read/write objects through the SerializationProxy.
 *
 * @param <T>
 */
public class Serializer<T extends Serializable> {
  private static final Logger LOGGER = SerializationProxy.LOGGER;

  /** The default instance of serializer. */
  static final Serializer<Serializable> SERIALIZER = new Serializer<>();
  /**
   * Default constructor.
   */
  public Serializer() {
  }

  /**
   * Called when an error occurs during read.
   * <p>Default is to throw a ProxyException.</p>
   *
   * @param xany an exception
   * @param msg the message
   */
  public void readError(Exception xany, String msg) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(msg, xany);
    }
    throw SerializationProxy.ProxyException.convert(xany);
  }

  /**
   * Called when a warning occurs during read.
   * <p>Default is to log a warning through the SerializationProxy logger.</p>
   *
   * @param xany an exception
   * @param msg the message
   */
  public void readWarning(Exception xany, String msg) {
    LOGGER.warn(msg, xany);
  }

  /**
   * Called when an error occurs during write.
   * <p>Default is to throw a ProxyException.</p>
   *
   * @param xany an exception
   * @param msg the message
   */
  public void writeError(Exception xany, String msg) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(msg, xany);
    }
    throw SerializationProxy.ProxyException.convert(xany);
  }

  /**
   * Called when an error occurs during write.
   * <p>Default is to log a warning through the SerializationProxy logger.</p>
   *
   * @param xany an exception
   * @param msg the message
   */
  public void writeWarning(Exception xany, String msg) {
    SerializationProxy.LOGGER.warn(msg, xany);
  }

  /**
   * Saves an object to persistent storage.
   *
   * @param <E> the class of the object to persist
   * @param file    the file to write to; it will be created if it does not exist, overwritten otherwise
   * @param persist the object to serialize
   * @param args    the proxy arguments
   * @return true if successful, false if file is null
   * @throws SerializationProxy.ProxyException in case of low level error
   */
  public final <E extends T> boolean write(File file, E persist, Object... args) {
    if (file != null) {
      try (OutputStream out = Files.newOutputStream(file.toPath())) {
        return write(out, persist, args);
      } catch (IOException xio) {
        writeError(xio, "error writing " + file);
      }
    }
    return false;
  }

  /**
   * Saves an object to persistent storage.
   *
   * @param <E> the class of the object to persist
   * @param out     the stream to write to
   * @param persist the object to serialize
   * @param args    the proxy arguments
   * @return true if successful, false if file is null
   * @throws SerializationProxy.ProxyException in case of low level error
   */
  public final <E extends T> boolean write(OutputStream out, E persist, Object... args) {
    if (out != null) {
      ObjectOutput oos = null;
      final Object[] stack = SerializationProxy.swapExtraArguments(args);
      try {
        oos = out instanceof ObjectOutputStream? (ObjectOutputStream) out : new ObjectOutputStream(out);
        oos.writeObject(persist);
        return true;
      } catch (IOException xio) {
        writeError(xio, "error writing stream " + out);
      } finally {
        SerializationProxy.swapExtraArguments(stack);
        if (oos != null) {
          try {
            oos.close();
          } catch (IOException xio) {
            writeWarning(xio, "closing object stream: ");
          }
        }
      }
    }
    return false;
  }

  /**
   * Loads an object from the persistent storage.
   *
   * @param <E> the class of the object to read
   * @param file the file to read from
   * @param args the proxy arguments
   * @return the object or null if file is null
   * @throws SerializationProxy.ProxyException in case of low level error
   */
  public final <E extends T> E read(File file, Object... args) {
    if (file != null && file.exists() && file.length() > 0) {
      try(InputStream in = Files.newInputStream(file.toPath())) {
        return read(in, args);
      } catch (ClassCastException | IOException xany) {
        readError(xany, "error reading " + file);
      }
    }
    return null;
  }

  /**
   * Loads an object from the persistent storage.
   *
   * @param <E> the class of the object to read
   * @param in   the stream to read from
   * @param args the proxy arguments
   * @return the object or null if file is null
   * @throws SerializationProxy.ProxyException in case of low level error
   */
  public final <E extends T> E read(InputStream in, Object... args) {
    if (in != null) {
      ObjectInput ois = null;
      Object[] stack = SerializationProxy.swapExtraArguments(args);
      try {
        ois = in instanceof ObjectInputStream? (ObjectInputStream) in : new ObjectInputStream(in);
        @SuppressWarnings("unchecked")
        E t = (E) ois.readObject();
        return t;
      } catch (ClassCastException | ClassNotFoundException | IOException xany) {
        readError(xany, "error reading " + in);
      } finally {
        SerializationProxy.swapExtraArguments(stack);
        if (ois != null) {
          try {
            ois.close();
          } catch (IOException xio) {
            readWarning(xio, "closing object stream: ");
          }
        }
      }
    }
    return null;
  }
}
