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
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.thrift.TException;

import static java.util.Objects.requireNonNull;

/**
 * Provides a simplified way of handling complex exceptions instead of catching the targeted exceptions
 * differently for some transformations of exceptions.
 */
public final class ExceptionHandler {
  // The exception needs to handle
  private final Exception e;

  private ExceptionHandler(Exception e) {
    this.e = e;
  }

  public static ExceptionHandler handleException(Exception e) {
    requireNonNull(e, "Exception e is null");
    return new ExceptionHandler(e);
  }

  /**
   * Throws if the input exception is the instance of the input class
   */
  public <T extends Exception> ExceptionHandler throwIfInstance(Class<T> t) throws T {
    if (t.isInstance(e)) {
      throw t.cast(e);
    }
    return this;
  }

  /**
   * Throws if the input exception is the instance of the one in the input classes
   */
  public <T1 extends Exception,
          T2 extends Exception>
  ExceptionHandler throwIfInstance(
      Class<T1> te1,
      Class<T2> te2) throws T1, T2 {
    throwIfInstance(te1);
    throwIfInstance(te2);
    return this;
  }

  /**
   * Throws if the input exception is the instance of the one in the input classes
   */
  public <T1 extends Exception,
          T2 extends Exception,
          T3 extends Exception>
  ExceptionHandler throwIfInstance(
      Class<T1> te1,
      Class<T2> te2,
      Class<T3> te3) throws T1, T2, T3 {
    throwIfInstance(te1);
    throwIfInstance(te2);
    throwIfInstance(te3);
    return this;
  }

  /**
   * Converts the input exception to the target instance of class {@code target},
   * if the input exception is the instance of class {@code source}, throws the
   * converted target exception.
   */
  public <S extends Exception, T extends TException> ExceptionHandler
      convertIfInstance(Class<S> source, Class<T> target) throws T {
    T targetException = null;
    if (source.isInstance(e)) {
      try {
        targetException = JavaUtils.newInstance(target,
            new Class[]{String.class}, new Object[]{e.getMessage()});
      } catch (Exception ex) {
        // This should not happen
        throw new RuntimeException(ex);
      }
    }
    if (targetException != null) {
      throw targetException;
    }
    return this;
  }

  /**
   * Creates the MetaException with the given message,
   * if the input exception is the instance of the one in the input classes,
   * throws the created MetaException.
   */
  public ExceptionHandler toMetaExceptionIfInstance(String message, Class<?>... clzs)
      throws MetaException {
    if (clzs != null && clzs.length > 0) {
      for (Class<?> clz : clzs) {
        if (clz.isInstance(e)) {
          // throw MetaException if matches
          throw new MetaException(message);
        }
      }
    }
    return this;
  }

  /**
   * Throws if the input exception is the instance of MetaException, NoSuchObjectException or TException,
   * otherwise converts the exception to the MetaException and throws.
   */
  public static void rethrowException(Exception e) throws TException {
    throw handleException(e)
        .throwIfInstance(MetaException.class, NoSuchObjectException.class)
        .throwIfInstance(TException.class)
        .defaultMetaException();
  }

  /**
   * Throws if the input exception is the instance of MetaException or NoSuchObjectException,
   * otherwise converts the exception to the MetaException and throws.
   */
  public static void throwMetaException(Exception e) throws MetaException, NoSuchObjectException {
    throw handleException(e)
        .throwIfInstance(MetaException.class, NoSuchObjectException.class)
        .defaultMetaException();
  }

  /**
   * Converts the input exception to MetaException and returns.
   */
  public static MetaException newMetaException(Exception e) {
    if (e instanceof MetaException) {
      return (MetaException) e;
    }
    MetaException me = new MetaException(e.toString());
    me.initCause(e);
    return me;
  }

  /**
   * Converts the input exception to RuntimeException and returns.
   */
  public RuntimeException defaultRuntimeException() {
    if (e instanceof RuntimeException) {
      return (RuntimeException) e;
    }
    return new RuntimeException(e);
  }

  public MetaException defaultMetaException() {
    return newMetaException(e);
  }

  public TException defaultTException() {
    if (e instanceof TException) {
      return (TException) e;
    }
    return newMetaException(e);
  }

}
