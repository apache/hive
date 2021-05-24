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

public final class ExceptionHandler {
  private final Exception e;

  private ExceptionHandler(Exception e) {
    this.e = e;
  }

  public static ExceptionHandler handleException(Exception e) {
    requireNonNull(e, "Exception e is null");
    return new ExceptionHandler(e);
  }

  /**
   * Throws if the input e is the instance of the class clz
   */
  public <T extends Exception> ExceptionHandler
      throwIfInstance(Class<T> t) throws T {
    if (t.isInstance(e)) {
      throw t.cast(e);
    }
    return this;
  }

  /**
   * Throws if the input e is the instance of the class clzt or  class clze in order
   */
  public <T extends Exception, E extends Exception> ExceptionHandler
      throwIfInstance(Class<T> t, Class<E> e) throws T, E {
    throwIfInstance(t);
    throwIfInstance(e);
    return this;
  }

  /**
   * Throws if the input e is the instance of the class clzt or  clze or clzc in order
   */
  public <T extends Exception, E extends Exception, C extends Exception> ExceptionHandler
      throwIfInstance(Class<T> t, Class<E> e, Class<C> c) throws T, E, C {
    throwIfInstance(t);
    throwIfInstance(e);
    throwIfInstance(c);
    return this;
  }

  /**
   * Converts the input e if it is the instance of class from to the instance of class to and throws
   */
  public <T extends Exception, D extends TException> ExceptionHandler
      convertIfInstance(Class<T> from, Class<D> to) throws D {
    D targetException = null;
    if (from.isInstance(e)) {
      try {
        targetException = JavaUtils.newInstance(to, new Class[]{String.class}, new Object[]{e.getMessage()});
      } catch (Exception ex) {
        // this should not happen
        throw new RuntimeException(ex);
      }
    }
    if (targetException != null) {
      throw targetException;
    }

    return this;
  }

  /**
   * Converts the input e if it is the instance of classes to MetaException with the given message
   */
  public ExceptionHandler convertToMetaExIfInstance(String message, Class<?>... classes)
      throws MetaException {
    if (classes != null && classes.length > 0) {
      for (Class<?> clz : classes) {
        if (clz.isInstance(e)) {
          // throw the exception if matches
          throw new MetaException(message);
        }
      }
    }
    return this;
  }

  public static TException rethrowException(Exception e) throws TException {
    return handleException(e)
        .throwIfInstance(MetaException.class, NoSuchObjectException.class)
        .throwIfInstance(TException.class)
        .defaultMetaException();
  }

  public static void throwMetaException(Exception e) throws MetaException, NoSuchObjectException {
    throw handleException(e)
        .throwIfInstance(MetaException.class, NoSuchObjectException.class)
        .defaultMetaException();
  }

  public static MetaException newMetaException(Exception e) {
    if (e instanceof MetaException) {
      return (MetaException) e;
    }
    MetaException me = new MetaException(e.toString());
    me.initCause(e);
    return me;
  }

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
