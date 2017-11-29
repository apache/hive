/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.utils;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class JavaUtils {
  public static final Logger LOG = LoggerFactory.getLogger(JavaUtils.class);
  private static final Method configureMethod;
  private static final Class<?> jobConfClass, jobConfigurableClass;

  static {
    Class<?> jobConfClassLocal, jobConfigurableClassLocal;
    Method configureMethodLocal;
    try {
      jobConfClassLocal = Class.forName("org.apache.hadoop.mapred.JobConf");
      jobConfigurableClassLocal = Class.forName("org.apache.hadoop.mapred.JobConfigurable");
      configureMethodLocal = jobConfigurableClassLocal.getMethod("configure", jobConfClassLocal);
    } catch (Throwable t) {
      // Meh.
      jobConfClassLocal = jobConfigurableClassLocal = null;
      configureMethodLocal = null;
    }
    jobConfClass = jobConfClassLocal;
    jobConfigurableClass = jobConfigurableClassLocal;
    configureMethod = configureMethodLocal;
  }

  /**
   * Standard way of getting classloader in Hive code (outside of Hadoop).
   *
   * Uses the context loader to get access to classpaths to auxiliary and jars
   * added with 'add jar' command. Falls back to current classloader.
   *
   * In Hadoop-related code, we use Configuration.getClassLoader().
   * @return the class loader
   */
  public static ClassLoader getClassLoader() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = JavaUtils.class.getClassLoader();
    }
    return classLoader;
  }

  @SuppressWarnings(value = "unchecked")
  public static <T> Class<? extends T> getClass(String className, Class<T> clazz)
      throws MetaException {
    try {
      return (Class<? extends T>) Class.forName(className, true, getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new MetaException(className + " class not found");
    }
  }

  /**
   * Create an object of the given class.
   * @param theClass
   * @param parameterTypes
   *          an array of parameterTypes for the constructor
   * @param initargs
   *          the list of arguments for the constructor
   */
  public static <T> T newInstance(Class<T> theClass, Class<?>[] parameterTypes,
                                  Object[] initargs) {
    // Perform some sanity checks on the arguments.
    if (parameterTypes.length != initargs.length) {
      throw new IllegalArgumentException(
          "Number of constructor parameter types doesn't match number of arguments");
    }
    for (int i = 0; i < parameterTypes.length; i++) {
      Class<?> clazz = parameterTypes[i];
      if (initargs[i] != null && !(clazz.isInstance(initargs[i]))) {
        throw new IllegalArgumentException("Object : " + initargs[i]
            + " is not an instance of " + clazz);
      }
    }

    try {
      Constructor<T> meth = theClass.getDeclaredConstructor(parameterTypes);
      meth.setAccessible(true);
      return meth.newInstance(initargs);
    } catch (Exception e) {
      throw new RuntimeException("Unable to instantiate " + theClass.getName(), e);
    }
  }

  /**
   * Create an object of the given class using a no-args constructor
   * @param theClass class to return new object of
   * @param <T> the type of the class to be returned
   * @return an object of the requested type
   */
  public static <T> T newInstance(Class<T> theClass) {
    try {
      return theClass.newInstance();
    } catch (InstantiationException|IllegalAccessException e) {
      throw new RuntimeException("Unable to instantiate " + theClass.getName(), e);
    }
  }
  private static final Class<?>[] EMPTY_ARRAY = new Class[] {};
  /**
   * Create an object for the given class and initialize it from conf
   * @param theClass class of which an object is created
   * @param conf Configuration
   * @return a new object
   */
  @SuppressWarnings("unchecked")
  public static <T> T newInstance(Class<T> theClass, Configuration conf) {
    T result;
    try {
      // TODO Do we need a constructor cache like Hive here?
      Constructor<?> ctor = theClass.getDeclaredConstructor(EMPTY_ARRAY);
      ctor.setAccessible(true);
      result = (T)ctor.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    setConf(result, conf);
    return result;
  }

  /**
   * Check and set 'configuration' if necessary.
   * 
   * @param theObject object for which to set configuration
   * @param conf Configuration
   */
  public static void setConf(Object theObject, Configuration conf) {
    if (conf != null) {
      if (theObject instanceof Configurable) {
        ((Configurable) theObject).setConf(conf);
      }
      setJobConf(theObject, conf);
    }
  }

  private static void setJobConf(Object theObject, Configuration conf) {
    if (configureMethod == null) return;
    try {
      if (jobConfClass.isAssignableFrom(conf.getClass()) &&
            jobConfigurableClass.isAssignableFrom(theObject.getClass())) {
        configureMethod.invoke(theObject, conf);
      }
    } catch (Exception e) {
      throw new RuntimeException("Error in configuring object", e);
    }
  }
  /**
   * @return name of current host
   */
  public static String hostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Unable to resolve my host name " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  /**
   * Utility method for ACID to normalize logging info.  Matches
   * org.apache.hadoop.hive.metastore.api.LockRequest#toString
   */
  public static String lockIdToString(long extLockId) {
    return "lockid:" + extLockId;
  }
  /**
   * Utility method for ACID to normalize logging info.  Matches
   * org.apache.hadoop.hive.metastore.api.LockResponse#toString
   */
  public static String txnIdToString(long txnId) {
    return "txnid:" + txnId;
  }
}
