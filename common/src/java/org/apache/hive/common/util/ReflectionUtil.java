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

package org.apache.hive.common.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import static java.lang.String.format;

/**
 * Same as Hadoop ReflectionUtils, but (1) does not leak classloaders (or shouldn't anyway, we
 * rely on Guava cache, and could fix it otherwise); (2) does not have a hidden epic lock.
 */
public class ReflectionUtil {

  // TODO: expireAfterAccess locks cache segments on put and expired get. It doesn't look too bad,
  //       but if we find some perf issues it might be a good idea to remove this - we are probably
  //       not caching that many constructors.
  // Note that weakKeys causes "==" to be used for key compare; this will only work
  // for classes in the same classloader. Should be ok in this case.
  private static final Cache<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE =
      CacheBuilder.newBuilder().expireAfterAccess(15, TimeUnit.MINUTES)
                               .concurrencyLevel(64)
                               .weakKeys().weakValues().build();
  private static final Class<?>[] EMPTY_ARRAY = new Class[] {};
  private static final Class<?> jobConfClass, jobConfigurableClass;
  private static final Method configureMethod;

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
   * Create an object for the given class and initialize it from conf
   * @param theClass class of which an object is created
   * @param conf Configuration
   * @return a new object
   */
  @SuppressWarnings("unchecked")
  public static <T> T newInstance(Class<T> theClass, Configuration conf) {
    T result;
    try {
      Constructor<?> ctor = CONSTRUCTOR_CACHE.getIfPresent(theClass);
      if (ctor == null) {
        ctor = theClass.getDeclaredConstructor(EMPTY_ARRAY);
        ctor.setAccessible(true);
        CONSTRUCTOR_CACHE.put(theClass, ctor);
      }
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
   * Sets a declared field in a given object.
   * Note: if you want to modify a field in a super class, use the {@link ReflectionUtil#setInAllFields } method.
   * @param object target instance
   * @param field name of the field to set
   * @param value new value
   * @throws RuntimeException in case the field is not found or cannot be set.
   */
  public static void setField(Object object, String field, Object value) {
    try {
      Field fieldToChange = object.getClass().getDeclaredField(field);
      fieldToChange.setAccessible(true);
      fieldToChange.set(object, value);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(format("Cannot set field %s in object %s", field, object.getClass()));
    }
  }

  /**
   * Sets a declared field in a given object. It finds the field, even if it is declared in a super class.
   * @param object target instance
   * @param field name of the field to set
   * @param value new value
   * @throws RuntimeException in case the field is not found or cannot be set.
   */
  public static void setInAllFields(Object object, String field, Object value) {
    try {
      Field fieldToChange = Arrays.stream(FieldUtils.getAllFields(object.getClass()))
              .filter(f -> f.getName().equals(field))
              .findFirst()
              .orElseThrow(NoSuchFieldException::new);

      fieldToChange.setAccessible(true);

      fieldToChange.set(object, value);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(format("Cannot set field %s in object %s", field, object.getClass()));
    }
  }
}
