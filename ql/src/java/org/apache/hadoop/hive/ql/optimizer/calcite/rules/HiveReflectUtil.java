/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import com.google.common.collect.ImmutableList;
import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectUtil.MethodDispatcher;
import org.apache.calcite.util.ReflectiveVisitor;

/**
 * Static utilities for Java reflection. This is based on Calcite
 * {@link ReflectUtil}. It contains methods to wrap a Calcite dispatcher
 * (based on reflection) into a Hive dispatcher as well as a Hive
 * dispatcher implementation based on {@link LambdaMetafactory}.
 */
public class HiveReflectUtil {

  /**
   * Creates a Hive dispatcher that wraps a Calcite one.
   */
  protected static <T, E> MethodDispatcherWrapper<T, E> createCalciteMethodDispatcherWrapper(
      final MethodDispatcher<T> methodDispatcher) {
    return new MethodDispatcherWrapper<>(methodDispatcher);
  }

  /**
   * Creates a dispatcher for calls to a single multi-method on a particular
   * object.
   *
   * <p>Calls to that multi-method are resolved by looking for a method on
   * the runtime type of that object, with the required name, and with
   * the correct type or a subclass for the first argument, and precisely the
   * same types for other arguments.
   *
   * <p>For instance, a dispatcher created for the method
   *
   * <blockquote>String foo(Vehicle, int, List)</blockquote>
   *
   * <p>could be used to call the methods
   *
   * <blockquote>String foo(Car, int, List)<br>
   * String foo(Bus, int, List)</blockquote>
   *
   * <p>(because Car and Bus are subclasses of Vehicle, and they occur in the
   * polymorphic first argument) but not the method
   *
   * <blockquote>String foo(Car, int, ArrayList)</blockquote>
   *
   * <p>(only the first argument is polymorphic).
   *
   * <p>You must create an implementation of the method for the base class.
   * Otherwise throws {@link IllegalArgumentException}.
   *
   * @param returnClazz     Return type of method
   * @param visitor         Object on which to invoke the method
   * @param methodName      Name of method
   * @param arg0Clazz       Base type of argument zero
   * @param otherArgClasses Types of remaining arguments
   */
  protected static <E, T> HiveMethodDispatcher<T, E> createMethodDispatcher(
      final Class<T> returnClazz,
      final ReflectiveVisitor visitor,
      final String methodName,
      final Class<E> arg0Clazz,
      final Class... otherArgClasses) {
    final List<Class> otherArgClassList =
        ImmutableList.copyOf(otherArgClasses);
    final VisitDispatcher<ReflectiveVisitor, E> dispatcher =
        createDispatcher((Class<ReflectiveVisitor>) visitor.getClass(), arg0Clazz);
    return new HiveMethodDispatcher<>(dispatcher, returnClazz, visitor, methodName,
        arg0Clazz, otherArgClassList);
  }

  /**
   * Creates a dispatcher for calls to {@link VisitDispatcher#lookupVisitFunc}. The
   * dispatcher caches methods between invocations and it is thread-safe.
   *
   * @param visitorBaseClazz Visitor base class
   * @param visiteeBaseClazz Visitee base class
   * @return cache of methods
   */
  private static <R extends ReflectiveVisitor, E> VisitDispatcher<R, E> createDispatcher(
      final Class<R> visitorBaseClazz,
      final Class<E> visiteeBaseClazz) {
    assert ReflectiveVisitor.class.isAssignableFrom(visitorBaseClazz);
    assert Object.class.isAssignableFrom(visiteeBaseClazz);
    return new VisitDispatcher<>();
  }

  private static Class<? extends VarArgsFunc> getVarArgsFuncClass(int length) {
    switch (length) {
      case 1:
        return VarArgsFunc1.class;
      case 2:
        return VarArgsFunc2.class;
      case 3:
        return VarArgsFunc3.class;
      case 4:
        return VarArgsFunc4.class;
      default:
        throw new RuntimeException("Unsupported function with length " + length);
    }
  }

  private static VarArgsFunc getVarArgsFunc(int length, CallSite site) throws Throwable {
    switch (length) {
      case 1:
        return (VarArgsFunc1) site.getTarget().invokeExact();
      case 2:
        return (VarArgsFunc2) site.getTarget().invokeExact();
      case 3:
        return (VarArgsFunc3) site.getTarget().invokeExact();
      case 4:
        return (VarArgsFunc4) site.getTarget().invokeExact();
      default:
        throw new RuntimeException("Unsupported function with length " + length);
    }
  }

  protected static class VisitDispatcher<R extends ReflectiveVisitor, E> {
    final Map<List<Object>, VarArgsFunc> map = new ConcurrentHashMap<>();

    public VarArgsFunc lookupVisitFunc(
        Class<? extends R> visitorClass,
        Class<? extends E> visiteeClass,
        String visitMethodName,
        List<Class> additionalParameterTypes)
          throws Throwable {
      final List<Object> key =
          ImmutableList.of(
              visitorClass,
              visiteeClass,
              visitMethodName,
              additionalParameterTypes);
      VarArgsFunc method = map.get(key);
      if (method == null) {
        if (map.containsKey(key)) {
          // We already looked for the method and found nothing.
        } else {
          Method method1 =
              ReflectUtil.lookupVisitMethod(
                  visitorClass,
                  visiteeClass,
                  visitMethodName,
                  additionalParameterTypes);
          MethodHandles.Lookup lookup = MethodHandles.lookup();
          MethodHandle methodHandle = lookup.unreflect(method1);
          int argsLength = 1 + method1.getParameterTypes().length;
          MethodType invokedType = MethodType.methodType(
              getVarArgsFuncClass(argsLength));
          MethodType functionMethodType = MethodType.methodType(
              method1.getReturnType(), visitorClass, method1.getParameterTypes());
          CallSite site = LambdaMetafactory.metafactory(
              lookup,
              "apply",
              invokedType,
              functionMethodType.generic(),
              methodHandle,
              methodHandle.type());
          method = getVarArgsFunc(argsLength, site);
          map.put(key, method);
        }
      }
      return method;
    }
  }

  protected static class HiveMethodDispatcher<T, E> implements ClassMethodDispatcher<T, E> {

    private final VisitDispatcher<ReflectiveVisitor, E> dispatcher;
    private final Class<T> returnClazz;
    private final ReflectiveVisitor visitor;
    private final String methodName;
    private final Class<E> arg0Clazz;
    private final List<Class> otherArgClassList;

    public HiveMethodDispatcher (
        final VisitDispatcher<ReflectiveVisitor, E> dispatcher,
        final Class<T> returnClazz,
        final ReflectiveVisitor visitor,
        final String methodName,
        final Class<E> arg0Clazz,
        final List<Class> otherArgClassList) {
      this.dispatcher = dispatcher;
      this.returnClazz = returnClazz;
      this.visitor = visitor;
      this.methodName = methodName;
      this.arg0Clazz = arg0Clazz;
      this.otherArgClassList = otherArgClassList;
    }

    @Override
    public T invoke(Object... args) {
      VarArgsFunc method = null;
      try {
        method = lookupVisitFunc(args[0]);
        final Object o = method.apply(visitor, args[0], args[1], args[2]);
        return returnClazz.cast(o);
      } catch (Throwable e) {
        throw new RuntimeException("While invoking method " +
            (method != null ? "'" + method + "'" : ""),
            e);
      }
    }

    private VarArgsFunc lookupVisitFunc(final Object arg0) throws Throwable {
      if (!arg0Clazz.isInstance(arg0)) {
        throw new IllegalArgumentException();
      }
      VarArgsFunc method =
          dispatcher.lookupVisitFunc(
              visitor.getClass(),
              (Class<? extends E>) arg0.getClass(),
              methodName,
              otherArgClassList);
      if (method == null) {
        List<Class> classList = new ArrayList<>();
        classList.add(arg0Clazz);
        classList.addAll(otherArgClassList);
        throw new IllegalArgumentException("Method not found: " + methodName
            + "(" + classList + ")");
      }
      return method;
    }

    @Override
    public void register(Iterable<Class<? extends E>> classes)
        throws Throwable {
      for (Class<? extends E> c : classes) {
        VarArgsFunc method =
            dispatcher.lookupVisitFunc(
                visitor.getClass(),
                c,
                methodName,
                otherArgClassList);
        if (method == null) {
          List<Class> classList = new ArrayList<>();
          classList.add(arg0Clazz);
          classList.addAll(otherArgClassList);
          throw new IllegalArgumentException("Method not found: " + methodName
              + "(" + classList + ")");
        }
      }
    }
  }

  private static class MethodDispatcherWrapper<T, E> implements ClassMethodDispatcher<T, E> {

    private final MethodDispatcher<T> methodDispatcher;

    public MethodDispatcherWrapper (
        final MethodDispatcher<T> methodDispatcher) {
      this.methodDispatcher = methodDispatcher;
    }

    @Override
    public T invoke(Object... args) {
      return this.methodDispatcher.invoke(args);
    }
  }

  public interface ClassMethodDispatcher<T, E> extends MethodDispatcher<T> {
    default void register(Iterable<Class<? extends E>> classes) throws Throwable  {
      // Do nothing by default
    }
  }

  @FunctionalInterface
  private interface VarArgsFunc1<T, R> extends VarArgsFunc<R> {
    default R apply(Object... args) {
      return apply((T) args[0]);
    }

    R apply(T t);
  }

  @FunctionalInterface
  private interface VarArgsFunc2<T, U, R> extends VarArgsFunc<R> {
    default R apply(Object... args) {
      return apply((T) args[0], (U) args[1]);
    }

    R apply(T t, U u);
  }

  @FunctionalInterface
  private interface VarArgsFunc3<T, U, V, R> extends VarArgsFunc<R> {
    default R apply(Object... args) {
      return apply((T) args[0], (U) args[1], (V) args[2]);
    }

    R apply(T t, U u, V v);
  }

  @FunctionalInterface
  private interface VarArgsFunc4<T, U, V, W, R> extends VarArgsFunc<R> {
    default R apply(Object... args) {
      return apply((T) args[0], (U) args[1], (V) args[2], (W) args[3]);
    }

    R apply(T t, U u, V v, W w);
  }

  private interface VarArgsFunc<R> {
    default R apply(Object... args) {
      throw new UnsupportedOperationException();
    }
  }

}
