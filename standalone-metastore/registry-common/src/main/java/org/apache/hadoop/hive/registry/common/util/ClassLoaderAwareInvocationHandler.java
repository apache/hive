package org.apache.hadoop.hive.registry.common.util;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;


public class ClassLoaderAwareInvocationHandler implements InvocationHandler {
  private final ClassLoader classLoader;
  private final Object actualObject;

  public ClassLoaderAwareInvocationHandler(ClassLoader classLoader, Object actualObject) {
    this.classLoader = classLoader;
    this.actualObject = actualObject;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    ClassLoader oldCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(classLoader);
      return method.invoke(actualObject, args);
    } finally {
      Thread.currentThread().setContextClassLoader(oldCl);
    }
  }
}
