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
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class ExceptUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ExceptUtils.class.getName());

  // We even wrap a MetaException with itself to indicate where the exception was processed by the client.
  public static MetaException wrapMetastoreClientException(String methodName,
                                                           Throwable cause) {
    Throwable rootCause = cause;
    while (true) {
      Throwable nextCause = rootCause.getCause();
      if (nextCause == null) {
        break;
      }
      rootCause = nextCause;
    }
    String rootMsg = rootCause.getMessage();
    String msg = methodName + " error: " + rootCause.getClass().getName() + (rootMsg.isEmpty() ? "" : " " + rootMsg);
    MetaException me = new MetaException(msg);
    // Get rid of the call to wrapMetastoreClientException.
    ExceptUtils.removeFirstStackTraceEle(me);
    return me;
  }

  /*
   * Technically, you are not suppose to catch java.lang.Error (OOM, etc.) but it is unavoidable with wrap
   * exceptions like UndeclaredThrowableException.
   *
   * Attempt to create wrapper Error to show we intercepted it here, log, and throw. Otherwise, rethrow.
   */
  public static void handleFatalError(String operationName, Error error) {
    boolean isRethrow = true;
    Error wrappedError = null;
    try {
      wrappedError = new Error(operationName + " fatal error: ", error);
      // Get rid of the call to handleFatalError.
      ExceptUtils.removeFirstStackTraceEle(wrappedError);
      LOG.error("Fatal error for " + operationName + ": ", wrappedError);
      isRethrow = false;
    } catch (Throwable t) {
      // Suppress..
    }
    if (!isRethrow) {
      throw wrappedError;
    } else {
      throw error;
    }
  }

  public static class MethodInvokeResult {
    private final ResultKind resultKind;
    private final Exception exception;
    private final Error error;

    public enum ResultKind {
      BASE_DECLARED_METHOD_EXCEPTION,
      SUBCLASS_DECLARED_METHOD_EXCEPTION,
      OTHER_DECLARED_METHOD_EXCEPTION,
      UNDECLARED_METHOD_EXCEPTION,
      INVOCATION_EXCEPTION,
      UNEXPECTED_EXCEPTION,
      ERROR;
    }

    private MethodInvokeResult(ResultKind resultKind, Exception e) {
      this.resultKind = resultKind;
      exception = e;
      error = null;
    }

    private MethodInvokeResult(Error error) {
      resultKind = ResultKind.ERROR;
      exception = null;
      this.error = error;
    }

    public ResultKind getResultKind() {
      return resultKind;
    }

    public Exception getException() {
      return exception;
    }

    public Error getError() {
      return error;
    }

    public static MethodInvokeResult createBaseDeclaredMethodException(Exception e) {
      return new MethodInvokeResult(ResultKind.BASE_DECLARED_METHOD_EXCEPTION, e);
    }

    public static MethodInvokeResult createSubClassDeclaredMethodException(Exception e) {
      return new MethodInvokeResult(ResultKind.SUBCLASS_DECLARED_METHOD_EXCEPTION, e);
    }

    public static MethodInvokeResult createOtherDeclaredMethodException(Exception e) {
      return new MethodInvokeResult(ResultKind.OTHER_DECLARED_METHOD_EXCEPTION, e);
    }

    public static MethodInvokeResult createUndeclaredMethodException(Exception e) {
      return new MethodInvokeResult(ResultKind.UNDECLARED_METHOD_EXCEPTION, e);
    }

    public static MethodInvokeResult createInvocationException(Exception e) {
      return new MethodInvokeResult(ResultKind.INVOCATION_EXCEPTION, e);
    }

    public static MethodInvokeResult createUnexpectedException(Exception e) {
      return new MethodInvokeResult(ResultKind.UNEXPECTED_EXCEPTION, e);
    }

    public static MethodInvokeResult createError(Error error) {
      return new MethodInvokeResult(error);
    }
  }

  public static MethodInvokeResult evaluateMethodInvokeThrowable(Method method, Throwable throwable, Class<? extends Exception>... baseExcClasses) {
    if (throwable instanceof InvocationTargetException) {
      Throwable cause = throwable.getCause();
      if (cause instanceof Error) {
        return MethodInvokeResult.createError((Error) cause);
      }
      // A checked or unchecked Exception.
      Exception e = (Exception) cause;
      Class<?> excClass = e.getClass();

      /*
       * Determine if it is an Exception in the throws clause of the method (i.e. declared).
       * Be careful because a base and subclass can both be in the throws class.
       */
      Class<?>[] declareExceptionTypes = method.getExceptionTypes();

      // Segregate base classes from others.
      List<Class<?>> declBaseExcTypes = new ArrayList<Class<?>>();
      List<Class<?>> declSubClassExcTypes = new ArrayList<Class<?>>();
      List<Class<?>> declOtherExcTypes = new ArrayList<Class<?>>();
      for (Class<?> declExcType : declareExceptionTypes) {
        boolean isConsumed = false;
        for (Class<?> baseClass : baseExcClasses) {
          if (declExcType.equals(baseClass)) {
            declBaseExcTypes.add(declExcType);
            isConsumed = true;
            break;
          } else if (baseClass.isAssignableFrom(declExcType)) {
            declSubClassExcTypes.add(declExcType);
            isConsumed = true;
            break;
          }
        }
        if (!isConsumed) {
          declOtherExcTypes.add(declExcType);
        }
      }
      // Now look for matching explicit other classes. Might be a subclass of a base class, might not.
      for (Class<?> subClassDeclExcType : declSubClassExcTypes) {
        if (excClass.equals(subClassDeclExcType)) {
          return MethodInvokeResult.createSubClassDeclaredMethodException(e);
        }
      }
      // Now look for matching base classes.
      for (Class<?> baseDeclExcType : declBaseExcTypes) {
        if (excClass.equals(baseDeclExcType)) {
          return MethodInvokeResult.createBaseDeclaredMethodException(e);
        }
      }
      // Now look for matching other classes.
      for (Class<?> otherDeclExcType : declOtherExcTypes) {
        if (excClass.equals(otherDeclExcType)) {
          return MethodInvokeResult.createOtherDeclaredMethodException(e);
        }
      }

      // We leave it to the caller to classify the undeclared Exception.
      return MethodInvokeResult.createUndeclaredMethodException(e);
    } else if (throwable instanceof IllegalAccessException | throwable instanceof IllegalArgumentException) {
      return MethodInvokeResult.createInvocationException((Exception) throwable);
    } else {
      // Unexpected. Perhaps an NPE from a null parameter.
      if (throwable instanceof Error) {
        return MethodInvokeResult.createError((Error) throwable);
      }
      return MethodInvokeResult.createUnexpectedException((Exception) throwable);
    }
  }

  public static <T> T wrapCaughtException(T caughtExc) {
    Throwable caughtThrowable = (Throwable) caughtExc;
    Throwable wrapThrowable;
    try {
      Class<?> excClass = caughtExc.getClass();
      Constructor constructor = excClass.getConstructor(String.class);
      String msg = caughtThrowable.getMessage();

      wrapThrowable = (Throwable) constructor.newInstance(msg);
      wrapThrowable.initCause(caughtThrowable);
      ExceptUtils.removeFirstStackTraceEle(wrapThrowable);
    } catch (Exception e) {
      return caughtExc;
    }
    return (T) wrapThrowable;
  }

  public static void removeFirstStackTraceEle(Throwable t) {
    StackTraceElement[] stackTrace = t.getStackTrace();
    final int len = stackTrace.length;
    StackTraceElement[] newStackTrace = new StackTraceElement[len - 1];
    for (int i = 1; i < len; i++) {
      newStackTrace[i - 1] = stackTrace[i];
    }
    t.setStackTrace(newStackTrace);
  }
}
