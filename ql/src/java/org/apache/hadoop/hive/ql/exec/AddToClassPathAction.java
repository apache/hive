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

package org.apache.hadoop.hive.ql.exec;

import java.net.URL;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * Helper class to create UDFClassLoader when running under a security manager. To create a class loader:
 * > AddToClassPathAction addAction = new AddToClassPathAction(parentLoader, newPaths, true);
 * > UDFClassLoader childClassLoader = AccessController.doPrivileged(addAction);
 * To try to add to the class path of the existing class loader; call the above without forceNewClassLoader=true.
 * Note that a class loader might be still created as fallback method.
 * <p>
 * This is slightly inconvenient, but forces the caller code to make the doPriviliged call, rather than us making the
 * call on the caller's behalf, in accordance with the security guidelines at:
 * https://docs.oracle.com/javase/8/docs/technotes/guides/security/doprivileged.html
 */
public class AddToClassPathAction implements PrivilegedAction<UDFClassLoader> {

  private final ClassLoader parentLoader;
  private final Collection<String> newPaths;
  private final boolean forceNewClassLoader;

  public AddToClassPathAction(ClassLoader parentLoader, Collection<String> newPaths, boolean forceNewClassLoader) {
    this.parentLoader = parentLoader;
    this.newPaths = newPaths != null ? newPaths : Collections.emptyList();
    this.forceNewClassLoader = forceNewClassLoader;
    if (parentLoader == null) {
      throw new IllegalArgumentException("UDFClassLoader is not designed to be a bootstrap class loader!");
    }
  }

  public AddToClassPathAction(ClassLoader parentLoader, Collection<String> newPaths) {
    this(parentLoader, newPaths, false);
  }

  @Override
  public UDFClassLoader run() {
    if (useExistingClassLoader()) {
      final UDFClassLoader udfClassLoader = (UDFClassLoader) parentLoader;
      for (String path : newPaths) {
        udfClassLoader.addURL(Utilities.urlFromPathString(path));
      }
      return udfClassLoader;
    } else {
      return createUDFClassLoader();
    }
  }

  private boolean useExistingClassLoader() {
    if (!forceNewClassLoader && parentLoader instanceof UDFClassLoader) {
      final UDFClassLoader udfClassLoader = (UDFClassLoader) parentLoader;
      // The classloader may have been closed, Cannot add to the same instance
      return !udfClassLoader.isClosed();
    }
    // Cannot use the same classloader if it is not an instance of {@code UDFClassLoader}, or new loader was explicily
    // requested
    return false;
  }

  private UDFClassLoader createUDFClassLoader() {
    return new UDFClassLoader(newPaths.stream()
        .map(Utilities::urlFromPathString)
        .filter(Objects::nonNull)
        .toArray(URL[]::new), parentLoader);
  }
}
