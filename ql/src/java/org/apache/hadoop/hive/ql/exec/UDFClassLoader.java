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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import org.apache.hadoop.hive.ql.session.SessionState;


/**
 * {@link UDFClassLoader} is used to dynamically register
 * udf (and related) jars
 *
 * This was introducted to fix HIVE-11878
 *
 * Each session will have its own instance of {@link UDFClassLoader}
 * This is to support HiveServer2 where there can be multiple
 * active sessions. Addition/removal of jars/resources in one
 * session should not affect other sessions.
 */
public class UDFClassLoader extends URLClassLoader {
  private boolean isClosed;

  public UDFClassLoader(URL[] urls) {
    super(urls);
    isClosed = false;
  }

  public UDFClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
    isClosed = false;
  }

  @Override
  public void addURL(URL url) {
    Preconditions.checkState(!isClosed, getClass().getSimpleName() + " is already closed");
    super.addURL(url);
  }

  /**
   * See {@link URLClassLoader#close}
   */
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public void close() throws IOException {
    isClosed = true;
    super.close();
  }
}
