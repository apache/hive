/**
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

package org.apache.hadoop.hive.ql.io;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;

/**
 * NOTE: before LLAP branch merge, there's no LLAP code here.
 * There used to be a global static map of IOContext-s inside IOContext (Hive style!).
 * Unfortunately, due to variety of factors, this is now a giant fustercluck.
 * 1) Spark doesn't apparently care about multiple inputs, but has multiple threads, so one
 *    threadlocal IOContext was added for it.
 * 2) LLAP has lots of tasks in the same process so globals no longer cut it either.
 * 3) However, Tez runs 2+ threads for one task (e.g. TezTaskEventRouter and TezChild), and these
 *    surprisingly enough need the same context. Tez, in its infinite wisdom, doesn't allow them
 *    to communicate in any way nor provide any shared context.
 * So we are going to...
 * 1) Keep the good ol' global map for MR and Tez. Hive style!
 * 2) Keep the threadlocal for Spark. Hive style!
 * 3) Create inheritable (TADA!) threadlocal with attemptId, only set in LLAP; that will propagate
 *    to all the little Tez threads, and we will keep a map per attempt. Hive style squared!
 */
public class IOContextMap {
  public static final String DEFAULT_CONTEXT = "";
  private static final Log LOG = LogFactory.getLog(IOContextMap.class);

  /** Used for Tez and MR */
  private static final ConcurrentHashMap<String, IOContext> globalMap =
      new ConcurrentHashMap<String, IOContext>();

  /** Used for Spark */
  private static final ThreadLocal<IOContext> sparkThreadLocal = new ThreadLocal<IOContext>(){
    @Override
    protected IOContext initialValue() { return new IOContext(); }
  };

  public static IOContext get(Configuration conf) {
    if (HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("spark")) {
      return sparkThreadLocal.get();
    }
    String inputName = conf.get(Utilities.INPUT_NAME);
    if (inputName == null) {
      inputName = DEFAULT_CONTEXT;
    }
    ConcurrentHashMap<String, IOContext> map;
    map = globalMap;

    IOContext ioContext = map.get(inputName);
    if (ioContext != null) return ioContext;
    ioContext = new IOContext();
    IOContext oldContext = map.putIfAbsent(inputName, ioContext);
    return (oldContext == null) ? ioContext : oldContext;
  }

  public static void clear() {
    sparkThreadLocal.remove();
    globalMap.clear();
  }
}
