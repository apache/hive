/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.daemon.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge.UdfWhitelistChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.IdentityHashMap;

public class StaticPermanentFunctionChecker implements UdfWhitelistChecker {
  private static final Logger LOG = LoggerFactory.getLogger(StaticPermanentFunctionChecker.class);

  public static final String PERMANENT_FUNCTIONS_LIST = "llap-udfs.lst";

  private final IdentityHashMap<Class<?>, Boolean> allowedUdfClasses = new IdentityHashMap<>();
  
  public StaticPermanentFunctionChecker(Configuration conf) {
    URL logger = conf.getResource(PERMANENT_FUNCTIONS_LIST);
    if (logger == null) {
      LOG.warn("Could not find UDF whitelist in configuration: " + PERMANENT_FUNCTIONS_LIST);
      return;
    }
    try (BufferedReader r = new BufferedReader(new InputStreamReader(logger.openStream()))) {
      String klassName = r.readLine();
      while (klassName != null) {
        try {
          Class<?> clazz = Class.forName(klassName.trim(), false, this.getClass().getClassLoader());
          allowedUdfClasses.put(clazz, true);
          // make a list before opening the RPC attack surface
        } catch (ClassNotFoundException ie) {
          // note: explicit format to use Throwable instead of var-args
          LOG.warn("Could not load class " + klassName + " declared in UDF whitelist", ie);
        }
        klassName = r.readLine();
      }
    } catch (IOException ioe) {
      LOG.warn("Could not read UDF whitelist: " + PERMANENT_FUNCTIONS_LIST, ioe);
    }
  }

  @Override
  public boolean isUdfAllowed(Class<?> clazz) {
      return FunctionRegistry.isBuiltInFuncClass(clazz) || allowedUdfClasses.containsKey(clazz);
  }

}
