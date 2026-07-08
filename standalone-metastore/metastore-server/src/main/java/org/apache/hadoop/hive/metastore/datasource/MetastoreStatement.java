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

package org.apache.hadoop.hive.metastore.datasource;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HMSHandlerContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
public final class MetastoreStatement implements InvocationHandler {
  private static final Logger LOG = LoggerFactory.getLogger(MetastoreStatement.class);
  private static final ThreadLocal<HMSHandlerContext.CallCtx> CALL_CTX = new ThreadLocal<>();
  static final String EXEC_HOOK = "metastore.jdbc.execution.hook";

  private final String rawSql;
  private final Statement delegate;
  private final Configuration configuration;
  private final MetastoreStatementHook hook;

  private MetastoreStatement(Configuration conf, Statement statement, String rawSql) {
    this.configuration = Objects.requireNonNull(conf);
    this.rawSql = rawSql;
    this.delegate = Objects.requireNonNull(statement);
    this.hook = resolveHook(conf);
  }

  private MetastoreStatementHook resolveHook(Configuration conf) {
    String className = conf.get(EXEC_HOOK, "");
    if (StringUtils.isEmpty(className)) {
      return new JdbcProfilerUtils(configuration);
    }
    try {
      return JavaUtils.newInstance(JavaUtils.getClass(className.trim(), MetastoreStatementHook.class),
          new Class[] {Configuration.class}, new Object[] {conf});
    } catch (MetaException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  public static <T extends Statement> T getProxyStatement(Configuration configuration, T delegate, String rawSql) {
    MetastoreStatement handler = new MetastoreStatement(configuration, delegate, rawSql);
    return (T) Proxy.newProxyInstance(JavaUtils.getClassLoader(),
        ClassUtils.getAllInterfaces(delegate.getClass()).toArray(new Class[0]), handler);
  }

  private void logSummary(boolean monitor) {
    Optional<HMSHandlerContext.CallCtx> currentCall = HMSHandlerContext.getCallCtx();
    HMSHandlerContext.CallCtx previousCall = CALL_CTX.get();
    if (currentCall.isEmpty()) {
      if (previousCall != null) {
        logSummary(previousCall);
        CALL_CTX.remove();
      }
      return;
    }
    if (previousCall == null) {
      if (monitor) {
        CALL_CTX.set(currentCall.get());
      }
      return;
    }
    if (!currentCall.get().equals(previousCall)) {
      logSummary(previousCall);
      if (monitor) {
        CALL_CTX.set(currentCall.get());
      } else {
        CALL_CTX.remove();
      }
    }
  }

  private void logSummary(HMSHandlerContext.CallCtx call) {
    long totalSpent = call.totalTime().get();
    LOG.debug("{} took {} ms to complete all queries", call.methodName(), totalSpent);
    if (isSlowExecution(configuration, totalSpent)) {
      LOG.warn("{} took {} ms to complete all queries", call.methodName(), totalSpent);
    }
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    Timer.Context ctx = null;
    try {
      boolean monitor = hook.profile(rawSql, method, args);
      logSummary(monitor);
      if (Metrics.getRegistry() != null && monitor) {
        String metricName = hook.getMetricName(method, args);
        Timer timer = Metrics.getOrCreateTimer(metricName);
        if (timer != null) {
          ctx = timer.time();
        }
      }
      long start = System.currentTimeMillis();
      hook.preRun(method, args);
      Object result = method.invoke(delegate, args);
      hook.postRun(method, args, result);
      long timeSpent = System.currentTimeMillis() - start;
      if (monitor) {
        String statement = rawSql != null ? rawSql
            : (args != null && args.length > 0 ? (String) args[0] : "no sql found");
        LOG.debug("Jdbc query: {} completed in {} ms", statement, timeSpent);
        HMSHandlerContext.CallCtx callCtx = CALL_CTX.get();
        if (callCtx != null) {
          callCtx.totalTime().addAndGet(timeSpent);
        }
      }
      logExecution(timeSpent, configuration, rawSql, method, args);
      return result;
    } catch (InvocationTargetException | UndeclaredThrowableException e) {
      throw e.getCause() != null ? e.getCause() : e;
    } finally {
      if (ctx != null) {
        ctx.stop();
      }
    }
  }

  static void logExecution(long timeSpent, Configuration configuration,
      String sql, Method method, Object[] args) {
    if (isSlowExecution(configuration, timeSpent)) {
      Object[] printableArgs = args;
      if (args != null && args.length > 10) {
        printableArgs = new Object[10];
        System.arraycopy(args, 0, printableArgs, 0, 7);
        System.arraycopy(args, args.length - 2, printableArgs, 8, 2);
        printableArgs[7] = "....";
      }
      LOG.warn("Slow execution detected, method: {}, time taken: {} ms, args size: {}, args: {}{}",
          method.getName(), timeSpent,
          args != null ? args.length : 0, Arrays.toString(printableArgs), sql != null ? ", sql: " + sql : "");
      if (Metrics.getRegistry() != null) {
        Counter slowQueries = Metrics.getOrCreateCounter(MetricsConstants.JDBC_SLOW_QUERIES);
        slowQueries.inc();
      }
    }
  }

  static boolean isSlowExecution(Configuration configuration, long timeSpent) {
    long threshold = MetastoreConf.getLongVar(configuration, MetastoreConf.ConfVars.METASTORE_JDBC_SLOW_QUERIES);
    return threshold > 0 && timeSpent > threshold;
  }

  public interface MetastoreStatementHook {
    /**
     * Whether should monitor the current call, this method gives a chance to profile a specific pattern of queries.
     * For example, we use {@link JdbcProfilerUtils} to profile the queries originated from a set of specific APIs.
     * @param sql The sql being executed, it might be null for {@link Statement#execute}, for this case
     *            need to obtain the sql from args, the method input.
     * @param method Method which is being called
     * @param args The method input
     * @return true for profiling this call, false otherwise
     */
    boolean profile(String sql, Method method, Object[] args);

    String getMetricName(Method method, Object[] args);

    /**
     * Invoked before the method call
     * @param method Method which is being called
     * @param args The method input
     */
    default void preRun(Method method, Object[] args) {
    }

    /**
     *  Invoked post the method call
     * @param method Method which is being called
     * @param args The method input
     * @param result The execution result from the call
     */
    default void postRun(Method method, Object[] args, Object result) {
    }
  }

  /**
   * This class is used to profile the underlying statement originated from specific thrift API calls
   */
  public static class JdbcProfilerUtils implements MetastoreStatementHook {
    static final Set<String> QUERY_EXECUTION =
        Set.of("executeQuery", "executeUpdate", "execute", "executeBatch");

    private final Configuration configuration;
    private final Set<String> profiledApis;

    public JdbcProfilerUtils(Configuration configuration) {
      this.configuration = Objects.requireNonNull(configuration);
      this.profiledApis = getProfiledApis();
    }

    private Set<String> getProfiledApis() {
      String thriftApis = MetastoreConf.getVar(configuration,
          MetastoreConf.ConfVars.METASTORE_PROFILE_JDBC_THRIFT_APIS);
      Set<String> profiledApis = new HashSet<>();
      for (String thriftApi : thriftApis.split(",")) {
        String trimmedThriftApi = thriftApi.trim();
        if (!trimmedThriftApi.isEmpty()) {
          profiledApis.add(trimmedThriftApi);
        }
      }
      return profiledApis;
    }

    @Override
    public boolean profile(String sql, Method method, Object[] args) {
      if (!QUERY_EXECUTION.contains(method.getName())) {
        return false;
      }
      if (profiledApis.isEmpty()) {
        return false;
      }
      Optional<HMSHandlerContext.CallCtx> ctxCall = HMSHandlerContext.getCallCtx();
      return ctxCall.isPresent() && profiledApis.contains(ctxCall.get().methodName());
    }

    @Override
    public String getMetricName(Method method, Object[] args) {
      return MetricsConstants.JDBC_EXECUTION + method.getName();
    }
  }

}
