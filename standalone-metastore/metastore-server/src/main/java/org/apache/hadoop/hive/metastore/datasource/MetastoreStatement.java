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
import com.codahale.metrics.MetricRegistry;
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
import java.util.concurrent.atomic.LongAdder;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HMSHandlerContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.datasource.MetastoreStatement.JdbcProfilerUtils.logSlowExecution;
import static org.apache.hadoop.hive.metastore.datasource.MetastoreStatement.JdbcProfilerUtils.isSlowExecution;

@SuppressWarnings("unchecked")
public final class MetastoreStatement implements InvocationHandler {
  private static final Logger LOG = LoggerFactory.getLogger(MetastoreStatement.class);
  static final String EXEC_HOOK = "metastore.jdbc.execution.hook";
  static final ThreadLocal<Pair<Pair<String, Long>, LongAdder>> CURRENT_CALL = new ThreadLocal<>();

  private final String rawSql;
  private final Statement delegate;
  private final Configuration configuration;
  private final MetastoreStatementHook hook;

  private MetastoreStatement(Configuration conf, Statement statement, String rawSql) {
    this.configuration = Objects.requireNonNull(conf);
    this.rawSql = rawSql;
    this.delegate = Objects.requireNonNull(statement);
    String className = conf.get(EXEC_HOOK, "");
    if (StringUtils.isEmpty(className)) {
      hook = new JdbcProfilerUtils(conf);
    } else {
      try {
        hook = JavaUtils.newInstance(JavaUtils.getClass(className.trim(), MetastoreStatementHook.class),
            new Class[] { Configuration.class}, new Object[] {conf});
      } catch (MetaException e) {
        throw new RuntimeException(e.getMessage());
      }
    }
  }

  public static <T extends Statement> T getProxyStatement(Configuration configuration, T delegate, String rawSql) {
    MetastoreStatement handler = new MetastoreStatement(configuration, delegate, rawSql);
    return (T) Proxy.newProxyInstance(JavaUtils.getClassLoader(),
        ClassUtils.getAllInterfaces(delegate.getClass()).toArray(new Class[0]), handler);
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    Timer.Context ctx = null;
    try {
      LongAdder adder = null;
      boolean shouldMonitor = hook.profile(rawSql, method, args);
      if (Metrics.getRegistry() != null && shouldMonitor) {
        Optional<Pair<String, Long>> ctxCall = HMSHandlerContext.getCallId();
        Pair<Pair<String, Long>, LongAdder> previousCall = CURRENT_CALL.get();
        if (ctxCall.isPresent()) {
          if ((previousCall == null || !ctxCall.get().equals(previousCall.getLeft()))) {
            // we approach the end of previous thrift call
            if (previousCall != null) {
              Pair<String, Long> call = previousCall.getLeft();
              long totalSpent = previousCall.getRight().longValue();
              LOG.debug("{} took {} ms to complete all jdbc queries", call.getLeft(), totalSpent);
              if (isSlowExecution(configuration, totalSpent)) {
                LOG.info("{} took {} ms to complete all jdbc queries", call.getLeft(), totalSpent);
              }
            }
            adder = new LongAdder();
            CURRENT_CALL.set(Pair.of(ctxCall.get(), adder));
          } else {
            adder = previousCall.getRight();
          }
        }
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
      if (shouldMonitor) {
        String statement = rawSql != null ? rawSql : (args != null && args.length > 0 ? (String) args[0] : "no sql found");
        LOG.debug("SQL query: {} completed in {} ms", statement, timeSpent);
      }
      logSlowExecution(timeSpent, configuration, rawSql, method, args);
      if (adder != null) {
        adder.add(timeSpent);
      }
      return result;
    } catch (InvocationTargetException | UndeclaredThrowableException e) {
      throw e.getCause();
    } finally {
      if (ctx != null) {
        ctx.stop();
      }
    }
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
  public static class JdbcProfilerUtils implements MetastoreStatement.MetastoreStatementHook  {
    private static final Set<String> PROFILED_APIS = new HashSet<>();
    static final Set<String> QUERY_EXECUTION =
        Set.of("executeQuery", "executeUpdate", "execute", "executeBatch");
    private static volatile boolean initialized = false;
    private static long logSlowQueriesThreshold;

    public JdbcProfilerUtils(Configuration configuration) {
      initialize(Objects.requireNonNull(configuration));
    }

    private static void initialize(Configuration configuration) {
      if (!initialized) {
        synchronized (JdbcProfilerUtils.class) {
          if (!initialized) {
            initialized = true;
            logSlowQueriesThreshold = MetastoreConf.getLongVar(configuration,
                MetastoreConf.ConfVars.METASTORE_JDBC_SLOW_QUERIES);
            if (logSlowQueriesThreshold > 0) {
              LOG.info("The slow query log enabled, will log the query that takes more than {} ms",
                  logSlowQueriesThreshold);
            }
            String thriftApis = MetastoreConf.getVar(configuration,
                MetastoreConf.ConfVars.METASTORE_PROFILE_JDBC_THRIFT_APIS);
            if (StringUtils.isNotEmpty(thriftApis)) {
              PROFILED_APIS.addAll(Arrays.asList(thriftApis.split(",")));
            }
          }
        }
      }
    }

    public static void logSlowExecution(long timeSpent, Configuration configuration,
        String sql, Method method, Object[] args) {
      if (isSlowExecution(configuration, timeSpent)) {
        Object[] printableArgs = args;
        if (args != null && args.length > 10) {
          printableArgs = new Object[10];
          System.arraycopy(args, 0, printableArgs, 0, 7);
          System.arraycopy(args, args.length - 2, printableArgs, 8, 2);
          args[7] = "....";
        }
        LOG.warn("Slow execution detected, method: {}, time taken: {} ms, args size: {}, args: {}{}",
            method.getName(), timeSpent,
            args != null ? args.length : 0, Arrays.toString(printableArgs), sql != null ? ", sql: " + sql : "");
        MetricRegistry registry = Metrics.getRegistry();
        if (registry != null) {
          Counter slowQueries = Metrics.getOrCreateCounter(MetricsConstants.JDBC_SLOW_QUERIES);
          slowQueries.inc();
        }
      }
    }

    public static boolean isSlowExecution(Configuration configuration, long timeSpent) {
      initialize(configuration);
      return logSlowQueriesThreshold > 0 && timeSpent > logSlowQueriesThreshold;
    }

    @Override
    public boolean profile(String sql, Method method, Object[] args) {
      if (PROFILED_APIS.isEmpty() || !QUERY_EXECUTION.contains(method.getName())) {
        // no api configured to profile
        return false;
      }
      Optional<Pair<String, Long>> ctxCall = HMSHandlerContext.getCallId();
      if (ctxCall.isPresent()) {
        String call = ctxCall.get().getLeft();
        return PROFILED_APIS.contains(call);
      }
      return false;
    }

    @Override
    public String getMetricName(Method method, Object[] args) {
      return MetricsConstants.JDBC_EXECUTION + method.getName();
    }
  }
}
