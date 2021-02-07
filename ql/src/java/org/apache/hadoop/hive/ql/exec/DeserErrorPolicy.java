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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.LongWritable;

public class DeserErrorPolicy {

  public static Policy createDeserErrorPolicy(Operator operator, Configuration hconf) {

    long maxDeserErrors = HiveConf.getLongVar(hconf, HiveConf.ConfVars.MAXDESERERRORS);
    if (maxDeserErrors == 0) {
      return new DefaultDeserErrorPolicy(operator, hconf);
    } else if (maxDeserErrors < 0) {
      return new IgnoreAllDeserErrorPolicy(operator, hconf);
    } else {
      return new MaxDeserErrorsPolicy(operator, hconf);
    }
  }

  public static abstract class Policy {

    protected Operator operator;
    protected Configuration hconf;
    protected String counterGroup;
    protected String counterName;

    Policy(Operator operator, Configuration hconf) {
      this.operator = operator;
      this.hconf = hconf;
      this.counterGroup = HiveConf.getVar(hconf, HiveConf.ConfVars.HIVECOUNTERGROUP);
      this.counterName = AbstractMapOperator.Counter.DESERIALIZE_ERRORS.toString();
    }

    /**
     * Calls when the deserialization error occurs
     * @param e the reason why fails to deserialize
     * @throws HiveException
     */
    public abstract void onDeserError(Exception e) throws HiveException;

    /**
     * @return the total count of the deserialization errors
     */
    public abstract long getErrorCount();

    /**
     * Calls when the operator closing himself
     * @param abort
     */
    public void close(boolean abort) {
      if (shouldReport()) {
        operator.reporter.incrCounter(counterGroup, counterName, getErrorCount());
      }
    }

    protected boolean shouldReport() {
      return operator != null && operator.reporter != null;
    }
  }

  private static class DefaultDeserErrorPolicy extends Policy {

    protected LongWritable deserialize_error_count = new LongWritable();
    DefaultDeserErrorPolicy(Operator operator, Configuration hconf) {
      super(operator, hconf);
    }

    @Override
    public void onDeserError(Exception e) throws HiveException {
      deserialize_error_count.set(deserialize_error_count.get() + 1);
      throw new HiveException("Hive Runtime Error while processing writable", e);
    }

    @Override
    public long getErrorCount() {
      return deserialize_error_count.get();
    }
  }

  private static class IgnoreAllDeserErrorPolicy extends DefaultDeserErrorPolicy {

    IgnoreAllDeserErrorPolicy(Operator operator, Configuration hconf) {
       super(operator, hconf);
    }

    @Override
    public void onDeserError(Exception e) throws HiveException {
      deserialize_error_count.set(deserialize_error_count.get() + 1);
    }
  }

}
