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

package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.tez.TezContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.AppMasterEventDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.InputInitializerEvent;

/**
 * AppMasterEventOperator sends any rows it receives to the Tez AM. This can be
 * used to control execution dynamically.
 */
@SuppressWarnings({ "deprecation", "serial" })
public class AppMasterEventOperator extends Operator<AppMasterEventDesc> {

  protected transient Serializer serializer;
  protected transient DataOutputBuffer buffer;
  protected transient boolean hasReachedMaxSize = false;
  protected transient long MAX_SIZE;

  @Override
  public Collection<Future<?>> initializeOp(Configuration hconf) throws HiveException {
    Collection<Future<?>> result = super.initializeOp(hconf);

    MAX_SIZE = HiveConf.getLongVar(hconf, ConfVars.TEZ_DYNAMIC_PARTITION_PRUNING_MAX_EVENT_SIZE);
    serializer =
        (Serializer) ReflectionUtils.newInstance(conf.getTable().getDeserializerClass(), null);
    initDataBuffer(false);
    return result;
  }

  protected void initDataBuffer(boolean skipPruning) throws HiveException {
    buffer = new DataOutputBuffer();
    try {
      // add any other header info
      getConf().writeEventHeader(buffer);

      // write byte to say whether to skip pruning or not
      buffer.writeBoolean(skipPruning);
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    if (hasReachedMaxSize) {
      return;
    }

    ObjectInspector rowInspector = inputObjInspectors[0];
    try {
      Writable writableRow = serializer.serialize(row, rowInspector);
      writableRow.write(buffer);
      if (buffer.getLength() > MAX_SIZE) {
	if (isLogInfoEnabled) {
	  LOG.info("Disabling AM events. Buffer size too large: " + buffer.getLength());
	}
        hasReachedMaxSize = true;
        buffer = null;
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }

    if (isLogDebugEnabled) {
      LOG.debug("AppMasterEvent: " + row);
    }
    forward(row, rowInspector);
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    if (!abort) {
      TezContext context = (TezContext) TezContext.get();

      String vertexName = getConf().getVertexName();
      String inputName = getConf().getInputName();

      byte[] payload = null;

      if (hasReachedMaxSize) {
        initDataBuffer(true);
      }

      payload = new byte[buffer.getLength()];
      System.arraycopy(buffer.getData(), 0, payload, 0, buffer.getLength());

      Event event =
          InputInitializerEvent.create(vertexName, inputName,
              ByteBuffer.wrap(payload, 0, payload.length));

      if (isLogInfoEnabled) {
	LOG.info("Sending Tez event to vertex = " + vertexName + ", input = " + inputName
	    + ". Payload size = " + payload.length);
      }

      context.getTezProcessorContext().sendEvents(Collections.singletonList(event));
    }
  }

  @Override
  public OperatorType getType() {
    return OperatorType.EVENT;
  }

  /**
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "EVENT";
  }
}
