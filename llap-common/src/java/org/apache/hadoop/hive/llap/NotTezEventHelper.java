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

package org.apache.hadoop.hive.llap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.NotTezEvent;
import org.apache.hadoop.hive.llap.security.LlapSigner.Signable;
import org.apache.tez.common.ProtoConverters;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.EventProtos.RootInputDataInformationEventProto;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.TezEvent;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * See NotTezEvent class/.proto comment.
 */
public class NotTezEventHelper {

  public static Signable createSignableNotTezEvent(
      InputDataInformationEvent event, String vertexName, String destInputName) {
    final NotTezEvent.Builder builder = NotTezEvent.newBuilder().setInputEventProtoBytes(
        ProtoConverters.convertRootInputDataInformationEventToProto(event).toByteString())
        .setVertexName(vertexName).setDestInputName(destInputName);
    return new Signable() {
      @Override
      public void setSignInfo(int masterKeyId) {
        builder.setKeyId(masterKeyId);
      }

      @Override
      public byte[] serialize() throws IOException {
        NotTezEvent nte = builder.build();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(nte.getSerializedSize());
        nte.writeTo(baos);
        return baos.toByteArray();
      }
    };
  }

  public static TezEvent toTezEvent(NotTezEvent nte) throws InvalidProtocolBufferException {
    EventMetaData sourceMetaData = new EventMetaData(EventMetaData.EventProducerConsumerType.INPUT,
        nte.getVertexName(), "NULL_VERTEX", null);
    EventMetaData destMetaData = new EventMetaData(EventMetaData.EventProducerConsumerType.INPUT,
        nte.getVertexName(), nte.getDestInputName(), null);
    InputDataInformationEvent event = ProtoConverters.convertRootInputDataInformationEventFromProto(
        RootInputDataInformationEventProto.parseFrom(nte.getInputEventProtoBytes()));
    TezEvent tezEvent = new TezEvent(event, sourceMetaData, System.currentTimeMillis());
    tezEvent.setDestinationInfo(destMetaData);
    return tezEvent;
  }
}