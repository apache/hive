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

package org.apache.hadoop.hive.contrib.serde2;

import org.apache.hadoop.io.Writable;
import org.apache.tez.dag.history.logging.proto.ProtoMessageWritable;

import com.google.protobuf.Message;

/**
 * Class to convert ProtoMessageWritable to hive formats.
 * @see ProtobufSerDe
 */
public class ProtobufMessageSerDe extends ProtobufSerDe {

  @SuppressWarnings("unchecked")
  protected Message toMessage(Writable writable) {
    return ((ProtoMessageWritable<Message>)writable).getMessage();
  }

}
