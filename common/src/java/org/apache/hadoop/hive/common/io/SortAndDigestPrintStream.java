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

package org.apache.hadoop.hive.common.io;

import org.apache.commons.codec.binary.Base64;

import java.io.OutputStream;
import java.security.MessageDigest;

public class SortAndDigestPrintStream extends SortPrintStream {

  private final MessageDigest digest;

  public SortAndDigestPrintStream(OutputStream out, String encoding) throws Exception {
    super(out, encoding);
    this.digest = MessageDigest.getInstance("MD5");
  }

  @Override
  public void processFinal() {
    while (!outputs.isEmpty()) {
      String row = outputs.removeFirst();
      digest.update(row.getBytes());
      printDirect(row);
    }
    printDirect(new String(Base64.encodeBase64(digest.digest())));
    digest.reset();
  }
}

