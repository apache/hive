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
package org.apache.hadoop.hive.ql.udf.generic;

import javax.crypto.Cipher;

import org.apache.hadoop.hive.ql.exec.Description;

/**
 * GenericUDFAesEncrypt.
 *
 */
@Description(name = "aes_encrypt", value = "_FUNC_(input string/binary, key string/binary) - Encrypt input using AES.",
    extended = "AES (Advanced Encryption Standard) algorithm. "
    + "Key lengths of 128, 192 or 256 bits can be used. 192 and 256 bits keys can be used if "
    + "Java Cryptography Extension (JCE) Unlimited Strength Jurisdiction Policy Files are installed. "
    + "If either argument is NULL or the key length is not one of the permitted values, the return value is NULL.\n"
    + "Example: > SELECT base64(_FUNC_('ABC', '1234567890123456'));\n 'y6Ss+zCYObpCbgfWfyNWTw=='")
public class GenericUDFAesEncrypt extends GenericUDFAesBase {

  @Override
  protected int getCipherMode() {
    return Cipher.ENCRYPT_MODE;
  }

  @Override
  protected boolean canParam0BeStr() {
    return true;
  }

  @Override
  protected String getFuncName() {
    return "aes_encrypt";
  }
}
