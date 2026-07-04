/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.search.exception;

import java.io.IOException;

/** Failure while reading, writing, syncing, or restoring index files and manifests. */
public class IndexIOException extends IOException {
  public IndexIOException(String message) {
    super(message);
  }

  public IndexIOException(String message, Throwable cause) {
    super(message, cause);
  }

  public static IndexIOException wrap(Exception cause) {
    if (cause instanceof IndexIOException indexIOException) {
      return indexIOException;
    }
    return new IndexIOException(cause.getMessage(), cause);
  }
}
