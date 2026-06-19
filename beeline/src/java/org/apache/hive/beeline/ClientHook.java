/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.beeline;

/**
 * This is the client's hook and used for new Hive CLI. For some configurations like
 * set and use, it may change some prompt information in the client side. So the hook
 * will be executed after some of the commands are used.
 */
public abstract class ClientHook {
  protected String sql;

  public ClientHook(String sql) {
    this.sql = sql;
  }

  abstract void postHook(BeeLine beeLine);
}
