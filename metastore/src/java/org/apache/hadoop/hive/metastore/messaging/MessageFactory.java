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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore.messaging;

/**
 * Abstract Factory for the construction of HCatalog message instances.
 */
public abstract class MessageFactory {

  // Common name constants for event messages
  public static final String ADD_PARTITION_EVENT = "ADD_PARTITION";
  public static final String ALTER_PARTITION_EVENT = "ALTER_PARTITION";
  public static final String DROP_PARTITION_EVENT = "DROP_PARTITION";
  public static final String CREATE_TABLE_EVENT = "CREATE_TABLE";
  public static final String ALTER_TABLE_EVENT = "ALTER_TABLE";
  public static final String DROP_TABLE_EVENT = "DROP_TABLE";
  public static final String CREATE_DATABASE_EVENT = "CREATE_DATABASE";
  public static final String DROP_DATABASE_EVENT = "DROP_DATABASE";
  public static final String INSERT_EVENT = "INSERT";
  public static final String CREATE_FUNCTION_EVENT = "CREATE_FUNCTION";
  public static final String DROP_FUNCTION_EVENT = "DROP_FUNCTION";
  public static final String CREATE_INDEX_EVENT = "CREATE_INDEX";
  public static final String DROP_INDEX_EVENT = "DROP_INDEX";
  public static final String ALTER_INDEX_EVENT = "ALTER_INDEX";
}