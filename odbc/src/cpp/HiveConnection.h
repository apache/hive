/**************************************************************************//**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************
 *
 * @file HiveConnection.h
 * @brief Provides the HiveConnection struct
 *
 *****************************************************************************/


#ifndef __hive_connection_h__
#define __hive_connection_h__

#include "ThriftHive.h"
#include <boost/shared_ptr.hpp>

using namespace boost;
using namespace apache::thrift::transport;


/*************************************************************************************************
 * HiveConnection Class Definition
 ************************************************************************************************/

/**
 * @brief Container class for Hive database connections.
 *
 * This class stores the Hive database connection information. It was only meant to be created by
 * DBOpenConnection and destroyed by DBCloseConnection.
 *
 * @see DBOpenConnection()
 * @see DBCloseConnection()
 */
struct HiveConnection {
  HiveConnection(shared_ptr<Apache::Hadoop::Hive::ThriftHiveClient> c, shared_ptr<TTransport> t) :
    client(c), transport(t) {}
  shared_ptr<Apache::Hadoop::Hive::ThriftHiveClient> client;
  shared_ptr<TTransport> transport;
};


#endif // __hive_connection_h__
