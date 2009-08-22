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
 * @file HiveColumnDesc.h
 * @brief Provides the HiveColumnDesc class
 *
 *****************************************************************************/


#ifndef __hive_column_desc_h__
#define __hive_column_desc_h__

#include "hive_metastore_types.h"
#include "hiveconstants.h"


/*************************************************************************************************
 * HiveColumnDesc Class Declaration
 ************************************************************************************************/

/**
 * @brief Descriptor for a column in a HiveResultSet.
 *
 * This class stores the information describing a column in a HiveResultSet.
 * It was only meant to be created by DBCreateColumnDesc and destroyed by DBCloseColumnDesc.
 *
 * @see DBCreateColumnDesc()
 * @see DBCloseColumnDesc()
 */
class HiveColumnDesc {
  public:
    HiveColumnDesc();
    virtual ~HiveColumnDesc();
    void initialize(Apache::Hadoop::Hive::FieldSchema& field_schema);
    void getColumnName(char* buffer, size_t buffer_len);
    void getColumnType(char* buffer, size_t buffer_len);
    HiveType getHiveType();
    int getIsNullable();
    int getIsCaseSensitive();
    size_t getMaxDisplaySize();
    size_t getFieldByteSize();

  private:
    Apache::Hadoop::Hive::FieldSchema m_field_schema;
    HiveType m_hive_type;
    bool m_is_nullable;
    bool m_is_case_sensitive;
    size_t m_max_display_size;
    size_t m_byte_size;

    size_t getMaxDisplaySize(HiveType type);
    size_t getByteSize(HiveType type);
};


#endif // __hive_column_desc_h__
