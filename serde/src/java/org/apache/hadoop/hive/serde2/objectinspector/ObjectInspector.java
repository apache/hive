/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.serde2.objectinspector;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.serde2.typeinfo.MetastoreTypeCategory;

/**
 * ObjectInspector helps us to look into the internal structure of a complex
 * object.
 *
 * A (probably configured) ObjectInspector instance stands for a specific type
 * and a specific way to store the data of that type in the memory.
 *
 * For native java Object, we can directly access the internal structure through
 * member fields and methods. ObjectInspector is a way to delegate that
 * functionality away from the Object, so that we have more control on the
 * behavior of those actions.
 *
 * An efficient implementation of ObjectInspector should rely on factory, so
 * that we can make sure the same ObjectInspector only has one instance. That
 * also makes sure hashCode() and equals() methods of java.lang.Object directly
 * works for ObjectInspector as well.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ObjectInspector extends Cloneable {

  /**
   * Category.
   *
   */
  public static enum Category {
    PRIMITIVE, LIST, MAP, STRUCT, UNION;

    /**
     * This utility method maps the MetastoreTypeCategory enum to Category enum.
     * @param metastoreTypeCategory
     * @return Category enum equivalent from MetastoreTypeCategory
     */
    public static Category fromMetastoreTypeCategory(MetastoreTypeCategory metastoreTypeCategory) {
      switch (metastoreTypeCategory) {
      case PRIMITIVE:
        return Category.PRIMITIVE;
      case LIST:
        return Category.LIST;
      case MAP:
        return Category.MAP;
      case STRUCT:
        return Category.STRUCT;
      case UNION:
        return Category.UNION;
      default:
        throw new RuntimeException("Unsupported metastore type category " + metastoreTypeCategory);
      }
    }

    /**
     * returns the MetastoreTypeCategory enum mapping of this enum value
     * @return
     */
    public MetastoreTypeCategory toMetastoreTypeCategory() {
      switch (this) {
      case PRIMITIVE:
        return MetastoreTypeCategory.PRIMITIVE;
      case LIST:
        return MetastoreTypeCategory.LIST;
      case MAP:
        return MetastoreTypeCategory.MAP;
      case STRUCT:
        return MetastoreTypeCategory.STRUCT;
      case UNION:
        return MetastoreTypeCategory.UNION;
      default:
        throw new RuntimeException(
            "Unsupported mapping to metastore type category " + this.toString());
      }
    }

    /**
     * Util method for mapping Category enum to MetastoreTypeCategory enum
     * @param metastoreTypeCategory
     * @return
     */
    public boolean equals(MetastoreTypeCategory metastoreTypeCategory) {
      return this.toMetastoreTypeCategory().equals(metastoreTypeCategory);
    }
  };

  /**
   * Returns the name of the data type that is inspected by this
   * ObjectInspector. This is used to display the type information to the user.
   *
   * For primitive types, the type name is standardized. For other types, the
   * type name can be something like "list&lt;int&gt;", "map&lt;int,string&gt;", java class
   * names, or user-defined type names similar to typedef.
   */
  String getTypeName();

  /**
   * An ObjectInspector must inherit from one of the following interfaces if
   * getCategory() returns: PRIMITIVE: PrimitiveObjectInspector LIST:
   * ListObjectInspector MAP: MapObjectInspector STRUCT: StructObjectInspector.
   */
  Category getCategory();
}
