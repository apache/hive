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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.tez.TezContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.io.LongWritable;

/**
 * This function is not a deterministic function, and not a runtime constant.
 * The return value is sequence within a query with a unique staring point based on write_id and task_id
 */
@UDFType(deterministic = false)
public class GenericUDFSurrogateKey extends GenericUDF {
  private static final int DEFAULT_WRITE_ID_BITS = 24;
  private static final int DEFAULT_TASK_ID_BITS = 16;
  private static final int DEFAULT_ROW_ID_BITS = 24;

  private int writeIdBits;
  private int taskIdBits;
  private int rowIdBits;

  private long maxWriteId;
  private long maxTaskId;
  private long maxRowId;

  private long writeId = -1;
  private long taskId = -1;
  private long rowId = 0;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length == 0) {
      writeIdBits = DEFAULT_WRITE_ID_BITS;
      taskIdBits = DEFAULT_TASK_ID_BITS;
      rowIdBits = DEFAULT_ROW_ID_BITS;
    } else if (arguments.length == 2) {
      for (int i = 0; i < 2; i++) {
        if (arguments[i].getCategory() != Category.PRIMITIVE) {
          throw new UDFArgumentTypeException(0,
              "SURROGATE_KEY input only takes primitive types, got " + arguments[i].getTypeName());
        }
      }

      writeIdBits = ((WritableConstantIntObjectInspector)arguments[0]).getWritableConstantValue().get();
      taskIdBits = ((WritableConstantIntObjectInspector)arguments[1]).getWritableConstantValue().get();
      rowIdBits = 64 - (writeIdBits + taskIdBits);

      if (writeIdBits < 1 || writeIdBits > 62) {
        throw new UDFArgumentException("Write ID bits must be between 1 and 62 (value: " + writeIdBits + ")");
      }
      if (taskIdBits < 1 || taskIdBits > 62) {
        throw new UDFArgumentException("Task ID bits must be between 1 and 62 (value: " + taskIdBits + ")");
      }
      if (writeIdBits + taskIdBits > 63) {
        throw new UDFArgumentException("Write ID bits + Task ID bits must be less than 63 (value: " +
            (writeIdBits + taskIdBits) + ")");
      }
    } else {
      throw new UDFArgumentLengthException(
          "The function SURROGATE_KEY takes 0 or 2 integer arguments (write id bits, taks id bits), but found " +
              arguments.length);
    }

    maxWriteId = (1L << writeIdBits) - 1;
    maxTaskId = (1L << taskIdBits) - 1;
    maxRowId = (1L << rowIdBits) - 1;

    return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
  }

  @Override
  public void configure(MapredContext context) {
    if (context instanceof TezContext) {
      taskId = ((TezContext)context).getTezProcessorContext().getTaskIndex();
    } else {
      throw new IllegalStateException("surrogate_key function is only supported if the execution engine is Tez");
    }

    if (taskId > maxTaskId) {
      throw new IllegalStateException(String.format("Task ID is out of range (%d bits) in surrogate_key", taskIdBits));
    }
  }

  public void setWriteId(long writeId) {
    this.writeId = writeId;
    if (writeId > maxWriteId) {
      throw new IllegalStateException(String.format("Write ID is out of range (%d bits) in surrogate_key", writeIdBits));
    }
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (writeId == -1) {
      throw new HiveException("Could not obtain Write ID for the surrogate_key function");
    }

    if (rowId > maxRowId) {
      throw new HiveException(String.format("Row ID is out of range (%d bits) in surrogate_key", rowIdBits));
    }

    long uniqueId = (writeId << (taskIdBits + rowIdBits)) + (taskId << rowIdBits) + rowId;
    rowId++;

    return new LongWritable(uniqueId);
  }

  @Override
  public String getDisplayString(String[] children) {
    return "SURROGATE_KEY()";
  }
}
