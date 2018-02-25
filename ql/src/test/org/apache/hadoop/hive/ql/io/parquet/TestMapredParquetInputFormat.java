/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet;

import static org.mockito.Mockito.mock;

import org.apache.hadoop.io.ArrayWritable;
import org.junit.Test;

import org.apache.parquet.hadoop.ParquetInputFormat;

public class TestMapredParquetInputFormat {
  @Test
  public void testDefaultConstructor() {
    new MapredParquetInputFormat();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testConstructorWithParquetInputFormat() {
    new MapredParquetInputFormat(
        (ParquetInputFormat<ArrayWritable>) mock(ParquetInputFormat.class)
        );
  }

}
