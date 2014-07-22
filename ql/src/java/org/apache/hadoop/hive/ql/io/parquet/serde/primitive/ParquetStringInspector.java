/**
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
package org.apache.hadoop.hive.ql.io.parquet.serde.primitive;

import java.io.UnsupportedEncodingException;
import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableStringObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

/**
 * The ParquetStringInspector inspects a BinaryWritable to give a Text or String.
 *
 */
public class ParquetStringInspector extends JavaStringObjectInspector implements SettableStringObjectInspector {

  ParquetStringInspector() {
    super();
  }

  @Override
  public Text getPrimitiveWritableObject(final Object o) {
    if (o == null) {
      return null;
    }

    if (o instanceof BytesWritable) {
      return new Text(((BytesWritable) o).getBytes());
    }

    if (o instanceof Text) {
      return (Text) o;
    }

    if (o instanceof String) {
      return new Text((String) o);
    }

    throw new UnsupportedOperationException("Cannot inspect " + o.getClass().getCanonicalName());
  }

  @Override
  public String getPrimitiveJavaObject(final Object o) {
    if (o == null) {
      return null;
    }

    if (o instanceof BytesWritable) {
      try {
        return Text.decode(((BytesWritable) o).getBytes());
      } catch (CharacterCodingException e) {
        throw new RuntimeException("Failed to decode string", e);
      }
    }

    if (o instanceof Text) {
      return ((Text) o).toString();
    }

    if (o instanceof String) {
      return (String) o;
    }

    throw new UnsupportedOperationException("Cannot inspect " + o.getClass().getCanonicalName());
  }

  @Override
  public Object set(final Object o, final Text text) {
    return new BytesWritable(text == null ? null : text.getBytes());
  }

  @Override
  public Object set(final Object o, final String string) {
    try {
      return new BytesWritable(string == null ? null : string.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("Failed to encode string in UTF-8", e);
    }
  }

  @Override
  public Object create(final Text text) {
    if (text == null) {
      return null;
    }
    return text.toString();
  }

  @Override
  public Object create(final String string) {
    return string;
  }
}
