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

import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Locale;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

/**
 * GenericUDFSentences: splits a natural language chunk of text into sentences and words.
 *
 */
@Description(name = "sentences", value = "_FUNC_(str, lang, country) - Splits str" 
    + " into arrays of sentences, where each sentence is an array of words. The 'lang' and"
    + "'country' arguments are optional, and if omitted, the default locale is used.",
    extended = "Example:\n"
    + "  > SELECT _FUNC_('Hello there! I am a UDF.') FROM src LIMIT 1;\n"
    + "  [ [\"Hello\", \"there\"], [\"I\", \"am\", \"a\", \"UDF\"] ]\n"
    + "  > SELECT _FUNC_(review, language) FROM movies;\n"
    + "Unnecessary punctuation, such as periods and commas in English, is automatically stripped."
    + " If specified, 'lang' should be a two-letter ISO-639 language code (such as 'en'), and "
    + "'country' should be a two-letter ISO-3166 code (such as 'us'). Not all country and "
    + "language codes are fully supported, and if an unsupported code is specified, a default "
    + "locale is used to process that string.")
public class GenericUDFSentences extends GenericUDF {
  private transient ObjectInspectorConverters.Converter[] converters;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 1 || arguments.length > 3) {
      throw new UDFArgumentLengthException(
          "The function sentences takes between 1 and 3 arguments.");
    }

    converters = new ObjectInspectorConverters.Converter[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    return ObjectInspectorFactory.getStandardListObjectInspector(
             ObjectInspectorFactory.getStandardListObjectInspector(   
              PrimitiveObjectInspectorFactory.writableStringObjectInspector));
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    assert (arguments.length >= 1 && arguments.length <= 3);
    if (arguments[0].get() == null) {
      return null;
    }
    
    // if there is more than 1 argument specified, a different natural language
    // locale is being specified
    Locale locale = null;
    if(arguments.length > 1 && arguments[1].get() != null) {
      Text language = (Text) converters[1].convert(arguments[1].get());
      Text country = null;
      if(arguments.length > 2 && arguments[2].get() != null) {
        country = (Text) converters[2].convert(arguments[2].get());
      }
      if(country != null) {
        locale = new Locale(language.toString().toLowerCase(), country.toString().toUpperCase());
      } else {
        locale = new Locale(language.toString().toLowerCase());
      }
    } else {
      locale = Locale.getDefault();
    }

    // get the input and prepare the output
    Text chunk = (Text) converters[0].convert(arguments[0].get());
    String text = chunk.toString();
    ArrayList<ArrayList<Text> > result = new ArrayList<ArrayList<Text> >();

    // Parse out sentences using Java's text-handling API
    BreakIterator bi = BreakIterator.getSentenceInstance(locale);
    bi.setText(text);
    int idx = 0;
    while(bi.next() != BreakIterator.DONE) {
      String sentence = text.substring(idx, bi.current());
      idx = bi.current();
      result.add(new ArrayList<Text>());

      // Parse out words in the sentence
      BreakIterator wi = BreakIterator.getWordInstance(locale);
      wi.setText(sentence);
      int widx = 0;
      ArrayList<Text> sent_array = result.get(result.size()-1);
      while(wi.next() != BreakIterator.DONE) {
        String word = sentence.substring(widx, wi.current());
        widx = wi.current();
        if(Character.isLetterOrDigit(word.charAt(0))) {
          sent_array.add(new Text(word));
        }
      }
    }

    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length >= 1 && children.length <= 3);
    return getStandardDisplayString("sentences", children);
  }
}
