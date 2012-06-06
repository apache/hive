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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

/**
 * TRANSLATE(string input, string from, string to) is an equivalent function to translate in
 * PostGresSQL. See explain extended annotation below to read more about how this UDF works
 * 
 */
@UDFType(deterministic = true)
//@formatter:off
@Description(
  name = "translate",
  value = "_FUNC_(input, from, to) - translates the input string by" +
          " replacing the characters present in the from string with the" +
          " corresponding characters in the to string",
  extended = "_FUNC_(string input, string from, string to) is an" +
             " equivalent function to translate in PostGreSQL. It works" +
             " on a character by character basis on the input string (first" +
             " parameter). A character in the input is checked for" +
             " presence in the from string (second parameter). If a" +
             " match happens, the character from to string (third " +
             "parameter) which appears at the same index as the character" +
             " in from string is obtained. This character is emitted in" +
             " the output string  instead of the original character from" +
             " the input string. If the to string is shorter than the" +
             " from string, there may not be a character present at" +
             " the same index in the to string. In such a case, nothing is" +
             " emitted for the original character and it's deleted from" +
             " the output string." +
             "\n" +
             "For example," +
             "\n" +
             "\n" +
             "_FUNC_('abcdef', 'adc', '19') returns '1b9ef' replacing" +
             " 'a' with '1', 'd' with '9' and removing 'c' from the input" +
             " string" +
             "\n" +
             "\n" +
             "_FUNC_('a b c d', ' ', '') return 'abcd'" +
             " removing all spaces from the input string" +
             "\n" +
             "\n" +
             "If the same character is present multiple times in the" +
             " input string, the first occurence of the character is the" +
             " one that's considered for matching. However, it is not recommended" +
             " to have the same character more than once in the from" +
             " string since it's not required and adds to confusion." +
             "\n" +
             "\n" +
             "For example," +
             "\n" +
             "\n" +
             "_FUNC_('abcdef', 'ada', '192') returns '1bc9ef' replaces" +
             " 'a' with '1' and 'd' with '9' ignoring the second" +
             " occurence of 'a' in the from string mapping it to '2'"
)
//@formatter:on
public class GenericUDFTranslate extends GenericUDF {

  // For all practical purposes a code point is a fancy name for character. A java char data type
  // can store characters that require 16 bits or less. However, the unicode specification has
  // changed to allow for characters whose representation requires more than 16 bits. Therefore we
  // need to represent each character (called a code point from hereon) as int. More details at
  // http://docs.oracle.com/javase/7/docs/api/java/lang/Character.html

  /**
   * If a code point needs to be replaced with another code point, this map with store the mapping.
   */
  private Map<Integer, Integer> replacementMap = new HashMap<Integer, Integer>();

  /**
   * This set stores all the code points which needed to be deleted from the input string. The
   * objects in deletionSet and keys in replacementMap are mutually exclusive
   */
  private Set<Integer> deletionSet = new HashSet<Integer>();
  /**
   * A placeholder for result.
   */
  private Text result = new Text();

  /**
   * The values of from parameter from the previous evaluate() call.
   */
  private Text lastFrom = null;
  /**
   * The values of to parameter from the previous evaluate() call.
   */
  private Text lastTo = null;
  /**
   * Converters for retrieving the arguments to the UDF.
   */
  private ObjectInspectorConverters.Converter[] converters;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 3) {
      throw new UDFArgumentLengthException("_FUNC_ expects exactly 3 arguments");
    }

    for (int i = 0; i < arguments.length; i++) {
      if (arguments[i].getCategory() != Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(i,
            "A string argument was expected but an argument of type " + arguments[i].getTypeName()
                + " was given.");

      }

      // Now that we have made sure that the argument is of primitive type, we can get the primitive
      // category
      PrimitiveCategory primitiveCategory = ((PrimitiveObjectInspector) arguments[i])
          .getPrimitiveCategory();

      if (primitiveCategory != PrimitiveCategory.STRING
          && primitiveCategory != PrimitiveCategory.VOID) {
        throw new UDFArgumentTypeException(i,
            "A string argument was expected but an argument of type " + arguments[i].getTypeName()
                + " was given.");

      }
    }

    converters = new ObjectInspectorConverters.Converter[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    // We will be returning a Text object
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    assert (arguments.length == 3);
    if (arguments[0].get() == null || arguments[1].get() == null || arguments[2].get() == null) {
      return null;
    }

    Text input = (Text) converters[0].convert(arguments[0].get());
    Text from = (Text) converters[1].convert(arguments[1].get());
    Text to = (Text) converters[2].convert(arguments[2].get());

    populateMappingsIfNecessary(from, to);
    String resultString = processInput(input);
    result.set(resultString);
    return result;
  }

  /**
   * Pre-processes the from and to strings by calling {@link #populateMappings(Text, Text)} if
   * necessary.
   * 
   * @param from
   *          from string to be used for translation
   * @param to
   *          to string to be used for translation
   */
  private void populateMappingsIfNecessary(Text from, Text to) {
    // If the from and to strings haven't changed, we don't need to preprocess again to regenerate
    // the mappings of code points that need to replaced or deleted
    if ((lastFrom == null) || (lastTo == null) || !from.equals(lastFrom) || !to.equals(lastTo)) {
      populateMappings(from, to);
      // These are null when evaluate() is called for the first time
      if (lastFrom == null) {
        lastFrom = new Text();
      }
      if (lastTo == null) {
        lastTo = new Text();
      }
      // Need to deep copy here since doing something like lastFrom = from instead, will make
      // lastFrom point to the same Text object which would make from.equals(lastFrom) always true
      lastFrom.set(from);
      lastTo.set(to);
    }
  }

  /**
   * Pre-process the from and to strings populate {@link #replacementMap} and {@link #deletionSet}.
   * 
   * @param from
   *          from string to be used for translation
   * @param to
   *          to string to be used for translation
   */
  private void populateMappings(Text from, Text to) {
    replacementMap.clear();
    deletionSet.clear();

    ByteBuffer fromBytes = ByteBuffer.wrap(from.getBytes(), 0, from.getLength());
    ByteBuffer toBytes = ByteBuffer.wrap(to.getBytes(), 0, to.getLength());

    // Traverse through the from string, one code point at a time
    while (fromBytes.hasRemaining()) {
      // This will also move the iterator ahead by one code point
      int fromCodePoint = Text.bytesToCodePoint(fromBytes);
      // If the to string has more code points, make sure to traverse it too
      if (toBytes.hasRemaining()) {
        int toCodePoint = Text.bytesToCodePoint(toBytes);
        // If the code point from from string already has a replacement or is to be deleted, we
        // don't need to do anything, just move on to the next code point
        if (replacementMap.containsKey(fromCodePoint) || deletionSet.contains(fromCodePoint)) {
          continue;
        }
        replacementMap.put(fromCodePoint, toCodePoint);
      } else {
        // If the code point from from string already has a replacement or is to be deleted, we
        // don't need to do anything, just move on to the next code point
        if (replacementMap.containsKey(fromCodePoint) || deletionSet.contains(fromCodePoint)) {
          continue;
        }
        deletionSet.add(fromCodePoint);
      }
    }
  }

  /**
   * Translates the input string based on {@link #replacementMap} and {@link #deletionSet} and
   * returns the translated string.
   * 
   * @param input
   *          input string to perform the translation on
   * @return translated string
   */
  private String processInput(Text input) {
    StringBuilder resultBuilder = new StringBuilder();
    // Obtain the byte buffer from the input string so we can traverse it code point by code point
    ByteBuffer inputBytes = ByteBuffer.wrap(input.getBytes(), 0, input.getLength());
    // Traverse the byte buffer containing the input string one code point at a time
    while (inputBytes.hasRemaining()) {
      int inputCodePoint = Text.bytesToCodePoint(inputBytes);
      // If the code point exists in deletion set, no need to emit out anything for this code point.
      // Continue on to the next code point
      if (deletionSet.contains(inputCodePoint)) {
        continue;
      }

      Integer replacementCodePoint = replacementMap.get(inputCodePoint);
      // If a replacement exists for this code point, emit out the replacement and append it to the
      // output string. If no such replacement exists, emit out the original input code point
      char[] charArray = Character.toChars((replacementCodePoint != null) ? replacementCodePoint
          : inputCodePoint);
      resultBuilder.append(charArray);
    }
    String resultString = resultBuilder.toString();
    return resultString;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 3);
    return "translate(" + children[0] + ", " + children[1] + ", " + children[2] + ")";
  }

}
