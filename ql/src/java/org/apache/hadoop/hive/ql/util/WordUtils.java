/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.util;

// This class is ported from commons-lang:2.6 and remove unreferenced code

/**
 * <p>Operations on Strings that contain words.</p>
 * 
 * <p>This class tries to handle <code>null</code> input gracefully.
 * An exception will not be thrown for a <code>null</code> input.
 * Each method documents its behaviour in more detail.</p>
 * 
 * @author Apache Jakarta Velocity
 * @author Apache Software Foundation
 * @author <a href="mailto:hps@intermeta.de">Henning P. Schmiedehausen</a>
 * @author Gary Gregory
 * @since 2.0
 * @version $Id: WordUtils.java 905636 2010-02-02 14:03:32Z niallp $
 */
public class WordUtils {

    /**
     * <p><code>WordUtils</code> instances should NOT be constructed in
     * standard programming. Instead, the class should be used as
     * <code>WordUtils.wrap("foo bar", 20);</code>.</p>
     *
     * <p>This constructor is public to permit tools that require a JavaBean
     * instance to operate.</p>
     */
    public WordUtils() {
      super();
    }

    // Capitalizing
    //-----------------------------------------------------------------------

    /**
     * <p>Capitalizes all the delimiter separated words in a String.
     * Only the first letter of each word is changed. To convert the 
     * rest of each word to lowercase at the same time, 
     * use {@link #capitalizeFully(String, char[])}.</p>
     *
     * <p>The delimiters represent a set of characters understood to separate words.
     * The first string character and the first non-delimiter character after a
     * delimiter will be capitalized. </p>
     *
     * <p>A <code>null</code> input String returns <code>null</code>.
     * Capitalization uses the unicode title case, normally equivalent to
     * upper case.</p>
     *
     * <pre>
     * WordUtils.capitalize(null, *)            = null
     * WordUtils.capitalize("", *)              = ""
     * WordUtils.capitalize(*, new char[0])     = *
     * WordUtils.capitalize("i am fine", null)  = "I Am Fine"
     * WordUtils.capitalize("i aM.fine", {'.'}) = "I aM.Fine"
     * </pre>
     * 
     * @param str  the String to capitalize, may be null
     * @param delimiters  set of characters to determine capitalization, null means whitespace
     * @return capitalized String, <code>null</code> if null String input
     * @see #capitalizeFully(String)
     * @since 2.1
     */
    public static String capitalize(String str, char[] delimiters) {
        int delimLen = (delimiters == null ? -1 : delimiters.length);
        if (str == null || str.length() == 0 || delimLen == 0) {
            return str;
        }
        int strLen = str.length();
        StringBuffer buffer = new StringBuffer(strLen);
        boolean capitalizeNext = true;
        for (int i = 0; i < strLen; i++) {
            char ch = str.charAt(i);

            if (isDelimiter(ch, delimiters)) {
                buffer.append(ch);
                capitalizeNext = true;
            } else if (capitalizeNext) {
                buffer.append(Character.toTitleCase(ch));
                capitalizeNext = false;
            } else {
                buffer.append(ch);
            }
        }
        return buffer.toString();
    }

    //-----------------------------------------------------------------------
    /**
     * <p>Converts all the whitespace separated words in a String into capitalized words, 
     * that is each word is made up of a titlecase character and then a series of 
     * lowercase characters.  </p>
     *
     * <p>Whitespace is defined by {@link Character#isWhitespace(char)}.
     * A <code>null</code> input String returns <code>null</code>.
     * Capitalization uses the unicode title case, normally equivalent to
     * upper case.</p>
     *
     * <pre>
     * WordUtils.capitalizeFully(null)        = null
     * WordUtils.capitalizeFully("")          = ""
     * WordUtils.capitalizeFully("i am FINE") = "I Am Fine"
     * </pre>
     * 
     * @param str  the String to capitalize, may be null
     * @return capitalized String, <code>null</code> if null String input
     */
    public static String capitalizeFully(String str) {
        return capitalizeFully(str, null);
    }

    /**
     * <p>Converts all the delimiter separated words in a String into capitalized words, 
     * that is each word is made up of a titlecase character and then a series of 
     * lowercase characters. </p>
     *
     * <p>The delimiters represent a set of characters understood to separate words.
     * The first string character and the first non-delimiter character after a
     * delimiter will be capitalized. </p>
     *
     * <p>A <code>null</code> input String returns <code>null</code>.
     * Capitalization uses the unicode title case, normally equivalent to
     * upper case.</p>
     *
     * <pre>
     * WordUtils.capitalizeFully(null, *)            = null
     * WordUtils.capitalizeFully("", *)              = ""
     * WordUtils.capitalizeFully(*, null)            = *
     * WordUtils.capitalizeFully(*, new char[0])     = *
     * WordUtils.capitalizeFully("i aM.fine", {'.'}) = "I am.Fine"
     * </pre>
     * 
     * @param str  the String to capitalize, may be null
     * @param delimiters  set of characters to determine capitalization, null means whitespace
     * @return capitalized String, <code>null</code> if null String input
     * @since 2.1
     */
    public static String capitalizeFully(String str, char[] delimiters) {
        int delimLen = (delimiters == null ? -1 : delimiters.length);
        if (str == null || str.length() == 0 || delimLen == 0) {
            return str;
        }
        str = str.toLowerCase();
        return capitalize(str, delimiters);
    }

    //-----------------------------------------------------------------------
    /**
     * Is the character a delimiter.
     *
     * @param ch  the character to check
     * @param delimiters  the delimiters
     * @return true if it is a delimiter
     */
    private static boolean isDelimiter(char ch, char[] delimiters) {
        if (delimiters == null) {
            return Character.isWhitespace(ch);
        }
        for (int i = 0, isize = delimiters.length; i < isize; i++) {
            if (ch == delimiters[i]) {
                return true;
            }
        }
        return false;
    }
}
