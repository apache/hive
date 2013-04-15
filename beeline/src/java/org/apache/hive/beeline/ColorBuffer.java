/*
 *  Copyright (c) 2002,2003,2004,2005 Marc Prud'hommeaux
 *  All rights reserved.
 *
 *
 *  Redistribution and use in source and binary forms,
 *  with or without modification, are permitted provided
 *  that the following conditions are met:
 *
 *  Redistributions of source code must retain the above
 *  copyright notice, this list of conditions and the following
 *  disclaimer.
 *  Redistributions in binary form must reproduce the above
 *  copyright notice, this list of conditions and the following
 *  disclaimer in the documentation and/or other materials
 *  provided with the distribution.
 *  Neither the name of the <ORGANIZATION> nor the names
 *  of its contributors may be used to endorse or promote
 *  products derived from this software without specific
 *  prior written permission.
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS
 *  AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 *  WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *  PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 *  INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 *  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 *  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
 *  OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 *  OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 *  IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 *  ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *  This software is hosted by SourceForge.
 *  SourceForge is a trademark of VA Linux Systems, Inc.
 */

/*
 * This source file is based on code taken from SQLLine 1.0.2
 * The license above originally appeared in src/sqlline/SqlLine.java
 * http://sqlline.sourceforge.net/
 */
package org.apache.hive.beeline;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * A buffer that can output segments using ANSI color.
 *
 */
final class ColorBuffer implements Comparable<Object> {
  private static final ColorBuffer.ColorAttr BOLD = new ColorAttr("\033[1m");
  private static final ColorBuffer.ColorAttr NORMAL = new ColorAttr("\033[m");
  private static final ColorBuffer.ColorAttr REVERS = new ColorAttr("\033[7m");
  private static final ColorBuffer.ColorAttr LINED = new ColorAttr("\033[4m");
  private static final ColorBuffer.ColorAttr GREY = new ColorAttr("\033[1;30m");
  private static final ColorBuffer.ColorAttr RED = new ColorAttr("\033[1;31m");
  private static final ColorBuffer.ColorAttr GREEN = new ColorAttr("\033[1;32m");
  private static final ColorBuffer.ColorAttr BLUE = new ColorAttr("\033[1;34m");
  private static final ColorBuffer.ColorAttr CYAN = new ColorAttr("\033[1;36m");
  private static final ColorBuffer.ColorAttr YELLOW = new ColorAttr("\033[1;33m");
  private static final ColorBuffer.ColorAttr MAGENTA = new ColorAttr("\033[1;35m");
  private static final ColorBuffer.ColorAttr INVISIBLE = new ColorAttr("\033[8m");

  private final List<Object> parts = new LinkedList<Object>();
  private int visibleLength = 0;

  private final boolean useColor;


  public ColorBuffer(boolean useColor) {
    this.useColor = useColor;
    append("");
  }

  public ColorBuffer(String str, boolean useColor) {
    this.useColor = useColor;
    append(str);
  }

  /**
   * Pad the specified String with spaces to the indicated length
   *
   * @param str
   *          the String to pad
   * @param len
   *          the length we want the return String to be
   * @return the passed in String with spaces appended until the
   *         length matches the specified length.
   */
  ColorBuffer pad(ColorBuffer str, int len) {
    while (str.getVisibleLength() < len) {
      str.append(" ");
    }
    return append(str);
  }

  ColorBuffer center(String str, int len) {
    StringBuilder buf = new StringBuilder(str);
    while (buf.length() < len) {
      buf.append(" ");
      if (buf.length() < len) {
        buf.insert(0, " ");
      }
    }
    return append(buf.toString());
  }

  ColorBuffer pad(String str, int len) {
    if (str == null) {
      str = "";
    }
    return pad(new ColorBuffer(str, false), len);
  }

  public String getColor() {
    return getBuffer(useColor);
  }

  public String getMono() {
    return getBuffer(false);
  }

  String getBuffer(boolean color) {
    StringBuilder buf = new StringBuilder();
    for (Object part : parts) {
      if (!color && part instanceof ColorBuffer.ColorAttr) {
        continue;
      }
      buf.append(part.toString());
    }
    return buf.toString();
  }


  /**
   * Truncate the ColorBuffer to the specified length and return
   * the new ColorBuffer. Any open color tags will be closed.
   * Do nothing if the specified length is <= 0.
   */
  public ColorBuffer truncate(int len) {
    if (len <= 0) {
      return this;
    }
    ColorBuffer cbuff = new ColorBuffer(useColor);
    ColorBuffer.ColorAttr lastAttr = null;
    for (Iterator<Object> i = parts.iterator(); cbuff.getVisibleLength() < len && i.hasNext();) {
      Object next = i.next();
      if (next instanceof ColorBuffer.ColorAttr) {
        lastAttr = (ColorBuffer.ColorAttr) next;
        cbuff.append((ColorBuffer.ColorAttr) next);
        continue;
      }
      String val = next.toString();
      if (cbuff.getVisibleLength() + val.length() > len) {
        int partLen = len - cbuff.getVisibleLength();
        val = val.substring(0, partLen);
      }
      cbuff.append(val);
    }

    // close off the buffer with a normal tag
    if (lastAttr != null && lastAttr != NORMAL) {
      cbuff.append(NORMAL);
    }

    return cbuff;
  }


  @Override
  public String toString() {
    return getColor();
  }

  public ColorBuffer append(String str) {
    parts.add(str);
    visibleLength += str.length();
    return this;
  }

  public ColorBuffer append(ColorBuffer buf) {
    parts.addAll(buf.parts);
    visibleLength += buf.getVisibleLength();
    return this;
  }

  private ColorBuffer append(ColorBuffer.ColorAttr attr) {
    parts.add(attr);
    return this;
  }

  public int getVisibleLength() {
    return visibleLength;
  }

  private ColorBuffer append(ColorBuffer.ColorAttr attr, String val) {
    parts.add(attr);
    parts.add(val);
    parts.add(NORMAL);
    visibleLength += val.length();
    return this;
  }

  public ColorBuffer bold(String str) {
    return append(BOLD, str);
  }

  public ColorBuffer lined(String str) {
    return append(LINED, str);
  }

  public ColorBuffer grey(String str) {
    return append(GREY, str);
  }

  public ColorBuffer red(String str) {
    return append(RED, str);
  }

  public ColorBuffer blue(String str) {
    return append(BLUE, str);
  }

  public ColorBuffer green(String str) {
    return append(GREEN, str);
  }

  public ColorBuffer cyan(String str) {
    return append(CYAN, str);
  }

  public ColorBuffer yellow(String str) {
    return append(YELLOW, str);
  }

  public ColorBuffer magenta(String str) {
    return append(MAGENTA, str);
  }

  private static class ColorAttr {
    private final String attr;

    public ColorAttr(String attr) {
      this.attr = attr;
    }

    @Override
    public String toString() {
      return attr;
    }
  }

  public int compareTo(Object other) {
    return getMono().compareTo(((ColorBuffer) other).getMono());
  }
}