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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import jline.Completor;
import jline.NullCompletor;

/**
 * An abstract implementation of CommandHandler.
 *
 */
public abstract class AbstractCommandHandler implements CommandHandler {
  private final BeeLine beeLine;
  private final String name;
  private final String[] names;
  private final String helpText;
  private Completor[] parameterCompletors = new Completor[0];


  public AbstractCommandHandler(BeeLine beeLine, String[] names, String helpText,
      Completor[] completors) {
    this.beeLine = beeLine;
    name = names[0];
    this.names = names;
    this.helpText = helpText;
    if (completors == null || completors.length == 0) {
      parameterCompletors = new Completor[] { new NullCompletor() };
    } else {
      List<Completor> c = new LinkedList<Completor>(Arrays.asList(completors));
      c.add(new NullCompletor());
      parameterCompletors = c.toArray(new Completor[0]);
    }
  }

  @Override
  public String getHelpText() {
    return helpText;
  }


  @Override
  public String getName() {
    return name;
  }


  @Override
  public String[] getNames() {
    return names;
  }


  @Override
  public String matches(String line) {
    if (line == null || line.length() == 0) {
      return null;
    }

    String[] parts = beeLine.split(line);
    if (parts == null || parts.length == 0) {
      return null;
    }

    for (String name2 : names) {
      if (name2.startsWith(parts[0])) {
        return name2;
      }
    }
    return null;
  }

  public void setParameterCompletors(Completor[] parameterCompletors) {
    this.parameterCompletors = parameterCompletors;
  }

  @Override
  public Completor[] getParameterCompletors() {
    return parameterCompletors;
  }
}
