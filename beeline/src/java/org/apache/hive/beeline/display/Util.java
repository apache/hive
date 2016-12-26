package org.apache.hive.beeline.display;

import jline.TerminalFactory;
import org.fusesource.jansi.Ansi;

import java.io.PrintStream;

import static org.fusesource.jansi.Ansi.ansi;
import static org.fusesource.jansi.internal.CLibrary.*;

public class Util {


  static final int MIN_TERMINAL_WIDTH = 94;

  private static boolean isUnixTerminal() {

    String os = System.getProperty("os.name");
    if (os.startsWith("Windows")) {
      // we do not support Windows, we will revisit this if we really need it for windows.
      return false;
    }

    // We must be on some unix variant..
    // check if standard out is a terminal
    try {
      // isatty system call will return 1 if the file descriptor is terminal else 0
      if (isatty(STDOUT_FILENO) == 0) {
        return false;
      }
      if (isatty(STDERR_FILENO) == 0) {
        return false;
      }
    } catch (NoClassDefFoundError | UnsatisfiedLinkError ignore) {
      // These errors happen if the JNI lib is not available for your platform.
      return false;
    }
    return true;
  }

  public static boolean canRenderInPlace() {
    // we need at least 80 chars wide terminal to display in-place updates properly
    return isUnixTerminal() && TerminalFactory.get().getWidth() >= MIN_TERMINAL_WIDTH;
  }

  static void reprintLine(PrintStream out, String line) {
    out.print(ansi().eraseLine(Ansi.Erase.ALL).a(line).a('\n').toString());
    out.flush();
  }
}
