package org.apache.hadoop.hive.ql.exec.tez;

import static org.fusesource.jansi.Ansi.ansi;
import static org.fusesource.jansi.internal.CLibrary.STDERR_FILENO;
import static org.fusesource.jansi.internal.CLibrary.STDOUT_FILENO;
import static org.fusesource.jansi.internal.CLibrary.isatty;

import java.io.PrintStream;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.fusesource.jansi.Ansi;

import jline.TerminalFactory;

public class InPlaceUpdates {

  public static final int MIN_TERMINAL_WIDTH = 94;

  static boolean isUnixTerminal() {

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
    } catch (NoClassDefFoundError ignore) {
      // These errors happen if the JNI lib is not available for your platform.
      return false;
    } catch (UnsatisfiedLinkError ignore) {
      // These errors happen if the JNI lib is not available for your platform.
      return false;
    }
    return true;
  }

  public static boolean inPlaceEligible(HiveConf conf) {
    boolean inPlaceUpdates = HiveConf.getBoolVar(conf, HiveConf.ConfVars.TEZ_EXEC_INPLACE_PROGRESS);

    // we need at least 80 chars wide terminal to display in-place updates properly
    return inPlaceUpdates && !SessionState.getConsole().getIsSilent() && isUnixTerminal()
      && TerminalFactory.get().getWidth() >= MIN_TERMINAL_WIDTH;
  }

  public static void reprintLine(PrintStream out, String line) {
    out.print(ansi().eraseLine(Ansi.Erase.ALL).a(line).a('\n').toString());
    out.flush();
  }

  public static void rePositionCursor(PrintStream ps) {
    ps.print(ansi().cursorUp(0).toString());
    ps.flush();
  }
}
