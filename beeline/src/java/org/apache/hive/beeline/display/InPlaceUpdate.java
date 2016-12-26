package org.apache.hive.beeline.display;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hive.service.rpc.thrift.TProgressUpdateResp;
import org.fusesource.jansi.Ansi;

import javax.annotation.Nullable;
import java.io.PrintStream;
import java.io.StringWriter;
import java.text.DecimalFormat;
import java.util.List;

import static org.fusesource.jansi.Ansi.ansi;

public class InPlaceUpdate {


  // keep this within 80 chars width. If more columns needs to be added then update min terminal
  // width requirement and SEPARATOR width accordingly
  private static final String HEADER_FORMAT = "%16s%10s %13s  %5s  %9s  %7s  %7s  %6s  %6s  ";
  private static final String VERTEX_FORMAT = "%-16s%10s %13s  %5s  %9s  %7s  %7s  %6s  %6s  ";
  private static final String FOOTER_FORMAT = "%-15s  %-30s %-4s  %-25s";

  private static final int PROGRESS_BAR_CHARS = 30;
  private static final String SEPARATOR = new String(new char[Util.MIN_TERMINAL_WIDTH]).replace("\0", "-");

  /* Pretty print the values */
  private final DecimalFormat secondsFormatter = new DecimalFormat("#0.00");
  private int lines = 0;
  private PrintStream out = System.out;

  /**
   * NOTE: Use this method only if isUnixTerminal is true.
   * Erases the current line and prints the given line.
   *
   * @param line - line to print
   */
  private void reprintLine(String line) {
    Util.reprintLine(out, line);
    lines++;
  }

  /**
   * NOTE: Use this method only if isUnixTerminal is true.
   * Erases the current line and prints the given line with the specified color.
   *
   * @param line  - line to print
   * @param color - color for the line
   */
  private void reprintLineWithColorAsBold(String line, Ansi.Color color) {
    out.print(ansi().eraseLine(Ansi.Erase.ALL).fg(color).bold().a(line).a('\n').boldOff().reset()
      .toString());
    out.flush();
    lines++;
  }

  /**
   * NOTE: Use this method only if isUnixTerminal is true.
   * Erases the current line and prints the given multiline. Make sure the specified line is not
   * terminated by linebreak.
   *
   * @param line - line to print
   */
  private void reprintMultiLine(String line) {
    int numLines = line.split("\r\n|\r|\n").length;
    out.print(ansi().eraseLine(Ansi.Erase.ALL).a(line).a('\n').toString());
    out.flush();
    lines += numLines;
  }

  /**
   * NOTE: Use this method only if isUnixTerminal is true.
   * Repositions the cursor back to line 0.
   */
  private void repositionCursor() {
    if (lines > 0) {
      out.print(ansi().cursorUp(lines).toString());
      out.flush();
      lines = 0;
    }
  }


  // [==================>>-----]
  private String getInPlaceProgressBar(double percent) {
    StringWriter bar = new StringWriter();
    bar.append("[");
    int remainingChars = PROGRESS_BAR_CHARS - 4;
    int completed = (int) (remainingChars * percent);
    int pending = remainingChars - completed;
    for (int i = 0; i < completed; i++) {
      bar.append("=");
    }
    bar.append(">>");
    for (int i = 0; i < pending; i++) {
      bar.append("-");
    }
    bar.append("]");
    return bar.toString();
  }

  public void render(TProgressUpdateResp progressResponse) {
    if (progressResponse == null) return;
    // position the cursor to line 0
    repositionCursor();

    // print header
    // -------------------------------------------------------------------------------
    //         VERTICES     STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
    // -------------------------------------------------------------------------------
    reprintLine(SEPARATOR);
    reprintLineWithColorAsBold(String.format(HEADER_FORMAT, progressResponse.getHeaderNames().toArray()),
      Ansi.Color.CYAN);
    reprintLine(SEPARATOR);


    // Map 1 .......... container  SUCCEEDED      7          7        0        0       0       0
    List<String> printReady = Lists.transform(progressResponse.getRows(), new Function<List<String>, String>() {
      @Nullable
      @Override
      public String apply(@Nullable List<String> row) {
        return String.format(VERTEX_FORMAT, row.toArray());
      }
    });
    reprintMultiLine(StringUtils.join(printReady, "\n"));

    // -------------------------------------------------------------------------------
    // VERTICES: 03/04            [=================>>-----] 86%  ELAPSED TIME: 1.71 s
    // -------------------------------------------------------------------------------
    String progressStr = "" + (int) (progressResponse.getProgressedPercentage() * 100) + "%";
    float et = (float) (System.currentTimeMillis() - progressResponse.getStartTime()) / (float) 1000;
    String elapsedTime = "ELAPSED TIME: " + secondsFormatter.format(et) + " s";
    String footer = String.format(
      FOOTER_FORMAT,
      progressResponse.getFooterSummary(),
      getInPlaceProgressBar(progressResponse.getProgressedPercentage()),
      progressStr,
      elapsedTime);

    reprintLineWithColorAsBold(footer, Ansi.Color.RED);
    reprintLine(SEPARATOR);
  }
}
