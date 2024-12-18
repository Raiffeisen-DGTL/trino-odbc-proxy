package ru.raiffeisen.trino.arrow.flight.sql.logging;

import org.slf4j.MDC;

import java.io.PrintWriter;
import java.io.StringWriter;

public final class LoggingUtils {
  
  private static final String CTX_USER = "ctx.user";
  private static final String CTX_EVENT = "ctx.event";

  /**
   * Updates logging context with current user.
   *
   * @param user Current user.
   */
  public static void setLogCtxUser(String user) {
    MDC.put(CTX_USER, user);
  }

  /**
   * Updates logging context with current event.
   *
   * @param event Current action.
   */
  public static void setLogCtxEvent(String event) {
    MDC.put(CTX_EVENT, event);
  }

  /**
   * Sets logging context by providing both current event and user.
   *
   * @param event Current action.
   * @param user Current user.
   */
  public static void setLogCtx(String event, String user) {
    MDC.put(CTX_EVENT, event);
    MDC.put(CTX_USER, user);
  }

  /**
   * Clears logging context
   */
  public static void clearLogCtx() { MDC.clear(); }

  /**
   * Prepares multiline log message for logging by removing new lines (adding escape backslash). Tab
   * and carriage return characters are replaces as well.
   *
   * @param msg Original message to log.
   * @return Prepared log message.
   */
  public static String prepMsg(String msg) {
    return msg
        .replace("\n", "\\n")
        .replace("\t", "  ")
        .replace("\r", "");
  }

  /**
   * Writes stacktrace of throwable into a string.
   * @param throwable Throwable to "stringify"
   * @return String containing stack trace of a throwable.
   */
  public static String getStackTraceAsString(Throwable throwable) {
    StringWriter sw = new StringWriter();
    throwable.printStackTrace(new PrintWriter(sw));
    return prepMsg(sw.toString());
  }
}
