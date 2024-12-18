package ru.raiffeisen.trino.arrow.flight.sql.logging;

import org.apache.arrow.flight.*;
import org.slf4j.Logger;
import org.slf4j.event.Level;
import ru.raiffeisen.trino.arrow.flight.sql.metrics.FlightSqlMetrics;

import static org.slf4j.LoggerFactory.getLogger;
import static ru.raiffeisen.trino.arrow.flight.sql.logging.LoggingUtils.*;

public class LoggingMiddleware implements FlightServerMiddleware {
  private static final Logger log = getLogger(LoggingMiddleware.class);

  public static class Factory implements FlightServerMiddleware.Factory<LoggingMiddleware> {
    public Factory() {}

    @Override
    public LoggingMiddleware onCallStarted(
        CallInfo callInfo, CallHeaders callHeaders, RequestContext requestContext) {
      return new LoggingMiddleware(callInfo, callHeaders, requestContext);
    }
  }

  private String headersToString(CallHeaders headers) {
    StringBuilder headersStr = new StringBuilder();
    for (String key : headers.keys()) {
      headersStr.append(" ").append(key).append("=");
      for (String value : headers.getAll(key)) {
        if (value.startsWith("Basic")) {
          headersStr.append("Basic ***masked***,");
        } else if (value.startsWith("Bearer") ){
          headersStr.append("Bearer ***masked***,");
        } else {
          headersStr.append(value).append(",");
        }
      }
      headersStr.deleteCharAt(headersStr.length() - 1).append(";");
    }
    return headersStr.toString();
  }

  private String contextToString(RequestContext context) {
    StringBuilder contextStr = new StringBuilder();
    for (String key : context.keySet()) {
      contextStr.append(" ").append(key).append("=").append(context.get(key)).append(";");
    }
    return contextStr.toString();
  }

  private LoggingMiddleware(
      CallInfo callInfo, CallHeaders callHeaders, RequestContext requestContext) {
    setLogCtxEvent("call_started");

    StringBuilder msg = new StringBuilder();
    msg.append("methodName=%s;".formatted(callInfo.method().name()));

    if (!callHeaders.keys().isEmpty()) {
      msg.append(" headers=[%s];".formatted(headersToString(callHeaders).substring(1)));
    }
    if (!requestContext.keySet().isEmpty()) {
      msg.append(" context=[%s];".formatted(contextToString(requestContext).substring(1)));
    }
    log.debug(msg.toString());
    clearLogCtx();
  }

  @Override
  public void onBeforeSendingHeaders(CallHeaders callHeaders) {
    if (!callHeaders.keys().isEmpty()) {
      setLogCtxEvent("call_send_headers");
      log.debug("headers=[{}]", headersToString(callHeaders).substring(1));
      clearLogCtx();
    }
  }

  @Override
  public void onCallCompleted(CallStatus callStatus) {
    setLogCtxEvent("call_completed");
    
    FlightSqlMetrics.STATUS_COUNT.increment(callStatus.code());
    
    StringBuilder msg = new StringBuilder();
    msg.append("code=%s;".formatted(callStatus.code()));

    if (!callStatus.description().isEmpty()) {
      msg.append(" description=%s;".formatted(callStatus.description()));
    }

    if (!callStatus.metadata().keys().isEmpty()) {
      String headers = headersToString(callStatus.metadata());
      msg.append(" metadata=[%s];".formatted(!headers.isEmpty() ? headers.substring(1) : "empty"));
    }

    if (callStatus.cause() != null) {
      msg.append(
          " cause=%s\\n%s;"
              .formatted(callStatus.cause().toString(), getStackTraceAsString(callStatus.cause())));
    }

    log.atLevel(callStatus.code() == FlightStatusCode.OK ? Level.DEBUG : Level.ERROR)
        .log(msg.toString());
    clearLogCtx();
  }

  @Override
  public void onCallErrored(Throwable e) {
    setLogCtxEvent("call_errored");
    log.error("{}\\n{}", e.toString(), getStackTraceAsString(e));
    clearLogCtx();
  }
}
