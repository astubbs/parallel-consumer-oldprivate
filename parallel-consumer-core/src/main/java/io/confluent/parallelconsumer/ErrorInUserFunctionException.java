package io.confluent.parallelconsumer;

public class ErrorInUserFunctionException extends RuntimeException {
    public ErrorInUserFunctionException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
