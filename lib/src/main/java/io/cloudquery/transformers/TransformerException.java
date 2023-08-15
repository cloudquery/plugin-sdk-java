package io.cloudquery.transformers;

public class TransformerException extends Exception {
    public TransformerException(String message) {
        super(message);
    }

    public TransformerException(String message, Throwable ex) {
        super(message, ex);
    }
}
