package org.bgi.flexlab.gaea.exception;


public class StingException extends RuntimeException {

	private static final long serialVersionUID = -8159825356155832176L;
	
	public StingException(String msg) {
        super(msg);
    }

    public StingException(String message, Throwable throwable) {
        super(message, throwable);
    }
}

