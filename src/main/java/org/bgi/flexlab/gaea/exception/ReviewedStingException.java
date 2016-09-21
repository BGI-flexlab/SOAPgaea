
package org.bgi.flexlab.gaea.exception;

/**
 * A trivial extension around StingException to mark exceptions that have been reviewed for correctness,
 * completeness, etc.  By using this exception you are saying "this is the right error message".  StingException
 * is now just a catch all for lazy users.
 */
public class ReviewedStingException extends StingException {
    /**
	 * 
	 */
	private static final long serialVersionUID = -3847160715053189382L;

	public ReviewedStingException(String msg) {
        super(msg);
    }

    public ReviewedStingException(String message, Throwable throwable) {
        super(message, throwable);
    }
}

