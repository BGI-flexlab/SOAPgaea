package org.bgi.flexlab.gaea.exception;

public class UserException extends RuntimeException {
	private static final long serialVersionUID = 4451398435363715205L;

	public UserException(String msg) {
		super(msg);
	}

	public UserException(String msg, Throwable e) {
		super(msg, e);
	}

	private UserException(Throwable e) {
		super("", e);
	}

	protected static String getMessage(Throwable t) {
		String message = t.getMessage();
		return message != null ? message : t.getClass().getName();
	}
	
	public static class BadInput extends UserException {

		private static final long serialVersionUID = 1708634772791836896L;

		public BadInput(String message) {
            super(String.format("Bad input: %s", message));
        }
    }
	
	public static class PileupException extends UserException{

		private static final long serialVersionUID = 1L;

		public PileupException(String message) {
			super(String.format("pileup exception :", message));
		}
	}
}
