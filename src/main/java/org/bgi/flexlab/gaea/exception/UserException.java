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
	
	public static class MalformedVCFHeader extends UserException {
	        /**
			 * 
			 */
			private static final long serialVersionUID = 7027922936632516646L;

			public MalformedVCFHeader(String message) {
	            super(String.format("The provided VCF file has a malformed header: %s", message));
	        }
    }
	
	public static class MalformedVCF extends UserException {
	        /**
			 * 
			 */
			private static final long serialVersionUID = 8121781154975706475L;

			public MalformedVCF(String message, String line) {
	            super(String.format("The provided VCF file is malformed at line %s: %s", line, message));
	        }

	        public MalformedVCF(String message) {
	            super(String.format("The provided VCF file is malformed: %s", message));
	        }

	        public MalformedVCF(String message, int lineNo) {
	            super(String.format("The provided VCF file is malformed at approximately line number %d: %s", lineNo, message));
	        }
    }
}
