package org.bgi.flexlab.gaea.data.exception;

public class BAMQCException extends RuntimeException{
	/**
	 * 
	 */
	private static final long serialVersionUID = -7654073424139393545L;


	public BAMQCException(String msg) { super(msg); }
    public BAMQCException(String msg, Throwable e) { super(msg, e); }
    public BAMQCException(Throwable e) { super("", e); } // cannot be called, private access

	
	public static class WrongNumOfColException extends BAMQCException {
		/**
		 * 
		 */
		private static final long serialVersionUID = -4596452748966703629L;

		public WrongNumOfColException(int columnNum) {
            super(String.format("column number should be: %s", columnNum));
        }
	}
	
	public static class GenderInformationException extends BAMQCException {
		/**
		 * 
		 */
		private static final long serialVersionUID = -6457897353260936041L;

		public GenderInformationException(String message) {
			super(String.format("Gender Information wrong: %s", message));
		}
	}
}
