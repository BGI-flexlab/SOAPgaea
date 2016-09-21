package org.bgi.flexlab.gaea.exception;

public class OptionsException extends UserException{

	private static final long serialVersionUID = -2570888294845500576L;

	public OptionsException(String msg) {
		super(msg);
	}
	
	public class OptionOutOfBoundaryException extends OptionsException{
		private static final long serialVersionUID = -8221162941638056879L;

		public OptionOutOfBoundaryException(String msg) {
			super(msg);
		}
	}
}
