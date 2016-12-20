package org.bgi.flexlab.gaea.data.exception;

public class MissingHeaderException extends UserException{

	private static final long serialVersionUID = -2064946170740876217L;

	public MissingHeaderException(String msg) {
		super(msg+" get header failed!!");
	}

}
