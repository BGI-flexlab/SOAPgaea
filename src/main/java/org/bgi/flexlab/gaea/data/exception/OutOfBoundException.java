package org.bgi.flexlab.gaea.data.exception;

public class OutOfBoundException extends UserException{

	private static final long serialVersionUID = 1632323971607287173L;

	public OutOfBoundException(int length,int offset) {
		super(String.format("Offset %d cannot be greater than length %d", offset,length));
	}

	public OutOfBoundException(String descr) {
		super(descr);
	}
}
