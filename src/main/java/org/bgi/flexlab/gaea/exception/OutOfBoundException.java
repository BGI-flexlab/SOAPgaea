package org.bgi.flexlab.gaea.exception;

public class OutOfBoundException extends UserException{

	private static final long serialVersionUID = 1632323971607287173L;

	public OutOfBoundException(int length,int offset) {
		super(String.format("Offset cannot be greater than length %d : %d", offset,length));
	}

}
