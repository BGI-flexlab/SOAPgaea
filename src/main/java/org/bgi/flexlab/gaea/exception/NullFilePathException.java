package org.bgi.flexlab.gaea.exception;

public class NullFilePathException extends UserException{

	private static final long serialVersionUID = 4009503598471836873L;

	public NullFilePathException(String IOType,String fileName) {
		super(String.format("%s : %s file is null", IOType, fileName));
	}

}
