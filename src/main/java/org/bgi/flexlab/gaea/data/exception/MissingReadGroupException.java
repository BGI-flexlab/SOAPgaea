package org.bgi.flexlab.gaea.data.exception;

public class MissingReadGroupException extends UserException {
	private static final long serialVersionUID = -5169730348988563636L;

	public MissingReadGroupException(String readGroup) {
		super(String.format("Read %s is either missing the read group or its read group is not defined in the BAM header!", readGroup));
	}
}
