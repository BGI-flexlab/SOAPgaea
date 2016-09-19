package org.bgi.flexlab.gaea.exception;

import htsjdk.samtools.SAMRecord;

public class MalformedReadException extends UserException {

	private static final long serialVersionUID = 7018504403137004242L;

	public MalformedReadException(String exception, SAMRecord read) {
		super(String.format(exception, read.getReadName(),
				read.getReadLength(), read.getBaseQualityString().length()));
	}

}
