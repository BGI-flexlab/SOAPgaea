package org.bgi.flexlab.gaea.exception;

public class UnsortedException extends UserException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 9013680156879378293L;

	public UnsortedException(String readName, int lastestStart, int start) {
		super(
				String.format(
						"incoming objects must ordered by alignment start,but saw the reads %s with alignment start %d after lastest start %d",
						readName, start, lastestStart));
	}
}
