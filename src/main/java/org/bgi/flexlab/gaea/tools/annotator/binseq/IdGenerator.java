package org.bgi.flexlab.gaea.tools.annotator.binseq;

/**
 * Generates Id
 * 
 * @author pcingola
 */
public class IdGenerator {

	private static long currentId = 0;

	public static synchronized long id() {
		return currentId++;
	}

}
