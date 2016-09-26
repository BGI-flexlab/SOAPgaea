package org.bgi.flexlab.gaea.tools.annotator.interval;

/**
 * This is a marker used as a 'fake' parent during data serialization
 * 
 * @author pcingola
 */
public class MarkerParentId extends Marker {

	private static final long serialVersionUID = 1234176709588449399L;
	
	int parentId;

	public MarkerParentId(int parentId) {
		this.parentId = parentId;
	}

	public int getParentId() {
		return parentId;
	}
}
