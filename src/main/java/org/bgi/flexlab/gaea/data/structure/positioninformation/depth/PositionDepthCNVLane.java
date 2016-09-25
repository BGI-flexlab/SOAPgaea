package org.bgi.flexlab.gaea.data.structure.positioninformation.depth;

import org.bgi.flexlab.gaea.data.structure.positioninformation.IntPositionInformation;

public class PositionDepthCNVLane extends IntPositionInformation{

	public PositionDepthCNVLane(int windowSize) {
		super(windowSize);
		// TODO Auto-generated constructor stub
	}
	
	public void add(int i) {
		info[i]++;
	}
}
