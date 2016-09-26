package org.bgi.flexlab.gaea.data.structure.positioninformation;

public class PositionInformationFactory {
	public static ShortPositionInformation produceShortPosInfo(int windowSize) {
		return new ShortPositionInformation(windowSize);
	}
	
	public static BooleanPositionInformation produceBooleanPosInfo(int windowSize) {
		return new BooleanPositionInformation(windowSize);
	}
	
	public static IntPositionInformation produceIntPosInfo(int windowSize) {
		return new IntPositionInformation(windowSize);
	}
}
