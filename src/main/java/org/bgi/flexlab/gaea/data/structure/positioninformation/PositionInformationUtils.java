package org.bgi.flexlab.gaea.data.structure.positioninformation;

import java.util.ArrayList;

public class PositionInformationUtils<T> {
	protected ArrayList<T> info;
	
	public PositionInformationUtils() {
		info = new ArrayList<T>();
	}
	
	public PositionInformationUtils(int windowSize) {
		info = new ArrayList<T>(windowSize);
	}
	
	public T get(int i) {
		return info.get(i);
	}
	
	public void add(T t) {
		info.add(t);
	}
	

}
