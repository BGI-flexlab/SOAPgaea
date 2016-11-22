package org.bgi.flexlab.gaea.data.structure.pileup2;

import java.util.ArrayList;


public abstract class Pileup {
	
	/**
	 * pileup struct;
	 */
	protected ArrayList<PileupReadInfo> plp = new ArrayList<PileupReadInfo>();
	
	/**
	 * position
	 */
	protected int position;
	
	/**
	 * need more ?
	 */
	private boolean needMore = true;

	/**
	 * get sample plp
	 * @return
	 */
	public abstract ArrayList<PileupReadInfo> getPileup();
	
	/**
	 * add readInfo to pileup
	 * @param pRead
	 */
	public void addReads(ReadInfo read) {
		if(position >= read.getPosition() && position <= read.getEnd()) {
			plp.add(new PileupReadInfo(read));
		} else {
			if(plp.size() == 0) {
				position = read.getPosition();
				plp.add(new PileupReadInfo(read));
			} else {
				throw new RuntimeException("add read to plp error.");
			}
		}
	}
	
	public void calculateQposition(){
		if(position != Integer.MAX_VALUE){
			for(int i = 0; i < plp.size(); i++) {
				PileupReadInfo posRead= plp.get(i);
				posRead.calculateQposition(position);
			}
		}
	}
	
	/*
	 * remove proccessed reads
	 * */
	public void remove(){
		for(int i = 0; i < plp.size(); i++) {
			PileupReadInfo posRead = plp.get(i);
			ReadInfo read = posRead.getReadInfo();
			if(position > read.getEnd()) {
				plp.remove(i);
				i--;
			}
		}
	}
	
	public void forwardPosition(int size) {
		position += size;
		
		remove();
		
		if(isEmpty())
			position = Integer.MAX_VALUE;
	}
	
	public boolean isEmpty() {
		return plp.size() == 0;
	}
	
	public int getPosition() {
		return position;
	}

	public void setPosition(int position) {
		this.position = position;
	}
	
	public void addPosition(int add) {
		this.position = position + add;
	}

	/**
	 * @return the needMore
	 */
	public boolean isNeedMore() {
		if(plp.size() == 0)
			return true;
		return needMore;
	}
}

