package org.bgi.flexlab.gaea.data.structure.pileup2;


import org.bgi.flexlab.gaea.data.structure.alignment.AlignmentsBasic;

import java.util.ArrayList;

public class Pileup implements PileupInterface<PileupReadInfo>{


	/**
	 * pileup struct;
	 */
	protected ArrayList<PileupReadInfo> plp;

	/**
	 * position
	 */
	protected int position;

	/**
	 * need more ?
	 */
	private boolean needMore;

	public Pileup() {
		position = -1;
		needMore = true;
		plp = new ArrayList<>();
	}

	public ArrayList<PileupReadInfo> getFinalPileup() {
		return plp;
	}

	/**
	 * add readInfo to pileup
	 * @param readInfo
	 */
	public void addReads(AlignmentsBasic readInfo) {
		PileupReadInfo read = new PileupReadInfo(readInfo);
		if(position >= read.getPosition() && position <= read.getEnd()) {
			plp.add(read);
		} else {
			if(plp.size() == 0) {
				position = read.getPosition();
				plp.add(read);
			} else {
				throw new RuntimeException("add read to plp error.");
			}
		}
	}

	@Override
	public void calculateQposition(){
		if(position != Integer.MAX_VALUE){
			for(int i = 0; i < plp.size(); i++) {
				PileupReadInfo posRead= plp.get(i);
				posRead.calculateQueryPosition(position);
			}
		}
	}

	/**
	 * remove proccessed reads
	 * */
	public void remove(){
		for(int i = 0; i < plp.size(); i++) {
			PileupReadInfo posRead = plp.get(i);

			if(position > posRead.getEnd()) {
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

	/**
	 * is plp empty
	 * @return
	 */
	public boolean isEmpty() {
		return plp.size() == 0;
	}

	/**
	 * get position
	 * @return
	 */
	public int getPosition() {
		return position;
	}

	/**
	 * set position
	 * @param position
	 */
	public void setPosition(int position) {
		this.position = position;
	}

	/**
	 * @return the needMore
	 */
	public boolean isNeedMore() {
		if(plp.size() == 0)
			return true;
		return needMore;
	}

	public ArrayList<PileupReadInfo> getPlp() {
		return plp;
	}
}

