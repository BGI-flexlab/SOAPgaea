package org.bgi.flexlab.gaea.data.structure.pileup2;

import org.bgi.flexlab.gaea.data.structure.alignment.AlignmentsBasic;

import java.util.ArrayList;

public class Pileup implements PileupInterface<PileupReadInfo> {

	public static int MAX_DEPTH = 50000;

	/**
	 * pileup struct;
	 */
	private ArrayList<PileupReadInfo> plp;

	/**
	 * position
	 */
	private int position;

	/**
	 * count of next base is deletion
	 */
	private int nextMatchCount;

	/**
	 * count of next base is insertion
	 */
	private int nextInsertionCount;

	/**
	 * count of next base is deletion
	 */
	private int nextDeletionCount;

	/**
	 * count of deletion base
	 */
	private int deletionCount;

	public Pileup() {
		position = -1;
		plp = new ArrayList<>();
	}

	public ArrayList<PileupReadInfo> getFinalPileup() {
		return plp;
	}

	/**
	 * add readInfo to pileup
	 * 
	 * @param readInfo
	 *            read info in AlignmentsBasic
	 */
	public void addReads(AlignmentsBasic readInfo) {
		PileupReadInfo read = new PileupReadInfo(readInfo);
		if (position >= read.getPosition() && position <= read.getEnd() && plp.size() < MAX_DEPTH) {
			plp.add(read);
		} else {
			if (plp.size() == 0) {
				position = read.getPosition();
				plp.add(read);
			} else if (position < read.getPosition() || position > read.getEnd()) {
				throw new RuntimeException("add read to plp error.");
			}
		}
	}

	/**
	 * remove proccessed reads
	 */
	public void remove() {
		for (int i = 0; i < plp.size(); i++) {
			PileupReadInfo posRead = plp.get(i);

			if (position > posRead.getEnd()) {
				plp.remove(i);
				i--;
			}
		}
	}

	public void forwardPosition(int size) {
		position += size;

		remove();

		if (isEmpty())
			position = Integer.MAX_VALUE;
	}

	@Override
	public void calculateBaseInfo() {
		deletionCount = 0;
		nextDeletionCount = 0;
		nextInsertionCount = 0;
		if (position != Integer.MAX_VALUE) {
			for (int i = 0; i < plp.size(); i++) {
				PileupReadInfo posRead = plp.get(i);
				posRead.calculateQueryPosition(position);
				if (posRead.isDeletionBase())
					deletionCount++;
				if (posRead.isNextDeletionBase())
					nextDeletionCount++;
				if (posRead.isNextInsertBase())
					nextInsertionCount++;
				if (posRead.isNextMatchBase())
					nextMatchCount++;
			}
		}
	}

	/**
	 * is plp empty
	 * 
	 * @return
	 */
	public boolean isEmpty() {
		return plp.size() == 0;
	}

	/**
	 * get position
	 * 
	 * @return
	 */
	public int getPosition() {
		return position;
	}

	/**
	 * set position
	 * 
	 * @param position
	 *            position
	 */
	public void setPosition(int position) {
		this.position = position;
	}

	/**
	 *
	 * @return plp array list.
	 */
	public ArrayList<PileupReadInfo> getPlp() {
		return plp;
	}

	public int getDeletionCount() {
		return deletionCount;
	}

	public int getNextDeletionCount() {
		return nextDeletionCount;
	}

	public int getNextInsertionCount() {
		return nextInsertionCount;
	}

	public double getNextIndelRate() {
		return (nextDeletionCount + nextInsertionCount) / (double) nextMatchCount;
	}

	public int getNumberOfElements() {
		return plp.size();
	}
}
