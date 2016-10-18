package org.bgi.flexlab.gaea.tools.baserecalibration;

import org.bgi.flexlab.gaea.util.EventType;

public class ReadCovariates {

	private final int eventTypeIndex = EventType.BASE_SUBSTITUTION.index;
	private int[][] keys;
	private int[][][] mkeys = null;
	private boolean largeKeys = false;

	private int currentCovariateIndex = 0;

	public ReadCovariates(final int readLength, final int numberOfCovariates) {
		keys = new int[readLength][numberOfCovariates];
	}

	public void setCovariateIndex(final int index) {
		currentCovariateIndex = index;
	}

	public void setLargeKeys(int readLength, int numberOfCovariates) {
		largeKeys = true;
		keys = null;
		mkeys = new int[EventType.values().length][readLength][numberOfCovariates];
	}

	public void addCovariate(final int mismatch, final int insertion,
			final int deletion, final int readOffset) {
		if (!largeKeys)
			addCovariate(mismatch,readOffset);
		else {
			mkeys[EventType.BASE_SUBSTITUTION.index][readOffset][currentCovariateIndex] = mismatch;
			mkeys[EventType.BASE_INSERTION.index][readOffset][currentCovariateIndex] = insertion;
			mkeys[EventType.BASE_DELETION.index][readOffset][currentCovariateIndex] = deletion;
		}
	}
	
	public void addCovariate(final int mismatch,final int readOffset){
		keys[readOffset][currentCovariateIndex] = mismatch;
	}

	public int[] getKeySet(final int readPosition, final EventType errorModel) {
		if (largeKeys)
			return mkeys[errorModel.index][readPosition];
		if (errorModel.index != eventTypeIndex)
			throw new RuntimeException("model not match");
		return keys[readPosition];
	}

	public int[][] getKeySet(final EventType errorModel) {
		if (largeKeys)
			return mkeys[errorModel.index];
		if (errorModel.index != eventTypeIndex)
			throw new RuntimeException("model not match");
		return keys;
	}

	public int[] getMismatchesKeySet(final int readPosition) {
		if (largeKeys)
			return mkeys[EventType.BASE_SUBSTITUTION.index][readPosition];
		return keys[readPosition];
	}

	public int[] getInsertionsKeySet(final int readPosition) {
		if (!largeKeys)
			throw new RuntimeException("insertion model not match");
		return mkeys[EventType.BASE_INSERTION.index][readPosition];
	}

	public int[] getDeletionsKeySet(final int readPosition) {
		if (!largeKeys)
			throw new RuntimeException("deletetion model not match");
		return mkeys[EventType.BASE_DELETION.index][readPosition];
	}

	protected int[][] getMismatchesKeySet() {
		if (largeKeys)
			return mkeys[EventType.BASE_SUBSTITUTION.index];
		else
			return keys;
	}

	protected int[][] getInsertionsKeySet() {
		if (!largeKeys)
			throw new RuntimeException("insertion model not match");
		return mkeys[EventType.BASE_INSERTION.index];
	}

	protected int[][] getDeletionsKeySet() {
		if (!largeKeys)
			throw new RuntimeException("deletetion model not match");
		return mkeys[EventType.BASE_DELETION.index];
	}

	public void clear() {
		keys = null;
	}
}
