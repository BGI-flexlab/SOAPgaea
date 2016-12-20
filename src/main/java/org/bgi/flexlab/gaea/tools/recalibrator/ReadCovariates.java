package org.bgi.flexlab.gaea.tools.recalibrator;

public class ReadCovariates {
	private int[][][] mkeys = null;
	private boolean largeKeys = false;
	private int currCovIndex = 0;
	
	public ReadCovariates(int readLength, int numberOfCovariates,boolean largeKeys) {
		this.largeKeys = largeKeys;
		int length = 1;
		if(largeKeys)
			length = EventType.values().length;
		initialize(length,readLength,numberOfCovariates);
	}
	
	public ReadCovariates(int readLength, int numberOfCovariates) {
		this.largeKeys = false;
		initialize(1,readLength,numberOfCovariates);
	}
	
	private void initialize(int length,final int readLength, final int numberOfCovariates) {
		mkeys = new int[length][readLength][numberOfCovariates];
	}

	public void setCovariateIndex(final int index) {
		currCovIndex = index;
	}

	public void addCovariate(final int readOffset, final int... keys) {
		int len = keys.length;
		for (int i = 0; i < len; i++) {
			mkeys[i][readOffset][currCovIndex] = keys[i];
		}
	}

	public void addCovariate(final int mismatch, final int insertion, final int deletion, final int readOffset) {
		if (!largeKeys)
			addCovariate(readOffset, mismatch);
		else {
			addCovariate(readOffset, mismatch, insertion, deletion);
		}
	}

	public int[] getKeySet(final int readPosition, final EventType errorModel) {
		if (largeKeys)
			return mkeys[errorModel.index][readPosition];
		if (errorModel.index != EventType.SNP.index)
			throw new RuntimeException("model not match");
		return mkeys[0][readPosition];
	}

	public int[][] getKeySet(final EventType errorModel) {
		if (largeKeys)
			return mkeys[errorModel.index];
		if (errorModel.index != EventType.SNP.index)
			throw new RuntimeException("model not match");
		return mkeys[0];
	}

	public int[] getMismatchesKeySet(final int readPosition) {
		return mkeys[0][readPosition];
	}

	public int[] getInsertionsKeySet(final int readPosition) {
		if (!largeKeys)
			throw new RuntimeException("insertion model not match");
		return mkeys[EventType.Insertion.index][readPosition];
	}

	public int[] getDeletionsKeySet(final int readPosition) {
		if (!largeKeys)
			throw new RuntimeException("deletetion model not match");
		return mkeys[EventType.Deletion.index][readPosition];
	}

	public int[][] getMismatchesKeySet() {
		return mkeys[0];
	}

	public int[][] getInsertionsKeySet() {
		if (!largeKeys)
			throw new RuntimeException("insertion model not match");
		return mkeys[EventType.Insertion.index];
	}

	public int[][] getDeletionsKeySet() {
		if (!largeKeys)
			throw new RuntimeException("deletetion model not match");
		return mkeys[EventType.Deletion.index];
	}

	public void clear() {
		mkeys = null;
	}
}
