package org.bgi.flexlab.gaea.data.structure.bam.clipper;

import java.util.ArrayList;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.bam.clipper.algorithm.ReadClippingAlgorithm;

public class ReadClipper {
	private boolean isClip;
	private final GaeaSamRecord read;
	private ArrayList<ClippingRegion> crs = null;

	public ReadClipper(GaeaSamRecord read) {
		this.read = read;
		isClip = false;
		crs = new ArrayList<ClippingRegion>();
	}

	public GaeaSamRecord getRead() {
		return this.read;
	}

	public boolean isClipper() {
		return isClip;
	}

	public GaeaSamRecord clipRead(ReadClippingAlgorithm algorithm) {
		if (crs.size() == 0)
			return read;

		isClip = true;

		GaeaSamRecord clippedRead = read;
		for (ClippingRegion op : crs) {
			final int readLength = clippedRead.getReadLength();

			if (op.getStart() < readLength) {
                ClippingRegion fixedOperation = op;
                if (op.getStop() >= readLength)
                    fixedOperation = new ClippingRegion(op.getStart(), readLength - 1);

                clippedRead = algorithm.apply(clippedRead,fixedOperation);
            }
		}
		isClip = true;
		crs.clear();
		if (clippedRead.isEmpty())
			return GaeaSamRecord.emptyRead(clippedRead);
		return clippedRead;
	}

	public GaeaSamRecord clipLowQualityEnds(byte lowQuality, ReadClippingAlgorithm algorithm) {
		if (read.isEmpty())
			return read;

		int leftIndex = 0;
		int readLength = read.getReadLength();
		int rightIndex = readLength - 1;

		byte[] qualities = read.getBaseQualities();

		while (leftIndex <= rightIndex && qualities[leftIndex] <= lowQuality)
			leftIndex++;
		while (rightIndex >= 0 && qualities[rightIndex] <= lowQuality)
			rightIndex--;

		if (rightIndex < leftIndex)
			return GaeaSamRecord.emptyRead(read);

		if(leftIndex >= readLength)
			leftIndex = readLength - 1;
		
		if(rightIndex < 0)
			rightIndex = 0;
		
		if (rightIndex != (read.getReadLength() - 1)){
			crs.add(new ClippingRegion(rightIndex, readLength - 1));
		}
		
		if (leftIndex != 0){
			crs.add(new ClippingRegion(0, leftIndex));
		}

		return clipRead(algorithm);
	}
}
