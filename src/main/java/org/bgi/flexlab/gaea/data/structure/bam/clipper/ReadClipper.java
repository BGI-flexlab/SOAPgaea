package org.bgi.flexlab.gaea.data.structure.bam.clipper;

import java.util.ArrayList;

import org.bgi.flexlab.gaea.data.exception.UserException;
import org.bgi.flexlab.gaea.data.structure.alignment.AlignmentsBasic;
import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.bam.clipper.algorithm.ReadClippingAlgorithm;
import org.bgi.flexlab.gaea.util.CigarState;
import org.bgi.flexlab.gaea.util.SystemConfiguration;

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

	public static AlignmentsBasic hardClipByReferenceCoordinatesLeftTail ( AlignmentsBasic read, int start) {
		return hardClipByReferenceCoordinates(read, start, 0);
	}

	public static AlignmentsBasic hardClipByReferenceCoordinatesRightTail (AlignmentsBasic read, int end) {
		return hardClipByReferenceCoordinates(read, 0, end);
	}

	public static AlignmentsBasic hardClipLowQualEnds(AlignmentsBasic read, byte lowQual) {
		if (read.isEmpty())
			return read;

		final byte [] quals = read.getQualities();
		final int readLength = read.getReadLength();
		int leftClipIndex = 0;
		int rightClipIndex = readLength - 1;

		// check how far we can clip both sides
		while (rightClipIndex >= 0 && quals[rightClipIndex] <= lowQual) rightClipIndex--;
		while (leftClipIndex < readLength && quals[leftClipIndex] <= lowQual) leftClipIndex++;

		AlignmentsBasic clippedRead = null;
		// if the entire read should be clipped, then return an empty read.
		if (leftClipIndex > rightClipIndex) {
			clippedRead = new AlignmentsBasic(read);
			clippedRead.emptyRead();
		}

		if (rightClipIndex < readLength - 1) {
			//this.addOp(new ClippingOp(rightClipIndex + 1, readLength - 1));
			clippedRead = hardClipByReferenceCoordinates(read, rightClipIndex, readLength - 1);
		}
		if (leftClipIndex > 0 ) {
			//this.addOp(new ClippingOp(0, leftClipIndex - 1));
			AlignmentsBasic clippedRead2 = clippedRead;
			if(clippedRead == null)
				clippedRead2 = read;
			hardClipByReferenceCoordinates(clippedRead2, 0, leftClipIndex - 1);

		}
		return clippedRead;
	}

	/**
	 * Generic functionality to hard clip a read, used internally by hardClipByReferenceCoordinatesLeftTail
	 * and hardClipByReferenceCoordinatesRightTail. Should not be used directly.
	 *
	 * Note, it REQUIRES you to give the directionality of your hard clip (i.e. whether you're clipping the
	 * left of right tail) by specifying either refStart < 0 or refStop < 0.
	 *
	 * @param refStart  first base to clip (inclusive)
	 * @param refStop last base to clip (inclusive)
	 * @return a new read, without the clipped bases
	 */
	//@Requires({"!read.getReadUnmappedFlag()", "refStart < 0 || refStop < 0"})  // can't handle unmapped reads, as we're using reference coordinates to clip
	protected static AlignmentsBasic hardClipByReferenceCoordinates(AlignmentsBasic read, int refStart, int refStop) {
		if (read.isEmpty() || read.isUnmapped())
			return read;

		int start;
		int stop;
		CigarState cigarState = new CigarState();
		cigarState.setCigarState(read.getCigars());

		// Determine the read coordinate to start and stop hard clipping
		if (refStart < 0) {
			if (refStop < 0)
				throw new UserException("Only one of refStart or refStop must be < 0, not both (" + refStart + ", " + refStop + ")");
			start = 0;
			stop = cigarState.getReadCoordWithRefCoord(refStop, read.getSoftStart());
		}
		else {
			if (refStop >= 0)
				throw new UserException("Either refStart or refStop must be < 0 (" + refStart + ", " + refStop + ")");
			start = cigarState.getReadCoordWithRefCoord(refStart, read.getSoftStart());
			stop = read.getReadLength() - 1;
		}

		if (start < 0 || stop > read.getReadLength() - 1)
			throw new UserException("Trying to clip before the start or after the end of a read");

		if ( start > stop )
			throw new UserException(String.format("START (%d) > (%d) STOP -- this should never happen -- call Mauricio!", start, stop));

		if ( start > 0 && stop < read.getReadLength() - 1)
			throw new UserException(String.format("Trying to clip the middle of the read: start %d, stop %d", start, stop));

		AlignmentsBasic clippedRead = new AlignmentsBasic(read);

		int[] newCigars;
		//clip start
		if(refStart > 0 ) {
			clippedRead.hardClip(start, 0);
			int currentCigarIndex = cigarState.getCigarState()[0];
			newCigars = new int[read.getCigars().length - currentCigarIndex + 1];
			int currentCigar = cigarState.getCurrentCigar();

			if((currentCigar & 0xf) == SystemConfiguration.BAM_CDEL) {
				newCigars[0] = ((currentCigar & 0xf) | (refStart - cigarState.getCigarState()[1] << 4));
			} else
				newCigars[0] = ((currentCigar & 0xf) | (start - cigarState.getCigarState()[2] << 4));
			for(int i = currentCigarIndex + 1; i < read.getCigars().length; i++) {
				newCigars[i - currentCigarIndex] = read.getCigars()[i];
			}
			clippedRead.setCigars(newCigars);
		} else if(refStop > 0) {
			clippedRead.hardClip(0, stop);
			int currentCigarIndex = cigarState.getCigarState()[0];
			newCigars = new int[currentCigarIndex + 1];
			int currentCigar = cigarState.getCurrentCigar();

			for(int i = 0; i < currentCigarIndex; i++) {
				newCigars[i] = read.getCigars()[i];
			}
			if((currentCigar & 0xf) == SystemConfiguration.BAM_CDEL) {
				newCigars[currentCigarIndex] = ((currentCigar & 0xf) | (refStart - cigarState.getCigarState()[1] << 4));
			} else
				newCigars[currentCigarIndex] = ((currentCigar & 0xf) | (start - cigarState.getCigarState()[2] << 4));
			clippedRead.setCigars(newCigars);
		}
		return clippedRead;
	}

}
