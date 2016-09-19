package org.bgi.flexlab.gaea.utils;

import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMUtils;

import java.util.Arrays;

import org.bgi.flexlab.gaea.exception.UserException;

public class SamRecordUtils {

	public static final String REDUCED_READ_CONSENSUS_TAG = "RR";
	public static final String REDUCED_READ_ORIGINAL_ALIGNMENT_START_SHIFT = "OP";
	public static final String REDUCED_READ_ORIGINAL_ALIGNMENT_END_SHIFT = "OE";

	public static final String BQSR_BASE_INSERTION_QUALITIES = "BI";
	public static final String BQSR_BASE_DELETION_QUALITIES = "BD";

	private final static int UNINITIALIZED = -1;

	public static byte[] getBaseQualities(SAMRecord sam,
			final EventType errorModel) {
		switch (errorModel) {
		case BASE_SUBSTITUTION:
			return sam.getBaseQualities();
		case BASE_INSERTION:
			return getBaseInsertionQualities(sam);
		case BASE_DELETION:
			return getBaseDeletionQualities(sam);
		default:
			throw new UserException("Unrecognized Base Recalibration type: "
					+ errorModel);
		}
	}

	public static byte[] getBaseInsertionQualities(SAMRecord sam) {
		byte[] quals = getExistingBaseInsertionQualities(sam);
		if (quals == null) {
			quals = new byte[sam.getBaseQualities().length];
			Arrays.fill(quals, (byte) 45); // 45 is default quality
		}
		return quals;
	}

	public static byte[] getBaseDeletionQualities(SAMRecord sam) {
		byte[] quals = getExistingBaseDeletionQualities(sam);
		if (quals == null) {
			quals = new byte[sam.getBaseQualities().length];
			Arrays.fill(quals, (byte) 45); // the original quality is a flat Q45
		}
		return quals;
	}

	public static byte[] getExistingBaseDeletionQualities(SAMRecord sam) {
		return SAMUtils.fastqToPhred(sam
				.getStringAttribute(BQSR_BASE_DELETION_QUALITIES));
	}

	public static byte[] getExistingBaseInsertionQualities(SAMRecord sam) {
		return SAMUtils.fastqToPhred(sam
				.getStringAttribute(BQSR_BASE_INSERTION_QUALITIES));
	}

	public static int getSoftStart(SAMRecord sam) {
		int softStart = UNINITIALIZED;
		if (softStart == UNINITIALIZED) {
			softStart = sam.getAlignmentStart();
			for (final CigarElement cig : sam.getCigar().getCigarElements()) {
				final CigarOperator op = cig.getOperator();

				if (op == CigarOperator.SOFT_CLIP)
					softStart -= cig.getLength();
				else if (op != CigarOperator.HARD_CLIP)
					break;
			}
		}
		return softStart;
	}

	public static boolean isReducedRead(SAMRecord sam) {
		return getReducedReadCounts(sam) != null;
	}

	public static byte[] getReducedReadCounts(SAMRecord sam) {

		byte[] reducedReadCounts = sam
				.getByteArrayAttribute(REDUCED_READ_CONSENSUS_TAG);

		return reducedReadCounts;
	}

	public static byte getReducedCount(SAMRecord sam, final int index) {
		byte firstCount = getReducedReadCounts(sam)[0];
		byte offsetCount = getReducedReadCounts(sam)[index];
		return (index == 0) ? firstCount : (byte) Math.min(firstCount
				+ offsetCount, Byte.MAX_VALUE);
	}
}
