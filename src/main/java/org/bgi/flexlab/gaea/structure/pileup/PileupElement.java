package org.bgi.flexlab.gaea.structure.pileup;

import org.bgi.flexlab.gaea.exception.MalformedReadException;
import org.bgi.flexlab.gaea.exception.UserException.PileupException;
import org.bgi.flexlab.gaea.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.utils.BaseUtils;
import org.bgi.flexlab.gaea.utils.MathUtils;

import com.google.java.contract.Ensures;
import com.google.java.contract.Requires;

public class PileupElement implements Comparable<PileupElement> {
	public static final byte DELETION_BASE = BaseUtils.D;
	public static final byte DELETION_QUAL = (byte) 16;
	public static final byte A_FOLLOWED_BY_INSERTION_BASE = (byte) 87;
	public static final byte C_FOLLOWED_BY_INSERTION_BASE = (byte) 88;
	public static final byte T_FOLLOWED_BY_INSERTION_BASE = (byte) 89;
	public static final byte G_FOLLOWED_BY_INSERTION_BASE = (byte) 90;

	protected final GaeaSamRecord read; // the read this base belongs to
	protected final int offset; // the offset in the bases array for this base
	protected final boolean isDeletion; // is this base a deletion
	protected final boolean isBeforeDeletedBase;
	protected final boolean isAfterDeletedBase;
	protected final boolean isBeforeInsertion;
	protected final boolean isAfterInsertion;
	protected final boolean isNextToSoftClip;
	protected final int eventLength; // indel length
	protected final String eventBases; // inserted bases

	/**
	 * Creates a new pileup element.
	 */
	@Requires({ "read != null", "offset >= -1",
			"offset <= read.getReadLength()" })
	public PileupElement(final GaeaSamRecord read, final int offset,
			final boolean isDeletion, final boolean isBeforeDeletion,
			final boolean isAfterDeletion, final boolean isBeforeInsertion,
			final boolean isAfterInsertion, final boolean isNextToSoftClip,
			final String nextEventBases, final int nextEventLength) {
		if (offset < 0 && isDeletion)
			throw new PileupException(
					"Pileup Element cannot create a deletion with a negative offset");

		this.read = read;
		this.offset = offset;
		this.isDeletion = isDeletion;
		this.isBeforeDeletedBase = isBeforeDeletion;
		this.isAfterDeletedBase = isAfterDeletion;
		this.isBeforeInsertion = isBeforeInsertion;
		this.isAfterInsertion = isAfterInsertion;
		this.isNextToSoftClip = isNextToSoftClip;
		if (isBeforeInsertion)
			eventBases = nextEventBases;
		else
			eventBases = null; // ignore argument in any other case
		if (isBeforeDeletion || isBeforeInsertion)
			eventLength = nextEventLength;
		else
			eventLength = -1;
	}

	public PileupElement(final GaeaSamRecord read, final int offset,
			final boolean isDeletion, final boolean isBeforeDeletion,
			final boolean isAfterDeletion, final boolean isBeforeInsertion,
			final boolean isAfterInsertion, final boolean isNextToSoftClip) {
		this(read, offset, isDeletion, isBeforeDeletion, isAfterDeletion,
				isBeforeInsertion, isAfterInsertion, isNextToSoftClip, null, -1);
	}

	public boolean isDeletion() {
		return isDeletion;
	}

	public boolean isBeforeDeletedBase() {
		return isBeforeDeletedBase;
	}

	public boolean isAfterDeletedBase() {
		return isAfterDeletedBase;
	}

	public boolean isBeforeDeletionStart() {
		return isBeforeDeletedBase && !isDeletion;
	}

	public boolean isAfterDeletionEnd() {
		return isAfterDeletedBase && !isDeletion;
	}

	public boolean isBeforeInsertion() {
		return isBeforeInsertion;
	}

	public boolean isAfterInsertion() {
		return isAfterInsertion;
	}

	public boolean isNextToSoftClip() {
		return isNextToSoftClip;
	}

	public boolean isInsertionAtBeginningOfRead() {
		return offset == -1;
	}

	@Ensures("result != null")
	public GaeaSamRecord getRead() {
		return read;
	}

	@Ensures("result == offset")
	public int getOffset() {
		return offset;
	}

	public byte getBase() {
		return getBase(offset);
	}

	public int getBaseIndex() {
		return getBaseIndex(offset);
	}

	public byte getQuality() {
		return getQuality(offset);
	}

	public byte getBaseInsertionQuality() {
		return getBaseInsertionQuality(offset);
	}

	public byte getBaseDeletionQuality() {
		return getBaseDeletionQuality(offset);
	}

	/**
	 * return the number of inserted or deleted bases
	 */
	public int getEventLength() {
		return eventLength;
	}

	/**
	 * @return actual sequence of inserted bases, or a null if the event is a
	 *         deletion or if there is no event in the associated read.
	 */
	public String getEventBases() {
		return eventBases;
	}

	public int getMappingQuality() {
		return read.getMappingQuality();
	}

	@Ensures("result != null")
	public String toString() {
		return String.format("%s @ %d = %c Q%d", getRead().getReadName(),
				getOffset(), (char) getBase(), getQuality());
	}

	protected byte getBase(final int offset) {
		return (isDeletion() || isInsertionAtBeginningOfRead()) ? DELETION_BASE
				: read.getReadBases()[offset];
	}

	protected int getBaseIndex(final int offset) {
		return BaseUtils
				.simpleBaseToBaseIndex((isDeletion() || isInsertionAtBeginningOfRead()) ? DELETION_BASE
						: read.getReadBases()[offset]);
	}

	protected byte getQuality(final int offset) {
		return (isDeletion() || isInsertionAtBeginningOfRead()) ? DELETION_QUAL
				: read.getBaseQualities()[offset];
	}

	protected byte getBaseInsertionQuality(final int offset) {
		return (isDeletion() || isInsertionAtBeginningOfRead()) ? DELETION_QUAL
				: read.getBaseInsertionQualities()[offset];
	}

	protected byte getBaseDeletionQuality(final int offset) {
		return (isDeletion() || isInsertionAtBeginningOfRead()) ? DELETION_QUAL
				: read.getBaseDeletionQualities()[offset];
	}

	@Override
	public int compareTo(final PileupElement pileupElement) {
		if (offset != pileupElement.offset)
			return offset - pileupElement.offset;
		return read.getAlignmentStart()
				- pileupElement.read.getAlignmentStart();
	}

	/**
	 * Returns the number of elements in the pileup element.
	 *
	 * Unless this is a reduced read, the number of elements in a pileup element
	 * is one. In the event of this being a reduced read and a deletion, we
	 * return the average number of elements between the left and right elements
	 * to the deletion. We assume the deletion to be left aligned.
	 */
	public int getRepresentativeCount() {
		int representativeCount = 1;

		if (read.isReducedRead() && !isInsertionAtBeginningOfRead()) {
			if (isDeletion() && (offset + 1 >= read.getReadLength()))
				throw new MalformedReadException(
						String.format("Adjacent I/D events in read %s -- cigar: %s"),
						read);

			representativeCount = (isDeletion()) ? MathUtils
					.fastRound((read.getReducedCount(offset) + read
							.getReducedCount(offset + 1)) / 2.0) : read
					.getReducedCount(offset);
		}
		return representativeCount;
	}
}
