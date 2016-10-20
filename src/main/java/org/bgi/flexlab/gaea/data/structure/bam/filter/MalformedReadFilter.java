package org.bgi.flexlab.gaea.data.structure.bam.filter;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.region.Region;
import org.bgi.flexlab.gaea.exception.UserException;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceRecord;

public class MalformedReadFilter extends ReadsFilter {

	protected boolean checkHasReadGroup(SAMRecord read) {
		if (read.getReadGroup() == null)
			throw new UserException.ReadMissingReadGroup(read);
		return true;
	}

	/**
	 * Check for the case in which the alignment start is inconsistent with the
	 * read unmapped flag.
	 * 
	 * @param read
	 *            The read to validate.
	 * @return true if read start is valid, false otherwise.
	 */
	protected boolean checkInvalidAlignmentStart(SAMRecord read) {
		// read is not flagged as 'unmapped', but alignment start is
		// NO_ALIGNMENT_START
		if (!read.getReadUnmappedFlag()
				&& read.getAlignmentStart() == GaeaSamRecord.NO_ALIGNMENT_START)
			return false;
		// Read is not flagged as 'unmapped', but alignment start is -1
		if (!read.getReadUnmappedFlag() && read.getAlignmentStart() == -1)
			return false;
		return true;
	}

	/**
	 * Check for invalid end of alignments.
	 * 
	 * @param read
	 *            The read to validate.
	 * @return true if read end is valid, false otherwise.
	 */
	protected boolean checkInvalidAlignmentEnd(SAMRecord read) {
		// Alignment aligns to negative number of bases in the reference.
		if (!read.getReadUnmappedFlag() && read.getAlignmentEnd() != -1
				&& (read.getAlignmentEnd() - read.getAlignmentStart() + 1) < 0)
			return false;
		return true;
	}

	/**
	 * Check to ensure that the alignment makes sense based on the contents of
	 * the header.
	 * 
	 * @param header
	 *            The SAM file header.
	 * @param read
	 *            The read to verify.
	 * @return true if alignment agrees with header, false othrewise.
	 */
	protected boolean checkAlignmentDisagreesWithHeader(
			SAMFileHeader header, SAMRecord read) {
		// Read is aligned to nonexistent contig
		if (read.getReferenceIndex() == GaeaSamRecord.NO_ALIGNMENT_REFERENCE_INDEX
				&& read.getAlignmentStart() != GaeaSamRecord.NO_ALIGNMENT_START)
			return false;
		SAMSequenceRecord contigHeader = header.getSequence(read
				.getReferenceIndex());
		// Read is aligned to a point after the end of the contig
		if (!read.getReadUnmappedFlag()
				&& read.getAlignmentStart() > contigHeader.getSequenceLength())
			return false;
		return true;
	}

	/**
	 * Check for inconsistencies between the cigar string and the
	 * 
	 * @param read
	 *            The read to validate.
	 * @return true if cigar agrees with alignment, false otherwise.
	 */
	protected boolean checkCigarDisagreesWithAlignment(SAMRecord read) {
		// Read has a valid alignment start, but the CIGAR string is empty
		if (!read.getReadUnmappedFlag() && read.getAlignmentStart() != -1
				&& read.getAlignmentStart() != GaeaSamRecord.NO_ALIGNMENT_START
				&& read.getAlignmentBlocks().size() < 0)
			return false;
		return true;
	}

	/**
	 * Check if the read has the same number of bases and base qualities
	 * 
	 * @param read
	 *            the read to validate
	 * @return true if they have the same number. False otherwise.
	 */
	protected boolean checkMismatchingBasesAndQuals(SAMRecord read,
			boolean filterMismatchingBaseAndQuals) {
		boolean result;
		if (read.getReadLength() == read.getBaseQualities().length)
			result = true;
		else if (filterMismatchingBaseAndQuals)
			result = false;
		else
			throw new UserException.MalformedBAM(
					read,
					String.format(
							"BAM file has a read with mismatching number of bases and base qualities. Offender: %s [%d bases] [%d quals]",
							read.getReadName(), read.getReadLength(),
							read.getBaseQualities().length));

		return result;
	}

	@Override
	public boolean filter(SAMRecord sam, Region region) {
		return !checkInvalidAlignmentStart(sam)
				|| !checkInvalidAlignmentEnd(sam)
				|| !checkAlignmentDisagreesWithHeader(this.mFileHeader, sam)
				|| !checkHasReadGroup(sam)
				|| !checkMismatchingBasesAndQuals(sam,
						false)
				|| !checkCigarDisagreesWithAlignment(sam);
	}
}
