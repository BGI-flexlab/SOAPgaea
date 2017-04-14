/*******************************************************************************
 * Copyright (c) 2017, BGI-Shenzhen
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 *
 * This file incorporates work covered by the following copyright and 
 * Permission notices:
 *
 * Copyright (c) 2009-2012 The Broad Institute
 *  
 *     Permission is hereby granted, free of charge, to any person
 *     obtaining a copy of this software and associated documentation
 *     files (the "Software"), to deal in the Software without
 *     restriction, including without limitation the rights to use,
 *     copy, modify, merge, publish, distribute, sublicense, and/or sell
 *     copies of the Software, and to permit persons to whom the
 *     Software is furnished to do so, subject to the following
 *     conditions:
 *  
 *     The above copyright notice and this permission notice shall be
 *     included in all copies or substantial portions of the Software.
 *  
 *     THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *     EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 *     OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *     NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 *     HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 *     WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 *     FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 *     OTHER DEALINGS IN THE SOFTWARE.
 *******************************************************************************/
package org.bgi.flexlab.gaea.util;

import htsjdk.samtools.*;
import org.bgi.flexlab.gaea.data.exception.OutOfBoundException;
import org.bgi.flexlab.gaea.data.exception.UserException;
import org.bgi.flexlab.gaea.data.structure.alignment.AlignmentsBasic;
import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.data.structure.sequenceplatform.NGSPlatform;

import java.util.*;

public class ReadUtils {

	private static final String OFFSET_NOT_ZERO_EXCEPTION = "We ran past the end of the read and never found the offset, something went wrong!";

	private static final int DEFAULT_ADAPTOR_SIZE = 100;
	public static final int CLIPPING_GOAL_NOT_REACHED = -1;

	public static int getMeanRepresentativeReadCount(GaeaSamRecord read) {
		if (!read.isReducedRead())
			return 1;

		// compute mean representative read counts
		final byte[] counts = read.getReducedReadCounts();
		return (int) Math.round((double) MathUtils.sum(counts) / counts.length);
	}

	/**
	 * A marker to tell which end of the read has been clipped
	 */
	public enum ClippingTail {
		LEFT_TAIL, RIGHT_TAIL
	}

	/**
	 * A HashMap of the SAM spec read flag names
	 *
	 * Note: This is not being used right now, but can be useful in the future
	 */
	private static final Map<Integer, String> readFlagNames = new HashMap<Integer, String>();

	static {
		readFlagNames.put(0x1, "Paired");
		readFlagNames.put(0x2, "Proper");
		readFlagNames.put(0x4, "Unmapped");
		readFlagNames.put(0x8, "MateUnmapped");
		readFlagNames.put(0x10, "Forward");
		readFlagNames.put(0x40, "FirstOfPair");
		readFlagNames.put(0x80, "SecondOfPair");
		readFlagNames.put(0x100, "NotPrimary");
		readFlagNames.put(0x200, "NON-PF");
		readFlagNames.put(0x400, "Duplicate");
	}

	public enum ReadAndIntervalOverlap {
		NO_OVERLAP_CONTIG, NO_OVERLAP_LEFT, NO_OVERLAP_RIGHT, NO_OVERLAP_HARDCLIPPED_LEFT, NO_OVERLAP_HARDCLIPPED_RIGHT, OVERLAP_LEFT, 
		OVERLAP_RIGHT, OVERLAP_LEFT_AND_RIGHT, OVERLAP_CONTAINED
	}

	/**
	 * is this base inside the adaptor of the read?
	 *
	 * There are two cases to treat here:
	 *
	 * 1) Read is in the negative strand => Adaptor boundary is on the left tail
	 * 2) Read is in the positive strand => Adaptor boundary is on the right
	 * tail
	 *
	 * Note: We return false to all reads that are UNMAPPED or have an weird big
	 * insert size (probably due to mismapping or bigger event)
	 */
	public static boolean isBaseInsideAdaptor(final SAMRecord read, long basePos) {
		Integer adaptorBoundary = getAdaptorBoundary(read);
		if (adaptorBoundary == null
				|| read.getInferredInsertSize() > DEFAULT_ADAPTOR_SIZE)
			return false;

		return read.getReadNegativeStrandFlag() ? basePos <= adaptorBoundary
				: basePos >= adaptorBoundary;
	}

	/**
	 * Finds the adaptor boundary around the read and returns the first base
	 * inside the adaptor that is closest to the read boundary. If the read is
	 * in the positive strand, this is the first base after the end of the
	 * fragment (Picard calls it 'insert'), if the read is in the negative
	 * strand, this is the first base before the beginning of the fragment.
	 *
	 * There are two cases we need to treat here:
	 *
	 * 1) Our read is in the reverse strand :
	 *     <----------------------| *
     *   |--------------------->
	 *
	 * in these cases, the adaptor boundary is at the mate start (minus one)
	 *
	 * 2) Our read is in the forward strand :
     *   |---------------------->   *
     *     <----------------------|
	 *
	 * in these cases the adaptor boundary is at the start of the read plus the
	 * inferred insert size (plus one)
	 */
	public static Integer getAdaptorBoundary(final SAMRecord read) {
		final int MAXIMUM_ADAPTOR_LENGTH = 8;
		final int insertSize = Math.abs(read.getInferredInsertSize());
		// mates reads in another chromosome or unmapped pairs
		if (insertSize == 0 || read.getReadUnmappedFlag())
			return null;

		Integer adaptorBoundary; 
		if (read.getReadNegativeStrandFlag()){
			// case 1
			adaptorBoundary = read.getMateAlignmentStart() - 1;
		}
		else{
			// case 2
			adaptorBoundary = read.getAlignmentStart() + insertSize + 1;
		}

		if ((adaptorBoundary < read.getAlignmentStart()
				- MAXIMUM_ADAPTOR_LENGTH)
				|| (adaptorBoundary > read.getAlignmentEnd()
						+ MAXIMUM_ADAPTOR_LENGTH))
			adaptorBoundary = null;

		return adaptorBoundary;
	}

	/**
	 * is the read a 454 read?
	 *
	 * @param read
	 *            the read to test
	 * @return checks the read group tag PL for the default 454 tag
	 */
	public static boolean is454Read(SAMRecord read) {
		return NGSPlatform.fromRead(read) == NGSPlatform.LS454;
	}

	/**
	 * IonTorrent read?
	 */
	public static boolean isIonRead(SAMRecord read) {
		return NGSPlatform.fromRead(read) == NGSPlatform.ION_TORRENT;
	}

	/**
	 * SOLiD read?
	 */
	public static boolean isSOLiDRead(SAMRecord read) {
		return NGSPlatform.fromRead(read) == NGSPlatform.SOLID;
	}

	/**
	 * SLX read?
	 */
	public static boolean isIlluminaRead(SAMRecord read) {
		return NGSPlatform.fromRead(read) == NGSPlatform.ILLUMINA;
	}

	/**
	 * checks if the read has a platform tag in the readgroup equal to 'name'.
	 */
	public static boolean isPlatformRead(SAMRecord read, String name) {

		SAMReadGroupRecord readGroup = read.getReadGroup();
		if (readGroup != null) {
			Object readPlatformAttr = readGroup.getAttribute("PL");
			if (readPlatformAttr != null)
				return readPlatformAttr.toString().toUpperCase().contains(name);
		}
		return false;
	}

	/**
	 * Returns the collections of reads sorted in coordinate order, according to
	 * the order defined in the reads themselves
	 *
	 * @param reads
	 * @return
	 */
	public final static List<GaeaSamRecord> sortReadsByCoordinate(
			List<GaeaSamRecord> reads) {
		final SAMRecordComparator comparer = new SAMRecordCoordinateComparator();
		Collections.sort(reads, comparer);
		return reads;
	}

	/**
	 * If a read starts in INSERTION, returns the first element length.
	 *
	 * Warning: If the read has Hard or Soft clips before the insertion this
	 * function will return 0.
	 */
	public final static int getFirstInsertionOffset(SAMRecord read) {
		CigarElement e = read.getCigar().getCigarElement(0);
		if (e.getOperator() == CigarOperator.I)
			return e.getLength();
		else
			return 0;
	}

	public final static int getFirstInsertionOffset(AlignmentsBasic read) {
		int cigar = read.getCigars()[0];
		int cigarOp = (cigar & SystemConfiguration.BAM_CIGAR_MASK);

		if(cigarOp == SystemConfiguration.BAM_CINS)
			return (cigar >> SystemConfiguration.BAM_CIGAR_SHIFT);
		else
			return 0;
	}

	/**
	 * If a read ends in INSERTION, returns the last element length.
	 *
	 * Warning: If the read has Hard or Soft clips after the insertion this
	 * function will return 0.
	 */
	public final static int getLastInsertionOffset(SAMRecord read) {
		CigarElement e = read.getCigar().getCigarElement(
				read.getCigarLength() - 1);
		if (e.getOperator() == CigarOperator.I)
			return e.getLength();
		else
			return 0;
	}

	public final static int getLastInsertionOffset(AlignmentsBasic read) {
		int cigar = read.getCigars()[read.getCigars().length - 1];
		int cigarOp = (cigar & SystemConfiguration.BAM_CIGAR_MASK);

		if(cigarOp == SystemConfiguration.BAM_CINS)
			return (cigar >> SystemConfiguration.BAM_CIGAR_SHIFT);
		else
			return 0;
	}

	/**
	 * Determines what is the position of the read in relation to the interval.
	 * Note: This function uses the UNCLIPPED ENDS of the reads for the
	 * comparison.
	 */
	public static ReadAndIntervalOverlap getReadAndIntervalOverlapType(
			GaeaSamRecord read, GenomeLocation interval) {

		int sStart = read.getSoftStart();
		int sStop = read.getSoftEnd();
		int uStart = read.getUnclippedStart();
		int uStop = read.getUnclippedEnd();

		if (!read.getReferenceName().equals(interval.getContig()))
			return ReadAndIntervalOverlap.NO_OVERLAP_CONTIG;

		else if (uStop < interval.getStart())
			return ReadAndIntervalOverlap.NO_OVERLAP_LEFT;

		else if (uStart > interval.getStop())
			return ReadAndIntervalOverlap.NO_OVERLAP_RIGHT;

		else if (sStop < interval.getStart())
			return ReadAndIntervalOverlap.NO_OVERLAP_HARDCLIPPED_LEFT;

		else if (sStart > interval.getStop())
			return ReadAndIntervalOverlap.NO_OVERLAP_HARDCLIPPED_RIGHT;

		else if ((sStart >= interval.getStart())
				&& (sStop <= interval.getStop()))
			return ReadAndIntervalOverlap.OVERLAP_CONTAINED;

		else if ((sStart < interval.getStart()) && (sStop > interval.getStop()))
			return ReadAndIntervalOverlap.OVERLAP_LEFT_AND_RIGHT;

		else if ((sStart < interval.getStart()))
			return ReadAndIntervalOverlap.OVERLAP_LEFT;

		else
			return ReadAndIntervalOverlap.OVERLAP_RIGHT;
	}

	/**
	 * Pre-processes the results of
	 * getReadCoordinateForReferenceCoordinate(GATKSAMRecord, int) to take care
	 * of two corner cases:
	 * 
	 * 1. If clipping the right tail (end of the read)
	 * getReadCoordinateForReferenceCoordinate and fall inside a deletion return
	 * the base after the deletion. If clipping the left tail (beginning of the
	 * read) it doesn't matter because it already returns the previous base by
	 * default.
	 * 
	 * 2. If clipping the left tail (beginning of the read)
	 * getReadCoordinateForReferenceCoordinate and the read starts with an
	 * insertion, and you're requesting the first read based coordinate, it will
	 * skip the leading insertion (because it has the same reference coordinate
	 * as the following base).
	 */
	public static int getReadCoordinateForReferenceCoordinate(SAMRecord read,
			int refCoord, ClippingTail tail) {
		return getReadCoordinateForReferenceCoordinate(
				SamRecordUtils.getSoftStart(read), read.getCigar(), refCoord,
				tail, false);
	}

	public static int getReadCoordinateForReferenceCoordinate(
			final int alignmentStart, final Cigar cigar, final int refCoord,
			final ClippingTail tail, final boolean allowGoalNotReached) {
		Pair<Integer, Boolean> result = getReadCoordinateForReferenceCoordinate(
				alignmentStart, cigar, refCoord, allowGoalNotReached);
		int readCoord = result.getFirst();

		if (result.getSecond() && tail == ClippingTail.RIGHT_TAIL)
			readCoord++;

		Pair<Boolean, CigarElement> firstElementIsInsertion = readStartsWithInsertion(cigar);
		if (readCoord == 0 && tail == ClippingTail.LEFT_TAIL
				&& firstElementIsInsertion.getFirst())
			readCoord = Math.min(firstElementIsInsertion.getSecond()
					.getLength(), cigar.getReadLength() - 1);

		return readCoord;
	}

	/**
	 * Returns the read coordinate corresponding to the requested reference
	 * coordinate.
	 *
	 * WARNING: if the requested reference coordinate happens to fall inside a
	 * deletion in the read, this function will return the last read base before
	 * the deletion. This function returns a Pair(int readCoord, boolean
	 * fallsInsideDeletion) so you can choose which readCoordinate to use when
	 * faced with a deletion.
	 *
	 * SUGGESTION: Use getReadCoordinateForReferenceCoordinate(GATKSAMRecord,
	 * int, ClippingTail) instead to get a pre-processed result according to
	 * normal clipping needs. Or you can use this function and tailor the
	 * behavior to your needs.
	 */
	public static Pair<Integer, Boolean> getReadCoordinateForReferenceCoordinate(
			GaeaSamRecord read, int refCoord) {
		return getReadCoordinateForReferenceCoordinate(read.getSoftStart(),
				read.getCigar(), refCoord, false);
	}

	public static Pair<Integer, Boolean> getReadCoordinateForReferenceCoordinate(
			final int alignmentStart, final Cigar cigar, final int refCoord,
			final boolean allowGoalNotReached) {
		int readBases = 0;
		int refBases = 0;
		boolean fallsInsideDeletion = false;

		int goal = refCoord - alignmentStart; // The goal is to move this many
												// reference bases
		if (goal < 0) {
			if (allowGoalNotReached) {
				return new Pair<Integer, Boolean>(CLIPPING_GOAL_NOT_REACHED,
						false);
			} else {
				throw new UserException(
						"Somehow the requested coordinate is not covered by the read. Too many deletions?");
			}
		}
		boolean goalReached = refBases == goal;

		Iterator<CigarElement> cigarElementIterator = cigar.getCigarElements()
				.iterator();
		while (!goalReached && cigarElementIterator.hasNext()) {
			CigarElement cigarElement = cigarElementIterator.next();
			int shift = 0;

			if (cigarElement.getOperator().consumesReferenceBases()
					|| cigarElement.getOperator() == CigarOperator.SOFT_CLIP) {
				if (refBases + cigarElement.getLength() < goal)
					shift = cigarElement.getLength();
				else
					shift = goal - refBases;

				refBases += shift;
			}
			goalReached = refBases == goal;

			if (!goalReached && cigarElement.getOperator().consumesReadBases())
				readBases += cigarElement.getLength();

			if (goalReached) {
				// Is this base's reference position within this cigar element?
				// Or did we use it all?
				boolean endsWithinCigar = shift < cigarElement.getLength();

				// If it isn't, we need to check the next one. There should
				// *ALWAYS* be a next one
				// since we checked if the goal coordinate is within the read
				// length, so this is just a sanity check.
				if (!endsWithinCigar && !cigarElementIterator.hasNext()) {
					if (allowGoalNotReached) {
						return new Pair<Integer, Boolean>(
								CLIPPING_GOAL_NOT_REACHED, false);
					} else {
						throw new UserException(
								"Reference coordinate corresponds to a non-existent base in the read. This should never happen -- call Mauricio");
					}
				}

				CigarElement nextCigarElement;

				// if we end inside the current cigar element, we just have to
				// check if it is a deletion
				if (endsWithinCigar)
					fallsInsideDeletion = cigarElement.getOperator() == CigarOperator.DELETION;

				// if we end outside the current cigar element, we need to check
				// if the next element is an insertion or deletion.
				else {
					nextCigarElement = cigarElementIterator.next();

					// if it's an insertion, we need to clip the whole insertion
					// before looking at the next element
					if (nextCigarElement.getOperator() == CigarOperator.INSERTION) {
						readBases += nextCigarElement.getLength();
						if (!cigarElementIterator.hasNext()) {
							if (allowGoalNotReached) {
								return new Pair<Integer, Boolean>(
										CLIPPING_GOAL_NOT_REACHED, false);
							} else {
								throw new UserException(
										"Reference coordinate corresponds to a non-existent base in the read. This should never happen -- call Mauricio");
							}
						}

						nextCigarElement = cigarElementIterator.next();
					}

					// if it's a deletion, we will pass the information on to be
					// handled downstream.
					fallsInsideDeletion = nextCigarElement.getOperator() == CigarOperator.DELETION;
				}

				// If we reached our goal outside a deletion, add the shift
				if (!fallsInsideDeletion
						&& cigarElement.getOperator().consumesReadBases())
					readBases += shift;

				// If we reached our goal inside a deletion, but the deletion is
				// the next cigar element then we need
				// to add the shift of the current cigar element but go back to
				// it's last element to return the last
				// base before the deletion (see warning in function contracts)
				else if (fallsInsideDeletion && !endsWithinCigar)
					readBases += shift - 1;

				// If we reached our goal inside a deletion then we must
				// backtrack to the last base before the deletion
				else if (fallsInsideDeletion && endsWithinCigar)
					readBases--;
			}
		}

		if (!goalReached) {
			if (allowGoalNotReached) {
				return new Pair<Integer, Boolean>(CLIPPING_GOAL_NOT_REACHED,
						false);
			} else {
				throw new UserException(
						"Somehow the requested coordinate is not covered by the read. Alignment "
								+ alignmentStart + " | " + cigar);
			}
		}

		return new Pair<Integer, Boolean>(readBases, fallsInsideDeletion);
	}

	/**
	 * Compares two SAMRecords only the basis on alignment start. Note that
	 * comparisons are performed ONLY on the basis of alignment start; any two
	 * SAM records with the same alignment start will be considered equal.
	 *
	 * Unmapped alignments will all be considered equal.
	 */

	public static int compareSAMRecords(GaeaSamRecord read1, GaeaSamRecord read2) {
		if(read1.getReferenceIndex() != read2.getReferenceIndex())
			return read1.getReferenceIndex() - read2.getReferenceIndex();
		return read1.getAlignmentStart() - read2.getAlignmentStart();
	}

	/**
	 * Is a base inside a read?
	 */
	public static boolean isInsideRead(final GaeaSamRecord read,
			final int referenceCoordinate) {
		return referenceCoordinate >= read.getAlignmentStart()
				&& referenceCoordinate <= read.getAlignmentEnd();
	}

	/**
	 * Is this read all insertion?
	 */
	public static boolean readIsEntirelyInsertion(GaeaSamRecord read) {
		for (CigarElement cigarElement : read.getCigar().getCigarElements()) {
			if (cigarElement.getOperator() != CigarOperator.INSERTION)
				return false;
		}
		return true;
	}

	/**
	 * Checks if a read starts with an insertion. It looks beyond Hard and Soft
	 * clips if there are any.
	 */
	public static Pair<Boolean, CigarElement> readStartsWithInsertion(
			GaeaSamRecord read) {
		return readStartsWithInsertion(read.getCigar());
	}

	public static Pair<Boolean, CigarElement> readStartsWithInsertion(
			final Cigar cigar) {
		for (CigarElement cigarElement : cigar.getCigarElements()) {
			if (cigarElement.getOperator() == CigarOperator.INSERTION)
				return new Pair<Boolean, CigarElement>(true, cigarElement);

			else if (cigarElement.getOperator() != CigarOperator.HARD_CLIP
					&& cigarElement.getOperator() != CigarOperator.SOFT_CLIP)
				break;
		}
		return new Pair<Boolean, CigarElement>(false, null);
	}

	/**
	 * Returns the coverage distribution of a list of reads within the desired
	 * region.
	 *
	 * See getCoverageDistributionOfRead for information on how the coverage is
	 * calculated.
	 */
	public static int[] getCoverageDistributionOfReads(
			List<GaeaSamRecord> list, int startLocation, int stopLocation) {
		int[] totalCoverage = new int[stopLocation - startLocation + 1];

		for (GaeaSamRecord read : list) {
			int[] readCoverage = getCoverageDistributionOfRead(read,
					startLocation, stopLocation);
			totalCoverage = MathUtils.addArrays(totalCoverage, readCoverage);
		}

		return totalCoverage;
	}

	/**
	 * Returns the coverage distribution of a single read within the desired
	 * region.
	 *
	 * Note: This function counts DELETIONS as coverage (since the main purpose
	 * is to downsample reads for variant regions, and deletions count as
	 * variants)
	 */
	public static int[] getCoverageDistributionOfRead(GaeaSamRecord read,
			int startLocation, int stopLocation) {
		int[] coverage = new int[stopLocation - startLocation + 1];
		int refLocation = read.getSoftStart();
		for (CigarElement cigarElement : read.getCigar().getCigarElements()) {
			switch (cigarElement.getOperator()) {
			case S:
			case M:
			case EQ:
			case N:
			case X:
			case D:
				for (int i = 0; i < cigarElement.getLength(); i++) {
					if (refLocation >= startLocation
							&& refLocation <= stopLocation) {
						int baseCount = read.isReducedRead() ? read
								.getReducedCount(refLocation
										- read.getSoftStart()) : 1;
						// this may be a reduced read,so add the proper number
						// of bases
						coverage[refLocation - startLocation] += baseCount;
					}
					refLocation++;
				}
				break;

			case P:
			case I:
			case H:
				break;
			}

			if (refLocation > stopLocation)
				break;
		}
		return coverage;
	}

	/**
	 * Makes association maps for the reads and loci coverage as described below
	 * :
	 *
	 * - First: locusToReadMap -- a HashMap that describes for each locus, which
	 * reads contribute to its coverage. Note: Locus is in reference
	 * coordinates. Example: Locus => {read1, read2, ..., readN}
	 *
	 * - Second: readToLocusMap -- a HashMap that describes for each read what
	 * loci it contributes to the coverage. Note: Locus is a boolean array,
	 * indexed from 0 (= startLocation) to N (= stopLocation), with value==true
	 * meaning it contributes to the coverage. Example: Read => {true, true,
	 * false, ... false}
	 */
	public static Pair<HashMap<Integer, HashSet<GaeaSamRecord>>, HashMap<GaeaSamRecord, Boolean[]>> getBothReadToLociMappings(
			List<GaeaSamRecord> readList, int startLocation, int stopLocation) {
		int arraySize = stopLocation - startLocation + 1;

		HashMap<Integer, HashSet<GaeaSamRecord>> locusToReadMap = new HashMap<Integer, HashSet<GaeaSamRecord>>(
				2 * (stopLocation - startLocation + 1), 0.5f);
		HashMap<GaeaSamRecord, Boolean[]> readToLocusMap = new HashMap<GaeaSamRecord, Boolean[]>(
				2 * readList.size(), 0.5f);

		for (int i = startLocation; i <= stopLocation; i++)
			locusToReadMap.put(i, new HashSet<GaeaSamRecord>()); // Initialize						

		for (GaeaSamRecord read : readList) {
			readToLocusMap.put(read, new Boolean[arraySize]); 

			int[] readCoverage = getCoverageDistributionOfRead(read,
					startLocation, stopLocation);

			for (int i = 0; i < readCoverage.length; i++) {
				int refLocation = i + startLocation;
				if (readCoverage[i] > 0) {
					// Update the hash for this locus
					HashSet<GaeaSamRecord> readSet = locusToReadMap
							.get(refLocation);
					readSet.add(read);

					// Add this locus to the read hash
					readToLocusMap.get(read)[refLocation - startLocation] = true;
				} else
					// Update the boolean array with a 'no coverage' from this
					// read to this locus
					readToLocusMap.get(read)[refLocation - startLocation] = false;
			}
		}
		return new Pair<HashMap<Integer, HashSet<GaeaSamRecord>>, HashMap<GaeaSamRecord, Boolean[]>>(
				locusToReadMap, readToLocusMap);
	}

	public static String prettyPrintSequenceRecords(
			SAMSequenceDictionary sequenceDictionary) {
		String[] sequenceRecordNames = new String[sequenceDictionary.size()];
		int sequenceRecordIndex = 0;
		for (SAMSequenceRecord sequenceRecord : sequenceDictionary
				.getSequences())
			sequenceRecordNames[sequenceRecordIndex++] = sequenceRecord
					.getSequenceName();
		return Arrays.deepToString(sequenceRecordNames);
	}

	/**
	 * Calculates the reference coordinate for a read coordinate
	 *
	 * @param read
	 *            the read
	 * @param offset
	 *            the base in the read (coordinate in the read)
	 * @return the reference coordinate correspondent to this base
	 */
	public static long getReferenceCoordinateForReadCoordinate(
			GaeaSamRecord read, int offset) {
		if (offset > read.getReadLength())
			throw new OutOfBoundException(offset,read.getReadLength());

		long location = read.getAlignmentStart();
		Iterator<CigarElement> cigarElementIterator = read.getCigar()
				.getCigarElements().iterator();
		while (offset > 0 && cigarElementIterator.hasNext()) {
			CigarElement cigarElement = cigarElementIterator.next();
			long move = 0;
			if (cigarElement.getOperator().consumesReferenceBases())
				move = (long) Math.min(cigarElement.getLength(), offset);
			location += move;
			offset -= move;
		}
		if (offset > 0 && !cigarElementIterator.hasNext())
			throw new UserException(OFFSET_NOT_ZERO_EXCEPTION);

		return location;
	}

	/**
	 * Creates a map with each event in the read (cigar operator) and the read
	 * coordinate where it happened.
	 *
	 * Example: D -> 2, 34, 75 I -> 55 S -> 0, 101 H -> 101
	 *
	 * @param read
	 *            the read
	 * @return a map with the properties described above. See example
	 */
	public static Map<CigarOperator, ArrayList<Integer>> getCigarOperatorForAllBases(
			GaeaSamRecord read) {
		Map<CigarOperator, ArrayList<Integer>> events = new HashMap<CigarOperator, ArrayList<Integer>>();

		int position = 0;
		for (CigarElement cigarElement : read.getCigar().getCigarElements()) {
			CigarOperator op = cigarElement.getOperator();
			if (op.consumesReadBases()) {
				ArrayList<Integer> list = events.get(op);
				if (list == null) {
					list = new ArrayList<Integer>();
					events.put(op, list);
				}
				for (int i = position; i < cigarElement.getLength(); i++)
					list.add(position++);
			} else {
				ArrayList<Integer> list = events.get(op);
				if (list == null) {
					list = new ArrayList<Integer>();
					events.put(op, list);
				}
				list.add(position);
			}
		}
		return events;
	}
}
