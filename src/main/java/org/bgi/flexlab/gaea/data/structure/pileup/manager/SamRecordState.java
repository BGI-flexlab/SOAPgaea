package org.bgi.flexlab.gaea.data.structure.pileup.manager;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocationParser;
import org.bgi.flexlab.gaea.exception.MalformedReadException;

import htsjdk.samtools.Cigar;
import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;

public class SamRecordState {
	GaeaSamRecord read = null;
	int readOffset = -1; // how far are we offset from the start of the read
							// bases?
	int genomeOffset = -1; // how far are we offset from the alignment start on
							// the genome?

	Cigar cigar = null;
	int cigarOffset = -1;
	CigarElement curElement = null;
	int nCigarElements = 0;

	int cigarElementCounter = -1; // how far are we into a single cigarElement

	public SamRecordState(GaeaSamRecord read) {
		this.read = read;
		cigar = read.getCigar();
		nCigarElements = cigar.numCigarElements();
	}

	public GaeaSamRecord getRead() {
		return read;
	}

	/**
	 * What is our current offset in the read's bases that aligns us with the
	 * reference genome?
	 */
	public int getReadOffset() {
		return readOffset;
	}

	/**
	 * What is the current offset w.r.t. the alignment state that aligns us to
	 * the readOffset?
	 */
	public int getGenomeOffset() {
		return genomeOffset;
	}

	public int getGenomePosition() {
		return read.getAlignmentStart() + getGenomeOffset();
	}

	public GenomeLocation getLocation(GenomeLocationParser genomeLocParser) {
		return genomeLocParser.createGenomeLoc(read.getReferenceName(),
				getGenomePosition());
	}

	public CigarOperator getCurrentCigarOperator() {
		return curElement.getOperator();
	}

	public String toString() {
		return String.format("%s ro=%d go=%d co=%d cec=%d %s",
				read.getReadName(), readOffset, genomeOffset, cigarOffset,
				cigarElementCounter, curElement);
	}

	public boolean hasNext() {
		return readOffset + 1 == read.getReadLength() ? false : true;
	}

	public CigarElement peekForwardOnGenome() {
		return (cigarElementCounter + 1 > curElement.getLength()
				&& cigarOffset + 1 < nCigarElements ? cigar
				.getCigarElement(cigarOffset + 1) : curElement);
	}

	public CigarElement peekBackwardOnGenome() {
		return (cigarElementCounter - 1 == 0 && cigarOffset - 1 > 0 ? cigar
				.getCigarElement(cigarOffset - 1) : curElement);
	}

	/**/
	public CigarOperator stepForwardOnGenome() {
		if (curElement == null
				|| ++cigarElementCounter > curElement.getLength()) {
			cigarOffset++;
			if (cigarOffset < nCigarElements) {
				curElement = cigar.getCigarElement(cigarOffset);
				cigarElementCounter = 0;
				return stepForwardOnGenome();
			} else {
				if (curElement != null
						&& curElement.getOperator() == CigarOperator.D)
					throw new MalformedReadException(
							"read ends with deletion. Cigar: "
									+ read.getCigarString()
									+ ". Although the SAM spec technically permits such reads, this is often indicative of malformed files. If you are sure you want to use this file, re-run your analysis with the extra option: -rf BadCigar",
							read);

				genomeOffset++;

				return null;
			}
		}

		boolean done = false;
		switch (curElement.getOperator()) {
		case H: // ignore hard clips
		case P: // ignore pads
			cigarElementCounter = curElement.getLength();
			break;
		case I: // insertion w.r.t. the reference
		case S: // soft clip
			cigarElementCounter = curElement.getLength();
			readOffset += curElement.getLength();
			break;
		case D: // deletion w.r.t. the reference
			if (readOffset < 0) // we don't want reads starting with deletion,
								// this is a malformed cigar string
				throw new MalformedReadException(
						"read starts with deletion. Cigar: "
								+ read.getCigarString()
								+ ". Although the SAM spec technically permits such reads, this is often indicative of malformed files. If you are sure you want to use this file, re-run your analysis with the extra option: -rf BadCigar",
						read);
			// should be the same as N case
			genomeOffset++;
			done = true;
			break;
		case N: // reference skip (looks and gets processed just like a
				// "deletion", just different logical meaning)
			genomeOffset++;
			done = true;
			break;
		case M:
		case EQ:
		case X:
			readOffset++;
			genomeOffset++;
			done = true;
			break;
		default:
			throw new IllegalStateException(
					"Case statement didn't deal with cigar op: "
							+ curElement.getOperator());
		}

		return done ? curElement.getOperator() : stepForwardOnGenome();
	}
}
