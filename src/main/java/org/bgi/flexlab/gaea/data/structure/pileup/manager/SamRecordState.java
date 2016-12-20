package org.bgi.flexlab.gaea.data.structure.pileup.manager;

import org.bgi.flexlab.gaea.data.exception.BadCigarException;
import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocationParser;

import htsjdk.samtools.Cigar;
import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;

public class SamRecordState {
	private int cigarElementOffset = 0;
	private int readOffset = -1;
	private int genomeOffset = -1;
	private int cigarElementsNumbers = -1;
	private int cigarOffset = -1;

	private Cigar cigar = null;
	private CigarElement currentCigarElement = null;

	private GaeaSamRecord read = null;

	public SamRecordState(GaeaSamRecord read) {
		this.read = read;
		cigar = read.getCigar();
		cigarElementsNumbers = cigar.numCigarElements();
		
		if(isBadCigar()){
			throw new BadCigarException(
					"read starts or ends with deletion. Cigar: "
							+ read.getCigarString() + ".");
		}
	}

	private boolean isBadCigar() {
		CigarOperator firstOp = cigar.getCigarElement(0).getOperator();
		CigarOperator lastOp = cigar.getCigarElement(cigarElementsNumbers - 1)
				.getOperator();
		
		if(firstOp == CigarOperator.D || lastOp == CigarOperator.D)
			return true;
		return false;
	}

	public int getReadOffset() {
		return readOffset;
	}

	public int getGenomeOffset() {
		return genomeOffset;
	}
	
	public GaeaSamRecord getRead(){
		return read;
	}

	public int getGenomePosition() {
		return read.getAlignmentStart() + genomeOffset;
	}

	public CigarElement getNextCigarElement() {
		boolean currElementAllPass = (cigarElementOffset + 1) > currentCigarElement
				.getLength() && cigarOffset + 1 < cigarElementsNumbers;
		return currElementAllPass ? cigar.getCigarElement(cigarOffset + 1)
				: currentCigarElement;
	}

	public CigarElement getLastCigarElement() {
		boolean atBeginOfElement = (cigarElementOffset - 1 == 0)
				&& (cigarOffset - 1 > 0);
		return atBeginOfElement ? cigar.getCigarElement(cigarOffset - 1)
				: currentCigarElement;
	}

	public CigarElement getCurrentCigarElement() {
		return currentCigarElement;
	}

	public String toString() {
		return String.format("read offset=%d genome offset=%d cigar offset=%d", readOffset, genomeOffset,
				cigarOffset);
	}

	public boolean hasNext() {
		return readOffset + 1 == read.getReadLength() ? false : true;
	}
	
	public GenomeLocation getLocation(GenomeLocationParser genomeLocParser){
		return genomeLocParser.createGenomeLocation(read.getReferenceName(), getGenomePosition());
	}

	public CigarOperator stepForwardOnGenome() {
		if (currentCigarElement == null
				|| ++cigarElementOffset > currentCigarElement.getLength()) {
			cigarOffset++;
			if (cigarOffset < cigarElementsNumbers) {
				currentCigarElement = cigar.getCigarElement(cigarOffset);
				cigarElementOffset = 0;
				return stepForwardOnGenome();
			} else {
				genomeOffset++;
				return null;
			}
		}

		boolean done = false;
		switch (currentCigarElement.getOperator()) {
		case H: // ignore hard clips
		case P: // ignore pads
			cigarElementOffset = currentCigarElement.getLength();
			break;
		case I: // insertion w.r.t. the reference
		case S: // soft clip
			cigarElementOffset = currentCigarElement.getLength();
			readOffset += currentCigarElement.getLength();
			break;
		case N: 
		case D: // reference skip 
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
			throw new BadCigarException(
					"Case statement didn't deal with cigar op: "
							+ currentCigarElement.getOperator());
		}

		return done ? currentCigarElement.getOperator() : stepForwardOnGenome();
	}
}
