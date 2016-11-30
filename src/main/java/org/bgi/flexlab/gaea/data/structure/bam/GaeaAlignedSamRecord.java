package org.bgi.flexlab.gaea.data.structure.bam;

import htsjdk.samtools.Cigar;
import htsjdk.samtools.CigarElement;

public class GaeaAlignedSamRecord {
	public static final String ORIGINAL_CIGAR_TAG = "OC";
	public static final String ORIGINAL_POSITION_TAG = "OP";
	public int MAX_POSITION_MOVE_ALLOWED = 200;
	private GaeaSamRecord read;
	private byte[] readBases = null;
	private byte[] baseQualities = null;
	private Cigar newCigar = null;
	private int newStart = -1;
	private int mismatchScore = 0;
	private int alignerMismatchScore = 0;
	private boolean ignoredOriginTag = false;

	public GaeaAlignedSamRecord(GaeaSamRecord read) {
		this.read = read;
		this.mismatchScore = 0;
	}

	public GaeaAlignedSamRecord(GaeaSamRecord read, int max_allow) {
		this(read);
		this.MAX_POSITION_MOVE_ALLOWED = max_allow;
	}

	public GaeaAlignedSamRecord(GaeaSamRecord read, int max_allow, boolean noTag) {
		this(read, max_allow);
		this.ignoredOriginTag = noTag;
	}

	public void setMismatchScore(int score) {
		this.mismatchScore = score;
	}

	public void setNewStart(int start) {
		this.newStart = start;
	}

	public void setAlignerMismatchScore(int score) {
		this.alignerMismatchScore = score;
	}

	public void setCigar(Cigar cigar) {
		setCigar(cigar, true);
	}

	public void setCigar(Cigar cigar, boolean reclip) {
		if (cigar == null) {
			return;
		}

		if (reclip && getReadBases().length < read.getReadLength()) {
			cigar = GaeaCigar.reclipCigar(cigar, read);
		}

		if (cigar.equals(read.getCigar())) {
			cigar = null;
		}

		newCigar = cigar;
	}

	public boolean statusFinalize() {
		if (newCigar == null) {
			return false;
		}

		newStart = getAlignmentStart();
		if (Math.abs(newStart - read.getAlignmentStart()) > this.MAX_POSITION_MOVE_ALLOWED)
			return false;

		if (!ignoredOriginTag) {
			read.setAttribute(ORIGINAL_CIGAR_TAG, read.getCigar().toString());
			if (newStart != read.getAlignmentStart())
				read.setAttribute(ORIGINAL_POSITION_TAG,
						read.getAlignmentStart());
		}

		read.setCigar(newCigar);
		read.setAlignmentStart(newStart);

		return true;
	}

	public byte[] getReadBases() {
		if (readBases == null)
			getUnclippedInformation();
		return readBases;
	}

	public byte[] getReadQualities() {
		if (baseQualities == null)
			getUnclippedInformation();
		return baseQualities;
	}

	private void getUnclippedInformation() {
		readBases = new byte[read.getReadLength()];
		baseQualities = new byte[read.getReadLength()];

		int startIndex = 0, baseCount = 0;

		byte[] reads = read.getReadBases();
		byte[] qualities = read.getBaseQualities();

		for (CigarElement element : read.getCigar().getCigarElements()) {
			int eLength = element.getLength();
			switch (element.getOperator()) {
			case S:
				startIndex += eLength;
				break;
			case M:
			case X:
			case EQ:
			case I:
				System.arraycopy(reads, startIndex, readBases, baseCount,
						eLength);
				System.arraycopy(qualities, startIndex, baseQualities,
						baseCount, eLength);
				startIndex += eLength;
				baseCount += eLength;
			default:
				break;
			}
		}

		if (startIndex != baseCount) {
			byte[] baseTemp = new byte[baseCount];
			byte[] qualTemp = new byte[baseCount];
			System.arraycopy(readBases, 0, baseTemp, 0, baseCount);
			System.arraycopy(baseQualities, 0, qualTemp, 0, baseCount);
			readBases = baseTemp;
			baseQualities = qualTemp;
		}
	}

	public GaeaSamRecord getRead() {
		return this.read;
	}

	public Cigar getCigar() {
		return newCigar == null ? read.getCigar() : newCigar;
	}

	public int getAlignmentStart() {
		return newStart == -1 ? read.getAlignmentStart() : newStart;
	}

	public int getMismatchScore() {
		return this.mismatchScore;
	}

	public int getAlignerMismatchScore() {
		return this.alignerMismatchScore;
	}

	public int getReadLength() {
		return getReadBases().length;
	}
}
