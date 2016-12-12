package org.bgi.flexlab.gaea.data.structure.reference;

import org.bgi.flexlab.gaea.exception.OutOfBoundException;

public class BaseAndSNPInformation {
	private boolean[] snps = null;
	private int start;
	private String sequences = null;

	public BaseAndSNPInformation(ChromosomeInformationShare chrInfo, int start, int end) {
		set(chrInfo, start, end);
	}

	public void set(ChromosomeInformationShare chrInfo, int start, int end) {
		this.start = start;
		byte[] bases = chrInfo.getBytes(start, end);

		sequences = chrInfo.getBaseSequence(bases, start, end);
		snps = chrInfo.isSNPs(bases, start, end);
	}

	public boolean[] getSNPs() {
		return snps;
	}

	public boolean getSNP(int pos) {
		int index = pos - start;
		if (index >= snps.length)
			throw new OutOfBoundException(pos, start + snps.length);

		return snps[index];
	}

	public String getSequences() {
		return sequences;
	}

	public char getBase(int pos) {
		int index = pos - start;
		if (index >= sequences.length())
			throw new OutOfBoundException(pos, start + sequences.length());

		return sequences.charAt(index);
	}
}
