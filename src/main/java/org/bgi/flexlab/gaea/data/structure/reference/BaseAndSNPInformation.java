package org.bgi.flexlab.gaea.data.structure.reference;

import org.bgi.flexlab.gaea.data.exception.OutOfBoundException;

public class BaseAndSNPInformation {
	private boolean[] snps = null;
	private int start;
	private String sequences = null;

	public BaseAndSNPInformation() {
	}

	public void set(ReferenceShare reference, String chrName, int start, int end) {
		ChromosomeInformationShare chrInfo = reference.getChromosomeInfo(chrName);
		set(chrInfo, start, end);
	}

	public void set(ChromosomeInformationShare chrInfo, int _start, int _end) {
		_start--;
		_end--;

		this.start = _start;
		byte[] bases = chrInfo.getBytes(_start, _end);

		sequences = chrInfo.getBaseSequence(bases, _start, _end);
		snps = chrInfo.isSNPs(bases, _start, _end);
	}

	public boolean[] getSNPs() {
		return snps;
	}

	public boolean getSNP(int pos) {
		int index = pos - 1 - start;
		if (index >= snps.length)
			throw new OutOfBoundException(pos, start + snps.length);

		return snps[index];
	}

	public String getSequences() {
		return sequences;
	}

	public String getSequences(int _start, int length) {
		int index = _start - 1 - start;
		return sequences.substring(index, index + length);
	}

	public char getBase(int pos) {
		int index = pos - 1 - start;
		if (index >= sequences.length())
			throw new OutOfBoundException(pos, start + sequences.length());

		return sequences.charAt(index);
	}
}
