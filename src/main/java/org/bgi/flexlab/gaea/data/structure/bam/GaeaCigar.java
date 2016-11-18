package org.bgi.flexlab.gaea.data.structure.bam;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import htsjdk.samtools.Cigar;
import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;

public class GaeaCigar {

	public static boolean isClipOperator(CigarOperator op) {
		return op == CigarOperator.S || op == CigarOperator.H
				|| op == CigarOperator.P;
	}

	public static Cigar unclipCigar(Cigar cigar) {
		ArrayList<CigarElement> elements = new ArrayList<CigarElement>(
				cigar.numCigarElements());
		for (CigarElement ce : cigar.getCigarElements()) {
			if (!isClipOperator(ce.getOperator()))
				elements.add(ce);
		}
		return new Cigar(elements);
	}

	public static Cigar reclipCigar(Cigar cigar, GaeaSamRecord read) {
		ArrayList<CigarElement> elements = new ArrayList<CigarElement>();
		int n = read.getCigar().numCigarElements();
		int i;
		for (i = 0; i < n; i++) {
			CigarOperator op = read.getCigar().getCigarElement(i).getOperator();
			if (isClipOperator(op))
				elements.add(read.getCigar().getCigarElement(i));
		}

		elements.addAll(cigar.getCigarElements());

		for (++i; i < n; i++) {
			CigarOperator op = read.getCigar().getCigarElement(i).getOperator();
			if (isClipOperator(op))
				elements.add(read.getCigar().getCigarElement(i));
		}

		return new Cigar(elements);
	}

	public static int numberOfMatchCigarOperator(Cigar cigar) {
		int number = 0;

		if (cigar != null) {
			for (CigarElement element : cigar.getCigarElements()) {
				if (element.getOperator() == CigarOperator.M)
					number++;
			}
		}

		return number;
	}

	public static int firstIndexOfIndel(Cigar cigar) {
		int indexOfIndel = -1;

		for (int i = 0; i < cigar.numCigarElements(); i++) {
			CigarOperator op = cigar.getCigarElement(i).getOperator();
			if (op == CigarOperator.I || op == CigarOperator.D) {
				if (indexOfIndel != -1)
					return -1;
				indexOfIndel = i;
			}
		}
		return indexOfIndel;
	}

	public static Cigar moveCigarLeft(Cigar cigar, int indexOfIndel, int step) {
		if (cigar == null)
			return null;

		ArrayList<CigarElement> cigarElements = new ArrayList<CigarElement>(
				cigar.numCigarElements());
		for (int i = 0; i < indexOfIndel - 1; i++)
			cigarElements.add(cigar.getCigarElement(i));

		CigarElement ce = cigar.getCigarElement(indexOfIndel - 1);
		if (ce.getLength() < step)
			return null;
		cigarElements.add(new CigarElement(ce.getLength() - step, ce
				.getOperator()));
		cigarElements.add(cigar.getCigarElement(indexOfIndel));
		if (indexOfIndel + 1 < cigar.numCigarElements()) {
			ce = cigar.getCigarElement(indexOfIndel + 1);
			cigarElements.add(new CigarElement(ce.getLength() + step, ce
					.getOperator()));
		} else {
			cigarElements.add(new CigarElement(step, CigarOperator.M));
		}

		for (int i = indexOfIndel + 2; i < cigar.numCigarElements(); i++)
			cigarElements.add(cigar.getCigarElement(i));

		return new Cigar(cigarElements);
	}

	public static boolean cigarHasZeroSizeElement(Cigar cigar) {
		if (cigar != null) {
			for (CigarElement ce : cigar.getCigarElements()) {
				if (ce.getLength() == 0)
					return true;
			}
		}
		return false;
	}

	public static Cigar cleanCigar(Cigar cigar) {
		ArrayList<CigarElement> elements = new ArrayList<CigarElement>(
				cigar.numCigarElements() - 1);
		for (CigarElement ce : cigar.getCigarElements()) {
			if (ce.getLength() != 0
					&& (elements.size() != 0 || ce.getOperator() != CigarOperator.D)) {
				elements.add(ce);
			}
		}
		return new Cigar(elements);
	}
	
	public static Cigar decode(final ByteBuffer binaryCigar) {
        final Cigar ret = new Cigar();
        while (binaryCigar.hasRemaining()) {
            final int cigarette = binaryCigar.getInt();
            ret.add(binaryCigarToCigarElement(cigarette));
        }
        return ret;
    }
	
	private static CigarElement binaryCigarToCigarElement(final int cigarette) {
        final int binaryOp = cigarette & 0xf;
        final int length = cigarette >> 4;
        return new CigarElement(length, CigarOperator.binaryToEnum(binaryOp));
    }
}
