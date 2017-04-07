package org.bgi.flexlab.gaea.tools.realigner.alternateconsensus;

import htsjdk.samtools.Cigar;
import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;
import htsjdk.samtools.util.StringUtil;
import htsjdk.variant.variantcontext.VariantContext;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;

import org.bgi.flexlab.gaea.util.BaseUtils;

public class AlternateConsensusBin {
	private Set<AlternateConsensus> consensus = null;
	
	public AlternateConsensusBin(){
		consensus = new LinkedHashSet<AlternateConsensus>();
	}
	
	public Set<AlternateConsensus> get(){
		return this.consensus;
	}
	
	public void addAlternateConsensus(int start, final byte[] reference,
			final VariantContext knowIndel, final byte[] indelStr) {
		if (start < 0 || start >= reference.length
				|| knowIndel.isComplexIndel())
			return;

		StringBuilder sb = new StringBuilder();
		ArrayList<CigarElement> elements = new ArrayList<CigarElement>();
		int refIdx;

		for (refIdx = 0; refIdx < start; refIdx++)
			sb.append((char) reference[refIdx]);
		if (start > 0)
			elements.add(new CigarElement(start, CigarOperator.M));

		if (knowIndel.isSimpleDeletion()) {
			refIdx += indelStr.length;
			elements.add(new CigarElement(indelStr.length, CigarOperator.D));
		} else if (knowIndel.isSimpleInsertion()) {
			for (byte b : indelStr)
				sb.append((char) b);
			elements.add(new CigarElement(indelStr.length, CigarOperator.I));
		}

		if (reference.length - refIdx > 0)
			elements.add(new CigarElement(reference.length - refIdx,
					CigarOperator.M));
		for (; refIdx < reference.length; refIdx++)
			sb.append((char) reference[refIdx]);
		byte[] altConsensus = StringUtil.stringToBytes(sb.toString());

		consensus.add(new AlternateConsensus(altConsensus, new Cigar(elements),
				0));
	}

	public void addAlternateConsensus(byte[] ref, int posOnRef, byte[] read,
			Cigar cigar) {
		if (posOnRef < 0
				|| posOnRef >= ref.length
				|| (cigar.numCigarElements() == 1 && cigar.getCigarElement(0)
						.getOperator() == CigarOperator.M))
			return;

		StringBuilder sb = new StringBuilder();

		int i, j;
		for (i = 0; i < posOnRef; i++)
			sb.append((char) ref[i]);

		int readIndex = 0;
		int refIndex = posOnRef;

		boolean valid = true;
		int indelCount = 0;
		ArrayList<CigarElement> elements = new ArrayList<CigarElement>();
		for (i = 0; i < cigar.numCigarElements(); i++) {
			CigarElement ce = cigar.getCigarElement(i);
			int length = ce.getLength();

			switch (ce.getOperator()) {
			case M:
			case X:
			case EQ:
				readIndex += length;
			case N:
				if (refIndex + length > ref.length) {
					valid = false;
				} else {
					for (j = 0; j < length; j++) {
						sb.append((char) ref[refIndex + j]);
					}
				}
				refIndex += length;
				elements.add(new CigarElement(ce.getLength(), CigarOperator.M));
				break;
			case I:
				indelCount++;
				for (j = 0; j < length; j++) {
					byte base = read[readIndex+j];
					if (!BaseUtils.isRegularBase(base)) {
						valid = false;
						break;
					}
					sb.append((char) base);
				}
				readIndex += length;
				elements.add(ce);
				break;
			case D:
				indelCount++;
				refIndex += length;
				elements.add(ce);
				break;
			default:
				break;
			}
		}

		if (!valid || indelCount != 1 || refIndex > ref.length)
			return;

		for (i = refIndex; i < ref.length; i++)
			sb.append((char) ref[i]);
		byte[] altConsensus = StringUtil.stringToBytes(sb.toString());

		consensus.add(new AlternateConsensus(altConsensus, new Cigar(elements),
				posOnRef));
	}
}
