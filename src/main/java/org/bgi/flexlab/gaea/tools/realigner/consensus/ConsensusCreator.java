package org.bgi.flexlab.gaea.tools.realigner.consensus;

import htsjdk.samtools.Cigar;
import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;
import htsjdk.samtools.util.StringUtil;
import htsjdk.variant.variantcontext.VariantContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

public class ConsensusCreator {
	private Set<AlternateConsensus> consensus = null;

	public ConsensusCreator() {
		consensus = new LinkedHashSet<AlternateConsensus>();
	}

	private void addAlternateConsensus(int start, final byte[] reference,
			final VariantContext knowIndel, final byte[] indelStr) {
		if (start < 0 || start >= reference.length)
			return;

		StringBuilder sb = new StringBuilder();
		Cigar cigar = new Cigar();
		int refIdx;

		for (refIdx = 0; refIdx < start; refIdx++)
			sb.append((char) reference[refIdx]);
		if (start > 0)
			cigar.add(new CigarElement(start, CigarOperator.M));

		if (knowIndel.isSimpleDeletion()) {
			refIdx += indelStr.length;
			cigar.add(new CigarElement(indelStr.length, CigarOperator.D));
		} else if (knowIndel.isSimpleInsertion()) {
			for (byte b : indelStr)
				sb.append((char) b);
			cigar.add(new CigarElement(indelStr.length, CigarOperator.I));
		} else {
			throw new IllegalStateException(
					"Creating an alternate consensus from a complex indel is not allows");
		}

		if (reference.length - refIdx > 0)
			cigar.add(new CigarElement(reference.length - refIdx,
					CigarOperator.M));
		for (; refIdx < reference.length; refIdx++)
			sb.append((char) reference[refIdx]);
		byte[] altConsensus = StringUtil.stringToBytes(sb.toString()); 

		consensus.add(new AlternateConsensus(altConsensus, cigar, 0));
	}

	public void consensusFromKnowIndels(
			final ArrayList<VariantContext> knowIndels,
			final int leftmostIndex, final byte[] reference) {
		for (VariantContext variant : knowIndels) {
			if (variant == null || variant.isComplexIndel()
					|| !variant.isIndel())
				continue;
			byte[] indelStr;
			if (variant.isSimpleInsertion()) {
				byte[] indel = variant.getAlternateAllele(0).getBases();
				indelStr = new byte[indel.length - 1];
				System.arraycopy(indel, 1, indelStr, 0, indel.length - 1);
			} else {
				indelStr = new byte[variant.getReference().length() - 1];
				Arrays.fill(indelStr, (byte) '-');
			}
			addAlternateConsensus(variant.getStart() - leftmostIndex + 1,
					reference, variant, indelStr);
		}
	}
}
