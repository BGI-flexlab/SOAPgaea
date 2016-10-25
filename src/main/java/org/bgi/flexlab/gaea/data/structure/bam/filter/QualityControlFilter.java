package org.bgi.flexlab.gaea.data.structure.bam.filter;

import java.util.Iterator;

import htsjdk.samtools.Cigar;
import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;
import htsjdk.samtools.SAMRecord;

import org.bgi.flexlab.gaea.data.structure.region.Region;

public class QualityControlFilter implements SamRecordFilter {
	private ReadsFilter readsFilter = new ReadsFilter();
	private MalformedReadFilter malformedReadFilter = new MalformedReadFilter();

	public boolean filterBadCigar(SAMRecord read) {
		final Cigar c = read.getCigar();

		// if there is no Cigar then it can't be bad
		if (c.isEmpty()) {
			return false;
		}

		Iterator<CigarElement> elementIterator = c.getCigarElements()
				.iterator();

		CigarOperator firstOp = CigarOperator.H;
		while (elementIterator.hasNext()
				&& (firstOp == CigarOperator.H || firstOp == CigarOperator.S)) {
			CigarOperator op = elementIterator.next().getOperator();

			// No reads with Hard/Soft clips in the middle of the cigar
			if (firstOp != CigarOperator.H && op == CigarOperator.H) {
				return true;
			}
			firstOp = op;
		}

		// No reads starting with deletions (with or without preceding clips)
		if (firstOp == CigarOperator.D) {
			return true;
		}

		boolean hasMeaningfulElements = (firstOp != CigarOperator.H && firstOp != CigarOperator.S);
		boolean previousElementWasIndel = firstOp == CigarOperator.I;
		CigarOperator lastOp = firstOp;
		CigarOperator previousOp = firstOp;

		while (elementIterator.hasNext()) {
			CigarOperator op = elementIterator.next().getOperator();

			if (op != CigarOperator.S && op != CigarOperator.H) {

				// No reads with Hard/Soft clips in the middle of the cigar
				if (previousOp == CigarOperator.S
						|| previousOp == CigarOperator.H)
					return true;

				lastOp = op;

				if (!hasMeaningfulElements && op.consumesReadBases()) {
					hasMeaningfulElements = true;
				}

				if (op == CigarOperator.I || op == CigarOperator.D) {

					// No reads that have consecutive indels in the cigar (II,
					// DD, ID or DI)
					if (previousElementWasIndel) {
						return true;
					}
					previousElementWasIndel = true;
				} else {
					previousElementWasIndel = false;
				}
			}
			// No reads with Hard/Soft clips in the middle of the cigar
			else if (op == CigarOperator.S && previousOp == CigarOperator.H) {
				return true;
			}

			previousOp = op;
		}

		// No reads ending in deletions (with or without follow-up clips)
		// No reads that are fully hard or soft clipped
		return lastOp == CigarOperator.D || !hasMeaningfulElements;
	}

	public boolean filterBadMate(SAMRecord read) {
		return (read.getReadPairedFlag() && !read.getMateUnmappedFlag() && !read
				.getReferenceIndex().equals(read.getMateReferenceIndex()));
	}

	@Override
	public boolean filter(SAMRecord sam, Region region) {
		return filterBadCigar(sam) || filterBadMate(sam)
				|| readsFilter.filter(sam, region)
				|| malformedReadFilter.filter(sam, region);
	}
}
