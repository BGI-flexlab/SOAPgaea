package org.bgi.flexlab.gaea.data.structure.bam.filter.util;

import htsjdk.samtools.Cigar;
import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;
import htsjdk.samtools.SAMRecord;

import java.util.Iterator;

/**
 * Created by zhangyong on 2017/3/9.
 */
public class FiltersMethod {

    public static boolean filterUnmappedReads(SAMRecord read) {
        return read.getReadUnmappedFlag() || read.getAlignmentStart() == SAMRecord.NO_ALIGNMENT_START;
    }

    public static boolean filterNotPrimaryAlignment(SAMRecord read) {
        return read.getNotPrimaryAlignmentFlag();
    }

    public static boolean filterDuplicateRead(SAMRecord read) {
        return read.getDuplicateReadFlag();
    }

    public static boolean filterMappingQualityUnavailable(SAMRecord read,int unavailableQuality) {
        return (read.getMappingQuality() == unavailableQuality);
    }

    public static boolean FailsVendorQualityCheckFilter(SAMRecord read) {
        return read.getReadFailsVendorQualityCheckFlag();
    }

    public static boolean filterBadCigar(SAMRecord read) {
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

    public static boolean filterBadMate(SAMRecord read) {
        return (read.getReadPairedFlag() && !read.getMateUnmappedFlag() && !read
                .getReferenceIndex().equals(read.getMateReferenceIndex()));
    }
}
