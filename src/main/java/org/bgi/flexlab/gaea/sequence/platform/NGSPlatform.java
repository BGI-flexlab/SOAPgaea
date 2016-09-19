package org.bgi.flexlab.gaea.sequence.platform;

import htsjdk.samtools.SAMReadGroupRecord;
import htsjdk.samtools.SAMRecord;

public enum NGSPlatform {
    ILLUMINA("ILLUMINA", "SLX", "SOLEXA"),
    SOLID("SOLID"),
    LS454("454"),
    COMPLETE_GENOMICS("COMPLETE"),
    PACBIO("PACBIO"),
    ION_TORRENT("IONTORRENT"),
    UNKNOWN("UNKNOWN");

    /**
     * Array of the prefix names in a BAM file for each of the platforms.
     */
    private final String[] BAM_PL_NAMES;

    private NGSPlatform(final String... BAM_PL_NAMES) {
        for ( int i = 0; i < BAM_PL_NAMES.length; i++ )
            BAM_PL_NAMES[i] = BAM_PL_NAMES[i].toUpperCase();
        this.BAM_PL_NAMES = BAM_PL_NAMES;
    }

    /**
     * Returns a representative PL string for this platform
     * @return
     */
    public final String getDefaultPlatform() {
        return BAM_PL_NAMES[0];
    }

    /**
     * Convenience constructor -- calculates the NGSPlatfrom from a SAMRecord.
     * Note you should not use this function if you have a GATKSAMRecord -- use the
     * accessor method instead.
     */
    public static final NGSPlatform fromRead(SAMRecord read) {
        return fromReadGroup(read.getReadGroup());
    }

    /**
     * Returns the NGSPlatform corresponding to the PL tag in the read group
     * @param rg
     * @return an NGSPlatform object matching the PL field of the header, of UNKNOWN if there was no match
     */
    public static final NGSPlatform fromReadGroup(SAMReadGroupRecord rg) {
        if ( rg == null ) return UNKNOWN;
        return fromReadGroupPL(rg.getPlatform());
    }

    /**
     * Returns the NGSPlatform corresponding to the PL tag in the read group
     */
    public static final NGSPlatform fromReadGroupPL(final String plFromRG) {
        if ( plFromRG == null ) return UNKNOWN;

        final String pl = plFromRG.toUpperCase();
        for ( final NGSPlatform ngsPlatform : NGSPlatform.values() ) {
            for ( final String bamPLName : ngsPlatform.BAM_PL_NAMES ) {
                if ( pl.contains(bamPLName) )
                    return ngsPlatform;
            }
        }

        return UNKNOWN;
    }

    /**
     * checks whether or not the requested platform is listed in the set (and is not unknown)
     */
    public static final boolean isKnown (final String platform) {
        return fromReadGroupPL(platform) != UNKNOWN;
    }
}
