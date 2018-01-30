package org.bgi.flexlab.gaea.tools.haplotypecaller.argumentcollection;

public class AssemblyRegionTrimmerArgumentCollection {
    private static final long serialVersionUID = 1L;

    public boolean dontTrimActiveRegions = false;

    /**
     * the maximum extent into the full active region extension that we're willing to go in genotyping our events
     */
    public int discoverExtension = 25;

    public int ggaExtension = 300;

    /**
     * Include at least this many bases around an event for calling it
     */
    public int indelPadding = 150;

    public int snpPadding = 20;
}
