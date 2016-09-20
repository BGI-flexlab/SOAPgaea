package org.bgi.flexlab.gaea.data.structure.pileup;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.pileup.filter.PileupElementFilter;
import org.bgi.flexlab.gaea.util.FragmentCollection;

public interface Pileup extends Iterable<PileupElement>{
    /**
     * Gets a pileup consisting of all those elements passed by a given filter.
     */
    public Pileup getFilteredPileup(PileupElementFilter filter);
    
    public Pileup getOverlappingFragmentFilteredPileup() ;

    /**
     * Gets a collection of all the read groups represented in this pileup.
     */
    public Collection<String> getReadGroups();

    /**
     * Gets a collection of *names* of all the samples stored in this pileup.
     */
    public Collection<String> getSamples();

    /**
     * Gets the particular subset of this pileup for all the given sample names.
     */
    public Pileup getPileupForSamples(Collection<String> sampleNames);

    /**
     * Gets the particular subset of this pileup for each given sample name.
     * Same as calling getPileupForSample for all samples, but in O(n) instead of O(n^2).
     */
    public Map<String, Pileup> getPileupsForSamples(Collection<String> sampleNames);


    /**
     * Gets the particular subset of this pileup with the given sample name.
     */
    public Pileup getPileupForSample(String sampleName);
    
    /**
     * Simple useful routine to count the number of deletion bases in this pileup
     */
    public int getNumberOfDeletions();

    /**
     * Simple useful routine to count the number of deletion bases in at the next position this pileup
     */
    public int getNumberOfDeletionsAfterThisElement();

    /**
     * Simple useful routine to count the number of insertions right after this pileup
     */
    public int getNumberOfInsertionsAfterThisElement();

    public int getNumberOfMappingQualityZeroReads();

    /**
     * @return the number of physical elements in this pileup (a reduced read is counted just once)
     */
    public int getNumberOfElements();

    /**
     * @return the number of abstract elements in this pileup (reduced reads are expanded to count all reads that they represent)
     */
    public int depthOfCoverage();

    /**
     * @return true if there are 0 elements in the pileup, false otherwise
     */
    public boolean isEmpty();

    /**
     * Get counts of A, C, G, T in order, which returns a int[4] vector with counts according
     * to BaseUtils.simpleBaseToBaseIndex for each base.
     */
    public int[] getBaseCounts();

    /**
     * return the string for the pileup 
     * */
    public String toString(Character ref);

    /**
     * Returns a list of the reads in this pileup.
     */
    public List<GaeaSamRecord> getReads();

    /**
     * Returns a list of the offsets in this pileup. 
     */
    public List<Integer> getOffsets();

    /**
     * Returns an array of the bases in this pileup. 
     * @return
     */
    public byte[] getBases();

    /**
    * Returns an array of the quals in this pileup. 
    */
    public byte[] getQualites();

    /**
     * Get an array of the mapping qualities
     */
    public byte[] getMappingQualites();

    /**
     * Converts this pileup into a FragmentCollection (see FragmentUtils for documentation)
     */
    public FragmentCollection<PileupElement> toFragments();
}
