
package org.bgi.flexlab.gaea.util;

import htsjdk.variant.variantcontext.*;
import org.bgi.flexlab.gaea.data.exception.UserException;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocationParser;

import java.io.Serializable;
import java.util.*;

public class GaeaVariantContextUtils {
    public final static String MERGE_INTERSECTION = "Intersection";
    public final static String MERGE_FILTER_IN_ALL = "FilteredInAll";
    public final static String MERGE_REF_IN_ALL = "ReferenceInAll";
    public final static String MERGE_FILTER_PREFIX = "filterIn";

    public enum GenotypeMergeType {
        /**
         * Make all sample genotypes unique by file. Each sample shared across RODs gets named sample.ROD.
         */
        UNIQUIFY,
        /**
         * Take genotypes in priority order (see the priority argument).
         */
        PRIORITIZE,
        /**
         * Take the genotypes in any order.
         */
        UNSORTED,
        /**
         * Require that all samples/genotypes be unique between all inputs.
         */
        REQUIRE_UNIQUE
    }

    public enum FilteredRecordMergeType {
        /**
         * Union - leaves the record if any record is unfiltered.
         */
        KEEP_IF_ANY_UNFILTERED,
        /**
         * Requires all records present at site to be unfiltered. VCF files that don't contain the record don't influence this.
         */
        KEEP_IF_ALL_UNFILTERED,
        /**
         * If any record is present at this site (regardless of possibly being filtered), then all such records are kept and the filters are reset.
         */
        KEEP_UNCONDITIONAL
    }

    static private void verifyUniqueSampleNames(Collection<VariantContext> unsortedVCs) {
        Set<String> names = new HashSet<String>();
        for ( VariantContext vc : unsortedVCs ) {
            for ( String name : vc.getSampleNames() ) {
                //System.out.printf("Checking %s %b%n", name, names.contains(name));
                if ( names.contains(name) )
                    throw new UserException("REQUIRE_UNIQUE sample names is true but duplicate names were discovered " + name);
            }

            names.addAll(vc.getSampleNames());
        }
    }

    public static List<VariantContext> sortVariantContextsByPriority(Collection<VariantContext> unsortedVCs, List<String> priorityListOfVCs, GenotypeMergeType mergeOption ) {
        if ( mergeOption == GenotypeMergeType.PRIORITIZE && priorityListOfVCs == null )
            throw new IllegalArgumentException("Cannot merge calls by priority with a null priority list");

        if ( priorityListOfVCs == null || mergeOption == GenotypeMergeType.UNSORTED )
            return new ArrayList<VariantContext>(unsortedVCs);
        else {
            ArrayList<VariantContext> sorted = new ArrayList<VariantContext>(unsortedVCs);
            Collections.sort(sorted, new CompareByPriority(priorityListOfVCs));
            return sorted;
        }
    }

    static class CompareByPriority implements Comparator<VariantContext>, Serializable {
        /**
         *
         */
        private static final long serialVersionUID = 6195505964868678001L;
        List<String> priorityListOfVCs;
        public CompareByPriority(List<String> priorityListOfVCs) {
            this.priorityListOfVCs = priorityListOfVCs;
        }

        private int getIndex(VariantContext vc) {
            int i = priorityListOfVCs.indexOf(vc.getSource());
            if ( i == -1 ) throw new UserException.BadArgumentValueException(Utils.join(",", priorityListOfVCs), "Priority list " + priorityListOfVCs + " doesn't contain variant context " + vc.getSource());
            return i;
        }

        public int compare(VariantContext vc1, VariantContext vc2) {
            return Integer.valueOf(getIndex(vc1)).compareTo(getIndex(vc2));
        }
    }

    static private Allele determineReferenceAllele(List<VariantContext> VCs) {
        Allele ref = null;

        for ( VariantContext vc : VCs ) {
            Allele myRef = vc.getReference();
            if ( ref == null || ref.length() < myRef.length() )
                ref = myRef;
            else if ( ref.length() == myRef.length() && ! ref.equals(myRef) )
                throw new UserException.BadInput(String.format("The provided variant file(s) have inconsistent references for the same position(s) at %s:%d, %s vs. %s", vc.getChr(), vc.getStart(), ref, myRef));
        }

        return ref;
    }

    private static class AlleleMapper {
        private VariantContext vc = null;
        private Map<Allele, Allele> map = null;
        public AlleleMapper(VariantContext vc)          { this.vc = vc; }
        public AlleleMapper(Map<Allele, Allele> map)    { this.map = map; }
        public boolean needsRemapping()                 { return this.map != null; }
        public Collection<Allele> values()              { return map != null ? map.values() : vc.getAlleles(); }
        public Allele remap(Allele a)                   { return map != null && map.containsKey(a) ? map.get(a) : a; }

        public List<Allele> remap(List<Allele> as) {
            List<Allele> newAs = new ArrayList<Allele>();
            for ( Allele a : as ) {
                //System.out.printf("  Remapping %s => %s%n", a, remap(a));
                newAs.add(remap(a));
            }
            return newAs;
        }
    }

    /**
     * create a genome location, given a variant context
     * @param genomeLocParser parser
     * @param vc the variant context
     * @return the genomeLoc
     */
    public static final GenomeLocation getLocation(GenomeLocationParser genomeLocParser,VariantContext vc) {
        return genomeLocParser.createGenomeLocation(vc.getChr(), vc.getStart(), vc.getEnd(), true);
    }


    static private AlleleMapper resolveIncompatibleAlleles(Allele refAllele, VariantContext vc, Set<Allele> allAlleles) {
        if ( refAllele.equals(vc.getReference()) )
            return new AlleleMapper(vc);
        else {
            // we really need to do some work.  The refAllele is the longest reference allele seen at this
            // start site.  So imagine it is:
            //
            // refAllele: ACGTGA
            // myRef:     ACGT
            // myAlt:     A
            //
            // We need to remap all of the alleles in vc to include the extra GA so that
            // myRef => refAllele and myAlt => AGA
            //

            Allele myRef = vc.getReference();
            if ( refAllele.length() <= myRef.length() ) throw new UserException("BUG: myRef="+myRef+" is longer than refAllele="+refAllele);
            byte[] extraBases = Arrays.copyOfRange(refAllele.getBases(), myRef.length(), refAllele.length());

            // System.out.printf("Remapping allele at %s%n", vc);
            /// System.out.printf("ref   %s%n", refAllele);
            // System.out.printf("myref %s%n", myRef );
            // System.out.printf("extrabases %s%n", new String(extraBases));

            Map<Allele, Allele> map = new HashMap<Allele, Allele>();
            for ( Allele a : vc.getAlleles() ) {
                if ( a.isReference() )
                    map.put(a, refAllele);
                else {
                    Allele extended = Allele.extend(a, extraBases);
                    for ( Allele b : allAlleles )
                        if ( extended.equals(b) )
                            extended = b;
                    //  System.out.printf("  Extending %s => %s%n", a, extended);
                    map.put(a, extended);
                }
            }

            // debugging
//            System.out.printf("mapping %s%n", map);

            return new AlleleMapper(map);
        }
    }

    private static final boolean hasPLIncompatibleAlleles(final Collection<Allele> alleleSet1, final Collection<Allele> alleleSet2) {
        final Iterator<Allele> it1 = alleleSet1.iterator();
        final Iterator<Allele> it2 = alleleSet2.iterator();

        while ( it1.hasNext() && it2.hasNext() ) {
            final Allele a1 = it1.next();
            final Allele a2 = it2.next();
            if ( ! a1.equals(a2) )
                return true;
        }

        // by this point, at least one of the iterators is empty.  All of the elements
        // we've compared are equal up until this point.  But it's possible that the
        // sets aren't the same size, which is indicated by the test below.  If they
        // are of the same size, though, the sets are compatible
        return it1.hasNext() || it2.hasNext();
    }

    public static GenotypesContext stripPLs(GenotypesContext genotypes) {
        GenotypesContext newGs = GenotypesContext.create(genotypes.size());

        for ( final Genotype g : genotypes ) {
            newGs.add(g.hasLikelihoods() ? removePLs(g) : g);
        }

        return newGs;
    }


    public static Genotype removePLs(Genotype g) {
        if ( g.hasLikelihoods() )
            return new GenotypeBuilder(g).noPL().make();
        else
            return g;
    }

    /**
     * Update the attributes of the attributes map given the VariantContext to reflect the
     * proper chromosome-based VCF tags
     *
     * @param vc          the VariantContext
     * @param attributes  the attributes map to populate; must not be null; may contain old values
     * @param removeStaleValues should we remove stale values from the mapping?
     * @return the attributes map provided as input, returned for programming convenience
     */
    public static Map<String, Object> calculateChromosomeCounts(VariantContext vc, Map<String, Object> attributes, boolean removeStaleValues) {
        return calculateChromosomeCounts(vc, attributes,  removeStaleValues, new HashSet<String>(0));
    }

    /**
     * Update the attributes of the attributes map given the VariantContext to reflect the
     * proper chromosome-based VCF tags
     *
     * @param vc          the VariantContext
     * @param attributes  the attributes map to populate; must not be null; may contain old values
     * @param removeStaleValues should we remove stale values from the mapping?
     * @param founderIds - Set of founders Ids to take into account. AF and FC will be calculated over the founders.
     *                  If empty or null, counts are generated for all samples as unrelated individuals
     * @return the attributes map provided as input, returned for programming convenience
     */
    public static Map<String, Object> calculateChromosomeCounts(VariantContext vc, Map<String, Object> attributes, boolean removeStaleValues, final Set<String> founderIds) {
        final int AN = vc.getCalledChrCount();

        // if everyone is a no-call, remove the old attributes if requested
        if ( AN == 0 && removeStaleValues ) {
            if ( attributes.containsKey(VCFConstants.ALLELE_COUNT_KEY) )
                attributes.remove(VCFConstants.ALLELE_COUNT_KEY);
            if ( attributes.containsKey(VCFConstants.ALLELE_FREQUENCY_KEY) )
                attributes.remove(VCFConstants.ALLELE_FREQUENCY_KEY);
            if ( attributes.containsKey(VCFConstants.ALLELE_NUMBER_KEY) )
                attributes.remove(VCFConstants.ALLELE_NUMBER_KEY);
            return attributes;
        }

        if ( vc.hasGenotypes() ) {
            attributes.put(VCFConstants.ALLELE_NUMBER_KEY, AN);

            // if there are alternate alleles, record the relevant tags
            if ( vc.getAlternateAlleles().size() > 0 ) {
                ArrayList<Double> alleleFreqs = new ArrayList<Double>();
                ArrayList<Integer> alleleCounts = new ArrayList<Integer>();
                ArrayList<Integer> foundersAlleleCounts = new ArrayList<Integer>();
                double totalFoundersChromosomes = (double)vc.getCalledChrCount(founderIds);
                int foundersAltChromosomes;
                for ( Allele allele : vc.getAlternateAlleles() ) {
                    foundersAltChromosomes = vc.getCalledChrCount(allele,founderIds);
                    alleleCounts.add(vc.getCalledChrCount(allele));
                    foundersAlleleCounts.add(foundersAltChromosomes);
                    if ( AN == 0 ) {
                        alleleFreqs.add(0.0);
                    } else {
                        final Double freq = (double)foundersAltChromosomes / totalFoundersChromosomes;
                        alleleFreqs.add(freq);
                    }
                }

                attributes.put(VCFConstants.ALLELE_COUNT_KEY, alleleCounts.size() == 1 ? alleleCounts.get(0) : alleleCounts);
                attributes.put(VCFConstants.ALLELE_FREQUENCY_KEY, alleleFreqs.size() == 1 ? alleleFreqs.get(0) : alleleFreqs);
            } else {
                // if there's no alt AC and AF shouldn't be present
                attributes.remove(VCFConstants.ALLELE_COUNT_KEY);
                attributes.remove(VCFConstants.ALLELE_FREQUENCY_KEY);
            }
        }

        return attributes;
    }

    private static void mergeGenotypes(GenotypesContext mergedGenotypes, VariantContext oneVC, AlleleMapper alleleMapping, boolean uniqifySamples) {
        for ( Genotype g : oneVC.getGenotypes() ) {
            String name = mergedSampleName(oneVC.getSource(), g.getSampleName(), uniqifySamples);
            if ( ! mergedGenotypes.containsSample(name) ) {
                // only add if the name is new
                Genotype newG = g;

                if ( uniqifySamples || alleleMapping.needsRemapping() ) {
                    final List<Allele> alleles = alleleMapping.needsRemapping() ? alleleMapping.remap(g.getAlleles()) : g.getAlleles();
                    newG = new GenotypeBuilder(g).name(name).alleles(alleles).make();
                }

                mergedGenotypes.add(newG);
            }
        }
    }

    public static String mergedSampleName(String trackName, String sampleName, boolean uniqify ) {
        return uniqify ? sampleName + "." + trackName : sampleName;
    }

    /**
     * Merges VariantContexts into a single hybrid.  Takes genotypes for common samples in priority order, if provided.
     * If uniquifySamples is true, the priority order is ignored and names are created by concatenating the VC name with
     * the sample name
     *
     * @param genomeLocParser         loc parser
     * @param unsortedVCs             collection of unsorted VCs
     * @param priorityListOfVCs       priority list detailing the order in which we should grab the VCs
     * @param filteredRecordMergeType merge type for filtered records
     * @param genotypeMergeOptions    merge option for genotypes
     * @param annotateOrigin          should we annotate the set it came from?
     * @param printMessages           should we print messages?
     * @param setKey                  the key name of the set
     * @param filteredAreUncalled     are filtered records uncalled?
     * @param mergeInfoWithMaxAC      should we merge in info from the VC with maximum allele count?
     * @return new VariantContext       representing the merge of unsortedVCs
     */
    public static VariantContext simpleMerge(final GenomeLocationParser genomeLocParser,
                                             final Collection<VariantContext> unsortedVCs,
                                             final List<String> priorityListOfVCs,
                                             final FilteredRecordMergeType filteredRecordMergeType,
                                             final GenotypeMergeType genotypeMergeOptions,
                                             final boolean annotateOrigin,
                                             final boolean printMessages,
                                             final String setKey,
                                             final boolean filteredAreUncalled,
                                             final boolean mergeInfoWithMaxAC) {
        if (unsortedVCs == null || unsortedVCs.size() == 0)
            return null;

        if (annotateOrigin && priorityListOfVCs == null)
            throw new IllegalArgumentException("Cannot merge calls and annotate their origins without a complete priority list of VariantContexts");

        if (genotypeMergeOptions == GenotypeMergeType.REQUIRE_UNIQUE)
            verifyUniqueSampleNames(unsortedVCs);

        final List<VariantContext> preFilteredVCs = sortVariantContextsByPriority(unsortedVCs, priorityListOfVCs, genotypeMergeOptions);
        // Make sure all variant contexts are padded with reference base in case of indels if necessary
        final List<VariantContext> VCs = new ArrayList<VariantContext>();

        for (final VariantContext vc : preFilteredVCs) {
            if (!filteredAreUncalled || vc.isNotFiltered())
                VCs.add(vc);
        }
        if (VCs.size() == 0) // everything is filtered out and we're filteredAreUncalled
            return null;

        // establish the baseline info from the first VC
        final VariantContext first = VCs.get(0);
        final String name = first.getSource();
        final Allele refAllele = determineReferenceAllele(VCs);
        //Byte referenceBaseForIndel = null;

        final Set<Allele> alleles = new LinkedHashSet<Allele>();
        final Set<String> filters = new HashSet<String>();
        final Map<String, Object> attributes = new LinkedHashMap<String, Object>();
        final Set<String> inconsistentAttributes = new HashSet<String>();
        final Set<String> variantSources = new HashSet<String>(); // contains the set of sources we found in our set of VCs that are variant
        final Set<String> rsIDs = new LinkedHashSet<String>(1); // most of the time there's one id

        GenomeLocation loc = getLocation(genomeLocParser, first);
        int depth = 0;
        int maxAC = -1;
        final Map<String, Object> attributesWithMaxAC = new LinkedHashMap<String, Object>();
        double log10PError = CommonInfo.NO_LOG10_PERROR;
        VariantContext vcWithMaxAC = null;
        GenotypesContext genotypes = GenotypesContext.create();

        // counting the number of filtered and variant VCs
        int nFiltered = 0;

        boolean remapped = false;

        // cycle through and add info from the other VCs, making sure the loc/reference matches

        for (final VariantContext vc : VCs) {
            if (loc.getStart() != vc.getStart())
                throw new UserException("BUG: attempting to merge VariantContexts with different start sites: first=" + first.toString() + " second=" + vc.toString());

            if (getLocation(genomeLocParser, vc).size() > loc.size())
                loc = getLocation(genomeLocParser, vc); // get the longest location

            nFiltered += vc.isFiltered() ? 1 : 0;
            if (vc.isVariant()) variantSources.add(vc.getSource());

            AlleleMapper alleleMapping = resolveIncompatibleAlleles(refAllele, vc, alleles);
            remapped = remapped || alleleMapping.needsRemapping();

            alleles.addAll(alleleMapping.values());

            mergeGenotypes(genotypes, vc, alleleMapping, genotypeMergeOptions == GenotypeMergeType.UNIQUIFY);

            // We always take the QUAL of the first VC with a non-MISSING qual for the combined value
            if (log10PError == CommonInfo.NO_LOG10_PERROR)
                log10PError = vc.getLog10PError();

            filters.addAll(vc.getFilters());

            //
            // add attributes
            //
            // special case DP (add it up) and ID (just preserve it)
            //
            if (vc.hasAttribute(VCFConstants.DEPTH_KEY))
                depth += vc.getAttributeAsInt(VCFConstants.DEPTH_KEY, 0);
            if (vc.hasID()) rsIDs.add(vc.getID());
            if (mergeInfoWithMaxAC && vc.hasAttribute(VCFConstants.ALLELE_COUNT_KEY)) {
                String rawAlleleCounts = vc.getAttributeAsString(VCFConstants.ALLELE_COUNT_KEY, null);
                // lets see if the string contains a , separator
                if (rawAlleleCounts.contains(VCFConstants.INFO_FIELD_ARRAY_SEPARATOR)) {
                    List<String> alleleCountArray = Arrays.asList(rawAlleleCounts.substring(1, rawAlleleCounts.length() - 1).split(VCFConstants.INFO_FIELD_ARRAY_SEPARATOR));
                    for (String alleleCount : alleleCountArray) {
                        final int ac = Integer.valueOf(alleleCount.trim());
                        if (ac > maxAC) {
                            maxAC = ac;
                            vcWithMaxAC = vc;
                        }
                    }
                } else {
                    final int ac = Integer.valueOf(rawAlleleCounts);
                    if (ac > maxAC) {
                        maxAC = ac;
                        vcWithMaxAC = vc;
                    }
                }
            }

            for (final Map.Entry<String, Object> p : vc.getAttributes().entrySet()) {
                String key = p.getKey();
                // if we don't like the key already, don't go anywhere
                if (!inconsistentAttributes.contains(key)) {
                    final boolean alreadyFound = attributes.containsKey(key);
                    final Object boundValue = attributes.get(key);
                    final boolean boundIsMissingValue = alreadyFound && boundValue.equals(VCFConstants.MISSING_VALUE_v4);

                    if (alreadyFound && !boundValue.equals(p.getValue()) && !boundIsMissingValue) {
                        // we found the value but we're inconsistent, put it in the exclude list
                        //System.out.printf("Inconsistent INFO values: %s => %s and %s%n", key, boundValue, p.getValue());
                        inconsistentAttributes.add(key);
                        attributes.remove(key);
                    } else if (!alreadyFound || boundIsMissingValue) { // no value
                        //if ( vc != first ) System.out.printf("Adding key %s => %s%n", p.getKey(), p.getValue());
                        attributes.put(key, p.getValue());
                    }
                }
            }
        }

        // if we have more alternate alleles in the merged VC than in one or more of the
        // original VCs, we need to strip out the GL/PLs (because they are no longer accurate), as well as allele-dependent attributes like AC,AF
        for (final VariantContext vc : VCs) {
            if (vc.getAlleles().size() == 1)
                continue;
            if (hasPLIncompatibleAlleles(alleles, vc.getAlleles())) {
                if (!genotypes.isEmpty())
                    System.out.println(String.format("Stripping PLs at %s due incompatible alleles merged=%s vs. single=%s",
                            genomeLocParser.createGenomeLocation(vc), alleles, vc.getAlleles()));
                genotypes = stripPLs(genotypes);
                // this will remove stale AC,AF attributed from vc
                calculateChromosomeCounts(vc, attributes, true);
                break;
            }
        }

        // take the VC with the maxAC and pull the attributes into a modifiable map
        if (mergeInfoWithMaxAC && vcWithMaxAC != null) {
            attributesWithMaxAC.putAll(vcWithMaxAC.getAttributes());
        }

        // if at least one record was unfiltered and we want a union, clear all of the filters
        if ((filteredRecordMergeType == FilteredRecordMergeType.KEEP_IF_ANY_UNFILTERED && nFiltered != VCs.size()) || filteredRecordMergeType == FilteredRecordMergeType.KEEP_UNCONDITIONAL)
            filters.clear();


        if (annotateOrigin) { // we care about where the call came from
            String setValue;
            if (nFiltered == 0 && variantSources.size() == priorityListOfVCs.size()) // nothing was unfiltered
                setValue = MERGE_INTERSECTION;
            else if (nFiltered == VCs.size())     // everything was filtered out
                setValue = MERGE_FILTER_IN_ALL;
            else if (variantSources.isEmpty())    // everyone was reference
                setValue = MERGE_REF_IN_ALL;
            else {
                final LinkedHashSet<String> s = new LinkedHashSet<String>();
                for (final VariantContext vc : VCs)
                    if (vc.isVariant())
                        s.add(vc.isFiltered() ? MERGE_FILTER_PREFIX + vc.getSource() : vc.getSource());
                setValue = Utils.join("-", s);
            }

            if (setKey != null) {
                attributes.put(setKey, setValue);
                if (mergeInfoWithMaxAC && vcWithMaxAC != null) {
                    attributesWithMaxAC.put(setKey, setValue);
                }
            }
        }

        if (depth > 0)
            attributes.put(VCFConstants.DEPTH_KEY, String.valueOf(depth));

        final String ID = rsIDs.isEmpty() ? VCFConstants.EMPTY_ID_FIELD : Utils.join(",", rsIDs);

        final VariantContextBuilder builder = new VariantContextBuilder().source(name).id(ID);
        builder.loc(loc.getContig(), loc.getStart(), loc.getStop());
        builder.alleles(alleles);
        builder.genotypes(genotypes);
        builder.log10PError(log10PError);
        builder.filters(filters.isEmpty() ? filters : new TreeSet<String>(filters));
        builder.attributes(new TreeMap<String, Object>(mergeInfoWithMaxAC ? attributesWithMaxAC : attributes));

        // Trim the padded bases of all alleles if necessary
        final VariantContext merged = builder.make();
        if (printMessages && remapped) System.out.printf("Remapped => %s%n", merged);
        return merged;
    }
}