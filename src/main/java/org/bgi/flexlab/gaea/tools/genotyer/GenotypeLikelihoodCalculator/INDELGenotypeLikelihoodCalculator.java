package org.bgi.flexlab.gaea.tools.genotyer.GenotypeLikelihoodCalculator;

import htsjdk.variant.variantcontext.*;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocationParser;
import org.bgi.flexlab.gaea.data.structure.pileup.Mpileup;
import org.bgi.flexlab.gaea.data.structure.pileup.Pileup;
import org.bgi.flexlab.gaea.data.structure.pileup.PileupReadInfo;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.tools.mapreduce.genotyper.GenotyperOptions;
import org.bgi.flexlab.gaea.util.BaseUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhangyong on 2016/12/29.
 *
 * mainly came from GATK 2.3.9-lite
 */
public class INDELGenotypeLikelihoodCalculator extends GenotypeLikelihoodCalculator {
    private static final int HAPLOTYPE_SIZE = 80;

    public static int REF_WIN_START = -200;

    public static int REF_WIN_STOP = 200;

    private boolean ignoreSNPAllelesWhenGenotypingIndels = false;

    private PairHMMIndelErrorModel pairModel;


    private LinkedHashMap<Allele, Haplotype> haplotypeMap;

    private List<Allele> alleleList = new ArrayList<>();

    /**
     * constrcutor
     * @param options
     */
    public INDELGenotypeLikelihoodCalculator(GenotyperOptions options) {
        super(options);
    }

    /**
     * calculate genotype likelihoods for SNP
     *
     * @param mpileup   multi-sample pileups
     * @param reference reference
     * @param options   options
     * @return variant context with likelihoods
     */
    public VariantContext genotypeLikelihoodCalculate(Mpileup mpileup, ChromosomeInformationShare reference, GenotyperOptions options, GenomeLocationParser locationParser, Map<String, PerReadAlleleLikelihoodMap> perReadAlleleLikelihoodMap) {
        Map<String, Pileup> pileups = mpileup.getNextPosPileup();
        int position = mpileup.getPosition();
        if (pileups == null || pileups.isEmpty())
            return null;

        haplotypeMap.clear();
        perReadAlleleLikelihoodMap.clear();

        int winStart = Math.max( position - REF_WIN_START, 1 );
        int winStop = Math.min( position + REF_WIN_STOP, reference.getLength() );
        GenomeLocation refWindows = locationParser.createGenomeLocation(reference.getChromosomeName(), winStart, winStop);

        //construct haplotypes
        ///get alleles from pileup
        alleleList = getConsensusAlleles(pileups, position, reference, options, locationParser);
        if (alleleList.isEmpty())
            return null;
        ///construct haplotypes
        getHaplotypeMapFromAlleles(alleleList, reference, position, refWindows,locationParser, haplotypeMap);
        if (haplotypeMap == null || haplotypeMap.isEmpty())
            return null;

        // start making the VariantContext
        // For all non-snp VC types, VC end location is just startLocation + length of ref allele including padding base.
        final int endLoc = position + alleleList.get(0).length() - 1;
        final int eventLength = getEventLength(alleleList);

        final VariantContextBuilder builder = new VariantContextBuilder("UG_call", reference.getChromosomeName(), position, endLoc, alleleList);

        // create the genotypes; no-call everyone for now
        GenotypesContext genotypes = GenotypesContext.create();
        final List<Allele> noCall = new ArrayList<Allele>();
        noCall.add(Allele.NO_CALL);

        // For each sample, get genotype likelihoods based on pileup
        // compute prior likelihoods on haplotypes, and initialize haplotype likelihood matrix with them.

        for (String sample : pileups.keySet()) {
            Pileup pileup = pileups.get(sample);

            if (!perReadAlleleLikelihoodMap.containsKey(sample)){
                // no likelihoods have been computed for this sample at this site
                PerReadAlleleLikelihoodMap pr = PerReadAlleleLikelihoodMap.getBestAvailablePerReadAlleleLikelihoodMap();
                pr.setRefAllele(alleleList.get(0));
                perReadAlleleLikelihoodMap.put(sample, pr);
            }
            if (pileup != null) {
                final GenotypeBuilder b = new GenotypeBuilder(sample);
                final double[] genotypeLikelihoods = pairModel.computeDiploidReadHaplotypeLikelihoods(pileup, haplotypeMap,
                        reference, eventLength, perReadAlleleLikelihoodMap.get(sample), refWindows, position, options.getContaminationFraction());
                b.PL(genotypeLikelihoods);
                b.DP(getFilteredDepth(pileup));
                genotypes.add(b.make());

            }
        }
        return builder.genotypes(genotypes).make();
    }

    public static List<Allele> getConsensusAlleles(Map<String, Pileup> pileups, int positon, ChromosomeInformationShare reference, GenotyperOptions options, GenomeLocationParser locationParser) {
        ConsensusAlleleCounter counter = new ConsensusAlleleCounter(false, options.getMinIndelCountForGenotyping(), options.getMinIndelFractionPerSample());
        return counter.computeConsensusAlleles(reference, pileups, positon, locationParser);
    }

    public static void getHaplotypeMapFromAlleles(final List<Allele> alleleList,
                                                  final ChromosomeInformationShare ref,
                                                  final int position,
                                                  final GenomeLocation refWindows,
                                                  final GenomeLocationParser locationParser,
                                                  final LinkedHashMap<Allele, Haplotype> haplotypeMap) {
        // protect against having an indel too close to the edge of a contig
        if (position <= HAPLOTYPE_SIZE) {
            haplotypeMap.clear();
        }
        // check if there is enough reference window to create haplotypes (can be an issue at end of contigs)
        else if (ref.getLength() - 1 < position + HAPLOTYPE_SIZE) {
            haplotypeMap.clear();
        }
        else if (alleleList.isEmpty())
            haplotypeMap.clear();
        else {
            final int eventLength = getEventLength(alleleList);
            final int hsize = refWindows.size() - Math.abs(eventLength) - 1;
            final int numPrefBases = position - refWindows.getStart() + 1;

            if (hsize <= 0) { // protect against event lengths larger than ref window sizes
                haplotypeMap.clear();
            }
            else {
                haplotypeMap.putAll(Haplotype.makeHaplotypeListFromAlleles(alleleList, position, ref, refWindows, locationParser, hsize, numPrefBases));
            }
        }
    }

    public static int getEventLength(List<Allele> alleleList) {
        Allele refAllele = alleleList.get(0);
        Allele altAllele = alleleList.get(1);
        // look for alt allele that has biggest length distance to ref allele
        int maxLenDiff = 0;
        for (Allele a : alleleList) {
            if (a.isNonReference()) {
                int lenDiff = Math.abs(a.getBaseString().length() - refAllele.getBaseString().length());
                if (lenDiff > maxLenDiff) {
                    maxLenDiff = lenDiff;
                    altAllele = a;
                }
            }
        }

        return altAllele.getBaseString().length() - refAllele.getBaseString().length();
    }

    // Overload function in GenotypeLikelihoodsCalculationModel so that, for an indel case, we consider a deletion as part of the pileup,
    // so that per-sample DP will include deletions covering the event.
    protected int getFilteredDepth(Pileup pileup) {
        int count = 0;
        for (PileupReadInfo p : pileup.getPlp()) {
            if (p.isDeletionBase() || p.isInsertionAtBeginningOfRead() || BaseUtils.isRegularBase(p.getByteBase()))
                count += 1; //p.getRepresentativeCount();
        }

        return count;
    }
}
