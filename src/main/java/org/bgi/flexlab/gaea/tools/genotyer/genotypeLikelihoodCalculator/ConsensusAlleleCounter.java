package org.bgi.flexlab.gaea.tools.genotyer.genotypeLikelihoodCalculator;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import org.bgi.flexlab.gaea.data.structure.alignment.AlignmentsBasic;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocationParser;
import org.bgi.flexlab.gaea.data.structure.pileup.Pileup;
import org.bgi.flexlab.gaea.data.structure.pileup.PileupReadInfo;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.util.GaeaVariantContextUtils;
import org.bgi.flexlab.gaea.util.Pair;

import java.util.*;

/**
 * Created by zhangyong on 2017/2/5.
 * mainly came from GATK 2.3.9-lite
 */
public class ConsensusAlleleCounter {
    private final int minIndelCountForGenotyping;
    private final boolean doMultiAllelicCalls;
    private final double minFractionInOneSample;

    public ConsensusAlleleCounter( final boolean doMultiAllelicCalls, final int minIndelCountForGenotyping, final double minFractionInOneSample) {
        this.minIndelCountForGenotyping = minIndelCountForGenotyping;
        this.doMultiAllelicCalls = doMultiAllelicCalls;
        this.minFractionInOneSample = minFractionInOneSample;
    }

    /**
     * Returns a list of Alleles at this locus that may be segregating
     * @param reference
     * @param pileups
     * @return
     */
    public List<Allele> computeConsensusAlleles(ChromosomeInformationShare reference, Map<String, Pileup> pileups, int position, GenomeLocationParser locationParser) {
        final Map<String, Integer> consensusIndelStrings = countConsensusAlleles(reference, pileups, position);
        return consensusCountsToAlleles(position, reference, consensusIndelStrings, locationParser);
    }

    /**
     * count indels string
     * @param reference
     * @param pileups
     * @return
     */
    private Map<String, Integer> countConsensusAlleles(ChromosomeInformationShare reference, Map<String, Pileup> pileups, int position) {
        HashMap<String, Integer> consensusIndelStrings = new HashMap<String, Integer>();

        int insCount = 0, delCount = 0;
        for(String sample : pileups.keySet()) {
            Pileup pileup = pileups.get(sample);
            insCount += pileup.getNextInsertionCount();
            delCount += pileup.getNextDeletionCount();
        }
        if(insCount < minIndelCountForGenotyping && delCount < minIndelCountForGenotyping) {
            return Collections.emptyMap();
        }

        for(String sample : pileups.keySet()) {
            Pileup pileup = pileups.get(sample);
            final int nIndelReads = pileup.getNextDeletionCount() + pileup.getNextInsertionCount();
            final int nReadsOverall = pileup.getNumberOfElements();

            System.err.println("indel reads number:" + nIndelReads + "\ntotal reads number:" + nReadsOverall);

            if (nIndelReads == 0 || (nIndelReads / (1.0 * nReadsOverall)) < minFractionInOneSample) {
                continue;
            }

            for (PileupReadInfo p : pileup.getFilteredPileup()) {
                AlignmentsBasic read = p.getReadInfo();
                if (read == null)
                    continue;
                //if (ReadUtils.is454Read(read)) {
                //    continue;
                //}

                String indelString = p.getEventBases();

                if (p.isNextInsertBase()) {
                    // edge case: ignore a deletion immediately preceding an insertion as p.getEventBases() returns null [EB]
                    if (indelString == null)
                        continue;

                    boolean foundKey = false;
                    // copy of hashmap into temp arrayList
                    ArrayList<Pair<String, Integer>> cList = new ArrayList<Pair<String, Integer>>();
                    for (Map.Entry<String, Integer> s : consensusIndelStrings.entrySet()) {
                        cList.add(new Pair<String, Integer>(s.getKey(), s.getValue()));
                    }

                    //FIXME::wierd for NGS data and alinger like BWA
                    if (p.getEnd() == position) {
                        // first corner condition: a read has an insertion at the end, and we're right at the insertion.
                        // In this case, the read could have any of the inserted bases and we need to build a consensus

                        for (int k = 0; k < cList.size(); k++) {
                            String s = cList.get(k).getFirst();
                            int cnt = cList.get(k).getSecond();
                            // case 1: current insertion is prefix of indel in hash map
                            if (s.startsWith(indelString)) {
                                cList.set(k, new Pair<String, Integer>(s, cnt + 1));
                                foundKey = true;
                            } else if (indelString.startsWith(s)) {
                                // case 2: indel stored in hash table is prefix of current insertion
                                // In this case, new bases are new key.
                                foundKey = true;
                                cList.set(k, new Pair<String, Integer>(indelString, cnt + 1));
                            }
                        }
                        if (!foundKey)
                            // none of the above: event bases not supported by previous table, so add new key
                            cList.add(new Pair<String, Integer>(indelString, 1));

                    } else if (read.getPosition() == position + 1) {
                        // opposite corner condition: read will start at current locus with an insertion
                        for (int k = 0; k < cList.size(); k++) {
                            String s = cList.get(k).getFirst();
                            int cnt = cList.get(k).getSecond();
                            if (s.endsWith(indelString)) {
                                // case 1: current insertion (indelString) is suffix of indel in hash map (s)
                                cList.set(k, new Pair<String, Integer>(s, cnt + 1));
                                foundKey = true;
                            } else if (indelString.endsWith(s)) {
                                // case 2: indel stored in hash table is prefix of current insertion
                                // In this case, new bases are new key.
                                foundKey = true;
                                cList.set(k, new Pair<String, Integer>(indelString, cnt + 1));
                            }
                        }
                        if (!foundKey)
                            // none of the above: event bases not supported by previous table, so add new key
                            cList.add(new Pair<String, Integer>(indelString, 1));


                    } else {
                        // normal case: insertion somewhere in the middle of a read: add count to arrayList
                        int cnt = consensusIndelStrings.containsKey(indelString) ? consensusIndelStrings.get(indelString) : 0;
                        cList.add(new Pair<String, Integer>(indelString, cnt + 1));
                    }

                    // copy back arrayList into hashMap
                    consensusIndelStrings.clear();
                    for (Pair<String, Integer> pair : cList) {
                        consensusIndelStrings.put(pair.getFirst(), pair.getSecond());
                    }

                } else if (p.isNextDeletionBase()) {
                    indelString = String.format("D%d", p.getEventLength());
                    int cnt = consensusIndelStrings.containsKey(indelString) ? consensusIndelStrings.get(indelString) : 0;
                    consensusIndelStrings.put(indelString, cnt + 1);

                }
                System.out.println("indel:" + indelString);
            }
        }
        return consensusIndelStrings;
    }

    private List<Allele> consensusCountsToAlleles(int position, ChromosomeInformationShare reference, Map<String, Integer> consensusIndelStrings, GenomeLocationParser locationParser) {
        final Collection<VariantContext> vcs = new ArrayList<VariantContext>();
        int maxAlleleCnt = 0;
        Allele refAllele, altAllele;

        for (final Map.Entry<String, Integer> elt : consensusIndelStrings.entrySet()) {
            final String s = elt.getKey();
            final int curCnt = elt.getValue();
            int stop = 0;

            // if observed count if above minimum threshold, we will genotype this allele
            if (curCnt < minIndelCountForGenotyping)
                continue;

            if (s.startsWith("D")) {
                // get deletion length
                final int dLen = Integer.valueOf(s.substring(1));
                // get ref bases of accurate deletion
                final byte[] refBases = reference.getBaseBytes(position, position + dLen);
                stop = position + dLen + 1;

                if (Allele.acceptableAlleleBases(refBases, false)) {
                    refAllele = Allele.create(refBases, true);
                    altAllele = Allele.create((byte) reference.getBase(position), false);
                    System.out.println("delete ref allele:" + refAllele + "\talt allele:" + altAllele);
                }
                else continue; // don't go on with this allele if refBases are non-standard
            } else {
                // insertion case
                final String insertionBases = reference.getBase(position) + s;  // add reference padding
                if (Allele.acceptableAlleleBases(insertionBases, false)) { // don't allow N's in insertions
                    refAllele = Allele.create((byte) reference.getBase(position), true);
                    altAllele = Allele.create(insertionBases, false);
                    System.out.println("insert ref allele:" + refAllele + "\talt allele:" + altAllele);
                    stop = position + 1;
                }
                else continue; // go on to next allele if consensus insertion has any non-standard base.
            }


            final VariantContextBuilder builder = new VariantContextBuilder().source("");
            builder.loc(reference.getChromosomeName(), position + 1, stop);
            builder.alleles(Arrays.asList(refAllele, altAllele));
            builder.noGenotypes();
            if (doMultiAllelicCalls) {
                vcs.add(builder.make());
                if (vcs.size() >= GenotypeLikelihoodCalculator.MAX_ALT_ALLELES_THAT_CAN_BE_GENOTYPED)
                    break;
            } else if (curCnt > maxAlleleCnt) {
                maxAlleleCnt = curCnt;
                vcs.clear();
                vcs.add(builder.make());
            }
        }

        if (vcs.isEmpty())
            return Collections.emptyList(); // nothing else to do, no alleles passed minimum count criterion

        final VariantContext mergedVC = GaeaVariantContextUtils.simpleMerge(locationParser, vcs, null, GaeaVariantContextUtils.FilteredRecordMergeType.KEEP_IF_ANY_UNFILTERED, GaeaVariantContextUtils.GenotypeMergeType.UNSORTED, false, false, null, false, false);
        //System.out.println("mergedVC:");
        //for(Allele a : mergedVC.getAlleles()) {
        //	System.out.println(a.getBaseString());
        //}
        return mergedVC.getAlleles();
    }
}
