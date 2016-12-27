package org.bgi.flexlab.gaea.tools.genotyer;

import htsjdk.variant.variantcontext.GenotypesContext;
import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocationParser;
import org.bgi.flexlab.gaea.data.structure.pileup.Pileup;
import org.bgi.flexlab.gaea.data.structure.pileup.manager.PileupState;
import org.bgi.flexlab.gaea.tools.genotyer.genotypelikelihoodcalculator.GenotypeLikelihoodCalculator;
import org.bgi.flexlab.gaea.tools.genotyer.genotypecaller.GenotyperCaller;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by zhangyong on 2016/12/20.
 */
public class VariantCallingEngine {
    /**
     * genome location parser
     */
    private GenomeLocationParser parser;

    /**
     * pileup information
     */
    private PileupState pState;

    /**
     * genotype likelihood calculator
     */
    private GenotypeLikelihoodCalculator genotypeLikelihoodCaculator;

    /**
     * genotype caller
     */
    private GenotyperCaller genotyperCaller;

    /**
     *
     * @param records FIXME::需要替换为Alignments
     */
    public VariantCallingEngine(ArrayList<GaeaSamRecord> records) {
        parser = new GenomeLocationParser();
        pState = new PileupState(records, parser);
    }

    public void reduce() {
        Pileup pileup;
        while (pState.hasNext()) {
            pileup = pState.next();

            Collection<String> samples = pileup.getSamples();
            for(String sample : samples) {
                Pileup samplePileup = pileup.getPileupForSample(sample);
            }
        }
    }


}
