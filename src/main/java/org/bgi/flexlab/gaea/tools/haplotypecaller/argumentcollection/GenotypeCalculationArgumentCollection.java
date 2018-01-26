package org.bgi.flexlab.gaea.tools.haplotypecaller.argumentcollection;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bgi.flexlab.gaea.tools.haplotypecaller.utils.HomoSapiensConstants;
import org.bgi.flexlab.gaea.util.Utils;

public final class GenotypeCalculationArgumentCollection implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a GenotypeCalculationArgumentCollection with default values.
     */
    public GenotypeCalculationArgumentCollection() {}

    /**
     * Creates a GenotypeCalculationArgumentCollection with the values from other
     *
     * @param other GenotypeCalculationArgumentCollection from which to copy values
     */
    public GenotypeCalculationArgumentCollection( final GenotypeCalculationArgumentCollection other ) {
        Utils.nonNull(other);

        this.USE_NEW_AF_CALCULATOR = other.USE_NEW_AF_CALCULATOR;
        this.ANNOTATE_NUMBER_OF_ALLELES_DISCOVERED = other.ANNOTATE_NUMBER_OF_ALLELES_DISCOVERED;
        this.snpHeterozygosity = other.snpHeterozygosity;
        this.indelHeterozygosity = other.indelHeterozygosity;
        this.STANDARD_CONFIDENCE_FOR_CALLING = other.STANDARD_CONFIDENCE_FOR_CALLING;
        this.MAX_ALTERNATE_ALLELES = other.MAX_ALTERNATE_ALLELES;
        this.inputPrior = new ArrayList<>(other.inputPrior);
        this.samplePloidy = other.samplePloidy;
    }

    /**
     * Use the new allele frequency / QUAL score model
     */
    public boolean USE_NEW_AF_CALCULATOR = false;

    /**
     * Depending on the value of the --max_alternate_alleles argument, we may genotype only a fraction of the alleles being sent on for genotyping.
     * Using this argument instructs the genotyper to annotate (in the INFO field) the number of alternate alleles that were originally discovered at the site.
     */
    public boolean ANNOTATE_NUMBER_OF_ALLELES_DISCOVERED = false;

    /**
     * The expected heterozygosity value used to compute prior probability that a locus is non-reference.
     *
     * The default priors are for provided for humans:
     *
     * het = 1e-3
     *
     * which means that the probability of N samples being hom-ref at a site is:
     *
     * 1 - sum_i_2N (het / i)
     *
     * Note that heterozygosity as used here is the population genetics concept:
     *
     * http://en.wikipedia.org/wiki/Zygosity#Heterozygosity_in_population_genetics
     *
     * That is, a hets value of 0.01 implies that two randomly chosen chromosomes from the population of organisms
     * would differ from each other (one being A and the other B) at a rate of 1 in 100 bp.
     *
     * Note that this quantity has nothing to do with the likelihood of any given sample having a heterozygous genotype,
     * which in the GATK is purely determined by the probability of the observed data P(D | AB) under the model that there
     * may be a AB het genotype.  The posterior probability of this AB genotype would use the het prior, but the GATK
     * only uses this posterior probability in determining the prob. that a site is polymorphic.  So changing the
     * het parameters only increases the chance that a site will be called non-reference across all samples, but
     * doesn't actually change the output genotype likelihoods at all, as these aren't posterior probabilities at all.
     *
     * The quantity that changes whether the GATK considers the possibility of a het genotype at all is the ploidy,
     * which determines how many chromosomes each individual in the species carries.
     */
    public Double snpHeterozygosity = HomoSapiensConstants.SNP_HETEROZYGOSITY;

    /**
     * This argument informs the prior probability of having an indel at a site.
     */
    public double indelHeterozygosity = HomoSapiensConstants.INDEL_HETEROZYGOSITY;

    /**
     * The standard deviation of the distribution of alt allele fractions.  The above heterozygosity parameters give the
     * *mean* of this distribution; this parameter gives its spread.
     */
    public double heterozygosityStandardDeviation = 0.01;

    /**
     * The minimum phred-scaled confidence threshold at which variants should be called. Only variant sites with QUAL equal
     * or greater than this threshold will be called. Note that since version 3.7, we no longer differentiate high confidence
     * from low confidence calls at the calling step. The default call confidence threshold is set low intentionally to achieve
     * high sensitivity, which will allow false positive calls as a side effect. Be sure to perform some kind of filtering after
     * calling to reduce the amount of false positives in your final callset. Note that when HaplotypeCaller is used in GVCF mode
     * (using either -ERC GVCF or -ERC BP_RESOLUTION) the call threshold is automatically set to zero. Call confidence thresholding
     * will then be performed in the subsequent GenotypeGVCFs command.
     */
    public double STANDARD_CONFIDENCE_FOR_CALLING = 10.0;

    /**
     * If there are more than this number of alternate alleles presented to the genotyper (either through discovery or GENOTYPE_GIVEN ALLELES),
     * then only this many alleles will be used.  Note that genotyping sites with many alternate alleles is both CPU and memory intensive and it
     * scales exponentially based on the number of alternate alleles.  Unless there is a good reason to change the default value, we highly recommend
     * that you not play around with this parameter.
     *
     * See also {@link #MAX_GENOTYPE_COUNT}.
     */
    public int MAX_ALTERNATE_ALLELES = 6;

    /**
     * If there are more than this number of genotypes at a locus presented to the genotyper, then only this many genotypes will be used.
     * The possible genotypes are simply different ways of partitioning alleles given a specific ploidy asumption.
     * Therefore, we remove genotypes from consideration by removing alternate alleles that are the least well supported.
     * The estimate of allele support is based on the ranking of the candidate haplotypes coming out of the graph building step.
     * Note that the reference allele is always kept.
     *
     * Note that genotyping sites with large genotype counts is both CPU and memory intensive.
     * Unless there is a good reason to change the default value, we highly recommend that you not play around with this parameter.
     *
     * The maximum number of alternative alleles used in the genotyping step will be the lesser of the two:
     * 1. the largest number of alt alleles, given ploidy, that yields a genotype count no higher than {@link #MAX_GENOTYPE_COUNT}
     * 2. the value of {@link #MAX_ALTERNATE_ALLELES}
     *
     * See also {@link #MAX_ALTERNATE_ALLELES}.
     */
    public int MAX_GENOTYPE_COUNT = 1024;

    /**
     * By default, the prior specified with the argument --heterozygosity/-hets is used for variant discovery at a particular locus, using an infinite sites model,
     * see e.g. Waterson (1975) or Tajima (1996).
     * This model asserts that the probability of having a population of k variant sites in N chromosomes is proportional to theta/k, for 1=1:N
     *
     * There are instances where using this prior might not be desireable, e.g. for population studies where prior might not be appropriate,
     * as for example when the ancestral status of the reference allele is not known.
     * By using this argument, user can manually specify priors to be used for calling as a vector for doubles, with the following restriciotns:
     * a) User must specify 2N values, where N is the number of samples.
     * b) Only diploid calls supported.
     * c) Probability values are specified in double format, in linear space.
     * d) No negative values allowed.
     * e) Values will be added and Pr(AC=0) will be 1-sum, so that they sum up to one.
     * f) If user-defined values add to more than one, an error will be produced.
     *
     * If user wants completely flat priors, then user should specify the same value (=1/(2*N+1)) 2*N times,e.g.
     *   -inputPrior 0.33 -inputPrior 0.33
     * for the single-sample diploid case.
     */
    public List<Double> inputPrior = Collections.emptyList();

    /**
     *   Sample ploidy - equivalent to number of chromosomes per pool. In pooled experiments this should be = # of samples in pool * individual sample ploidy
     */
    public int samplePloidy = HomoSapiensConstants.DEFAULT_PLOIDY;
}
