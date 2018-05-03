package org.bgi.flexlab.gaea.tools.vcfstats;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;

public enum VariantType {
    SNP// Single nucleotide polymorphism (i.e. 1 base is changed)
    , MNP // Multiple nucleotide polymorphism (i.e. several bases are changed)
    , INS // Insertion (i.e. some bases added)
    , DEL // Deletion (some bases removed)
    , MIXED // A mixture of insertion, deletions, SNPs and or MNPs (a.k.a. substitution)
    , NO_VARIATION
    , SYMBOLIC
    , INV // Inversion (structural variant)
    , DUP // Duplication (structural variant)
    , BND; // Break-ends (rearrangement)


    public static VariantType determineType(VariantContext vc, String sample) {
        VariantType type;

        switch ( vc.getNAlleles() ) {
            case 0:
                throw new IllegalStateException("Unexpected error: requested type of VariantContext with no alleles!");
            case 1:
                // note that this doesn't require a reference allele.  You can be monomorphic independent of having a
                // reference allele
                type = NO_VARIATION;
                break;
            default:
                type = determinePolymorphicType(vc, sample);
        }
        return type;
    }

    private static VariantType determinePolymorphicType(VariantContext vc, String sample) {
        Genotype gt = vc.getGenotype(sample);
        VariantType type = null;

        vc.getReference();

        // do a pairwise comparison of all alleles against the reference allele
        for ( Allele allele : gt.getAlleles() ) {
            if ( allele.isReference())
                continue;

            // find the type of this allele relative to the reference
            VariantType biallelicType = typeOfBiallelicVariant(vc.getReference(), allele);

            // for the first alternate allele, set the type to be that one
            if ( type == null ) {
                type = biallelicType;
            }
            // if the type of this allele is different from that of a previous one, assign it the MIXED type and quit
            else if ( biallelicType != type ) {
                type = MIXED;
            }
        }
        return type;
    }

    private static VariantType typeOfBiallelicVariant(Allele ref, Allele allele) {
        if ( ref.isSymbolic() )
            throw new IllegalStateException("Unexpected error: encountered a record with a symbolic reference allele");

        if ( allele.isSymbolic() )
            return SYMBOLIC;

        if ( ref.length() == allele.length() ) {
            if ( allele.length() == 1 )
                return SNP;
            else
                return MNP;
        } else if ( ref.length() > allele.length() ) {
            return DEL;
        } else
            return INS;
    }
}
