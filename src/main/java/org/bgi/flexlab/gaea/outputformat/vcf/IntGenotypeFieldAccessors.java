package org.bgi.flexlab.gaea.outputformat.vcf;

import java.util.HashMap;

import org.bgi.flexlab.gaea.structure.header.VCFConstants;

import htsjdk.variant.variantcontext.Genotype;

/**
 * A convenient way to provide a single view on the many int and int[] field values we work with,
 * for writing out the values.  This class makes writing out the inline AD, GQ, PL, DP fields
 * easy and fast
 *
 */
class IntGenotypeFieldAccessors {
    // initialized once per writer to allow parallel writers to work
    private final HashMap<String, Accessor> intGenotypeFieldEncoders = new HashMap<String, Accessor>();

    public IntGenotypeFieldAccessors() {
        intGenotypeFieldEncoders.put(VCFConstants.DEPTH_KEY, new IntGenotypeFieldAccessors.DPAccessor());
        intGenotypeFieldEncoders.put(VCFConstants.GENOTYPE_ALLELE_DEPTHS, new IntGenotypeFieldAccessors.ADAccessor());
        intGenotypeFieldEncoders.put(VCFConstants.GENOTYPE_PL_KEY, new IntGenotypeFieldAccessors.PLAccessor());
        intGenotypeFieldEncoders.put(VCFConstants.GENOTYPE_QUALITY_KEY, new IntGenotypeFieldAccessors.GQAccessor());
    }

    /**
     * Return an accessor for field, or null if none exists
     * @param field
     * @return
     */
    public Accessor getAccessor(final String field) {
        return intGenotypeFieldEncoders.get(field);
    }

    public static abstract class Accessor {
        public abstract int[] getValues(final Genotype g);

        public final int getSize(final Genotype g) {
            final int[] v = getValues(g);
            return v == null ? 0 : v.length;
        }
    }

    private static abstract class AtomicAccessor extends Accessor {
        private final int[] singleton = new int[1];

        @Override
        public int[] getValues(final Genotype g) {
            singleton[0] = getValue(g);
            return singleton[0] == -1 ? null : singleton;
        }

        public abstract int getValue(final Genotype g);
    }

    public static class GQAccessor extends AtomicAccessor {
        @Override public int getValue(final Genotype g) { return Math.min(g.getGQ(), VCFConstants.MAX_GENOTYPE_QUAL); }
    }

    public static class DPAccessor extends AtomicAccessor {
        @Override public int getValue(final Genotype g) { return g.getDP(); }
    }

    public static class ADAccessor extends Accessor {
        @Override public int[] getValues(final Genotype g) { return g.getAD(); }
    }

    public static class PLAccessor extends Accessor {
        @Override public int[] getValues(final Genotype g) { return g.getPL(); }
    }
}

