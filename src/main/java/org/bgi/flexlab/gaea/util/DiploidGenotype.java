package org.bgi.flexlab.gaea.util;

public enum DiploidGenotype {
    AA ('A', 'A'),
    AC ('A', 'C'),
    CC ('C', 'C'),
    AG ('A', 'G'),
    CG ('C', 'G'),
    GG ('G', 'G'),
    AT ('A', 'T'),
    CT ('C', 'T'),
    GT ('G', 'T'),
    TT ('T', 'T');

    public byte base1, base2;

    @Deprecated
    private DiploidGenotype(char base1, char base2) {
        this((byte)base1, (byte)base2);
    }

    private DiploidGenotype(byte base1, byte base2) {
        this.base1 = base1;
        this.base2 = base2;
    }

    public boolean isHomRef(byte r) {
        return isHom() && r == base1;
    }

    public boolean isHomVar(byte r) {
        return isHom() && r != base1;
    }

    public boolean isHetRef(byte r) {
        if ( base1 == r )
            return r != base2;
        else
            return base2 == r;
    }

    public boolean isHom() {
        return ! isHet();
    }

    public boolean isHet() {
        return base1 != base2;
    }


    /**
     * create a diploid genotype, given a character to make into a hom genotype
     * @param hom the character to turn into a hom genotype, i.e. if it is A, then returned will be AA
     * @return the diploid genotype
     */
    public static DiploidGenotype createHomGenotype(byte hom) {
        int index = BaseUtils.simpleBaseToBaseIndex(hom);
        if ( index == -1 )
            throw new IllegalArgumentException(hom + " is not a valid base character");
        return conversionMatrix[index][index];
    }

    /**
     * create a diploid genotype, given 2 chars which may not necessarily be ordered correctly
     * @param base1 base1
     * @param base2 base2
     * @return the diploid genotype
     */
    public static DiploidGenotype createDiploidGenotype(byte base1, byte base2) {
        int index1 = BaseUtils.simpleBaseToBaseIndex(base1);
        if ( index1 == -1 )
            throw new IllegalArgumentException(base1 + " is not a valid base character");
        int index2 = BaseUtils.simpleBaseToBaseIndex(base2);
        if ( index2 == -1 )
            throw new IllegalArgumentException(base2 + " is not a valid base character");
        return conversionMatrix[index1][index2];
    }

    /**
     * create a diploid genotype, given 2 base indexes which may not necessarily be ordered correctly
     * @param baseIndex1 base1
     * @param baseIndex2 base2
     * @return the diploid genotype
     */
    public static DiploidGenotype createDiploidGenotype(int baseIndex1, int baseIndex2) {
        if ( baseIndex1 == -1 )
            throw new IllegalArgumentException(baseIndex1 + " does not represent a valid base character");
        if ( baseIndex2 == -1 )
            throw new IllegalArgumentException(baseIndex2 + " does not represent a valid base character");
        return conversionMatrix[baseIndex1][baseIndex2];
    }

    private static final DiploidGenotype[][] conversionMatrix = {
            { DiploidGenotype.AA, DiploidGenotype.AC, DiploidGenotype.AT, DiploidGenotype.AG },
            { DiploidGenotype.AC, DiploidGenotype.CC, DiploidGenotype.CT, DiploidGenotype.CG },
            { DiploidGenotype.AT, DiploidGenotype.CT, DiploidGenotype.TT, DiploidGenotype.GT },
            { DiploidGenotype.AG, DiploidGenotype.CG, DiploidGenotype.GT, DiploidGenotype.GG }
    };
}