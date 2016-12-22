package org.bgi.flexlab.gaea.tools.variantrecalibratioin.model;

import java.util.Random;

public class GaeaRandom {
	private static final long Gaea_RANDOM_SEED = 47382911L;
    private static Random randomGenerator = new Random(Gaea_RANDOM_SEED);
    public static Random getRandomGenerator() { return randomGenerator; }
    public static void resetRandomGenerator() { randomGenerator.setSeed(Gaea_RANDOM_SEED); }
    public static void resetRandomGenerator(long seed) { randomGenerator.setSeed(seed); }
}
