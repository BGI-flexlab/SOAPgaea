package org.bgi.flexlab.gaea.util;

import java.util.Random;

public class RandomUtils {
	public RandomUtils() {}

	private static final long RANDOM_SEED = 47382911L;
	private static Random randomGenerator = new Random(RANDOM_SEED);

	public static Random getRandomGenerator() {
		return randomGenerator;
	}

	public static Random getNewRandomGenerator() {
		return new Random(RANDOM_SEED);
	}

	public static void resetRandomGenerator() {
		randomGenerator.setSeed(RANDOM_SEED);
	}

	public static void resetRandomGenerator(long seed) {
		randomGenerator.setSeed(seed);
	}
}
