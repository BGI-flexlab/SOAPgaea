package org.bgi.flexlab.gaea.tools.recalibrator.covariate;

import java.util.ArrayList;
import java.util.List;

import org.bgi.flexlab.gaea.data.exception.UserException;
import org.bgi.flexlab.gaea.tools.mapreduce.realigner.BaseRecalibratorOptions;
import org.bgi.flexlab.gaea.util.Pair;

import htsjdk.samtools.SAMFileHeader;

public class CovariateUtil {
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static List<Class<? extends Covariate>> initConvariate() {
		List<Class<? extends Covariate>> covariates = new ArrayList();
		covariates.add(ContextCovariate.class);
		covariates.add(CycleCovariate.class);
		covariates.add(QualityCovariate.class);
		covariates.add(ReadGroupCovariate.class);
		return covariates;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static List<Class<? extends RequiredCovariate>> initRequiredCovariate() {
		List<Class<? extends RequiredCovariate>> require = new ArrayList();
		require.add(QualityCovariate.class);
		require.add(ReadGroupCovariate.class);
		return require;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static List<Class<? extends OptionalCovariate>> initOptionalCovariate() {
		List<Class<? extends OptionalCovariate>> optional = new ArrayList();
		optional.add(ContextCovariate.class);
		optional.add(CycleCovariate.class);
		return optional;
	}

	private static ArrayList<Covariate> addRequiredCovariatesToList(List<Class<? extends RequiredCovariate>> classes) {
		ArrayList<Covariate> dest = new ArrayList<Covariate>(classes.size());
		if (classes.size() != 2)
			throw new UserException("Require covariate had changed?");

		dest.add(new ReadGroupCovariate());
		dest.add(new QualityCovariate());
		return dest;
	}

	private static ArrayList<Covariate> addOptionalCovariatesToList(List<Class<? extends OptionalCovariate>> classes) {
		ArrayList<Covariate> dest = new ArrayList<Covariate>(classes.size());
		for (Class<?> covClass : classes) {
			try {
				final Covariate covariate = (Covariate) covClass.newInstance();
				dest.add(covariate);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return dest;
	}

	private static Pair<ArrayList<Covariate>, ArrayList<Covariate>> initializeCovariates(
			BaseRecalibratorOptions option) {
		final List<Class<? extends Covariate>> covariateClasses = initConvariate();
		final List<Class<? extends RequiredCovariate>> requiredClasses = initRequiredCovariate();
		final List<Class<? extends OptionalCovariate>> standardClasses = initOptionalCovariate();

		final ArrayList<Covariate> requiredCovariates = addRequiredCovariatesToList(requiredClasses);
		ArrayList<Covariate> optionalCovariates = new ArrayList<Covariate>();
		if (!option.DO_NOT_USE_STANDARD_COVARIATES)
			optionalCovariates = addOptionalCovariatesToList(standardClasses);

		if (option.COVARIATES != null) {
			for (String requestedCovariateString : option.COVARIATES) {
				boolean foundClass = false;
				for (Class<? extends Covariate> covClass : covariateClasses) {
					if (requestedCovariateString.equalsIgnoreCase(covClass.getSimpleName())) {
						foundClass = true;
						if (!requiredClasses.contains(covClass)
								&& (option.DO_NOT_USE_STANDARD_COVARIATES || !standardClasses.contains(covClass))) {
							try {
								final Covariate covariate = covClass.newInstance();
								optionalCovariates.add(covariate);
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
				}

				if (!foundClass) {
					throw new UserException.CommandLineException("The requested covariate type ("
							+ requestedCovariateString + ") isn't a valid covariate option.");
				}
			}
		}
		return new Pair<ArrayList<Covariate>, ArrayList<Covariate>>(requiredCovariates, optionalCovariates);
	}

	public static Covariate[] initializeCovariates(BaseRecalibratorOptions option, SAMFileHeader mHeader) {
		Pair<ArrayList<Covariate>, ArrayList<Covariate>> covariates = initializeCovariates(option);
		ArrayList<Covariate> requiredCovariates = covariates.getFirst();
		ArrayList<Covariate> optionalCovariates = covariates.getSecond();

		Covariate[] requestedCovariates = new Covariate[requiredCovariates.size() + optionalCovariates.size()];
		int covariateIndex = 0;
		for (final Covariate covariate : requiredCovariates)
			requestedCovariates[covariateIndex++] = covariate;
		for (final Covariate covariate : optionalCovariates)
			requestedCovariates[covariateIndex++] = covariate;

		for (Covariate cov : requestedCovariates) {
			cov.initialize(option);
			if (cov instanceof ReadGroupCovariate)
				((ReadGroupCovariate) cov).initializeReadGroup(mHeader);
		}

		return requestedCovariates;
	}
}
