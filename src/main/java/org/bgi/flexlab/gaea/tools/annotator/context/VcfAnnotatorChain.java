package org.bgi.flexlab.gaea.tools.annotator.context;

import java.util.LinkedList;
import java.util.List;

import org.bgi.flexlab.gaea.tools.annotator.conf.Config;

/**
 * Maintains a list of VcfAnnotators and applies them one by one
 * in the specified order
 */
public class VcfAnnotatorChain implements VcfAnnotator {

	List<VcfAnnotator> annotators;

	public VcfAnnotatorChain() {
		annotators = new LinkedList<>();
	}

	/**
	 * Add a new annotator
	 */
	public void add(VcfAnnotator vcfAnnotator) {
		annotators.add(vcfAnnotator);
	}

	@Override
	public boolean addHeaders(VcfFileIterator vcfFile) {
		boolean error = false;

		for (VcfAnnotator vcfAnnotator : annotators)
			error |= vcfAnnotator.addHeaders(vcfFile);

		return error;
	}

	@Override
	public boolean annotate(VcfEntry vcfEntry) {
		boolean annotated = false;
		for (VcfAnnotator vcfAnnotator : annotators)
			annotated |= vcfAnnotator.annotate(vcfEntry);

		return annotated;
	}

	@Override
	public boolean annotateFinish() {
		boolean error = false;

		for (VcfAnnotator vcfAnnotator : annotators)
			error |= vcfAnnotator.annotateFinish();

		return error;
	}

	@Override
	public boolean annotateInit(VcfFileIterator vcfFile) {
		boolean error = false;

		for (VcfAnnotator vcfAnnotator : annotators)
			error |= vcfAnnotator.annotateInit(vcfFile);

		return error;
	}

	@Override
	public String[] getArgs() {
		return null;
	}

	@Override
	public void parseArgs(String[] args) {
	}

	@Override
	public boolean run() {
		return false;
	}

	@Override
	public void setConfig(Config config) {
		for (VcfAnnotator vcfAnnotator : annotators)
			vcfAnnotator.setConfig(config);
	}

	@Override
	public void setDebug(boolean debug) {
		for (VcfAnnotator vcfAnnotator : annotators)
			vcfAnnotator.setDebug(debug);
	}

	@Override
	public void setVerbose(boolean verbose) {
		for (VcfAnnotator vcfAnnotator : annotators)
			vcfAnnotator.setVerbose(verbose);
	}

	@Override
	public void usage(String message) {
	}

}
