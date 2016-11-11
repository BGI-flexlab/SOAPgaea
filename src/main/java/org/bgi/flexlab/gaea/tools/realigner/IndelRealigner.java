package org.bgi.flexlab.gaea.tools.realigner;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.variant.variantcontext.VariantContext;

import java.util.ArrayList;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocationParser;
import org.bgi.flexlab.gaea.data.structure.location.RealignerIntervalFilter;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.tools.mapreduce.realigner.RealignerOptions;
import org.bgi.flexlab.gaea.util.Window;
import org.bgi.flexlab.gaea.variant.filter.VariantRegionFilter;

public class IndelRealigner {
	private ArrayList<GenomeLocation> intervals = null;
	private ArrayList<VariantContext> variants = null;
	private ChromosomeInformationShare chrInfo = null;
	private GenomeLocationParser parser = null;
	private VariantRegionFilter filter = null;
	private Window win = null;
	private RealignerOptions option = null;

	public IndelRealigner(SAMFileHeader mHeader,
			ArrayList<VariantContext> vatiants, Window win,
			ChromosomeInformationShare chrInfo, RealignerOptions option) {
		this.parser = new GenomeLocationParser(mHeader.getSequenceDictionary());
		this.variants = variants;
		filter = new VariantRegionFilter();
		this.chrInfo = chrInfo;
		this.option = option;
	}

	public void setIntervals(ArrayList<GenomeLocation> intervals) {
		this.intervals = intervals;
	}

	private ArrayList<VariantContext> filterKnowIndels(GaeaSamRecord read) {
		GenomeLocation location = parser.createGenomeLocation(read);
		return filter.listFilter(variants, location.getStart(),
				location.getStop());
	}

	private void updateWindowByInterval() {
		RealignerIntervalFilter filter = new RealignerIntervalFilter();
		ArrayList<GenomeLocation> filtered = filter.filterList(intervals, win);

		if (filtered == null || filtered.isEmpty()) {
			return;
		}

		GenomeLocation start = filtered.get(0);
		if (start.getStart() < win.getStart()
				&& start.getStop() >= win.getStart()) {
			win.setStart(start.getStop() + 1);
		}

		GenomeLocation end = filtered.get(filtered.size() - 1);
		if (end.getStart() <= win.getStop() && end.getStop() > win.getStop()) {
			win.setStop(end.getStop());
		}
		filtered.clear();
	}

	public void traversals(ArrayList<GaeaSamRecord> records) {
		updateWindowByInterval();

		ArrayList<VariantContext> filtered = null;
		for (GaeaSamRecord sam : records) {
			filtered = filterKnowIndels(sam);

		}
	}
}
