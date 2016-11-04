package org.bgi.flexlab.gaea.tools.realigner;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.variant.variantcontext.VariantContext;

import java.util.ArrayList;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.reference.GenomeShare;
import org.bgi.flexlab.gaea.data.structure.vcf.index.VCFLoader;
import org.bgi.flexlab.gaea.tools.mapreduce.realigner.RealignerOptions;
import org.bgi.flexlab.gaea.util.Window;
import org.bgi.flexlab.gaea.variant.filter.VariantRegionFilter;

public class RealignerEngine {
	private RealignerOptions option = null;
	private GenomeShare genomeShare = null;
	private VCFLoader loader = null;
	private ChromosomeInformationShare chrInfo = null;
	private ArrayList<VariantContext> knowIndels = null;
	private ArrayList<GaeaSamRecord> records = null;
	private ArrayList<GaeaSamRecord> filterRecords = null;
	private Window win = null;
	private SAMFileHeader mHeader = null;
	private VariantRegionFilter indelFilter = null;
	private IndelRealigner indelRealigner = null;

	public RealignerEngine(RealignerOptions option, GenomeShare genomeShare,
			VCFLoader loader, SAMFileHeader mHeader) {
		this.option = option;
		this.genomeShare = genomeShare;
		this.loader = loader;
		this.mHeader = mHeader;
	}

	public void set(Window win, ArrayList<GaeaSamRecord> records,
			ArrayList<GaeaSamRecord> filterRecords) {
		this.win = win;
		this.records = records;
		this.filterRecords = filterRecords;
		indelFilter = new VariantRegionFilter();
		setChromosome(genomeShare);
		setKnowIndels(loader);
		indelRealigner = new IndelRealigner(mHeader, knowIndels, win, chrInfo,
				option);
	}

	private void setChromosome(GenomeShare genomeShare) {
		chrInfo = genomeShare.getChromosomeInfo(win.getContigName());
	}

	private void setKnowIndels(VCFLoader loader) {
		if (loader == null)
			return;
		String referenceName = win.getContigName();
		int WINDOWS_EXTEND = option.getExtendSize();
		knowIndels = indelFilter
				.loadFilter(loader, referenceName, win.getStart()
						- WINDOWS_EXTEND, win.getStop() + WINDOWS_EXTEND);
	}

	public void reduce() {
		IdentifyRegionsCreator creator = new IdentifyRegionsCreator(option,
				filterRecords, mHeader, chrInfo, knowIndels);
		creator.regionCreator();
		ArrayList<GenomeLocation> intervals = creator.getIntervals();
		filterRecords.clear();

		indelRealigner.setIntervals(intervals);
		indelRealigner.traversals(records);
	}
}
