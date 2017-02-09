package org.bgi.flexlab.gaea.tools.realigner;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.variant.variantcontext.VariantContext;

import java.util.ArrayList;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.dbsnp.DbsnpShare;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.reference.ReferenceShare;
import org.bgi.flexlab.gaea.data.structure.reference.index.VcfIndex;
import org.bgi.flexlab.gaea.data.structure.vcf.VCFLocalLoader;
import org.bgi.flexlab.gaea.data.variant.filter.VariantRegionFilter;
import org.bgi.flexlab.gaea.tools.mapreduce.realigner.RealignerOptions;
import org.bgi.flexlab.gaea.util.Window;

public class RealignerEngine {
	private RealignerOptions option = null;
	private ReferenceShare genomeShare = null;
	private VCFLocalLoader loader = null;
	private ChromosomeInformationShare chrInfo = null;
	private ArrayList<VariantContext> knowIndels = null;
	private ArrayList<GaeaSamRecord> records = null;
	private ArrayList<GaeaSamRecord> filterRecords = null;
	private Window win = null;
	private SAMFileHeader mHeader = null;
	private VariantRegionFilter indelFilter = null;
	private IndelRealigner indelRealigner = null;
	private RealignerWriter writer = null;
	private DbsnpShare dbsnpShare = null;

	private int start;
	private int end;

	public RealignerEngine(RealignerOptions option, ReferenceShare genomeShare, DbsnpShare dbsnpShare,
			VCFLocalLoader loader, SAMFileHeader mHeader, RealignerWriter writer) {
		this.option = option;
		this.genomeShare = genomeShare;
		this.loader = loader;
		this.mHeader = mHeader;
		this.writer = writer;
		this.dbsnpShare = dbsnpShare;
	}

	public void set(Window win, ArrayList<GaeaSamRecord> records, ArrayList<GaeaSamRecord> filterRecords) {
		this.win = win;
		if (win == null)
			throw new RuntimeException("window is null");
		this.records = records;
		this.filterRecords = filterRecords;
		indelFilter = new VariantRegionFilter();
		setChromosome(genomeShare);
		setKnowIndels(loader);
		indelRealigner = new IndelRealigner(mHeader, knowIndels, win, chrInfo, option);
	}

	private void setChromosome(ReferenceShare genomeShare) {
		chrInfo = genomeShare.getChromosomeInfo(win.getContigName());
	}

	private void setKnowIndels(VCFLocalLoader loader) {
		if (loader == null) {
			throw new RuntimeException("loader is null!!");
		}
		String referenceName = win.getContigName();
		int WINDOWS_EXTEND = option.getExtendSize();

		start = (win.getStart() - WINDOWS_EXTEND) > 0 ? (win.getStart() - WINDOWS_EXTEND) : 0;
		end = (win.getStop() + WINDOWS_EXTEND) < mHeader.getSequence(referenceName).getSequenceLength()
				? (win.getStop() + WINDOWS_EXTEND) : mHeader.getSequence(referenceName).getSequenceLength();

		long startPosition = dbsnpShare.getStartPosition(referenceName, start / VcfIndex.WINDOW_SIZE,
				VcfIndex.WINDOW_SIZE);

		if (startPosition >= 0)
			knowIndels = indelFilter.loadFilter(loader, referenceName, startPosition, end);
	}

	public void reduce() {
		IdentifyRegionsCreator creator = new IdentifyRegionsCreator(option, filterRecords, mHeader, chrInfo,
				knowIndels);
		creator.regionCreator(win.getChrIndex(), 0, Integer.MAX_VALUE);

		ArrayList<GenomeLocation> intervals = creator.getIntervals();
		filterRecords.clear();

		indelRealigner.setIntervals(intervals);
		indelRealigner.traversals(records, writer);
	}
}
