package org.bgi.flexlab.gaea.tools.realigner;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.variant.variantcontext.VariantContext;

import java.io.IOException;
import java.util.ArrayList;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.reference.GenomeShare;
import org.bgi.flexlab.gaea.data.structure.vcf.index.VCFLoader;
import org.bgi.flexlab.gaea.tools.mapreduce.realigner.RealignerOptions;
import org.bgi.flexlab.gaea.util.Window;

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
		setChromosome(genomeShare);
		setKnowIndels(loader);
	}

	private void setChromosome(GenomeShare genomeShare) {
		chrInfo = genomeShare.getChromosomeInfo(win.getContigName());
	}

	private void setKnowIndels(VCFLoader loader) {
		if (loader == null)
			return;
		try {
			String referenceName = win.getContigName();
			int WINDOWS_EXTEND = option.getExtendSize();
			knowIndels = loader.load(referenceName, win.getStart()
					- WINDOWS_EXTEND, win.getStop() + WINDOWS_EXTEND);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void reduce() {
		IdentifyRegionsCreator creator = new IdentifyRegionsCreator(option,
				filterRecords, mHeader, chrInfo, knowIndels);
		creator.regionCreator();
		ArrayList<GenomeLocation> intervals = creator.getIntervals();
		filterRecords.clear();
		
		
	}
}
