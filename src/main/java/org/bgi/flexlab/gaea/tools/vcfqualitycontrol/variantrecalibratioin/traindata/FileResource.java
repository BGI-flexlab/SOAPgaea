package org.bgi.flexlab.gaea.tools.vcfqualitycontrol.variantrecalibratioin.traindata;

import java.io.IOException;
import java.util.ArrayList;

import org.bgi.flexlab.gaea.data.structure.dbsnp.ChromosomeDbsnpShare;
import org.bgi.flexlab.gaea.data.structure.dbsnp.DbsnpShare;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.data.structure.reference.index.VcfIndex;
import org.bgi.flexlab.gaea.data.structure.vcf.VCFLocalLoader;
import htsjdk.variant.variantcontext.VariantContext;

public class FileResource implements ResourceType{
	private VCFLocalLoader loader;
	private DbsnpShare snpShare;
	
	@Override
	public void initialize(String reference, String dbSnp) {
		// TODO Auto-generated method stub
		snpShare = new DbsnpShare(dbSnp, reference);
		snpShare.loadChromosomeList(dbSnp + VcfIndex.INDEX_SUFFIX);
		try {
			loader = new VCFLocalLoader(dbSnp);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		}
	}

	@Override
	public ArrayList<VariantContext> get(GenomeLocation loc) {
		// TODO Auto-generated method stub
		ChromosomeDbsnpShare share = snpShare.getChromosomeDbsnp(loc.getContig());
		int winNum = loc.getStart() / VcfIndex.WINDOW_SIZE;
		long start = share.getStartPosition(winNum);
		try {
			ArrayList<VariantContext> vcl = loader.query(loc.getContig(), start, loc.getStop());
			return vcl;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
