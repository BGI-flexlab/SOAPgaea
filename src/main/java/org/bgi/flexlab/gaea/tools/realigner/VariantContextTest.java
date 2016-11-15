package org.bgi.flexlab.gaea.tools.realigner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeSet;

import htsjdk.variant.variantcontext.VariantContext;

import org.bgi.flexlab.gaea.data.structure.vcf.index.VCFLoader;
import org.bgi.flexlab.gaea.variant.filter.VariantRegionFilter;

public class VariantContextTest {
	public static void main(String[] args){
		String dbsnp = args[0];
		VCFLoader loader = new VCFLoader(dbsnp);
		try {
			loader.loadHeader();
		} catch (IOException e) {
			e.printStackTrace();
		}
		VariantRegionFilter filter = new VariantRegionFilter();
		ArrayList<VariantContext> contexts = filter.loadFilter(loader, "chr1", 10000, 100000);
		
		@SuppressWarnings("unchecked")
		TreeSet<VariantContext> set = new TreeSet<VariantContext>(new KnowIndelComparator());
		
		for(VariantContext context : contexts){
			set.add(context);
		}
		
		for(VariantContext context : contexts){
			set.add(context);
		}
		
		for(VariantContext context : set){
			System.out.println(context.getContig()+"\t"+context.getStart()+"\t"+context.getEnd());
		}
		
		set.clear();
	}
}
