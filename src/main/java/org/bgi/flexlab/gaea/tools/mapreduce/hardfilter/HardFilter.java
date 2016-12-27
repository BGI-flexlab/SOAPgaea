package org.bgi.flexlab.gaea.tools.mapreduce.hardfilter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.bgi.flexlab.gaea.data.structure.vcf.VCFLocalWriter;

import htsjdk.tribble.readers.AsciiLineReader;
import htsjdk.tribble.readers.AsciiLineReaderIterator;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.variantcontext.VariantContextUtils;
import htsjdk.variant.variantcontext.VariantContextUtils.JexlVCMatchExp;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFFilterHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLine;

public class HardFilter {
	private List<JexlVCMatchExp> snpFilter = null;
	private List<JexlVCMatchExp> indelFilter = null;
	protected Boolean FAIL_MISSING_VALUES = false;
	
	public HardFilter(String filterName, String snpFilter, String indelFilter) {
		if(snpFilter != null)
			this.snpFilter = VariantContextUtils.initializeMatchExps(new String[]{filterName+"snp"}, new String[]{snpFilter});
		if(indelFilter != null)
			this.indelFilter = VariantContextUtils.initializeMatchExps(new String[]{filterName+"indel"}, new String[]{indelFilter});
		VariantContextUtils.engine.get().setSilent(true);
	}
	
	public Set<VCFHeaderLine> filterHeaders() {
		Set<VCFHeaderLine> hInfo = new HashSet<VCFHeaderLine>();
		for ( VariantContextUtils.JexlVCMatchExp exp : snpFilter )
            hInfo.add(new VCFFilterHeaderLine(exp.name, exp.exp.toString()));
		for ( VariantContextUtils.JexlVCMatchExp exp : indelFilter )
            hInfo.add(new VCFFilterHeaderLine(exp.name, exp.exp.toString()));
		return hInfo;
	}
	
	public VariantContext filter(VariantContext vc) {
		final VariantContextBuilder builder = new VariantContextBuilder(vc);
		Set<String> filters = new LinkedHashSet<String>(vc.getFilters());
		if(vc.isSNP() && snpFilter != null) {
			for ( VariantContextUtils.JexlVCMatchExp exp : snpFilter ) {
				try {
	                if ( VariantContextUtils.match(vc, exp) ) {
	                	filters.add(exp.name);
	                }
	            } catch (Exception e) {
	                // do nothing unless specifically asked to; it just means that the expression isn't defined for this context
	                e.printStackTrace();
	            	if ( FAIL_MISSING_VALUES )
	                    filters.add(exp.name);                         
	            }
			}
		}
		
		if(vc.isIndel() && indelFilter != null) {
			for ( VariantContextUtils.JexlVCMatchExp exp : indelFilter ) {
				try {
	                if ( VariantContextUtils.match(vc, exp) )
	                    filters.add(exp.name);
	            } catch (Exception e) {
	                // do nothing unless specifically asked to; it just means that the expression isn't defined for this context
	                if ( FAIL_MISSING_VALUES )
	                    filters.add(exp.name);                         
	            }
			}
		}
		if ( filters.isEmpty() )
            builder.passFilters();
        else 
            builder.filters(filters);
        
		return builder.make();
	}
	
	public static void main(String[] args) throws IOException {
		HardFilter filter = new HardFilter("GaeaFilter", "MBASD>50.0 && MQRankSum > 0.5", "MQ>50.0");
		AsciiLineReader reader = new AsciiLineReader(new FileInputStream(new File("F:\\BGIBigData\\TestData\\VCF\\DNA1425995.vcf")));
		AsciiLineReaderIterator iterator = new AsciiLineReaderIterator(reader);
		VCFLocalWriter writer = new VCFLocalWriter("F:\\BGIBigData\\TestData\\VCF\\DNA1425995.filtered.vcf", false, false);
		VCFCodec codec = new VCFCodec();
		VCFHeader header = null;
		header = (VCFHeader) codec.readHeader(iterator).getHeaderValue();
		
		for(VCFHeaderLine headerLine : filter.filterHeaders())
			header.addMetaDataLine(headerLine);
		
		writer.writeHeader(header);
		while(iterator.hasNext()) {
			VariantContext vc = codec.decode(iterator.next());
			vc = filter.filter(vc);
			writer.add(vc);
		}
	}
}
