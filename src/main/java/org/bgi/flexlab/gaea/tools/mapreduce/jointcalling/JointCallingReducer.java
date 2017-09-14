package org.bgi.flexlab.gaea.tools.mapreduce.jointcalling;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.bgi.flexlab.gaea.data.mapreduce.output.vcf.GaeaVCFOutputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.writable.WindowsBasedWritable;
import org.bgi.flexlab.gaea.data.structure.dbsnp.DbsnpShare;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocationParser;
import org.bgi.flexlab.gaea.data.structure.reference.ReferenceShare;
import org.bgi.flexlab.gaea.data.structure.reference.index.VcfIndex;
import org.bgi.flexlab.gaea.data.structure.vcf.VCFLocalLoader;
import org.bgi.flexlab.gaea.data.variant.filter.VariantRegionFilter;
import org.bgi.flexlab.gaea.tools.jointcalling.JointCallingEngine;
import org.bgi.flexlab.gaea.tools.jointcalling.util.MultipleVCFHeaderForJointCalling;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import org.seqdoop.hadoop_bam.util.VCFHeaderReader;
import org.seqdoop.hadoop_bam.util.WrapSeekable;

import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.variant.variantcontext.CommonInfo;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFContigHeaderLine;
import htsjdk.variant.vcf.VCFHeader;

public class JointCallingReducer
		extends Reducer<WindowsBasedWritable, VariantContextWritable, NullWritable, VariantContextWritable> {

	private int windowSize = 10000;
	private HashMap<Integer, String> contigs = null;
	private JointCallingEngine engine = null;
	private VariantContextWritable outValue = new VariantContextWritable();

	private JointCallingOptions options = null;
	private GenomeLocationParser parser = null;
	private ReferenceShare genomeShare = null;
	private DbsnpShare dbsnpShare = null;
	private VCFLocalLoader loader = null;
	private VariantRegionFilter filter = null;
	private VCFHeader header = null;
	private MultipleVCFHeaderForJointCalling headers = new MultipleVCFHeaderForJointCalling();

	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		contigs = new HashMap<Integer, String>();
		
		Path path = new Path(conf.get(GaeaVCFOutputFormat.OUT_PATH_PROP));
		SeekableStream in = WrapSeekable.openPath(path.getFileSystem(conf), path);
		header = VCFHeaderReader.readHeaderFrom(in);
		in.close();
		
		if(header == null)
			throw new RuntimeException("header is null !!!");
		
		List<VCFContigHeaderLine> lines = header.getContigLines();

		for (int i = 0; i < lines.size(); i++) {
			VCFContigHeaderLine line = lines.get(i);
			contigs.put(line.getContigIndex(), line.getID());
		}

		options = new JointCallingOptions();
		options.getOptionsFromHadoopConf(conf);
		
		windowSize = options.getWindowsSize();
		parser = new GenomeLocationParser(header.getSequenceDictionary());
		headers.readHeaders(conf);
		engine = new JointCallingEngine(options, parser,header,headers);
		genomeShare = new ReferenceShare();
		genomeShare.loadChromosomeList(options.getReference());
		dbsnpShare = new DbsnpShare(options.getDBSnp(), options.getReference());
		dbsnpShare.loadChromosomeList(options.getDBSnp() + VcfIndex.INDEX_SUFFIX);
		loader = new VCFLocalLoader(options.getDBSnp());
		filter = new VariantRegionFilter();
		header = engine.getVCFHeader();
		
		if(header == null)
			throw new RuntimeException("header is null!!!");
	}

	@Override
	public void reduce(WindowsBasedWritable key, Iterable<VariantContextWritable> values, Context context)
			throws IOException, InterruptedException {
		int winNum = key.getWindowsNumber();
		int start = winNum * windowSize;
		if(start == 0)
			start = 1;
		int end = start + windowSize - 1;
		String chr = contigs.get(key.getChromosomeIndex());
		
		long startPosition = dbsnpShare.getStartPosition(chr, winNum, options.getWindowsSize());
		ArrayList<VariantContext> dbsnps = null;
		if(startPosition >= 0)
			dbsnps = filter.loadFilter(loader, chr, startPosition, end);
		engine.init(dbsnps);
		
		for (int iter = start; iter <= end; iter++) {
			VariantContext variantContext = engine.variantCalling(values.iterator(),
					parser.createGenomeLocation(chr, iter), genomeShare.getChromosomeInfo(chr));
			if(variantContext == null)
				continue;
			CommonInfo info = variantContext.getCommonInfo();
			HashMap<String,Object> maps = new HashMap<String,Object>();
			maps.putAll(info.getAttributes());
			maps.remove("SM");
			info.setAttributes(maps);
			
			outValue.set(variantContext,header);
			context.write(NullWritable.get(), outValue);
		}
	}
}
