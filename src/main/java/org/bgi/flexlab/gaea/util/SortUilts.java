package org.bgi.flexlab.gaea.util;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.bgi.flexlab.gaea.data.structure.header.MultipleVCFHeader;
import org.bgi.flexlab.gaea.data.structure.header.SingleVCFHeader;
import org.bgi.flexlab.gaea.data.structure.reference.ReferenceShare;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.BioJob;
import org.bgi.flexlab.gaea.tools.mapreduce.vcf.sort.VCFSortOptions;

import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.VCFHeader;

public class SortUilts {
	
	public static void configureSampling(Path outPath, BioJob job, VCFSortOptions options) throws IOException{
    	Configuration conf = job.getConfiguration();
		final Path partition = outPath.getFileSystem(conf).makeQualified(new Path(outPath, "_partitioning" + "VCF"));
    	
    	TotalOrderPartitioner.setPartitionFile(conf, partition);
    	try {
    		final URI partitionURI = new URI(partition.toString() + "#" + partition.getName());
    		
    		if(partitionURI.getScheme().equals("file"))
    			return;
    		
    		ReferenceShare.distributeCache(partitionURI.toString(), job);
    	} catch (URISyntaxException e) { throw new RuntimeException(e); }
    	
	}
	
	public static void merge(MultipleVCFHeader mVcfHeader, VCFSortOptions options, Configuration conf) {
		try {
			System.out.println("vcf-MultiSampleSort :: Merging output...");

			// First, place the VCF or BCF header.
            final Path outpath = new Path(options.getOutputPath());
			final Path wrkPath = new Path(options.getWorkPath());
			final FileSystem srcFS = wrkPath.getFileSystem(conf);
			final FileSystem dstFS = outpath.getFileSystem(conf);

			Map<String, OutputStream> outs = new HashMap<String, OutputStream>();
			Map<Integer, String> multiOutputs = options.getMultiOutputs();
			for(String result : multiOutputs.values()){
				Path sPath = new Path(options.getOutputPath() + "/" + result + ".vcf");
				OutputStream os = dstFS.create(sPath);
				outs.put(result, os);
			}
			
		    final VariantContextWriterBuilder builder = new VariantContextWriterBuilder();
			VariantContextWriter writer;
			Map<Integer, SingleVCFHeader> id2VcfHeader = mVcfHeader.getID2SingleVcfHeader();
            for( int id : multiOutputs.keySet()){
            	VCFHeader newHeader = id2VcfHeader.get(id).getHeader();
            	writer = builder.setOutputStream(
            			new FilterOutputStream(outs.get(multiOutputs.get(id))) {
            				@Override public void close() throws IOException {
            					this.out.flush();
            				}
            			}).setOptions(VariantContextWriterBuilder.NO_OPTIONS).build();
            	
            	writer.writeHeader(newHeader);
            	writer.close();
            	
            	final FileStatus[] parts = srcFS.globStatus(new Path(options.getWorkPath(), multiOutputs.get(id) + "-*-[0-9][0-9][0-9][0-9][0-9]*"));
            	
            	int i = 0;
            	
            	for(final FileStatus part : parts){
            		System.out.printf("sort:: Merging part %d ( size %d)...\n", i++, part.getLen());
            		System.out.flush();
            		
            		final FSDataInputStream ins = srcFS.open(part.getPath());
            		IOUtils.copyBytes(ins, outs.get(multiOutputs.get(id)), conf, false);
            		ins.close();            		
            	}
               for (final FileStatus part : parts)
					srcFS.delete(part.getPath(), false);  			

			    outs.get(multiOutputs.get(id)).close();

            }
		} catch (IOException e) {
			System.err.printf("vcf-MultiSampleSort :: Output merging failed: %s\n", e);
		}
	}

}
