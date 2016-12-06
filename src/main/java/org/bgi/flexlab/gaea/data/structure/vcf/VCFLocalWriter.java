package org.bgi.flexlab.gaea.data.structure.vcf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.bgi.flexlab.gaea.data.mapreduce.input.vcf.VCFHdfsLoader;

import htsjdk.samtools.util.BlockCompressedOutputStream;
import htsjdk.samtools.util.IOUtil;
import htsjdk.samtools.util.RuntimeEOFException;
import htsjdk.tribble.AbstractFeatureReader;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder.OutputType;

public class VCFLocalWriter extends VCFFileWriter {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 3915150472678717360L;
	
	public enum OutputType {
		VCF,
		BLOCK_COMPRESSED_VCF
	}
	
	public final EnumSet<OutputType> FILE_TYPES = EnumSet.allOf(OutputType.class);
	
	private OutputType outputType;
	
	public VCFLocalWriter(String filePath, boolean doNotWriteGenotypes,
			final boolean allowMissingFieldsInHeader) throws IOException {
		super(filePath, doNotWriteGenotypes, allowMissingFieldsInHeader, null);
		this.outputType = determineOutputTypeFromFile(filePath);
	}
/**
 * htsjdk for now only support BlockCompressedOutputStream for local file system rather than hdfs.
 */
	@Override
	public void initOutputStream(String filePath, Configuration conf) {
		// TODO Auto-generated method stub
		
		OutputType typeTobuild = this.outputType;
		try {
			switch (typeTobuild) {
			case VCF:
				os = new FileOutputStream(new File(filePath));
				break;
			case BLOCK_COMPRESSED_VCF:
				os = new BlockCompressedOutputStream(new File(filePath));
			}
		} catch (IOException e) {
			throw new RuntimeEOFException(e);
		}
	}
	
    public static OutputType determineOutputTypeFromFile(final String f) {
        if (isCompressedVCF(f)) {
            return OutputType.BLOCK_COMPRESSED_VCF;
        } else {
            return OutputType.VCF;
        }
    }
    
    private static boolean isCompressedVCF(final String outFile) {
        if (outFile == null)
            return false;

        return VCFHdfsLoader.hasBlockCompressedExtension(outFile);
    }
}
