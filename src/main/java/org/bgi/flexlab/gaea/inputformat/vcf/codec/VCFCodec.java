package org.bgi.flexlab.gaea.inputformat.vcf.codec;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import htsjdk.tribble.TribbleException;
import htsjdk.tribble.readers.LineIterator;
import htsjdk.tribble.readers.LineReader;
import htsjdk.variant.variantcontext.VariantContext;
import org.bgi.flexlab.gaea.structure.header.VCFConstants;
import org.bgi.flexlab.gaea.structure.header.VCFHeader;
import org.bgi.flexlab.gaea.structure.header.VCFHeaderVersion;

/**
 * A feature codec for the VCF 4 specification
 *
 * <p>
 * VCF is a text file format (most likely stored in a compressed manner). It contains meta-information lines, a
 * header line, and then data lines each containing information about a position in the genome.
 * </p>
 * <p>One of the main uses of next-generation sequencing is to discover variation amongst large populations
 * of related samples. Recently the format for storing next-generation read alignments has been
 * standardised by the SAM/BAM file format specification. This has significantly improved the
 * interoperability of next-generation tools for alignment, visualisation, and variant calling.
 * We propose the Variant Call Format (VCF) as a standarised format for storing the most prevalent
 * types of sequence variation, including SNPs, indels and larger structural variants, together
 * with rich annotations. VCF is usually stored in a compressed manner and can be indexed for
 * fast data retrieval of variants from a range of positions on the reference genome.
 * The format was developed for the 1000 Genomes Project, and has also been adopted by other projects
 * such as UK10K, dbSNP, or the NHLBI Exome Project. VCFtools is a software suite that implements
 * various utilities for processing VCF files, including validation, merging and comparing,
 * and also provides a general Perl and Python API.
 * The VCF specification and VCFtools are available from http://vcftools.sourceforge.net.</p>
 *
 * <p>
 * See also: @see <a href="http://vcftools.sourceforge.net/specs.html">VCF specification</a><br>
 * See also: @see <a href="http://www.ncbi.nlm.nih.gov/pubmed/21653522">VCF spec. publication</a>
 * </p>
 *
 * <h2>File format example</h2>
 * <pre>
 *     ##fileformat=VCFv4.0
 *     #CHROM  POS     ID      REF     ALT     QUAL    FILTER  INFO    FORMAT  NA12878
 *     chr1    109     .       A       T       0       PASS  AC=1    GT:AD:DP:GL:GQ  0/1:610,327:308:-316.30,-95.47,-803.03:99
 *     chr1    147     .       C       A       0       PASS  AC=1    GT:AD:DP:GL:GQ  0/1:294,49:118:-57.87,-34.96,-338.46:99
 * </pre>
 *
 */
public class VCFCodec extends AbstractVCFCodec implements Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = 6709164217824212461L;
	// Our aim is to read in the records and convert to VariantContext as quickly as possible, relying on VariantContext to do the validation of any contradictory (or malformed) record parameters.
    public final static String VCF4_MAGIC_HEADER = "##fileformat=VCFv4";
    
    private boolean foundHeaderVersion = false;
    
    public void setHeader(VCFHeader header) {
 
    	this.header = header;
    }
    
    public VCFHeader getHeader() {
    	return header;
    }
    
    /**
     * @param reader the line reader to take header lines from
     * @return the number of header lines
     */
    public Object readHeader(LineReader reader) {
        List<String> headerStrings = new ArrayList<String>();
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                lineNo++;
                readLines(line, headerStrings);
            }
        } catch (IOException e) {
            throw new RuntimeException("IO Exception ", e);
        }
        if(this.header != null) {
        	return this.header;
        }
        throw new TribbleException.InvalidHeader("We never saw the required CHROM header line (starting with one #) for the input VCF file");
    }

    public Object readHeader(ArrayList<String> headerLine) {
        List<String> headerStrings = new ArrayList<String>();
        for ( String line : headerLine) {
            lineNo++;
            readLines(line, headerStrings);
        }
        if(this.header != null) {
        	return this.header;
        }
        throw new TribbleException.InvalidHeader("We never saw the required CHROM header line (starting with one #) for the input VCF file");
    }
    
    private void readLines(String line, List<String> headerStrings) {
    	if (line.startsWith(VCFHeader.METADATA_INDICATOR)) {
            String[] lineFields = line.substring(2).split("=");
            if (lineFields.length == 2 && VCFHeaderVersion.isFormatString(lineFields[0]) ) {
                if ( !VCFHeaderVersion.isVersionString(lineFields[1]) ) {
                    throw new TribbleException.InvalidHeader(lineFields[1] + " is not a supported version");
                }
                foundHeaderVersion = true;
                version = VCFHeaderVersion.toHeaderVersion(lineFields[1]);
                if ( version == VCFHeaderVersion.VCF3_3 || version == VCFHeaderVersion.VCF3_2 ) {
                    throw new TribbleException.InvalidHeader("This codec is strictly for VCFv4; please use the VCF3 codec for " + lineFields[1]);
                }
                if ( version != VCFHeaderVersion.VCF4_0 && version != VCFHeaderVersion.VCF4_1 ) {
                    throw new TribbleException.InvalidHeader("This codec is strictly for VCFv4 and does not support " + lineFields[1]);
                }
            }
            headerStrings.add(line);
        } else if (line.startsWith(VCFHeader.HEADER_INDICATOR)) {
            if (!foundHeaderVersion) {
                throw new TribbleException.InvalidHeader("We never saw a header line specifying VCF version");
            }
            headerStrings.add(line);
            super.parseHeaderFromLines(headerStrings, version);
        } else {
            throw new TribbleException.InvalidHeader("We never saw the required CHROM header line (starting with one #) for the input VCF file");
        }
        
    }
    /**
     * parse the filter string, first checking to see if we already have parsed it in a previous attempt
     *
     * @param filterString the string to parse
     * @return a set of the filters applied or null if filters were not applied to the record (e.g. as per the missing value in a VCF)
     */
    protected List<String> parseFilters(String filterString) {
        // null for unfiltered
        if ( filterString.equals(VCFConstants.UNFILTERED) ) {
            return null;
        }
        if ( filterString.equals(VCFConstants.PASSES_FILTERS_v4) ) {
            return Collections.emptyList();
        }
        if ( filterString.equals(VCFConstants.PASSES_FILTERS_v3) ) {
            generateException(VCFConstants.PASSES_FILTERS_v3 + " is an invalid filter name in vcf4", lineNo);
        }
        if ( filterString.length() == 0 ) {
            generateException("The VCF specification requires a valid filter status: filter was " + filterString, lineNo);
        }
        // do we have the filter string cached?
        if ( filterHash.containsKey(filterString) ) {
            return filterHash.get(filterString);
        }
        // empty set for passes filters
        List<String> fFields = new LinkedList<String>();
        // otherwise we have to parse and cache the value
        if ( filterString.indexOf(VCFConstants.FILTER_CODE_SEPARATOR) == -1 ) {
            fFields.add(filterString);
        } else {
            fFields.addAll(Arrays.asList(filterString.split(VCFConstants.FILTER_CODE_SEPARATOR)));
        }
        filterHash.put(filterString, Collections.unmodifiableList(fFields));

        return fFields;
    }

    @Override
    public boolean canDecode(final String potentialInput) {
        return canDecodeFile(potentialInput, VCF4_MAGIC_HEADER);
    }
    
    @SuppressWarnings("unused")
	public void VCFencode(String input)
    {
    	VCFHeader header=null;
    	ArrayList<String> headerLine=new ArrayList<String>();
    	File file=new File(input);
    	BufferedReader reader=null;
    	try{
    		reader=new BufferedReader(new FileReader(file));
    		String tempString=null;
    		
    		//read header
    		while((tempString=reader.readLine())!=null)
    		{
    			if(tempString.startsWith("#"))
    			{
    				headerLine.add(tempString.trim());
    			}else {
    				break;
    			}
    		}
    		header=(VCFHeader)readHeader(headerLine);
    		while((tempString=reader.readLine())!=null)
    		{
    			VariantContext var=this.decode(tempString);
    		}
    		reader.close();
    	}catch(IOException e)
    	{
    		e.printStackTrace();
    	}finally{
    		if(reader!=null)
    		{
    			try{
    				reader.close();
    			}catch(IOException e1)
    			{
    			}
    		}
    	}
    }
    

    
    public static void main(String[] args)
    {
    	VCFCodec codec=new VCFCodec();
    	codec.VCFencode("D:\\workspace\\Gaea\\data\\mm9.snp128.vcf");
    }

	@Override
	public Object readActualHeader(LineIterator arg0) {
		// TODO Auto-generated method stub
		return null;
	}
}
