package org.bgi.flexlab.gaea.tools.variantrecalibratioin;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.bgi.flexlab.gaea.data.exception.UserException;
import org.bgi.flexlab.gaea.data.mapreduce.util.HdfsFileManager;
import org.bgi.flexlab.gaea.data.structure.header.GaeaVCFHeader;
import org.bgi.flexlab.gaea.data.structure.header.MultipleVCFHeader;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocationParser;
import org.bgi.flexlab.gaea.tools.mapreduce.variantrecalibratioin.VariantRecalibration;
import org.bgi.flexlab.gaea.tools.mapreduce.variantrecalibratioin.VariantRecalibrationOptions;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.traindata.ResourceManager;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.traindata.VariantDatumMessenger;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.tranche.Tranche;
import org.seqdoop.hadoop_bam.VariantContextWritable;

import htsjdk.tribble.readers.AsciiLineReader;
import htsjdk.tribble.readers.AsciiLineReaderIterator;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFFilterHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLine;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFHeaderVersion;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import htsjdk.variant.vcf.VCFRecordCodec;
import htsjdk.variant.vcf.VCFStandardHeaderLines;

public class VCFRecalibrator {
	/**
	 * filter tranches
	 */
    final private List<Tranche> tranches = new ArrayList<Tranche>();
    
    /**
     * ignore filters of input vcf
     */
    final private Set<String> ignoreInputFilterSet = new TreeSet<String>();
    
    /**
     * options
     */
    private VariantRecalibrationOptions options;
    
    /**
     * recal table which store recal table data
     */
    private VCFRecalibrationTable recalTable;
    
    /**
     * headers which for get original vcf file
     */
    private MultipleVCFHeader headers;
    
    /**
     * hadoop job Configuration file
     */
    private Configuration conf;
    
    public static VCFHeader finalHeader;
    /**
     * construct function
     * @param conf
     * @throws IOException
     */
    public VCFRecalibrator(VariantRecalibrationOptions options, Configuration conf) throws IOException {
    	this.conf = conf;
        this.options = options;
        headers = (MultipleVCFHeader) GaeaVCFHeader.loadVcfHeader(false, this.conf);
        recalTable = new VCFRecalibrationTable(options);
        if (options.getIgnoreInputFilters() != null) {
            ignoreInputFilterSet.addAll(options.getIgnoreInputFilters());
        }
    }
    
    public void addData(VariantDatumMessenger msg) {
    	recalTable.addData(msg);
    }
    /**
     * recal one vcf file
     * @param id
     * @param values
     * @throws InterruptedException 
     * @throws IOException
     */
    public void recalVCF(int id, Context context) throws IOException, InterruptedException{
    	
    	recalTable.getRecalibrationTable();
    	recalTable.indexData();
    	for (final Tranche t : recalTable.getTranches()) {
            if (t.ts >= options.getTSFilterLevel()) {
                tranches.add(t);
            }
        }
    	// this algorithm wants the tranches ordered from best (lowest truth sensitivity) to worst (highest truth sensitivity)
        Collections.reverse(tranches); 
    }

    public VCFHeader addHeaderLine(VCFHeader header) {
    	for(VCFHeaderLine headerLine : getAddingHeader()) {
			header.addMetaDataLine(headerLine);
		}
    	finalHeader = header;
    	return header;
    }
    
    /**
     * headers for vqsr
     * @return
     */
    public Set<VCFHeaderLine> getAddingHeader() {
    	 // setup the header fields
        final Set<VCFHeaderLine> hInfo = new HashSet<VCFHeaderLine>();
        //hInfo.addAll(VCFUtils.getHeaderFields(getToolkit(), inputNames));
        addVQSRStandardHeaderLines(hInfo);

        //final TreeSet<String> samples = new TreeSet<String>();
        //samples.addAll(SampleUtils.getUniqueSamplesFromRods(getToolkit(), inputNames));

        if (tranches.size() >= 2) {
            for (int iii = 0; iii < (tranches.size() - 1); iii++) {
                final Tranche t = tranches.get(iii);
                hInfo.add(new VCFFilterHeaderLine(t.name,
                        String.format("Truth sensitivity tranche level for " + 
                        	t.model.toString() + " model at VQS Lod: " +
                            t.minVQSLod + " <= x < " +
                            tranches.get(iii + 1).minVQSLod)));
            }
        }

        if (tranches.size() >= 1) {
            hInfo.add(new VCFFilterHeaderLine(tranches.get(0).name + "+",
                    String.format("Truth sensitivity tranche level for " +
                        tranches.get(0).model.toString() +
                        " model at VQS Lod < " + tranches.get(0).minVQSLod)));
        } else {
            throw new UserException(
                "No tranches were found in the file or were above the truth sensitivity filter level " +
                options.getTSFilterLevel());
        }

        System.out.println("Keeping all variants in tranche " +
            tranches.get(tranches.size() - 1));
        
        return hInfo;
    }

    /**
     * sub func of add headers
     * @param hInfo
     */
    public static void addVQSRStandardHeaderLines(final Set<VCFHeaderLine> hInfo) {
        hInfo.add(VCFStandardHeaderLines.getInfoLine(VCFConstants.END_KEY));
        hInfo.add(new VCFInfoHeaderLine(VariantRecalibration.VQS_LOD_KEY, 1,
                VCFHeaderLineType.Float,
                "Log odds of being a true variant versus being false under the trained gaussian mixture model"));
        hInfo.add(new VCFInfoHeaderLine(VariantRecalibration.CULPRIT_KEY, 1,
                VCFHeaderLineType.String,
                "The annotation which was the worst performing in the Gaussian mixture model, likely the reason why the variant was filtered out"));
    }

    /**
     * recal vc
     * @param vc
     * @return
     * @throws IOException
     */
    public VariantContext applyRecalibration(VariantContext vc) throws IOException {
        if (vc == null) { // For some reason RodWalkers get map calls with null trackers
            return null;
        }

        final VariantContext recalDatum = recalTable.getData(vc.getChr(), vc.getStart(), vc.getEnd());

        if (ResourceManager.checkVariationClass(vc, options.getMode()) && (vc.isNotFiltered() ||
        ignoreInputFilterSet.containsAll(vc.getFilters()))) {
//        	final VariantContext recalDatum = getMatchingRecalVC(vc, recals);

        	if (recalDatum == null) {
        		throw new UserException(
                    "Encountered input variant which isn't found in the input recal file. Please make sure VariantRecalibration and ApplyRecalibration were run on the same set of input variants. First seen at: " + vc);
                }

        	final String lodString = recalDatum.getAttributeAsString(VariantRecalibration.VQS_LOD_KEY, null);

        	if (lodString == null) {
        		throw new UserException(
        			"Encountered a malformed record in the input recal file. There is no lod for the record at: " + vc);
                }

        	final double lod;

        	try {
        		lod = Double.valueOf(lodString);
        	} catch (NumberFormatException e) {
        		throw new UserException(
        			"Encountered a malformed record in the input recal file. The lod is unreadable for the record at: " + vc);
            }

        	VariantContextBuilder builder = new VariantContextBuilder(vc);
        	String filterString = null;

        	// Annotate the new record with its VQSLOD and the worst performing annotation
        	builder.attribute(VariantRecalibration.VQS_LOD_KEY, lod);
        	builder.attribute(VariantRecalibration.CULPRIT_KEY,
        	recalDatum.getAttribute(VariantRecalibration.CULPRIT_KEY));

        	for (int i = tranches.size() - 1; i >= 0; i--) {
        		final Tranche tranche = tranches.get(i);

        		if (lod >= tranche.minVQSLod) {
        			if (i == (tranches.size() - 1)) {
        				filterString = VCFConstants.PASSES_FILTERS_v4;
        			} else {
        				filterString = tranche.name;
        			}

        			break;
        		}
        	}

        	if (filterString == null) {
        		filterString = tranches.get(0).name + "+";
        	}

        	if (filterString.equals(VCFConstants.PASSES_FILTERS_v4)) {
        		builder.passFilters();
        	} else {
        		builder.filters(filterString);
        	}

            //vcfWriter.add(builder.make());
        	return builder.make();
        } else { // valid VC but not compatible with this mode, so just emit the variant untouched
        	//vcfWriter.add(vc);
        	return vc;
        }
    }

//    /**
//     * get right data from recal data
//     * @param target
//     * @param recalVCs
//     * @return
//     */
//    private static VariantContext getMatchingRecalVC( final VariantContext target, final List<VariantContext> recalVCs) {
//        if(recalVCs == null)
//        	System.err.println(target.toString());
//    	for (final VariantContext recalVC : recalVCs) {
//            if (target.getEnd() == recalVC.getEnd()) {
//                return recalVC;
//            }
//        }
//
//        return null;
//    }
  
}
