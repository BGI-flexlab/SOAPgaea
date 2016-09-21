package org.bgi.flexlab.gaea.outputformat.vcf;

import htsjdk.samtools.util.RuntimeEOFException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import htsjdk.tribble.TribbleException;
import htsjdk.tribble.util.ParsingUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.bgi.flexlab.gaea.exception.ReviewedStingException;
import org.bgi.flexlab.gaea.exception.UserException;
import org.bgi.flexlab.gaea.structure.header.VCFConstants;
import org.bgi.flexlab.gaea.structure.header.VCFFormatHeaderLine;
import org.bgi.flexlab.gaea.structure.header.VCFHeader;
import org.bgi.flexlab.gaea.structure.header.VCFHeaderLine;
import org.bgi.flexlab.gaea.structure.header.VCFHeaderLineCount;
import org.bgi.flexlab.gaea.structure.header.VCFHeaderVersion;
import org.bgi.flexlab.gaea.structure.header.VCFInfoHeaderLine;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;



public class VCFFileWriter implements VariantContextWriter {
	private final static String VERSION_LINE = VCFHeader.METADATA_INDICATOR
			+ VCFHeaderVersion.VCF4_1.getFormatString() + "="
			+ VCFHeaderVersion.VCF4_1.getVersionString();

	// should we write genotypes or just sites?
	final protected boolean doNotWriteGenotypes;

	protected BufferedWriter writer = null;
	// the VCF header we're storing
	protected VCFHeader mHeader = null;

	final private boolean allowMissingFieldsInHeader;

    protected String singleSample = null;

	private IntGenotypeFieldAccessors intGenotypeFieldAccessors = new IntGenotypeFieldAccessors();

	public VCFFileWriter(String filePath, boolean doNotWriteGenotypes,
			final boolean allowMissingFieldsInHeader) {
		this(filePath, doNotWriteGenotypes, allowMissingFieldsInHeader, null);
	}
	
	public VCFFileWriter(String filePath, boolean doNotWriteGenotypes,
			final boolean allowMissingFieldsInHeader, Configuration conf) {
		this.doNotWriteGenotypes = doNotWriteGenotypes;
		this.allowMissingFieldsInHeader = allowMissingFieldsInHeader;
		
		initWriter(filePath, conf);
    }
	
	private void initWriter(String filePath, Configuration conf) {
		if(conf != null) {
			FileSystem fs;
			try {
				Path vcfPath = new Path(filePath);
				fs = vcfPath.getFileSystem(conf);
				writer = new BufferedWriter(new OutputStreamWriter(fs.create(vcfPath)));
			} catch (IOException e) {
				throw new RuntimeEOFException(e);
			}
		} else {
			try {
				writer = new BufferedWriter(new FileWriter(new File(filePath)));
			} catch (IOException e) {
				throw new RuntimeEOFException(e);
			}
		}
	}

	// --------------------------------------------------------------------------------
	//
	// VCFWriter interface functions
	//
	// --------------------------------------------------------------------------------

	public void writeHeader(VCFHeader header) {

		// note we need to update the mHeader object after this call because
		// they header
		// may have genotypes trimmed out of it, if doNotWriteGenotypes is true
		try {
			mHeader = writeHeader(header, doNotWriteGenotypes, getVersionLine());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static final String getVersionLine() {
		return VERSION_LINE;
	}

	public VCFHeader writeHeader(VCFHeader header,
			final boolean doNotWriteGenotypes, final String versionLine)
			throws IOException {
		header = doNotWriteGenotypes ? new VCFHeader(
				header.getMetaDataInSortedOrder()) : header;

		// the file format field needs to be written first
		writer.write(versionLine + "\n");

		for (VCFHeaderLine line : header.getMetaDataInSortedOrder()) {
			if (VCFHeaderVersion.isFormatString(line.getKey())) {
				continue;
			}
			writer.write(VCFHeader.METADATA_INDICATOR);
			writer.write(line.toString());
			writer.write("\n");
		}

		// write out the column line
		writer.write(VCFHeader.HEADER_INDICATOR);
		boolean isFirst = true;
		for (VCFHeader.HEADER_FIELDS field : header.getHeaderFields()) {
			if (isFirst) {
				isFirst = false; // don't write out a field separator
			} else {
				writer.write(VCFConstants.FIELD_SEPARATOR);
			}
			writer.write(field.toString());
		}

		if (header.hasGenotypingData()) {

			writer.write(VCFConstants.FIELD_SEPARATOR);
			writer.write("FORMAT");
			for (String sample : header.getGenotypeSamples()) {
				writer.write(VCFConstants.FIELD_SEPARATOR);
				writer.write(sample);
			}
		}

		writer.write("\n");
		// writer.flush(); // necessary so that writing to an output stream will
		// work

		return header;
	}

	/**
	 * attempt to close the VCF file
	 */
	public void close() {
		try {
			writer.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * add a record to the file
	 *
	 * @param vc
	 *            the Variant Context object
	 */
	public void add(VariantContext vc) {
		if (mHeader == null) {
			throw new IllegalStateException(
					"The VCF Header must be written before records can be added: ");
		}
		if (doNotWriteGenotypes) {
			vc = new VariantContextBuilder(vc).noGenotypes().make();
		}
		try {
			// super.add(vc);

			Map<Allele, String> alleleMap = buildAlleleMap(vc);

			// CHROM
			writer.write(vc.getChr());
			writer.write(VCFConstants.FIELD_SEPARATOR);

			// POS
			writer.write(String.valueOf(vc.getStart()));
			writer.write(VCFConstants.FIELD_SEPARATOR);

			// ID
			String ID = vc.getID();
			writer.write(ID);
			writer.write(VCFConstants.FIELD_SEPARATOR);

			// REF
			String refString = vc.getReference().getDisplayString();
			writer.write(refString);
			writer.write(VCFConstants.FIELD_SEPARATOR);

			// ALT
			writeAlt(vc);

			// QUAL
			if (!vc.hasLog10PError()) {
				writer.write(VCFConstants.MISSING_VALUE_v4);
			} else {
				writer.write(formatQualValue(vc.getPhredScaledQual()));
			}
			writer.write(VCFConstants.FIELD_SEPARATOR);

			// FILTER
			String filters = getFilterString(vc);
			writer.write(filters);
			writer.write(VCFConstants.FIELD_SEPARATOR);
			// INFO
			Map<String, String> infoFields = getInfoFields(vc);
			writeInfoString(infoFields);
			// FORMAT
			// final GenotypesContext gc = vc.getGenotypes();

			List<String> genotypeAttributeKeys = calcVCFGenotypeKeys(vc,
					mHeader);
			writeFormat(vc, alleleMap);
			writer.write("\n");
			// writer.flush(); // necessary so that writing to an output stream
			// will work
		} catch (IOException e) {
			throw new RuntimeException("Unable to write the VCF object to ");
		}
	}

	private void writeAlt(VariantContext vc) throws IOException {
		if (vc.isVariant()) {
			Allele altAllele = vc.getAlternateAllele(0);
			String alt = altAllele.getDisplayString();
			writer.write(alt);

			for (int i = 1; i < vc.getAlternateAlleles().size(); i++) {
				altAllele = vc.getAlternateAllele(i);
				alt = altAllele.getDisplayString();
				writer.write(",");
				writer.write(alt);
			}
		} else {
			writer.write(VCFConstants.EMPTY_ALTERNATE_ALLELE_FIELD);
		}
		writer.write(VCFConstants.FIELD_SEPARATOR);
	}
	
	private Map<String, String> getInfoFields(VariantContext vc){
		Map<String, String> infoFields = new TreeMap<String, String>();
		for (Map.Entry<String, Object> field : vc.getAttributes().entrySet()) {
			String key = field.getKey();

			if (!mHeader.hasInfoLine(key)) {
				fieldIsMissingFromHeaderError(vc, key, "INFO");
			}
			String outputValue = formatVCFField(field.getValue());
			if (outputValue != null) {
				infoFields.put(key, outputValue);
			}
		}
		return infoFields;
	}
	
	private void writeFormat(VariantContext vc, Map<Allele, String> alleleMap) throws IOException{
		List<String> genotypeAttributeKeys = calcVCFGenotypeKeys(vc,
				mHeader);
		if (!genotypeAttributeKeys.isEmpty()) {
			for (final String format : genotypeAttributeKeys) {
				if (!mHeader.hasFormatLine(format)) {
					fieldIsMissingFromHeaderError(vc, format, "FORMAT");
				}
			}
			final String genotypeFormatString = ParsingUtils.join(
					VCFConstants.GENOTYPE_FIELD_SEPARATOR,
					genotypeAttributeKeys);

			writer.write(VCFConstants.FIELD_SEPARATOR);
			writer.write(genotypeFormatString);

			addGenotypeData(vc, alleleMap, genotypeAttributeKeys, singleSample);
		}
	}
	
	private static Map<Allele, String> buildAlleleMap(final VariantContext vc) {
		final Map<Allele, String> alleleMap = new HashMap<Allele, String>(vc
				.getAlleles().size() + 1);
		alleleMap.put(Allele.NO_CALL, VCFConstants.EMPTY_ALLELE); // convenience
																	// for
																	// lookup

		final List<Allele> alleles = vc.getAlleles();
		for (int i = 0; i < alleles.size(); i++) {
			alleleMap.put(alleles.get(i), String.valueOf(i));
		}

		return alleleMap;
	}

	// --------------------------------------------------------------------------------
	//
	// implementation functions
	//
	// --------------------------------------------------------------------------------

	private final String getFilterString(final VariantContext vc) {
		if (vc.isFiltered()) {
			for (final String filter : vc.getFilters()) {
				if (!mHeader.hasFilterLine(filter)) {
					fieldIsMissingFromHeaderError(vc, filter, "FILTER");
				}
			}
			return ParsingUtils.join(";",
					ParsingUtils.sortList(vc.getFilters()));
		} else if (vc.filtersWereApplied()) {
			return VCFConstants.PASSES_FILTERS_v4;
		} else {
			return VCFConstants.UNFILTERED;
		}
	}

	private static final String QUAL_FORMAT_STRING = "%.2f";
	private static final String QUAL_FORMAT_EXTENSION_TO_TRIM = ".00";

	private String formatQualValue(double qual) {
		String s = String.format(QUAL_FORMAT_STRING, qual);
		if (s.endsWith(QUAL_FORMAT_EXTENSION_TO_TRIM)) {
			s = s.substring(0, s.length() - QUAL_FORMAT_EXTENSION_TO_TRIM.length());
		}
		return s;
	}

	/**
	 * create the info string; assumes that no values are null
	 *
	 * @param infoFields
	 *            a map of info fields
	 * @throws IOException
	 *             for writer
	 */
	private void writeInfoString(Map<String, String> infoFields)
			throws IOException {
		if (infoFields.isEmpty()) {
			writer.write(VCFConstants.EMPTY_INFO_FIELD);
			return;
		}

		boolean isFirst = true;
		for (Map.Entry<String, String> entry : infoFields.entrySet()) {
			if (isFirst) {
				isFirst = false;
			} else {
				writer.write(VCFConstants.INFO_FIELD_SEPARATOR);
			}
			String key = entry.getKey();
			writer.write(key);

			if (!entry.getValue().equals("")) {
				VCFInfoHeaderLine metaData = mHeader.getInfoHeaderLine(key);
				boolean isValidMetaData = (metaData == null
						|| metaData.getCountType() != VCFHeaderLineCount.INTEGER
						|| metaData.getCount() != 0);
				if (isValidMetaData) {
					writer.write("=");
					writer.write(entry.getValue());
				}
			}
		}
	}

	/**
	 * add the genotype data
	 *
	 * @param vc
	 *            the variant context
	 * @param genotypeFormatKeys
	 *            Genotype formatting string
	 * @param alleleMap
	 *            alleles for this context
	 * @throws IOException
	 *             for writer
	 */

	private void addGenotypeData(VariantContext vc, Map<Allele, String> alleleMap, 
			List<String> genotypeFormatKeys, String singleSample) throws IOException {
		final int ploidy = vc.getMaxPloidy(2);
    	
    	if(singleSample != null) {
            writeGenotypeData(vc, ploidy, singleSample, alleleMap, genotypeFormatKeys);
    	} else {
	        for ( String sample : mHeader.getGenotypeSamples() ) {	
	            writeGenotypeData(vc, ploidy, sample, alleleMap, genotypeFormatKeys);
	        }
    	}
	}
	
	private void writeGenotypeData(VariantContext vc, int ploidy, String sample, Map<Allele, String> alleleMap, 
			List<String> genotypeFormatKeys) throws IOException {
		writer.write(VCFConstants.FIELD_SEPARATOR);

		Genotype g = vc.getGenotype(sample);
		if ( g == null) {
        	g = GenotypeBuilder.createMissing(singleSample, ploidy);
		}
		final List<String> attrs = new ArrayList<String>(genotypeFormatKeys.size());
		for (String field : genotypeFormatKeys) {
			if (field.equals(VCFConstants.GENOTYPE_KEY)) {
				if (!g.isAvailable()) {
					throw new ReviewedStingException(
							"GTs cannot be missing for some samples if they are available for others in the record");
				}

				writeAllele(g.getAllele(0), alleleMap);
				for (int i = 1; i < g.getPloidy(); i++) {
					writer.write(g.isPhased() ? VCFConstants.PHASED
							: VCFConstants.UNPHASED);
					writeAllele(g.getAllele(i), alleleMap);
				}

				continue;
			} else {
				String outputValue = getOutputValue(field, g, vc);

				if (outputValue != null)
					attrs.add(outputValue);
			}
		}

		// strip off trailing missing values
		for (int i = attrs.size() - 1; i >= 0; i--) {
			if (isMissingValue(attrs.get(i))) {
				attrs.remove(i);
			} else {
				break;
			}
		}

		for (int i = 0; i < attrs.size(); i++) {
			if (i > 0 || genotypeFormatKeys.contains(VCFConstants.GENOTYPE_KEY)) {
				writer.write(VCFConstants.GENOTYPE_FIELD_SEPARATOR);
			}
			writer.write(attrs.get(i));
		}
	}
        

	private String getOutputValue(String field, Genotype g, VariantContext vc){
		String outputValue;
		if (field.equals(VCFConstants.GENOTYPE_FILTER_KEY)) {
			outputValue = g.isFiltered() ? g.getFilters()
					: VCFConstants.PASSES_FILTERS_v4;
		} else {
			final IntGenotypeFieldAccessors.Accessor accessor = intGenotypeFieldAccessors.getAccessor(field);
			if (accessor != null) {
				final int[] intValues = accessor.getValues(g);
				if (intValues == null) {
					outputValue = VCFConstants.MISSING_VALUE_v4;
				} else if (intValues.length == 1) {// fast path
					outputValue = Integer.toString(intValues[0]);
				} else {
					StringBuilder sb = new StringBuilder();
					sb.append(intValues[0]);
					for (int i = 1; i < intValues.length; i++) {
						sb.append(",");
						sb.append(intValues[i]);
					}
					outputValue = sb.toString();
				}
			} else {
				// assume that if key is absent, then the given
				// string encoding suffices
				outputValue = formatVCFField(getVCFField(g, field, vc));
			}
		}
		
		return outputValue;
	}
	
	private Object getVCFField(Genotype g, String field, VariantContext vc){
		Object val = g.hasExtendedAttribute(field) ? g
				.getExtendedAttribute(field)
				: VCFConstants.MISSING_VALUE_v4;

		VCFFormatHeaderLine metaData = mHeader.getFormatHeaderLine(field);
		if (metaData != null) {
			int numInFormatField = metaData.getCount(vc);
			if (numInFormatField > 1
					&& val.equals(VCFConstants.MISSING_VALUE_v4)) {
				// If we have a missing field but multiple
				// values are expected, we need to construct
				// a new string with all fields.
				// For example, if Number=2, the string has
				// to be ".,."
				StringBuilder sb = new StringBuilder(
						VCFConstants.MISSING_VALUE_v4);
				for (int i = 1; i < numInFormatField; i++) {
					sb.append(",");
					sb.append(VCFConstants.MISSING_VALUE_v4);
				}
				val = sb.toString();
			}
		}
		
		return val;
	}
	
	public static final void missingSampleError(final VariantContext vc,
			final VCFHeader header) {
		final List<String> badSampleNames = new ArrayList<String>();
		for (final String x : header.getGenotypeSamples())
			if (!vc.hasGenotype(x)) {
				badSampleNames.add(x);
			}
		throw new ReviewedStingException(
				"BUG: we now require all samples in VCFheader to have genotype objects.  Missing samples are "
						+ ParsingUtils.join(",", badSampleNames));
	}

	private boolean isMissingValue(String s) {
		// we need to deal with the case that it's a list of missing values
		return (countOccurrences(VCFConstants.MISSING_VALUE_v4.charAt(0), s)
				+ countOccurrences(',', s) == s.length());
	}

	private void writeAllele(Allele allele, Map<Allele, String> alleleMap)
			throws IOException {
		String encoding = alleleMap.get(allele);
		if (encoding == null) {
			throw new TribbleException.InternalCodecException("Allele "
					+ allele + " is not an allele in the variant context");
		}
		writer.write(encoding);
	}

	/**
	 * Takes a double value and pretty prints it to a String for display
	 *
	 * Large doubles => gets %.2f style formatting Doubles < 1 / 10 but > 1/100
	 * </>=> get %.3f style formatting Double < 1/100 => %.3e formatting
	 * 
	 * @param d
	 * @return
	 */
	public static final String formatVCFDouble(final double d) {
		String format;
		if (d < 1) {
			if (d < 0.01) {
				if (Math.abs(d) >= 1e-20)
					format = "%.3e";
				else {
					// return a zero format
					return "0.00";
				}
			} else {
				format = "%.3f";
			}
		} else {
			format = "%.2f";
		}

		return String.format(format, d);
	}

	public static String formatVCFField(Object val) {
		String result;
		if (val == null) {
			result = VCFConstants.MISSING_VALUE_v4;
		} else if (val instanceof Double) {
			result = formatVCFDouble((Double) val);
		} else if (val instanceof Boolean) {
			result = (Boolean) val ? "" : null; // empty string for true, null
		} // for false
		else if (val instanceof List) {
			result = formatVCFField(((List<?>) val).toArray());
		} else if (val.getClass().isArray()) {
			int length = Array.getLength(val);
			if (length == 0) {
				return formatVCFField(null);
			}
			StringBuffer sb = new StringBuffer(formatVCFField(Array.get(val, 0)));
			for (int i = 1; i < length; i++) {
				sb.append(",");
				sb.append(formatVCFField(Array.get(val, i)));
			}
			result = sb.toString();
		} else {
			result = val.toString();
		}
		return result;
	}

	/**
	 * Determine which genotype fields are in use in the genotypes in VC
	 * 
	 * @param vc
	 * @return an ordered list of genotype fields in use in VC. If vc has
	 *         genotypes this will always include GT first
	 */
	public static List<String> calcVCFGenotypeKeys(final VariantContext vc,
			final VCFHeader header) {
		Set<String> keys = new HashSet<String>();

		boolean sawGoodGT = false;
		boolean sawGoodQual = false;
		boolean sawGenotypeFilter = false;
		boolean sawDP = false;
		boolean sawAD = false;
		boolean sawPL = false;
		for (final Genotype g : vc.getGenotypes()) {
			keys.addAll(g.getExtendedAttributes().keySet());
			if (g.isAvailable()) {
				sawGoodGT = true;
			}
			if (g.hasGQ()) {
				sawGoodQual = true;
			}
			if (g.hasDP()) {
				sawDP = true;
			} 
			if (g.hasAD()) {
				sawAD = true;
			} 
			if (g.hasPL()) {
				sawPL = true;
			}
			if (g.isFiltered()) {
				sawGenotypeFilter = true;
			}
		}

		if (sawGoodQual) {
			keys.add(VCFConstants.GENOTYPE_QUALITY_KEY);
		}
		if (sawDP) {
			keys.add(VCFConstants.DEPTH_KEY);
		}
		if (sawAD) {
			keys.add(VCFConstants.GENOTYPE_ALLELE_DEPTHS);
		}
		if (sawPL) {
			keys.add(VCFConstants.GENOTYPE_PL_KEY);
		}
		if (sawGenotypeFilter) {
			keys.add(VCFConstants.GENOTYPE_FILTER_KEY);
		}
		List<String> sortedList = ParsingUtils.sortList(new ArrayList<String>(
				keys));

		// make sure the GT is first
		if (sawGoodGT) {
			List<String> newList = new ArrayList<String>(sortedList.size() + 1);
			newList.add(VCFConstants.GENOTYPE_KEY);
			newList.addAll(sortedList);
			sortedList = newList;
		}

		if (sortedList.isEmpty() && header.hasGenotypingData()) {
			// this needs to be done in case all samples are no-calls
			return Collections.singletonList(VCFConstants.GENOTYPE_KEY);
		} else {
			return sortedList;
		}
	}

	private static int countOccurrences(char c, String s) {
		int count = 0;
		for (int i = 0; i < s.length(); i++) {
			count += s.charAt(i) == c ? 1 : 0;
		}
		return count;
	}

	private final void fieldIsMissingFromHeaderError(final VariantContext vc,
			final String id, final String field) {
		if (!allowMissingFieldsInHeader) {
			throw new UserException.MalformedVCFHeader(
					"Key "
							+ id
							+ " found in VariantContext field "
							+ field
							+ " at "
							+ vc.getChr()
							+ ":"
							+ vc.getStart()
							+ " but this key isn't defined in the VCFHeader.  The GATK now requires all VCFs to have"
							+ " complete VCF headers by default.  This error can be disabled with the engine argument"
							+ " -U LENIENT_VCF_PROCESSING");
		}
	}

	public VCFHeader getHeader() {
		return mHeader;
	}

	public void setHeader(VCFHeader mHeader) {
		this.mHeader = mHeader;
	}
	
	/**
	 * @return the singleSample
	 */
	public String getSingleSample() {
		return singleSample;
	}

	/**
	 * @param singleSample the singleSample to set
	 */
	public void setSingleSample(String singleSample) {
		this.singleSample = singleSample;
	}
}
