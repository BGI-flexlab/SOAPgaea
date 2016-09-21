
package org.bgi.flexlab.gaea.util;


import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.bgi.flexlab.gaea.structure.header.VCFCompoundHeaderLine;
import org.bgi.flexlab.gaea.structure.header.VCFContigHeaderLine;
import org.bgi.flexlab.gaea.structure.header.VCFFilterHeaderLine;
import org.bgi.flexlab.gaea.structure.header.VCFHeader;
import org.bgi.flexlab.gaea.structure.header.VCFHeaderLine;
import org.bgi.flexlab.gaea.structure.header.VCFHeaderLineType;
import org.bgi.flexlab.gaea.structure.header.VCFIDHeaderLine;
import org.bgi.flexlab.gaea.utils.sam.VariantDataTracker;
import htsjdk.variant.variantcontext.VariantContext;

/**
 * A set of static utility methods for common operations on VCF files/records.
 */
public class VCFUtils {
    /**
     * Constructor access disallowed...static utility methods only!
     */
    private VCFUtils() { }

    public static Map<String, VCFHeader> getVCFHeadersFromRods(List<VariantDataTracker> Tracker) {
    	return getVCFHeadersFromRods(Tracker,(Collection<String>)null);
    }
    public static Map<String, VCFHeader> getVCFHeadersFromRods(List<VariantDataTracker> Tracker, Collection<String> rodNames) {
        Map<String, VCFHeader> data = new HashMap<String, VCFHeader>();

        // iterate to get all of the sample names
        //List<ReferenceOrderedDataSource> dataSources = toolkit.getRodDataSources();
        for ( VariantDataTracker source : Tracker ) {
            // ignore the rod if it's not in our list
            if ( rodNames != null && !rodNames.contains(source.getName()) ) {
                continue;
            }
            if ( source.getHeader() != null && source.getHeader() instanceof VCFHeader ) {
                data.put(source.getName(), (VCFHeader)source.getHeader());
            }
        }

        return data;
    }

   
   



    /** Only displays a warning if a logger is provided and an identical warning hasn't been already issued */
    private static final class HeaderConflictWarner {
        Logger logger;
        Set<String> alreadyIssued = new HashSet<String>();

        private HeaderConflictWarner(final Logger logger) {
            this.logger = logger;
        }

        public void warn(final VCFHeaderLine line, final String msg) {
            if ( logger != null && ! alreadyIssued.contains(line.getKey()) ) {
                alreadyIssued.add(line.getKey());
                logger.warn(msg);
            }
        }
    }

    public static Set<VCFHeaderLine> smartMergeHeaders(Collection<VCFHeader> headers, Logger logger) throws IllegalStateException {
        HashMap<String, VCFHeaderLine> map = new HashMap<String, VCFHeaderLine>(); // from KEY.NAME -> line
        HeaderConflictWarner conflictWarner = new HeaderConflictWarner(logger);

        // todo -- needs to remove all version headers from sources and add its own VCF version line
        for ( VCFHeader source : headers ) {
            //System.out.printf("Merging in header %s%n", source);
            for ( VCFHeaderLine line : source.getMetaDataInSortedOrder()) {

                String key = line.getKey();
                if ( line instanceof VCFIDHeaderLine ) {
                    key = key + "-" + ((VCFIDHeaderLine)line).getID();
                }
                if ( map.containsKey(key) ) {
                    VCFHeaderLine other = map.get(key);
                    if ( line.equals(other) ) {
                        // continue;
                    } else if ( ! line.getClass().equals(other.getClass()) ) {
                        throw new IllegalStateException("Incompatible header types: " + line + " " + other );
                    } else if ( line instanceof VCFFilterHeaderLine ) {
                        String lineName = ((VCFFilterHeaderLine) line).getID();
                        String otherName = ((VCFFilterHeaderLine) other).getID();
                        if ( ! lineName.equals(otherName) ) {
                            throw new IllegalStateException("Incompatible header types: " + line + " " + other );
                        }
                    } else if ( line instanceof VCFCompoundHeaderLine ) {
                        VCFCompoundHeaderLine compLine = (VCFCompoundHeaderLine)line;
                        VCFCompoundHeaderLine compOther = (VCFCompoundHeaderLine)other;

                        // if the names are the same, but the values are different, we need to quit
                        if (! (compLine).equalsExcludingDescription(compOther) ) {
                            if ( compLine.getType().equals(compOther.getType()) ) {
                                // The Number entry is an Integer that describes the number of values that can be
                                // included with the INFO field. For example, if the INFO field contains a single
                                // number, then this value should be 1. However, if the INFO field describes a pair
                                // of numbers, then this value should be 2 and so on. If the number of possible
                                // values varies, is unknown, or is unbounded, then this value should be '.'.
                                conflictWarner.warn(line, "Promoting header field Number to . due to number differences in header lines: " + line + " " + other);
                                compOther.setNumberToUnbounded();
                            } else if ( compLine.getType() == VCFHeaderLineType.Integer && compOther.getType() == VCFHeaderLineType.Float ) {
                                // promote key to Float
                                conflictWarner.warn(line, "Promoting Integer to Float in header: " + compOther);
                                map.put(key, compOther);
                            } else if ( compLine.getType() == VCFHeaderLineType.Float && compOther.getType() == VCFHeaderLineType.Integer ) {
                                // promote key to Float
                                conflictWarner.warn(line, "Promoting Integer to Float in header: " + compOther);
                            } else {
                                throw new IllegalStateException("Incompatible header types, collision between these two types: " + line + " " + other );
                            }
                        }
                        if ( ! compLine.getDescription().equals(compOther.getDescription()) ) {
                            conflictWarner.warn(line, "Allowing unequal description fields through: keeping " + compOther + " excluding " + compLine);
                        }
                    } else {
                        // we are not equal, but we're not anything special either
                        conflictWarner.warn(line, "Ignoring header line already in map: this header line = " + line + " already present header = " + other);
                    }
                } else {
                    map.put(key, line);
                    //System.out.printf("Adding header line %s%n", line);
                }
            }
        }

        return new HashSet<VCFHeaderLine>(map.values());
    }

    public static String rsIDOfFirstRealVariant(List<VariantContext> VCs, VariantContext.Type type) {
        if ( VCs == null ) {
            return null;
        }

        String rsID = null;
        for ( VariantContext vc : VCs ) {
            if ( vc.getType() == type ) {
                rsID = vc.getID();
                break;
            }
        }

        return rsID;
    }

    /**
     * Add / replace the contig header lines in the VCFHeader with the in the reference file and master reference dictionary
     *
     * @param oldHeader the header to update
     * @param referenceFile the file path to the reference sequence used to generate this vcf
     * @param refDict the SAM formatted reference sequence dictionary
     */
    public static VCFHeader withUpdatedContigs(final VCFHeader oldHeader, final File referenceFile, final SAMSequenceDictionary refDict) {
        return new VCFHeader(withUpdatedContigsAsLines(oldHeader.getMetaDataInInputOrder(), referenceFile, refDict), oldHeader.getGenotypeSamples());
    }

    public static Set<VCFHeaderLine> withUpdatedContigsAsLines(final Set<VCFHeaderLine> oldLines, final File referenceFile, final SAMSequenceDictionary refDict) {
        return withUpdatedContigsAsLines(oldLines, referenceFile, refDict, false);
    }

    public static Set<VCFHeaderLine> withUpdatedContigsAsLines(final Set<VCFHeaderLine> oldLines, final File referenceFile, final SAMSequenceDictionary refDict, boolean referenceNameOnly) {
        final Set<VCFHeaderLine> lines = new LinkedHashSet<VCFHeaderLine>(oldLines.size());

        for ( final VCFHeaderLine line : oldLines ) {
            if ( line instanceof VCFContigHeaderLine ) {
                continue; // skip old contig lines
            }
            if ( line.getKey().equals(VCFHeader.REFERENCE_KEY) ) {
                continue; // skip the old reference key
            }
            lines.add(line);
        }

        for ( final VCFHeaderLine contigLine : makeContigHeaderLines(refDict, referenceFile) ) {
            lines.add(contigLine);
        }
        String referenceValue;
        if (referenceFile != null) {
            if (referenceNameOnly) {
                referenceValue = FilenameUtils.getBaseName(referenceFile.getName());
            } else {
                referenceValue = "file://" + referenceFile.getAbsolutePath();
            }
            lines.add(new VCFHeaderLine(VCFHeader.REFERENCE_KEY, referenceValue));
        }
        return lines;
    }

    /**
     * Create VCFHeaderLines for each refDict entry, and optionally the assembly if referenceFile != null
     * @param refDict reference dictionary
     * @param referenceFile for assembly name.  May be null
     * @return list of vcf contig header lines
     */
    public static List<VCFContigHeaderLine> makeContigHeaderLines(final SAMSequenceDictionary refDict,
                                                                  final File referenceFile) {
        final List<VCFContigHeaderLine> lines = new ArrayList<VCFContigHeaderLine>();
        final String assembly = referenceFile != null ? getReferenceAssembly(referenceFile.getName()) : null;
        for ( SAMSequenceRecord contig : refDict.getSequences() ) {
            lines.add(makeContigHeaderLine(contig, assembly));
        }
        return lines;
    }

    private static VCFContigHeaderLine makeContigHeaderLine(final SAMSequenceRecord contig, final String assembly) {
        final Map<String, String> map = new LinkedHashMap<String, String>(3);
        map.put("ID", contig.getSequenceName());
        map.put("length", String.valueOf(contig.getSequenceLength()));
        if ( assembly != null ) {
        	map.put("assembly", assembly);
        }
        return new VCFContigHeaderLine(map, contig.getSequenceIndex());
    }

    private static String getReferenceAssembly(final String refPath) {
        // This doesn't need to be perfect as it's not a required VCF header line, but we might as well give it a shot
        String assembly = null;
        if (refPath.contains("b37") || refPath.contains("v37")) {
            assembly = "b37";
        } else if (refPath.contains("b36")) {
            assembly = "b36";
        } else if (refPath.contains("hg18")) {
            assembly = "hg18";
        } else if (refPath.contains("hg19")) {
            assembly = "hg19";
        }
        return assembly;
    }
    
    
    public static Set<VCFHeaderLine> getHeaderFields(List<VariantDataTracker> source ) {
        return getHeaderFields(source, null);
    }

    /**
     * Gets the header fields from all VCF rods input by the user
     *
     * @param toolkit    GATK engine
     * @param rodNames   names of rods to use, or null if we should use all possible ones
     *
     * @return a set of all fields
     */
    public static Set<VCFHeaderLine> getHeaderFields(List<VariantDataTracker> source, Collection<String> rodNames) {

        // keep a map of sample name to occurrences encountered
        TreeSet<VCFHeaderLine> fields = new TreeSet<VCFHeaderLine>();

        // iterate to get all of the sample names
        for ( VariantDataTracker sou : source ) {
            // ignore the rod if it's not in our list
            if ( rodNames != null && !rodNames.contains(sou.getName()) ) {
                continue;
            }
            if ( sou.getHeader() instanceof VCFHeader) {
                VCFHeader header = (VCFHeader)sou.getHeader();
                if ( header != null ) {
                    fields.addAll(header.getMetaDataInSortedOrder());
                }
            }
        }

        return fields;
    }

    
}