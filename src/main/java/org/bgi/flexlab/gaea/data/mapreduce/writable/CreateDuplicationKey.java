package org.bgi.flexlab.gaea.data.mapreduce.writable;

import org.bgi.flexlab.gaea.util.RandomUtils;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;

public class CreateDuplicationKey {
	private String LB;
	private int chrIndex;
	private int position;
	private boolean forward;
	
	private final static String UNKNOW_LIBARY = "UNKNOW_LIBARY";
	private final SAMFileHeader header; 
	
	public CreateDuplicationKey(SAMFileHeader header) {
		this.header = header;
	}
	
	/**
	 * get duplication key;
	 */
	public DuplicationKeyWritable getkey(SAMRecord sam) {
		sam.setHeader(header);
		sam.setDuplicateReadFlag(false);
		DuplicationKeyWritable key;
		LB = getLibrary(sam);
					
		//unmaped
		if(sam.getReadUnmappedFlag()) {
			chrIndex = -1;
			position = RandomUtils.getRandomGenerator().nextInt()%100;
			key = new DuplicationKeyWritable(LB, chrIndex, position, forward);
			return key;
		}
			
		//SE && single-mapped
		if(!sam.getReadPairedFlag() || sam.getMateUnmappedFlag()) {
			chrIndex = sam.getReferenceIndex();
			position = sam.getReadNegativeStrandFlag() ? sam.getUnclippedEnd() : sam.getUnclippedStart();
			forward = !sam.getReadNegativeStrandFlag();
		}
		
		//both mapped
		if(sam.getReadPairedFlag() && !sam.getMateUnmappedFlag()) {
			if(sam.getReferenceIndex() > sam.getMateReferenceIndex() || (sam.getReferenceIndex() == sam.getMateReferenceIndex() && sam.getAlignmentStart() >= sam.getMateAlignmentStart())) {
				chrIndex = sam.getReferenceIndex();
				position = sam.getReadNegativeStrandFlag() ? sam.getUnclippedEnd() : sam.getUnclippedStart();
				forward = !sam.getReadNegativeStrandFlag();
			} else {
				chrIndex = sam.getMateReferenceIndex();
				position = (Integer) (sam.getMateNegativeStrandFlag() ? sam.getAttribute("ME") : sam.getAttribute("MS"));
				forward = !sam.getMateNegativeStrandFlag();
			}
		}
		
		key = new DuplicationKeyWritable(LB, chrIndex, position, forward);
		return key;
	}
	
	/**
	 * get library ID from RG of SAM record
	 */
	private String getLibrary(SAMRecord sam) {
		String Library;
		String readgroup = (String)sam.getAttribute("RG");
		if (readgroup == null) {
			Library = UNKNOW_LIBARY;		
		} else {
			Library=header.getReadGroup(readgroup).getLibrary();
			if (Library == null) {
				Library = UNKNOW_LIBARY;
			}
		}
		return Library;
	}
}
