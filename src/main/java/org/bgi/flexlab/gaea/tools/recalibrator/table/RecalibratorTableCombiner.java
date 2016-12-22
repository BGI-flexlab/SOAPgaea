package org.bgi.flexlab.gaea.tools.recalibrator.table;

import org.bgi.flexlab.gaea.tools.mapreduce.realigner.RecalibratorOptions;
import org.bgi.flexlab.gaea.util.GaeaFilesReader;

import htsjdk.samtools.SAMFileHeader;

public class RecalibratorTableCombiner {
	private GaeaFilesReader reader = null;
	private SAMFileHeader header = null;
	
	public RecalibratorTableCombiner(RecalibratorOptions option,SAMFileHeader header){
		this.header = header;
		
	}
}
