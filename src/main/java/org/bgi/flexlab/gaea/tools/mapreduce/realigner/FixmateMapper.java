package org.bgi.flexlab.gaea.tools.mapreduce.realigner;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.bgi.flexlab.gaea.data.mapreduce.input.header.SamHdfsFileHeader;
import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.PairEndAggregatorMapper;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

public class FixmateMapper extends PairEndAggregatorMapper {
	private final String DefaultReadGroup = "UK";
	private String RG;
	private SAMFileHeader header = null;
	private Text readID = new Text();

	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		header = SamHdfsFileHeader.getHeader(conf);
	}

	protected Writable getKey(Writable keyin, Writable valuein) {
		SAMRecord record = ((SAMRecordWritable) valuein).get();
		GaeaSamRecord sam = new GaeaSamRecord(header, record);
		RG = (String) sam.getAttribute("RG");
		if (RG == null)
			RG = DefaultReadGroup;

		readID.set(RG + ":" + sam.getReadName());
		return readID;
	}
}
