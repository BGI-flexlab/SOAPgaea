package org.bgi.flexlab.gaea.tools.mapreduce.realigner;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.bgi.flexlab.gaea.data.mapreduce.input.header.SamHdfsFileHeader;
import org.bgi.flexlab.gaea.data.mapreduce.writable.SamRecordWritable;
import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.PairEndAggregatorMapper;
import org.bgi.flexlab.gaea.tools.recalibrator.report.RecalibratorReport;

public class FixmateMapper extends PairEndAggregatorMapper {
	private final String DefaultReadGroup = "UK";
	private String RG;
	private SAMFileHeader header = null;
	private Text readID = new Text();
	private RealignerExtendOptions option = new RealignerExtendOptions();
	private RecalibratorReport report = null;
	private SamRecordWritable writable = new SamRecordWritable();

	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		header = SamHdfsFileHeader.getHeader(conf);
		option.getOptionsFromHadoopConf(conf);

		if (option.isRecalibration()) {
			String input = conf.get(Realigner.RECALIBRATOR_REPORT_TABLE_NAME);
			if (input == null)
				throw new RuntimeException("bqsr report table is null!!!");
			RecalibratorOptions bqsrOption = option.getBqsrOptions();
			report = new RecalibratorReport(input, header, bqsrOption.QUANTIZING_LEVELS,
					bqsrOption.PRESERVE_QSCORES_LESS_THAN);
		}
	}

	protected Writable getKey(Writable keyin, Writable valuein) {
		if(!option.isRealignment())
			return NullWritable.get();
		
		SAMRecord record = ((SamRecordWritable) valuein).get();
		GaeaSamRecord sam = new GaeaSamRecord(header, record);
		RG = (String) sam.getAttribute("RG");
		if (RG == null)
			RG = DefaultReadGroup;

		readID.set(RG + ":" + sam.getReadName());
		return readID;
	}

	protected Writable getValue(Writable value) {
		if (!option.isRecalibration())
			return value;

		if (value instanceof SamRecordWritable) {
			SamRecordWritable temp = (SamRecordWritable) value;

			GaeaSamRecord sam = new GaeaSamRecord(header, temp.get());
			report.readRecalibrator(sam);
			writable.set(sam);
			return writable;
		}

		return value;
	}
}
