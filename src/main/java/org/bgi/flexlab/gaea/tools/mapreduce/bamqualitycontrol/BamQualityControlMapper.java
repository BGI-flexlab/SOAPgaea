package org.bgi.flexlab.gaea.tools.mapreduce.bamqualitycontrol;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bgi.flexlab.gaea.data.mapreduce.input.header.SamHdfsFileHeader;
import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.SamRecordDatum;
import org.bgi.flexlab.gaea.util.SamRecordUtils;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMReadGroupRecord;

public class BamQualityControlMapper extends Mapper<LongWritable, SAMRecordWritable, Text, Text>{
	/**
	 * FileHeader
	 */
	private SAMFileHeader mFileHeader=null;
	
	private String sampleName = "";
	
	private int unmappedReadsNum = 0;
	
	private int randomkey = RandomUtils.nextInt();
	
	private Text outK;
	
	private Text outV;
	
	private Map<String, Integer> rg2Index = new HashMap<String, Integer>();
	
	@Override
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		//header
		mFileHeader = SamHdfsFileHeader.getHeader(conf);
		assignIndexToReadGroup(mFileHeader);
	}

	@Override
	public void map(LongWritable key, SAMRecordWritable value,Context context) throws IOException, InterruptedException {
		SamRecordDatum datum = new SamRecordDatum();
		String rgID = SamRecordUtils.getReadGroup(value.get());
		GaeaSamRecord record = new GaeaSamRecord(mFileHeader, value.get());
		sampleName = mFileHeader.getReadGroup(rgID).getSample();
		if(datum.parseSAM(record)) {
			long winNum = -1;
			// read所在的窗口号（以read的起始点在参考基因组上的坐标划分窗口）
			winNum = datum.getPosition() / BamQualityControl.WINDOW_SIZE;
			// 输出<K, V>：Key格式为"chrName:winNum"，Value为比对结果文件的一行
			formatKeyValue(datum, rgID, winNum);
			context.write(outK, outV);
			// 当read跨越窗口时
			if (winNum != (datum.getEnd() / BamQualityControl.WINDOW_SIZE)) {
				winNum++; // 窗口号+1，将该read放入下一个窗口
				// 输出<K, V>：Key格式为"chrName:winNum"，Value为比对结果文件的一行
				formatKeyValue(datum, rgID, winNum);
				context.write(outK, outV);
			}
		} else {
			if(unmappedReadsNum > 10000) {
				randomkey = RandomUtils.nextInt();
				unmappedReadsNum = 0;
			}
			context.write(new Text(formatKey(":-1:-1:", randomkey)), new Text("1"));
			unmappedReadsNum++;
		}
	}
	

	private void formatKeyValue(SamRecordDatum datum, String rgID, long winNum) {
		outK = new Text(formatKey(datum.getChrName(), winNum));
		outV = new Text(formatValue(datum, rgID));
	}

	private String formatValue(SamRecordDatum datum, String rgID) {
		// TODO Auto-generated method stub
		StringBuilder vBuilder = new StringBuilder();
		vBuilder.append(datum.getFlag());
		vBuilder.append("\t");
		vBuilder.append(datum.getReadsSequence());
		vBuilder.append("\t");
		vBuilder.append(datum.getInsertSize());
		vBuilder.append("\t");
		vBuilder.append(datum.getPosition());
		vBuilder.append("\t");
		vBuilder.append(datum.getCigarString());
		vBuilder.append("\t");
		vBuilder.append(datum.getBestHitCount());
		vBuilder.append("\t");
		vBuilder.append("0");
		vBuilder.append("\t");
		vBuilder.append(datum.getMappingQual());
		vBuilder.append("\t");
		vBuilder.append(rg2Index.get(rgID));
		vBuilder.append("\t");
		vBuilder.append(datum.getQualityString());
		return vBuilder.toString();
	}
	
	private String formatKey(String chr, long winNum) {
		// TODO Auto-generated method stub
		StringBuilder kBuilder = new StringBuilder();
		kBuilder.append(sampleName);
		kBuilder.append(":");
		kBuilder.append(chr);
		kBuilder.append(":");
		kBuilder.append(winNum);
		return kBuilder.toString();
	}

	private void assignIndexToReadGroup(SAMFileHeader mFileHeader2) {
		// TODO Auto-generated method stub
		int rgIndex = 0;
		for(SAMReadGroupRecord rg : mFileHeader.getReadGroups()) {
			rg2Index.put(rg.getId(), rgIndex);
			rgIndex++;
		}
	}
}
