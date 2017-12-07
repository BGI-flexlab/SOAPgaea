package org.bgi.flexlab.gaea.tools.mapreduce.callsv;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.bgi.flexlab.gaea.data.mapreduce.writable.SamRecordWritable;
import org.bgi.flexlab.gaea.tools.callsv.Format;
import org.bgi.flexlab.gaea.tools.callsv.NewMapKey;

import htsjdk.samtools.SAMRecord;

/**
 * 这是第一个MapReducer中的Mapper类 <br>
 * 主要是用于读输入的bam文件，并做以下事情：<br>
 * 1.每个分区取100个正常的insert size值来计算upper和lower；<br>
 * 2.将异常的reads信息输出给reducer；<br>
 * @author xiaohui
 * @version v2.0
 */

public class CallStructuralVariationMapper1 extends Mapper<LongWritable, SamRecordWritable, NewMapKey, Format>{
	
	private Configuration conf;
	private FSDataOutputStream out;
	private CallStructuralVariationOptions options = new CallStructuralVariationOptions();
	private NewMapKey newkey = new NewMapKey();
	private Format f = new Format();
	private Map<String, Integer> chrreadnum = new TreeMap<String, Integer>();
	

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		options.getOptionsFromHadoopConf(conf);
		String libpath = options.getHdfsdir() + "/MP1/LibConf/" + UUID.randomUUID().toString();
		out = FileSystem.get(conf).create(new Path(libpath));
		
	}
	
	@Override
	protected void map(LongWritable key, SamRecordWritable value, Context context) throws IOException, InterruptedException {
		SAMRecord record = value.get();
		saveInsert(record); //save insert
		readClassify(context, record);	//classify all reads
		
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		out.flush();
		out.close();
		conf = null;
		options = null;
		newkey = null;
		f = null;
	}

	/**
	 * readClassify方法<br>
	 * 用于判断reads是否是异常reads（APRs），如果是，根据flag值判断属于哪一种类型；<br>
	 * 将reads信息保存到Format对象中，context.write到reducer中<br>
	 * @param context 上下文
	 * @param record bam文件中每一个记录，也就是每一条read的比对情况
	 * @throws IOException 抛出IO异常
	 * @throws InterruptedException 抛出中断异常
	 */
	private void readClassify(Context context, SAMRecord record) throws IOException, InterruptedException {
		String type = null;
		int start1 = record.getAlignmentStart();
		int start2 = record.getMateAlignmentStart();
		
		if(record.getMappingQuality() <= options.getMinqual()) //low quality
			return;
		else if(record.getDuplicateReadFlag())
		//else if(record.getDuplicateReadFlag() || record.getNotPrimaryAlignmentFlag()) //重复
			return;
		else if(record.getReadPairedFlag()) { //pair read
			if(record.getReadUnmappedFlag()) //unmap
				return;
			else if(record.getMateUnmappedFlag())  //mate unmap
				return;
			else if(Math.abs(record.getInferredInsertSize()) >= options.getMaxsvsize()) //too long sv size
				return;
			else if(!(record.getMateReferenceName().equals("=") || record.getMateReferenceName().equals(record.getReferenceName())))
				type = "Diff";
			else if(record.getReadNegativeStrandFlag() && record.getMateNegativeStrandFlag()) //--
				type = "FF_RR";
			else if(!record.getReadNegativeStrandFlag() && !record.getMateNegativeStrandFlag()) //++
				type = "FF_RR";
			else {
				
				if(start1 < start2) {
					if(record.getReadNegativeStrandFlag())
						type = "RF";
					else
						type = "FR";
				}else if(start1 >= start2){
					if(record.getReadNegativeStrandFlag())
						type = "FR";
					else
						type = "RF";
				}else
					return;
			}
		}
		
		if(!type.equals(null)) {
			f.set(record, type);
			newkey.setChr(record.getReferenceName());
			newkey.setPos(record.getAlignmentStart());
			context.write(newkey, f);
		}
	}

	/**
	 * saveInsert方法<br>
	 * 将正常的reads的insert size保存到中间文件中，中间文件名:/HDFSdir/MP1/LibConf/UUID <br>
	 * 一般一个map分片会选取100个正常的insert size<br>
	 * <br>
	 * @param record bam文件中每一个记录，也就是每一条read的比对情况
	 * @throws IOException 抛出IO异常
	 */
	private void saveInsert(SAMRecord record) throws IOException {
		
		String chr = record.getReferenceName();
		
		if(chrreadnum.containsKey(chr) && chrreadnum.get(chr) >= 200)
			return;

		//else if(record.getAlignmentStart() <= record.getMateAlignmentStart() && record.getMateAlignmentStart() <= record.getAlignmentEnd())
		//	return;
		
		if(record.getProperPairFlag() && record.getReadPairedFlag() &&
				record.getInferredInsertSize() > 0 ) {
			String writer = record.getReadGroup().getLibrary() + "\t" + chr + "\t" + record.getInferredInsertSize() + "\n";
			out.write(writer.getBytes());
			out.flush();
			
			int num = 1;
			if(chrreadnum.containsKey(chr)) 
				num = chrreadnum.get(chr) + 1;	
			chrreadnum.put(chr, num);
		}
		
		
	}

}
