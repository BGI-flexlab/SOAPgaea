package org.bgi.flexlab.gaea.tools.mapreduce.callsv;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.bgi.flexlab.gaea.data.mapreduce.writable.SamRecordWritable;
import org.bgi.flexlab.gaea.tools.callsv.Format;
import org.bgi.flexlab.gaea.tools.callsv.NewMapKey;
import org.seqdoop.hadoop_bam.FileVirtualSplit;

import htsjdk.samtools.SAMRecord;

public class CallStructuralVariationMapper extends Mapper<LongWritable, SamRecordWritable, NewMapKey, Format>{

	private Configuration conf;
	private FSDataOutputStream out;
	private CallStructuralVariationOptions option = new CallStructuralVariationOptions();
	private NewMapKey newkey = new NewMapKey();
	private Format f = new Format();
	private Map<Integer, Integer> insertsize = new TreeMap<Integer, Integer>();
	

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		option.getOptionsFromHadoopConf(conf);
		
		FileVirtualSplit input = (FileVirtualSplit)context.getInputSplit();
		String filename = input.getPath().getName();
		String libpath = option.getHdfsdir() + "/Sort/LibConf/" + filename + "-" + input.getStartVirtualOffset();
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
		
		for(Map.Entry<Integer, Integer> entry : insertsize.entrySet()) {
			String writer = entry.getKey() + "\t" + entry.getValue() + "\n";
			out.write(writer.getBytes());
		}
		
		out.close();
		conf = null;
		option = null;
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
		
		if(record.getMappingQuality() <= option.getMinqual()) //low quality
			return;
		else if(record.getDuplicateReadFlag())
		//else if(record.getDuplicateReadFlag() || record.getNotPrimaryAlignmentFlag()) //重复
			return;
		else if(record.getReadPairedFlag()) { //pair read
			if(record.getReadUnmappedFlag()) //unmap
				return;
			else if(record.getMateUnmappedFlag())  //mate unmap
				return;
			else if(Math.abs(record.getInferredInsertSize()) >= option.getMaxsvsize()) //too long sv size
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
			newkey.setEnd(record.getAlignmentEnd());
			context.write(newkey, f);
		}
	
	}


	/**
	 * saveInsert方法<br>
	 * 将正常的reads的insert size保存到中间文件中，中间文件名:/HDFSdir/Sort/LibConf/UUID <br>
	 * 一般一个map分片会选取100个正常的insert size<br>
	 * <br>
	 * @param record bam文件中每一个记录，也就是每一条read的比对情况
	 * @throws IOException 抛出IO异常
	 */
	private void saveInsert(SAMRecord record) throws IOException {

		int insert = record.getInferredInsertSize();
		if(insert > 2000)
			return;
		if(insert <= 0)
			return;
		if(record.getMappingQuality() < option.getMinqual())
			return;
		if(!record.getReadPairedFlag())
			return;
		if(!record.getProperPairFlag())
			return;
		
		int num = 0;
		if(!insertsize.containsKey(insert))
			num = 1;
		else {
			num = insertsize.get(insert);
			num ++ ;
		}
		insertsize.put(insert, num);
		
	}

}
