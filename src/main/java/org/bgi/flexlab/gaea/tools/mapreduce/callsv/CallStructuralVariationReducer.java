package org.bgi.flexlab.gaea.tools.mapreduce.callsv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bgi.flexlab.gaea.tools.callsv.BuildConnection;
import org.bgi.flexlab.gaea.tools.callsv.Format;
import org.bgi.flexlab.gaea.tools.callsv.LinkRegType;
import org.bgi.flexlab.gaea.tools.callsv.LinkRegion;
import org.bgi.flexlab.gaea.tools.callsv.NewMapKey;
import org.bgi.flexlab.gaea.tools.callsv.Reads;
import org.bgi.flexlab.gaea.tools.callsv.Region;
import org.bgi.flexlab.gaea.tools.callsv.Score;
import org.bgi.flexlab.gaea.tools.callsv.TxtReader;

public class CallStructuralVariationReducer extends Reducer<NewMapKey, Format, NullWritable, Text>{

	/**
	 * 程序参数类Parameter，用于保存程序输入的参数
	 */
	private CallStructuralVariationOptions option = new CallStructuralVariationOptions();
	private Configuration conf;
	private FSDataOutputStream out;
	private int mean;
	private float upper = 0;
	private float lower = 0;
	private float dist = 0;
	private int ref_length = 0;
	

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		option.getOptionsFromHadoopConf(conf);
		setUpperLower();
		
	}
	
	@Override
	protected void reduce(NewMapKey key, Iterable<Format> values, Context context) throws IOException, InterruptedException {

		List<Format> APRs = getAPRs(key, values);
		
		BuildConnection bc = new BuildConnection(option, dist, ref_length);
		Map<Integer, Region> regInfoMap = bc.getRegion(APRs);
		Map<LinkRegion, List<Reads>> linkRegMap = bc.buildLink();
		svCaller(linkRegMap, regInfoMap, context);
		
	}

	private void setUpperLower() {
		TxtReader mc = new TxtReader(conf);
		Map<Integer, Integer> insert = mc.readInsertFile(option.getHdfsdir() + "/Sort/LibConf/");
		
		int maxnum = 0;
		
		for(Map.Entry<Integer, Integer> entry : insert.entrySet()) {
			//get mean
			int num = entry.getValue();
			if(num > maxnum) {
				this.mean = entry.getKey();
				maxnum = num;
			}

		}

		long lowsum = 0;
		long upsum = 0;
		long lownum = 0;
		long upnum = 0;
		for(Map.Entry<Integer, Integer> entry : insert.entrySet()) {
			if(entry.getKey() < mean) {
				lowsum = (long) (lowsum + Math.pow((entry.getKey() - mean),2) * entry.getValue());
				lownum = lownum + entry.getValue();
			}else {
				upsum = (long) (upsum + Math.pow((entry.getKey() - mean),2) * entry.getValue());
				upnum = lownum + entry.getValue();
			}
		}
		
		float lowstd = (float) Math.sqrt(lowsum/(lownum-1));
		float upstd = (float) Math.sqrt(upsum/(upnum-1));
		
		System.out.println("Low Std : " + lowstd);
		System.out.println("Up Std : " + upstd);
		
		upper = mean + option.getStdtimes() * upstd;
		lower = mean - option.getStdtimes() * lowstd;
		
		String w = "Mean : " + mean + "\tLower: " + lower + "\tUpper: " + upper + "\tLowstd: " + lowstd + "\tUpstd: " + upstd + "\n";
		writeFile(new Path(option.getHdfsdir() + "/Sort/Mean_UpLow/" + UUID.randomUUID().toString()), w);
		
	}
	
	
	private List<Format> getAPRs(NewMapKey key, Iterable<Format> values){
		
		List<Format> aprs = new ArrayList<Format>();
		float d = 100000000;
		int max_sd = 1000000000;
		int indel_num = 0;
		int min_pos = Integer.MAX_VALUE;
		int max_pos = 0;
		
		Iterator<Format> vs = values.iterator();
		while (vs.hasNext()) {
			Format value = vs.next();
			
			float tmp1 = mean - value.getReadLen()*2;
			d = Math.min(d, tmp1);
			d = Math.max(d, 50);
			
			min_pos = Math.min(value.getStart(), min_pos);
			max_pos = Math.max(value.getEnd(), max_pos);
			
			if(value.getInsert() > max_sd) {continue;}
			
			/**
			 * 判断FR类型的reads是DEL或者INS
			 */
			if(value.getType().equals("FR")) {
				if(Math.abs(value.getInsert()) > upper) {
					value.setType("FR_long");
					indel_num++;
				}else if(Math.abs(value.getInsert()) < lower) {
					value.setType("FR_short");
					indel_num++;
				}else {
					continue;
				}
			}
			
			value.setType(changeType(value.getType()));
			Format f = new Format(value.toString());
			aprs.add(f);
		}
		
		ref_length = max_pos - min_pos + 1;
		if(indel_num == 0 || ref_length == 0) {
			dist = d;
		}else {
			dist = Math.min(d, ref_length/indel_num);
		}
		dist = dist < 50 ? 50 : dist;
		
		/**
		 * 将dist存到中间文件中
		 */
		Path distPath = new Path(option.getHdfsdir() + "/Sort/Dist/" + key.getChr());
		String writer = key.getChr() + "\t" + dist + "\t" + ref_length + "\n";
		writeFile(distPath , writer);
		return aprs;
	}

	
	/**
	 * 将reads的APRs所支持的类型转变成SV类型
	 * @param t reads的类型（"FR_long"， "FR_short"， "FF_RR"， "RF"， "Diff"）
	 * @return 返回字符串类型的SV Type（"DEL"， "INS"， "INV"， "ITX"， "CTX"）
	 */
	private String changeType(String t) {
		if(t.equals("FR_long"))
			return "DEL";
		else if(t.equals("FR_short"))
			return "INS";
		else if(t.equals("RF"))
			return "ITX";
		else if(t.equals("FF_RR"))
			return "INV";
		else if(t.equals("Diff"))
			return "CTX";
		else
			return null;

	}
	
	
	private void writeFile(Path path, String writer) {
		
		try {
			out = FileSystem.get(conf).create(path,true);
			out.write(writer.getBytes());
			out.flush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	
	/**
	 * 遍历每一对有联系的区域，做最终的calling
	 * @param linkRegMap Map集合，key是有联系的两个区域，value是支持这两个区域有联系的reads列表
	 * @throws InterruptedException 抛出中断异常
	 * @throws IOException 抛出IO异常
	 */
	private void svCaller(Map<LinkRegion, List<Reads>> linkRegMap, Map<Integer, Region> regInfoMap, Context context) throws IOException, InterruptedException {
		
		for(Map.Entry<LinkRegion, List<Reads>> linkRegEntry : linkRegMap.entrySet()) {
			LinkRegion linkReg = linkRegEntry.getKey();
			
			/**
			 *得到相互连通的两个区域，firstReg和secondReg
			 */
			Region firstReg = regInfoMap.get(linkReg.getFirstRegion());
			Region secondReg = regInfoMap.get(linkReg.getSecondRegion());
			
			/**
			 * 遍历这对相互连通对区域中的reads，保存每一种sv类型的信息
			 */
			Map<String, LinkRegType> linkRegTypeMap = saveTypeInfo(linkRegEntry.getValue());
			
			/**
			 * 选取最终的sv类型finalType
			 */
			String finalType = selectFinalType(linkRegTypeMap);
			if(finalType == null)
				continue;
			
			/**
			 * 当最终的sv类型不为null时，计算finalType的分数score
			 */
			LinkRegType finalTypeInfo = linkRegTypeMap.get(finalType);
			int score = computeProbScore(firstReg, secondReg, finalTypeInfo);
			
			/**
			 * 当分数大于输出的分数时，输出到parts
			 */
			int size = finalTypeInfo.getSize()/finalTypeInfo.getReadNum();
			
			String writer = firstReg.firstToString() + "\t" + 
					secondReg.secondToString() + "\t" +
					finalType + "\t" + size + "\t" + 
					score + "\t" + finalTypeInfo.getReadNum();
			
			context.write(null, new Text(writer));
		}
	}

	

	/**
	 * 计算选定的最终类型的分数
	 * @param firstReg 这个SV相连的两个区域中第一个区域
	 * @param secondReg 这个SV相连的两个区域中第二个区域
	 * @param finalReg LinkRegType对象，保存最终类型的信息
	 * @return 计算得到的分数
	 */
	private int  computeProbScore(Region firstReg, Region secondReg, LinkRegType finalTypeInfo) {
		
		int totalRegSize = firstReg.getRegLength() + secondReg.getRegLength();
		
		double logP = 0;
		for(Integer libNum : finalTypeInfo.getLibNum().values()) {
			
			double lambda = (double)totalRegSize*libNum/ref_length; //total_reg_size*lib_read_num/ref_length
			Score sc = new Score();
			logP = logP + sc.logPoissionTailProb(libNum, lambda);
		}
		
		double phredQ = -10*(logP/Math.log(10));
		int score = (int) ((phredQ > 99) ? 99 : phredQ + 0.5);
		return score;
	}

	
	/**
	 * 遍历一对连通的区域中所有的reads，保存每一种Type的信息
	 * @param linkRegReads 一对连通的区域中所有的reads列表
	 * @return Map集合，key是type，value是LinkRegType对象
	 */
	private Map<String, LinkRegType> saveTypeInfo( List<Reads> linkRegReads) {
		
		Map<String, LinkRegType> linkRegType = new TreeMap<String, LinkRegType>();
		
		for(Reads r : linkRegReads) {
			LinkRegType typeInfo = linkRegType.get(r.getType());
			if(typeInfo==null)
				typeInfo = new LinkRegType(r);
			
			int size = Math.abs(r.getInsert()-mean);
			
			typeInfo.updateType(r, size);
			linkRegType.put(r.getType(), typeInfo);
		}
		return linkRegType;
	}

	
	/**
	 * 根据保存的每一种Type的信息，选取终的type
	 * @param linkRegType Map集合，保存了同一对连通的区域中，每一种类型的信息
	 * @return 最终选定的类型
	 */
	private String selectFinalType(Map<String, LinkRegType> linkRegType) {
		int finalNum = 0;
		String finalType = null;
		for(Map.Entry<String, LinkRegType> linkTypeEntry: linkRegType.entrySet()) {
			if(finalNum < linkTypeEntry.getValue().getReadNum()) {
				finalNum = linkTypeEntry.getValue().getReadNum();
				finalType = linkTypeEntry.getValue().getType();
			}
		}
		finalType = (finalNum >= option.getMinpair()) ? finalType : null;
		return finalType;
	}
	
}
