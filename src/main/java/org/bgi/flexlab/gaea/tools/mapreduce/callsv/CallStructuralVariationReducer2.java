package org.bgi.flexlab.gaea.tools.mapreduce.callsv;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
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

/**
 * 这是第二个MapReducer中的reducer类 <br>
 * 接收上一个Map传递过来的reads信息，并通过计算得到ctx文件输出
 * 
 * @author Huifang Lu
 * @version v2.0 <br>
 * 
 */ 
public class CallStructuralVariationReducer2 extends Reducer<NewMapKey, Format, NullWritable, Text>{
	
	private Configuration conf;
	private CallStructuralVariationOptions options;
	private TxtReader reader;
	
	/**
	 * Map类型成员变量dist，保存每一个chr及其对应的dist
	 */
	private Map<String, List<Integer>> dist = new TreeMap<String, List<Integer>>();
	
	/**
	 * Map类型的成员变量mean，保存每一个文库及其对应的mean
	 */
	private Map<String, List<Integer>> mean = new TreeMap<String, List<Integer>>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		options = new CallStructuralVariationOptions();
		options.getOptionsFromHadoopConf(conf);
		reader = new TxtReader(conf);
		dist = reader.readFile(options.getHdfsdir() + "/MP1/Dist/");
	}
	
	
	@Override
	protected void reduce(NewMapKey key, Iterable<Format> values, Context context) throws IOException, InterruptedException {		
		if(!(options.isSetMean() && options.isSetStd()))
			mean = reader.readConfFile(options.getHdfsdir() + "/MP1/Mean/", key.getChr());
		
		Iterator<Format> vIterator = values.iterator();
		
		BuildConnection bc = new BuildConnection(options, dist);
		
		Map<Integer, Region> regInfoMap = bc.getRegion(vIterator);
		
		Map<LinkRegion, List<Reads>> linkRegMap = bc.buildLink();
		
		svCaller(linkRegMap, regInfoMap, context);
		
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
			 * 如果相互连通的两个区域的reads支持数小于参数，则不往后处理
			 */
			if(linkRegEntry.getValue().size() < options.getMinpair())
				continue;
			
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
			if(score < options.getMinscore()) 
				continue;
			
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
			
			double lambda = (double)totalRegSize*libNum/dist.get(firstReg.getChr()).get(1); //total_reg_size*lib_read_num/ref_length
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
			
			int size;
			if(!(options.isSetMean() && options.isSetStd()))
				size = Math.abs(r.getInsert()-mean.get(r.getLib()).get(0));
			else
				size = Math.abs(r.getInsert()-options.getMean());
			
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
		finalType = (finalNum >= options.getMinpair()) ? finalType : null;
		return finalType;
	}

}
