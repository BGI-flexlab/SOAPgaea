package org.bgi.flexlab.gaea.tools.mapreduce.callsv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bgi.flexlab.gaea.tools.callsv.Format;
import org.bgi.flexlab.gaea.tools.callsv.ListComputer;
import org.bgi.flexlab.gaea.tools.callsv.NewMapKey;
import org.bgi.flexlab.gaea.tools.callsv.TxtReader;

	
/**
 *  这是第一个MapReducer中的reducer类 <br>
 * 主要是用于读取从mapper中传过来的reads信息，并做以下事情：<br>
 * 1.setup方法读取中间文件LibConf（记录了一部分正常reads的insert size），并且计算出chr的upper和lower；<br>
 * 2.根据计算得到的upper和lower，将reads彻底分成5种SV类型的reads并输出；<br>
 * 3.遍历完reads之后计算dist并保存到Dist目录下的中间文件<br>
 * @author xiaohui
 * @version v2.0
 */
public class CallStructuralVariationReducer1 extends Reducer<NewMapKey, Format, NullWritable, Text>{
	
	/**
	 * 程序参数类Parameter，用于保存程序输入的参数
	 */
	private CallStructuralVariationOptions options = new CallStructuralVariationOptions();
	private Configuration conf;
	private FSDataOutputStream out;
	private Map<String, Float> lower = new TreeMap<String, Float>();
	private Map<String, Float> upper = new TreeMap<String, Float>();


	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		conf.setStrings("mapreduce.reduce.shuffle.memory.limit.percent", "0.05");
		options.getOptionsFromHadoopConf(conf);
	}
	
	@Override
	protected void reduce(NewMapKey key, Iterable<Format> values, Context context) throws IOException, InterruptedException {
		setUpperLower(key.getChr());
		reducerComputer(key, values, context);
		
	}

	/**
	 * Reducer Task的计算过程<br>
	 * 利用mean，upper和lower，将接收到的同一个key的reads最终判断为各种类型的APRs并输出
	 * @param key <br>NewMapKey类型的key，包含了chr和pos，但是以chr来划分分区
	 * @param values <br> 一个迭代器，包含了同一个chr的所有APRs，并且APRs是以pos从小到大排序的
	 * @param context <br> Context
	 * @throws IOException 抛出IO异常
	 * @throws InterruptedException 抛出中断异常
	 * @return 没有返回值
	 */
	private void reducerComputer(NewMapKey key, Iterable<Format> values, Context context)
			throws IOException, InterruptedException {

		float d = 100000000;
		int max_sd = 1000000000;
		int indel_num = 0;
		int min_pos = Integer.MAX_VALUE;
		int max_pos = 0;
		float dist = 0;
		
		
		for(Format value : values) {
			
			float mean = conf.getFloat(value.getLib() + "_mean", 0);
			float tmp1 = mean - value.getReadLen()*2;
			d = Math.min(d, tmp1);
			d = Math.max(d, 50);
			
			min_pos = Math.min(value.getStart(), min_pos);
			max_pos = Math.max(value.getEnd(), max_pos);
			
			//float upper = conf.getFloat(value.getLib() + "_upper", 0);
			//float lower = conf.getFloat(value.getLib() + "_lower", 0);
			
			float up = upper.get(value.getLib());
			float low = lower.get(value.getLib());
			
			if(value.getInsert() > max_sd) {continue;}
			
			/**
			 * 判断FR类型的reads是DEL或者INS
			 */
			if(value.getType().equals("FR")) {
				if(Math.abs(value.getInsert()) > up) {
					value.setType("FR_long");
					indel_num++;
				}else if(Math.abs(value.getInsert()) < low) {
					value.setType("FR_short");
					indel_num++;
				}else {
					continue;
				}
			}
			
			/**
			 * 将所有的APRs用context.write()输出
			 */
			value.setType(changeType(value.getType()));
			context.write(null, new Text(value.toString()));
		}
		
		int ref_length = max_pos - min_pos + 1;
		if(indel_num == 0 || ref_length == 0) {
			dist = d;
		}else {
			dist = Math.min(d, ref_length/indel_num);
		}
		dist = dist < 50 ? 50 : dist;
		
		/**
		 * 将dist存到中间文件中
		 */
		Path distPath = new Path(options.getHdfsdir() + "/MP1/Dist/" + UUID.randomUUID().toString());
		String writer = key.getChr() + "\t" + dist + "\t" + ref_length + "\n";
		writeFile(distPath , writer);
	}

	
	/**
	 * 遍历／LibConf目录下的中间文件，读取insert size值来计算每一个lib的upper和lower值，并保存到conf中
	 * @param 
	 * @return 没有返回值
	 */
	private void setUpperLower(String chr) {
		TxtReader mc = new TxtReader(conf);
		Map<String, List<Integer>> insert = mc.readConfFile(options.getHdfsdir() + "/MP1/LibConf/", chr);
		
		for(Entry<String, List<Integer>> entry : insert.entrySet()) {
			float mean = ListComputer.getMean(entry.getValue());
			float sd = ListComputer.getStd(entry.getValue());
			
			/**
			 * 删除异常值，再次计算平均值和标准差
			 */
			List<Integer> finalList = ListComputer.delectOutlier(entry.getValue(), mean, sd, 5);
			mean = ListComputer.getMean(finalList);
			
			conf.setFloat(entry.getKey() + "_mean", mean);
			
			/**
			 * 将平均值mean保存到/MP1/Mean/目录下的中间文件中
			 */
			Path meanPath = new Path(options.getHdfsdir() + "/MP1/Mean/" + chr);
			String writer = entry.getKey() + "\t" + mean + "\n";
			writeFile(meanPath, writer);
			
			/**
			 * 计算每一个lib的upper和lower并保存到conf中
			 */
			List<Integer> uplist = new ArrayList<Integer>();
			List<Integer> lowlist = new ArrayList<Integer>();
			for(int i : finalList) {

				if(i >= mean) {
					uplist.add(i);
				}else {
					lowlist.add(i);
				}
			}
			
			float up = mean + options.getStdtimes() * ListComputer.getUpLowStd(uplist, mean);
			float low = mean - options.getStdtimes() * ListComputer.getUpLowStd(lowlist, mean);
			
			System.out.println("getKey: " + entry.getKey() + "  lower: " + low + "  upper: " + up);
			lower.put(entry.getKey(), low);
			upper.put(entry.getKey(), up);
		}
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



}
