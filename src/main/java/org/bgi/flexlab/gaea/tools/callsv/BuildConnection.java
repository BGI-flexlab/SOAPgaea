package org.bgi.flexlab.gaea.tools.callsv;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.bgi.flexlab.gaea.tools.mapreduce.callsv.CallStructuralVariationOptions;


/**
 * BuildConnection类，用作CallSV主要的计算类
 * @author Huifang Lu
 *
 */
public class BuildConnection {
	
	/**
	 * 保存了程序的输入参数
	 */
	private CallStructuralVariationOptions options;
	/**
	 * Map集合，key是染色体编号chr，value是所对应的dist值，用作区分区域的标准
	 */
	private Map<String, List<Integer>> dist;

	/**
	 * Map集合，key是ReadName，value是Reads的对象，保存了reads的信息
	 */
	private Map<String, Reads> readInfoMap;
	
	//public BuildConnection() {}
	
	/**
	 * 带有参数para的构造函数<br>
	 * 如果使用此构造函数来构造对象，则初始化reg_id等变量
	 * @param para Parameter类型的参数，将上层的para传递进来
	 * @param dist Map集合，保存了每个染色体的dist和ref_length
	 */
	public BuildConnection(CallStructuralVariationOptions options, Map<String, List<Integer>> dist) {
		this.options = options;
		this.dist = dist;
		this.readInfoMap = new TreeMap<String, Reads>();
	}

	public Map<String, List<Integer>> getDist() {
		return dist;
	}

	public void setDist(Map<String, List<Integer>> dist) {
		this.dist = dist;
	}

	public Map<String, Reads> getReadInfoMap() {
		return readInfoMap;
	}

	public void setReadInfoMap(Map<String, Reads> readInfoMap) {
		this.readInfoMap = readInfoMap;
	}

	
	/**
	 * 将APRs划分区域，同时也保存了每一条reads的信息
	 * @param it 此参数输入的是一个iterator迭代器，保存了此reducer任务接收到的reads信息
	 * @return 一个Map集合，保存了所有reg信息，key是regid，value是Region对象
	 */
	public  Map<Integer, Region> getRegion(Iterator<Format> it) {
		Map<Integer, Region> regInfoMap = new TreeMap<Integer, Region>();
		
		Region reg = new Region();
		int regId = 0;
		
		while(it.hasNext()) {
			Format r = it.next(); //read record
			
			/**
			 * chr不相同，并且间隔大于dist，满足划分为两个区域的条件，做break
			 * 但是还要判断是真的break还是要去掉的break
			 */
			if(!r.getChr().equals(reg.getChr()) || (r.getStart() - reg.getRegEnd()) > dist.get(r.getChr()).get(0)) { //break
				float coverage = reg.getBaseNum()/(reg.getRegEnd() - reg.getRegStart() + 1);
				
				if(coverage > 0 && coverage < options.getMaxcoverage() && (reg.getRegEnd() - reg.getRegStart() > options.getMinlen())) { //real break
					regInfoMap.put(regId, reg);
					regId ++;
				}else { //false break
					for(String readid : reg.getRegReads()) {
						readInfoMap.remove(readid);  //delete false reads form readInfoMap
					}
					reg = null;
				}
				reg = new Region(r);
				reg.setRegId(regId);
			}
			
			/**
			 * 还是同一个区域，更新区域的信息
			 */
			reg.updateReg(r); // Update region
			regInfoMap.put(regId, reg);
			saveReadInfo(regId, r);
		}
		
		return regInfoMap;
	}

	/**
	 * 将read信息保存到readInfoMap集合中
	 * @param regId 当前区域编号
	 * @param r 当前read
	 */
	private void saveReadInfo(int regId, Format r) {
		Reads read = readInfoMap.get(r.getReadName());
		if(read == null)
			read = new Reads(r);
		read.getReg().add(regId);
		readInfoMap.put(r.getReadName(), read);
		
	}
	
	

	/**
	 * 获取每一对有联系的区域，并保存其有联系的reads
	 * @return Map集合，key是有联系的两个区域，value是支持这两个区域有联系的reads列表
	 */
	public Map<LinkRegion, List<Reads>> buildLink() {
		Map<LinkRegion, List<Reads>> linkRegMap = new TreeMap<LinkRegion, List<Reads>>();
		
		for(Reads r: readInfoMap.values()) {
			/**
			 * 同一对reads不是比对到两个区域或者两个区域相等，去掉这对reads
			 */
			if( r.getReg().size() !=2 || r.getReg().get(0).equals(r.getReg().get(1))) 
				continue;
				
			/**
			 * 同一对reads只比对到两个区域，并且两个区域不相同，则这两个区域为相互连通的区域，保存下来
			 */
			LinkRegion tmpLinkReg = new LinkRegion(r.getReg());
			
			List<Reads> readList = linkRegMap.get(tmpLinkReg);
			if(readList == null)
				readList = new ArrayList<Reads>();
			readList.add(r);
			linkRegMap.put(tmpLinkReg, readList);
			
		}
		
		readInfoMap = null; 
		return linkRegMap;
	}
	
}
