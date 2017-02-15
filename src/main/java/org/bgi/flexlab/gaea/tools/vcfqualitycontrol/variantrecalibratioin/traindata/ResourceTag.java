package org.bgi.flexlab.gaea.tools.vcfqualitycontrol.variantrecalibratioin.traindata;

public enum ResourceTag {
	NAME("name", ""), KNOWN("known", "false"), TRAINING("training", "false"), ANTITRAINING("antiTraining", "false"),
	TRUTH("truth", "false"), CONSENSUS("consensus", "false"), PRIOR("prior", "0.0"), DB("db", ""), FILE("file", "");
	
	private String tag;
	private String property;
	private ResourceTag(String tag, String property) {
		// TODO Auto-generated constructor stub
		this.tag = tag;
		this.property = property;
	}
	
	public static void parseTag(String resource) {
		String[] tags = resource.split(",");
		for(String tag:tags) {
			String[] keyValue = tag.split("=");
			ResourceTag.valueOf(keyValue[0].toUpperCase()).setProperty(keyValue[1]);
		}
	}
	
	public void setProperty(String property) {
		this.property = property;
	}
	
	public String getProperty() {
		return property;
	}
}
