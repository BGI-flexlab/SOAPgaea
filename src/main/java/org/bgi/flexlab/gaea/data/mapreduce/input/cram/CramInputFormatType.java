package org.bgi.flexlab.gaea.data.mapreduce.input.cram;

public enum CramInputFormatType{
	ALL(0),CHROMOSOME(1);
	
	private int value = 0;
	
	private CramInputFormatType(int value){
		this.value = value;
	}
	
	public static CramInputFormatType valueOf(int value) {
        switch (value) {
        case 1:
            return CHROMOSOME;
        default:
            return ALL;
        }
    }
	
	public int value() {
        return this.value;
    }
}
