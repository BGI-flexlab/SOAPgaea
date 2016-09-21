package org.bgi.flexlab.gaea.inputformat.vcf.codec;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.bgi.flexlab.gaea.structure.header.VCFHeader;
import org.junit.Test;

import junit.framework.Assert;

public class VCFCodecTest {

	@Test
	public void test() {
		VCFCodec codec = new VCFCodec();
		String input = "F:\\BGIBigData\\TestData\\VCF\\DNA1425995.vcf";
		VCFHeader header=null;
    	ArrayList<String> headerLine=new ArrayList<String>();
    	File file=new File(input);
    	BufferedReader reader=null;
    	try{
    		reader=new BufferedReader(new FileReader(file));
    		String tempString=null;
    		
    		//read header
    		while((tempString=reader.readLine())!=null) {
    			if(tempString.startsWith("#"))
    			{
    				headerLine.add(tempString.trim());
    			}else
    				break;
    		}
    		header=(VCFHeader)codec.readHeader(headerLine);		
    		reader.close();
    		Assert.assertEquals(28, header.getMetaDataInInputOrder().size());
    	}catch(IOException e)
    	{
    		e.printStackTrace();
    	}finally{
    		if(reader!=null)
    		{
    			try{
    				reader.close();
    			}catch(IOException e1)
    			{
    			}
    		}
    	}
    }
}
