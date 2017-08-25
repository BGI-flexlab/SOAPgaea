package org.bgi.flexlab.gaea.tools.annotator.db;

import htsjdk.samtools.util.CloseableIterator;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by 黄凯文 on 2017/7/28.
 */
public class VCFAdapter implements DBAdapterInterface{

    private VCFFileReader vcfReader = null;
    private String filepath = null;

    public VCFAdapter(String confDir) {
        filepath = confDir;
    }
    @Override
    public void connection(String dbName) throws IOException{


    };
    @Override
    public void disconnection() throws IOException{
        if(vcfReader != null)
            vcfReader.close();

    };
    @Override
    public HashMap<String, String> getResult(String tableName, String rowKey, HashMap<String, String> fieldMap) throws IOException{
        return null;
    }
    @Override
    public HashMap<String, String> getResult(String tableName,
                                             String rowKey) throws IOException{
        if(vcfReader == null)
        {
            String fileName = filepath + "/" + tableName;// + ".vcf.gz";
            vcfReader = new VCFFileReader(new File(fileName));
        }
        HashMap<String,String> resultMap = new HashMap<>();

        String[] arr = rowKey.split("\t");
        String chr = arr[0];
        int start = Integer.valueOf(arr[1]);
        int end = Integer.valueOf(arr[2]);
        //直接根据chr-start-end查找数据
        CloseableIterator<VariantContext> vcfIter = vcfReader.query(chr, start, end);

        while (vcfIter.hasNext()) {
            VariantContext vc = vcfIter.next();
            if (start == vc.getStart() && end == vc.getEnd()) {//输出start，end完全一致的数据
                Map<String, Object> mp = vc.getAttributes();
                for (Map.Entry<String, Object> entry : mp.entrySet()) {
                    resultMap.put(entry.getKey(), entry.getValue().toString());
                }

                resultMap.put("POS", String.valueOf(vc.getStart()));
                resultMap.put("ID", vc.getID());
                resultMap.put("REF", vc.getReference().getDisplayString());
                StringBuilder altStr = new StringBuilder();
                for(Allele allele: vc.getAlternateAlleles()){
                    altStr.append(allele.getDisplayString()+ ",");
                }
                altStr.deleteCharAt(altStr.length() - 1);
                resultMap.put("ALT", altStr.toString());

            }
        }
        //System.err.println(arr[0] + " "+ arr[1] + " " + arr[2] + ":" +resultMap.size());

        return resultMap;

    }
}
