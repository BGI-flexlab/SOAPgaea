package org.bgi.flexlab.gaea.tools.variantrecalibratioin.traindata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.bgi.flexlab.gaea.data.structure.header.VCFConstants;

import htsjdk.samtools.util.RuntimeEOFException;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.vcf.AbstractVCFCodec;
import htsjdk.variant.vcf.VCFCodec;

public class KnownDatabase {
	private static final String DBDRIVER = "com.mysql.jdbc.Driver";
    private static final String DBURL = "jdbc:mysql://10.1.0.60:3306/KnownSNP";
	//private static final String DBURL = "jdbc:mysql://172.30.4.57:3306/KnownSNP";
    private static final String DBUSER = "gaea";
    private static final String DBPASSWORD = "gaea";
    private static Connection conn;
    private static Statement statement;
    private static int lineNo;

    public KnownDatabase() throws ClassNotFoundException {
    	Class.forName(DBDRIVER);
    	try {
			conn = DriverManager.getConnection(DBURL, DBUSER, DBPASSWORD);
			if(!conn.isClosed()) 
	             System.out.println("Succeeded connecting to the Database!");
	    	 statement = conn.createStatement();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
    	 
    	 lineNo = 0;
    }
    public ResultSet getSourceId(String refType, String name) throws SQLException {
    	StringBuilder sql = new StringBuilder();
    	sql.append("select * from source where ");
    	if(refType != null) {
    		sql.append("ref_type='");
    		sql.append(refType);
    		sql.append("'");
    	}
    	if(name != null) {
    		if(refType != null)
    			sql.append(" && ");
    		sql.append("name='");
    		sql.append(name);
    		sql.append("'");
    	}
    	sql.append(";");
    	return statement.executeQuery(sql.toString());
    }

    
    public ArrayList<VariantContext> getTrainData(int resourceId, String chrName, int start, int end) {
    	ArrayList<VariantContext> vcl = new ArrayList<VariantContext>();
        ResultSet rs;
		try {
			StringBuilder sql = new StringBuilder();
			sql.append("select * from variation where ");
			sql.append("source_id=");
			sql.append(resourceId);
			sql.append(" && chr='");
			sql.append(chrName);
			sql.append("' && pos=");
			sql.append(start);
			sql.append(";");
			rs = statement.executeQuery(sql.toString());
			while(rs.next()) {
				VariantContextBuilder vcb = new VariantContextBuilder();
		       	buildVariationContext(vcb, rs);
		       	VariantContext vc = vcb.make();
		       	if(vc.getEnd() <= end)
		       		vcl.add(vc);
			 }
		} catch (SQLException e) {
			throw new RuntimeEOFException(e.toString());
		}     
    	
    	return vcl;
    }
    
    private void buildVariationContext(VariantContextBuilder vcb, ResultSet rs) throws SQLException {
    	lineNo++;
    	vcb.chr(rs.getString(2));
    	vcb.start(rs.getInt(3));
    	vcb.id(rs.getString(4));
    	
    	Allele ref = Allele.create(rs.getString(5), true);
    	Allele alt = Allele.create(rs.getString(6), false);
    	final List<Allele> alleles = new ArrayList<>();
    	Collections.addAll(alleles, ref, alt);
    	vcb.alleles(alleles);
    	
    	vcb.stop(rs.getInt(3) + rs.getString(5).length() - 1);
    	String filterString = rs.getString(7);
    	if(!filterString.equals(VCFConstants.UNFILTERED) && !filterString.equals(VCFConstants.PASSES_FILTERS_v4))
    		vcb.filter(filterString);
    }
}
