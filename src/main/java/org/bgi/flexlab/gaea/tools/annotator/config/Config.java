package org.bgi.flexlab.gaea.tools.annotator.config;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.bgi.flexlab.gaea.tools.annotator.effect.SnpEffectPredictor;
import org.bgi.flexlab.gaea.tools.annotator.interval.Genome;
import org.bgi.flexlab.gaea.tools.annotator.util.CountByType;
import org.bgi.flexlab.gaea.tools.annotator.util.Gpr;
import org.bgi.flexlab.gaea.tools.annotator.util.Timer;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

public class Config implements Serializable {
	
	private static final long serialVersionUID = 2793968455002586646L;
	
	private static Config configInstance = null; 
	
	public static final String ANNO_FIELDS_SUFIX = ".fields";
	public static int MAX_WARNING_COUNT = 20;
	
	private static final String dbConfigFileName = "dbconf.json";
	private static String configFileName = "config.properties";
	
	private String  ref = null;
	private String  geneInfo = null;
	private String  cytoBandFile = null;
	
	boolean debug = false; // Debug mode?
	boolean verbose = false; // Verbose
	boolean treatAllAsProteinCoding;
	boolean onlyRegulation; // Only use regulation features
	boolean errorOnMissingChromo; // Error if chromosome is missing
	boolean errorChromoHit; // Error if chromosome is not hit in a query
	boolean hgvs = true; // Use HGVS notation?
	boolean hgvsShift = true; // Shift variants according to HGVS notation (towards the most 3prime possible coordinate)
	boolean hgvsOneLetterAa = false; // Use HGVS 1 letter amino acid in HGVS notation?
	boolean hgvsTrId = false; // Use HGVS transcript ID in HGVS notation?
	String genomeVersion;
	Properties properties;
	Genome genome;
	HashMap<String, Genome> genomeById;
	SnpEffectPredictor snpEffectPredictor;
	
	HashMap<String, String[]> tagsByDB = null;
	HashMap<String, TableInfo> dbInfo = null;
	
	CountByType warningsCounter = new CountByType();

	
	public Config() {
//		this(configFileName);
	}
	
	public Config(String configFileName) {
		this.setUserConf(configFileName);
		loadProperties(configFileName);
		setFieldByDB();
		loadJson();
		configInstance = this;
	}
	
	public HashMap<String, String[]> getTagsByDB() {
		return tagsByDB;
	}
	
	public HashMap<String, TableInfo> getDbInfo() {
		return dbInfo;
	}
	
	public String getUserConf() {
		return configFileName;
	}

	private void setUserConf(String userConf) {
		Config.configFileName = userConf;
	}
	
	boolean loadJson() {

		URL url = Config.class.getClassLoader().getResource(dbConfigFileName);
		String fileName = url.getPath();
		
		
		Gson gson = new Gson();
		try {
			Reader reader =  new InputStreamReader(Config.class.getResourceAsStream(fileName), "UTF-8");
			dbInfo =  gson.fromJson(reader, new TypeToken<HashMap<String, TableInfo>>() {  
            }.getType());
		} catch ( JsonSyntaxException | IOException e) {
			dbInfo = null;
			Timer.showStdErr("Read a wrong json config file:" + fileName);
			e.printStackTrace();
		}
		
		if (dbInfo != null) {
			return true;
		}
		return false;
	}

	private static void testProp() {
		Properties pps = new Properties();
		try {
			pps.load(Config.class.getClassLoader().getResourceAsStream("dbconf.properties")); 
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		pps.getProperty("codon.Standard", "");	
		
		Enumeration<?> enum1 = pps.propertyNames();//得到配置文件的名字
		while(enum1.hasMoreElements()) {
		           String strKey = (String) enum1.nextElement();
		           String strValue = pps.getProperty(strKey);
		            System.out.println(strKey + "=" + strValue);
        }
		
		if( pps.containsKey("test.test1xtest2") ){
			System.out.println("lll");
		}

	}
	
	/**
	 * Load properties from configuration file
	 * @return true if success
	 */
	boolean loadProperties(String configFileName) {
		try {
			File confFile = new File(configFileName);
			if (verbose) Timer.showStdErr("Reading config file: " + confFile.getCanonicalPath());

			if (Gpr.canRead(configFileName)) {
				// Load properties
				properties.load(new FileReader(confFile));
				return true;
			}else {
				System.out.println(configFileName);
				//	used for debug
				properties.load(Config.class.getClassLoader().getResourceAsStream("dbconf.properties"));
				if (!properties.isEmpty()) {
					return true;
				}
			}
		} catch (Exception e) {
			properties = null;
			throw new RuntimeException(e);
		}

		return false;
	}

	
	private HashMap<String, String[]> setFieldByDB() {
		
		// Sorted keys
		ArrayList<String> keys = new ArrayList<String>();
		for (Object k : properties.keySet())
			keys.add(k.toString());
		Collections.sort(keys);

		tagsByDB = new HashMap<String, String[]>();
		for (String key : keys) {
			if (key.equalsIgnoreCase("ref")) {
				setRef(properties.getProperty(key));
			}else if (key.equalsIgnoreCase("geneInfo")) {
				setGeneInfo(properties.getProperty(key));
			}else if (key.endsWith(ANNO_FIELDS_SUFIX)) {
				String dbName = key.substring(0, key.length() - ANNO_FIELDS_SUFIX.length());

				// Add full name
				String[] fields = properties.getProperty(key).split(",");
				if (fields != null && fields.length != 0) {
					tagsByDB.put(dbName, fields);
				}
			}
		}
		return tagsByDB;
	}

	public static Config getConfigInstance() {
		return configInstance;
	}
	
	public static Config get(){
		return configInstance;
	}

	public Genome getGenome() {
		return genome;
	}

	public Genome getGenome(String genomeId) {
		return genomeById.get(genomeId);
	}

	public String getGenomeVersion() {
		return genomeVersion;
	}
	
	/**
	 * Reads a properties file
	 * @return The path where config file is located
	 */
	String readProperties(String configFileName) {
		properties = new Properties();
		try {
			// Build error message
			File confFile = new File(configFileName);
			if (loadProperties(configFileName)) return configFileName;

			// Absolute path? Nothing else to do...
			if (confFile.isAbsolute()) throw new RuntimeException("Cannot read config file '" + confFile.getCanonicalPath() + "'");

			// Try reading from current execution directory
			String confPath = getRelativeConfigPath() + "/" + configFileName;
			confFile = new File(confPath);
			if (loadProperties(confPath)) return confPath;

			throw new RuntimeException("Cannot read config file '" + configFileName + "'\n");
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Cannot find config file '" + configFileName + "'");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Read config file
	 */
	String readProperties(String configFileName, Map<String, String> override) {
		String configFile = readProperties(configFileName);

		if (override != null) {
			for (String key : override.keySet()) {
				properties.setProperty(key, override.get(key));
			}
		}

		return configFile;
	}
	
	public void setHgvsOneLetterAA(boolean hgvsOneLetterAa) {
		this.hgvsOneLetterAa = hgvsOneLetterAa;
	}

	public void setHgvsShift(boolean hgvsShift) {
		this.hgvsShift = hgvsShift;
	}

	public void setHgvsTrId(boolean hgvsTrId) {
		this.hgvsTrId = hgvsTrId;
	}

	public void setOnlyRegulation(boolean onlyRegulation) {
		this.onlyRegulation = onlyRegulation;
	}

	public void setString(String propertyName, String value) {
		properties.setProperty(propertyName, value);
	}

	public void setTreatAllAsProteinCoding(boolean treatAllAsProteinCoding) {
		this.treatAllAsProteinCoding = treatAllAsProteinCoding;
	}

	public void setUseHgvs(boolean useHgvs) {
		hgvs = useHgvs;
	}

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}
	
	/**
	 * Get the relative path to a config file
	 */
	String getRelativeConfigPath() {
		URL url = Config.class.getProtectionDomain().getCodeSource().getLocation();
		try {
			File path = new File(url.toURI());
			return path.getParent();
		} catch (Exception e) {
			throw new RuntimeException("Cannot get path '" + url + "'", e);
		}
	}

	public boolean isDebug() {
		return debug;
	}

	public boolean isErrorChromoHit() {
		return errorChromoHit;
	}

	public boolean isErrorOnMissingChromo() {
		return errorOnMissingChromo;
	}

	public boolean isHgvs() {
		return hgvs;
	}

	public boolean isHgvs1LetterAA() {
		return hgvsOneLetterAa;
	}

	public boolean isHgvsShift() {
		return hgvsShift;
	}

	public boolean isHgvsTrId() {
		return hgvsTrId;
	}

	public boolean isOnlyRegulation() {
		return onlyRegulation;
	}

	public boolean isTreatAllAsProteinCoding() {
		return treatAllAsProteinCoding;
	}

	public boolean isVerbose() {
		return verbose;
	}
	
	/**
	 * Show a warning message and exit
	 */
	public void warning(String warningType, String details) {
		long count = warningsCounter.inc(warningType);

		if (debug || count < MAX_WARNING_COUNT) {
			if (debug) Gpr.debug(warningType + details, 1);
			else System.err.println(warningType + details);
		} else if (count <= MAX_WARNING_COUNT) {
			String msg = "Too many '" + warningType + "' warnings, no further warnings will be shown:\n" + warningType + details;
			if (debug) Gpr.debug(msg, 1);
			else System.err.println(msg);
		}
	}

	public String getGeneInfo() {
		return geneInfo;
	}

	public void setGeneInfo(String geneInfo) {
		this.geneInfo = geneInfo;
	}

	public String getRef() {
		return ref;
	}

	public void setRef(String ref) {
		this.ref = ref;
	}

	public String getCytoBandFile() {
		return cytoBandFile;
	}

	public void setCytoBandFile(String cytoBandFile) {
		this.cytoBandFile = cytoBandFile;
	}
	
	public static void main(String[] args) throws Exception {
		Config config = new Config();
		Config.testProp();
	}

	public SnpEffectPredictor loadSnpEffectPredictor() {
		// TODO Auto-generated method stub
		return null;
	}

	public SnpEffectPredictor getSnpEffectPredictor() {
		return snpEffectPredictor;
	}

}
