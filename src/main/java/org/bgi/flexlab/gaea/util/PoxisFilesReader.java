package org.bgi.flexlab.gaea.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class PoxisFilesReader extends GaeaFilesReader {
	private ArrayList<File> files = new ArrayList<File>();
	private BufferedReader bufferedReader = null;

	@Override
	public void traversal(String path) {
		File file = new File(path);

		if (file.isDirectory()) {
			File[] fileArray = file.listFiles();
			for (File f : fileArray) {
				if (f.isDirectory())
					traversal(file.getAbsolutePath());
				else {
					if (!filter(f.getName()))
						files.add(f);
				}
			}
		} else {
			if (!filter(file.getName()))
				files.add(file);
		}
		
		if(size() == 0)
			return;
		
		try {
			bufferedReader = new BufferedReader(new FileReader(files.get(0)));
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e.toString());
		}
	}

	protected int size() {
		return files.size();
	}

	@Override
	public boolean hasNext() {
		if (bufferedReader != null) {
			String str;
			try {
				if ((str = bufferedReader.readLine()) != null) {
					currentLine = str;
					return true;
				} else {
					currentFileIndex++;
					bufferedReader.close();
					if (currentFileIndex < size()) {
						bufferedReader = new BufferedReader(new FileReader(files.get(currentFileIndex)));
						if ((str = bufferedReader.readLine()) != null) {
							currentLine = str;
							return true;
						}
					}
				}
			} catch (IOException e) {
				throw new RuntimeException(e.toString());
			}
		}

		currentLine = null;
		return false;
	}

	@Override
	public void clear() {
		if(bufferedReader != null){
			try {
				bufferedReader.close();
			} catch (IOException e) {
				throw new RuntimeException(e.toString());
			}
		}
		files.clear();
		currentLine = null;
		currentFileIndex = 0;
	}
}
