package org.apache.hcatalog.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hcatalog.data.transfer.HCatWriter;
import org.apache.hcatalog.data.transfer.WriterContext;

public class DataWriterSlave {

	public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException {
		
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(args[0]));
		WriterContext cntxt = (WriterContext) ois.readObject();
		ois.close();
		
		HCatWriter writer = DataTransferFactory.getHCatWriter(cntxt);
		writer.write(new HCatRecordItr(args[1]));
		
	}
	
	private static class HCatRecordItr implements Iterator<HCatRecord> {

		BufferedReader reader;
		String curLine;
		
		public HCatRecordItr(String fileName) throws FileNotFoundException {
			reader = new BufferedReader(new FileReader(new File(fileName)));
		}
		
		@Override
		public boolean hasNext() {
			try {
				curLine = reader.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null == curLine ? false : true;
		}

		@Override
		public HCatRecord next() {

			String[] fields = curLine.split("\t");
			List<Object> data = new ArrayList<Object>(3);
			data.add(fields[0]);
			data.add(Integer.parseInt(fields[1]));
			data.add(Double.parseDouble(fields[2]));
			return new DefaultHCatRecord(data);
		}

		@Override
		public void remove() {
			// TODO Auto-generated method stub
			
		}
	}
}
