package org.apache.hcatalog.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hcatalog.data.transfer.HCatReader;
import org.apache.hcatalog.data.transfer.ReaderContext;

public class DataReaderSlave {

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(new File(args[0])));
		ReaderContext cntxt = (ReaderContext) ois.readObject();
		ois.close();
		
		String[] inpSlitsToRead = args[1].split(",");
		List<InputSplit> splits = cntxt.getSplits();
		
		for (int i = 0; i < inpSlitsToRead.length; i++){
			InputSplit split = splits.get(Integer.parseInt(inpSlitsToRead[i]));
			HCatReader reader = DataTransferFactory.getHCatReader(split, cntxt.getConf());
			Iterator<HCatRecord> itr = reader.read();
			File f = new File(args[2]+"-"+i);
			f.delete();
			BufferedWriter outFile = new BufferedWriter(new FileWriter(f)); 
			while(itr.hasNext()){
				String rec = itr.next().toString().replaceFirst("\\s+$", "");
				System.err.println(rec);
				outFile.write(rec+"\n");
			}
			outFile.close();
		}
	}
}
