package bm.hadoop.join;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;

public class TextDistributedCacheFileReader implements
		IDistributedCacheFileReader<String, String>, Iterator<Pair<String, String>> {
	private LineIterator iter;
	
	public Iterator<Pair<String, String>> iterator() {
		 return this;
	}

	public boolean hasNext() {
		return iter.hasNext();
	}

	public Pair<String, String> next() {
		String line = iter.next();
		Pair<String, String> pair = new Pair<String, String>();
		
		String[] parts = StringUtils.split(line, "\t", 2);
	    pair.setKey(parts[0]);
	    if(parts.length > 1) {
	      pair.setData(parts[1]);
	    }
		
		return pair;
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}

	public void init(File f) throws IOException {
		iter = FileUtils.lineIterator(f);		
	}

	public void close() {
		iter.close();
	}

}
