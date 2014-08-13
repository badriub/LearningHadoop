package bm.hadoop.join;

import java.io.File;
import java.io.IOException;

public interface IDistributedCacheFileReader<K, V> extends Iterable<Pair<K,V>> {
	public void init(File f) throws IOException;

	public void close();
}
