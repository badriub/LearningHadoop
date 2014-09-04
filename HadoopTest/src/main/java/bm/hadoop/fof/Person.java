package bm.hadoop.fof;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Person implements WritableComparable<Person> {

	private String name;
	private long commonFriends;
	
	/**
	 * Need to have empty constructor for hadoop
	 */
	public Person() {
	}

	public Person(String name, long commonFriends) {
		this.name = name;
		this.commonFriends = commonFriends;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeLong(this.commonFriends);
	}

	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
		this.commonFriends = in.readLong();
	}

	public int compareTo(Person other) {
		if (this.name.compareTo(other.name) != 0) {
			return this.name.compareTo(other.name);
		} else if (this.commonFriends != other.commonFriends) {
			return commonFriends < other.commonFriends ? -1 : 1;
		} else {
			return 0;
		}
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getCommonFriends() {
		return commonFriends;
	}

	public void setCommonFriends(long commonFriends) {
		this.commonFriends = commonFriends;
	}

}
