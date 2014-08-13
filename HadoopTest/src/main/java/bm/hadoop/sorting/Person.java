package bm.hadoop.sorting;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Person implements WritableComparable<Person> {
	public Person() {
	}

	private String firstName;
	private String lastName;

	public void write(DataOutput outputStream) throws IOException {
		outputStream.writeUTF(firstName);
		outputStream.writeUTF(lastName);
	}

	public void readFields(DataInput inputStream) throws IOException {
		firstName = inputStream.readUTF();
		lastName = inputStream.readUTF();
	}

	public int compareTo(Person other) {
		int cmp = this.firstName.compareTo(other.firstName);
		if (cmp != 0) {
			return cmp;
		}
		return this.lastName.compareTo(other.lastName);
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public void set(String lastName, String firstName) {
		this.lastName = lastName;
		this.firstName = firstName;
	}
}
