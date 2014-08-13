package bm.hadoop.sorting;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Grouping sort comparator
 * 
 * @author badari
 *
 */
public class PersonNameComparator extends WritableComparator {
	protected PersonNameComparator() {
		super(Person.class, true);
	}

	@Override
	public int compare(WritableComparable p1, WritableComparable p2) {
		Person person1 = (Person) p1;
		Person person2 = (Person) p2;
		return person1.getLastName().compareTo(person2.getLastName());
	}

}
