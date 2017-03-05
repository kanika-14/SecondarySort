import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DateTemperatureSortComparator extends WritableComparator {

  protected DateTemperatureSortComparator() {
		super(DateTemperaturePair.class, true);
	}
  
    @SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		DateTemperaturePair key1 = (DateTemperaturePair) w1;
		DateTemperaturePair key2 = (DateTemperaturePair) w2;

		int cmpResult = key1.getYearMonth().compareTo(key2.getYearMonth());
		if (cmpResult == 0) { // same year month
			return key1.getTemperature().compareTo(key2.getTemperature());
			//If the minus is taken out, the values will be in
			//ascending order
		}
		return cmpResult;
	}
}