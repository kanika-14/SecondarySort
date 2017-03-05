import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

  public class DateTemperaturePair implements Writable, WritableComparable<DateTemperaturePair> {
 
      private Text yearMonth = new Text();                 // natural key
      private Text day = new Text();
      private Double temperature = 0.0; // secondary key

      public DateTemperaturePair(){
    	  
      }
      
      public DateTemperaturePair(Text yearMonth, DoubleWritable temperature){
    	  this.yearMonth = yearMonth;
    	  this.temperature = temperature.get();
      }

     public Text getYearMonth() {
		return yearMonth;
	}

	public void setYearMonth(Text yearMonth) {
		this.yearMonth = yearMonth;
	}

	public Text getDay() {
		return day;
	}

	public void setDay(Text day) {
		this.day = day;
	}

	public Double getTemperature() {
		return temperature;
	}

	public void setTemperature(Double temperature) {
		this.temperature = temperature;
	}

	@Override
     /**
      * This comparator controls the sort order of the keys.
      */
     public int compareTo(DateTemperaturePair pair) {
         int compareValue = this.yearMonth.compareTo(pair.getYearMonth());
         if (compareValue == 0) {
             compareValue = this.temperature.compareTo(pair.getTemperature());
         }
         return 1*compareValue;    // sort ascending
         //return -1*compareValue;   // sort descending
     }

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		/*yearMonth = new Text(dataInput.readUTF());
		day = new Text(dataInput.readUTF());*/
		if(dataInput  != null){
			yearMonth = new Text(WritableUtils.readString(dataInput));
			day = new Text(WritableUtils.readString(dataInput));
			temperature = dataInput.readDouble();
			//temperature = new DoubleWritable(dataInput.readDouble());
			//temperature = new Text(WritableUtils.readString(dataInput));
		}
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, yearMonth.toString());
		WritableUtils.writeString(dataOutput, day.toString());
		//WritableUtils.writeString(dataOutput, temperature.toString());
		dataOutput.writeDouble(temperature);
	}
 }
