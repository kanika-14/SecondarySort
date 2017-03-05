  import org.apache.hadoop.io.WritableComparable;
  import org.apache.hadoop.io.WritableComparator;
 
  public class DateTemperatureGroupingComparator
     extends WritableComparator {
 
      public DateTemperatureGroupingComparator() {
          super(DateTemperaturePair.class, true);
      }
      
      @SuppressWarnings("rawtypes")
      @Override
     /**
      * This comparator controls which keys are grouped
      * together into a single call to the reduce() method
      */
     public int compare(WritableComparable wc1, WritableComparable wc2) {
         DateTemperaturePair pair = (DateTemperaturePair) wc1;
         DateTemperaturePair pair2 = (DateTemperaturePair) wc2;
         System.out.println("** Comparator key 1 "+pair.getYearMonth()+" key 2 "+pair2.getYearMonth());
         return pair.getYearMonth().compareTo(pair2.getYearMonth());
     }
 }