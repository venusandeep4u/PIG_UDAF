package myUDAF;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

public class Commision_UDAF extends EvalFunc<Double>
{
    public Double exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        Double sum = 0.0,temp=0.0;
        String sad = null;
        try{
            DataBag ref_regions = (DataBag)input.get(0);
           
          for (Iterator<Tuple> iterator = ref_regions.iterator(); iterator.hasNext();) {
           
        	  temp = (Double) iterator.next().get(3);
              
        	  sum = sum+temp;
             
          }
         
            return sum;
         }catch(Exception e){
             System.err.println(e.getMessage());
             return (Double)null;
         }
    }
}
