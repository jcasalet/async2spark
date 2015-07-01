package asyncrepl;

import java.util.List;
import java.util.Iterator;

import com.google.common.base.Optional;

import java.util.regex.Pattern;

import scala.Tuple2;

import com.google.common.collect.Lists;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


public final class SlidingWindowThreshold {
	
   static final Pattern SPACE = Pattern.compile("[\\s+\\t]");
   static int WINDOWSIZE=10000;
   static int SLIDEINTERVAL=5000;
   static int ANOMALY_FREQUENCY=10;
  
   static MyReceiver myReceiver;
   static int port;
   static String host;
   static String masterURL;
   static int interval;
   static StorageLevel storageLevel;

  
   public static void main(String[] args) {
    if (args.length < 3) {
      System.err.println("Usage: StreamingWordCount <port> <masterURL> <interval>");
      System.exit(1);
    }
    port = Integer.parseInt(args[0]);
	masterURL = args[1];
	interval =Integer.parseInt(args[2]);

	// start the receiver
	storageLevel = StorageLevel.MEMORY_AND_DISK();
	myReceiver = new MyReceiver(host, port, storageLevel);	
	JavaStreamingContext jssc = new JavaStreamingContext(masterURL, "MapRDB2SparkAnomalizer", new Duration(interval));
	
    jssc.checkpoint("/tmp/streaming-wordcount.log");

    JavaReceiverInputDStream<String> messages = jssc.receiverStream(myReceiver);

   JavaDStream<String> lines = messages.map(new Function<String, String>() {
      /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public String call(String input) {
        return input;
      }
    }); 
   
   lines.cache();
   JavaDStream<String> windowDstream = lines.window(new Duration(WINDOWSIZE), new Duration(SLIDEINTERVAL));
   
   Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
	 new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
		private static final long serialVersionUID = 1L;
		public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {
	      Iterator<Integer> it = values.iterator();
	     Integer sum = state.or(0);
	      while(it.hasNext()) {
	    	  sum += it.next().intValue();
	      } 
	    	 Integer newSum = new Integer(sum);
	         return Optional.of(newSum);
	    }
	  };
	  
	  Function<Tuple2<String, Integer>, Boolean> thresholdFunction = 
			  new Function<Tuple2<String, Integer>, Boolean>() {
		  private static final long serialVersionUID = 1L;
			public Boolean call(Tuple2<String, Integer> value) {
				  if (value._2() > ANOMALY_FREQUENCY) {
					  System.out.println("anomaly detected on " + value._1()); 
					  return true;
					  }
				  else return false;  
			  }
	  };
	  
	  JavaDStream<String> words = windowDstream.flatMap(new FlatMapFunction<String, String>() {
		private static final long serialVersionUID = 1L;
			public Iterable<String> call(String x) {
		        return Lists.newArrayList(SPACE.split(x));
		      }
		    });

    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
      new PairFunction<String, String, Integer>() {
		private static final long serialVersionUID = 1L;
		public Tuple2<String, Integer> call(String s) {
          return new Tuple2<String, Integer>(s, 1);
        }
      }).reduceByKey(new Function2<Integer, Integer, Integer>() {
		private static final long serialVersionUID = 1L;
		public Integer call(Integer i1, Integer i2) {
          return i1 + i2;
        }
      });

    JavaPairDStream<String, Integer> runningCounts = wordCounts.updateStateByKey(updateFunction);
    
    JavaPairDStream<String, Integer> filterString = runningCounts.filter(thresholdFunction);
  
    filterString.print();
    jssc.start();
    jssc.awaitTermination();
    jssc.close();
  }
}


