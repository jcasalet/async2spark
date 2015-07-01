package asyncrepl;

import java.util.Collections;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;



public class FromMapRDB {
	static MyReceiver myReceiver;
	static int port;
	static String host;
	static String masterURL;
	static int interval;
	static StorageLevel storageLevel;

	public static void main(String[] args) {
		if (args.length != 3) {
			System.err.println("usage: FromMapRDB <port> <masterURL> <interval>");
			System.exit(1);
		}
		
		// parse command-line args
		port = Integer.parseInt(args[0]);
		masterURL = args[1];
		interval =Integer.parseInt(args[2]);
		
		// start the receiver
		storageLevel = StorageLevel.MEMORY_AND_DISK();
		myReceiver = new MyReceiver(host, port, storageLevel);	
		JavaStreamingContext jsc = new JavaStreamingContext(masterURL, "MapRDB2Spark", new Duration(interval));
		//jsc.checkpoint("/tmp/mystreaming-checkpoint.txt");

		
		// configure logging
		 @SuppressWarnings("unchecked")
		List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
		    loggers.add(LogManager.getRootLogger());
		    for ( Logger logger : loggers ) {
		        logger.setLevel(Level.ERROR);
		    }
		
		// create RDD based on receiver stream
		JavaDStream<String> myReceiverStream = jsc.receiverStream(myReceiver);
		
		// create RDD from receiver stream
		JavaDStream<String> lines = myReceiverStream.map(new Function<String, String>() {
			private static final long serialVersionUID = 1L;
			public String call(String input) {
		          return input;
		    }
		 }); 
		//lines.cache();
		
		lines.print();
		jsc.start();
		jsc.awaitTermination();
		
	}
}
