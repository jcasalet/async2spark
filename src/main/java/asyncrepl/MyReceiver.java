package asyncrepl;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.util.logging.Level;
import java.util.logging.Logger;

public class MyReceiver extends Receiver<String> {
	
	private static final long serialVersionUID = 1L;
		String host = null;
		int port = -1;
		Socket socket = null;
		ServerSocket serverSocket = null;
		String userInput = null;
		BufferedReader reader = null;
		private static Logger logger=null;
	  

	public MyReceiver(String host , int port, StorageLevel storageLevel) {
		super(storageLevel);
		this.host = host;
		this.port = port;
		logger = Logger.getLogger(MyReceiver.class.getName()); 
	}

	  @Override
	  public void onStart() {
	    // Start the thread that receives data over a connection
		  try {
		      // connect to the socket
		      serverSocket = new ServerSocket(port);
		  }
		  catch (Exception e) {
		 	System.err.println(e.toString());
		 	}
		  new Thread()  {
		      @Override public void run() {
		    	  receive();
		      }
		  }.start();
	  }

	  @Override
	  public void onStop() {
	    // There is nothing much to do as the thread calling receive()
	    // is designed to stop by itself isStopped() returns false
		  try {
			  socket.close();
			  reader.close();
		  }
		  catch (Exception e) {System.err.println(e.toString()); }
	  }

	  /** Create a socket connection and receive data until receiver is stopped */
	  private void receive() {
	    try {
	      // Until stopped or connection broken continue reading
	    	do {
	 	        socket = serverSocket.accept();
	 	        reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
	    		userInput = reader.readLine();
	 		   	store(userInput);
	    	} while (! isStopped());    
	    	
	    	reader.close();
	    	socket.close();
	    	
	    } catch(ConnectException ce) {
	      // restart if could not connect to server
	     	logger.log(Level.SEVERE, this.getClass().getName() + " Failed to connect: host " + host + "port: " + port);
	     	restart("Could not connect", ce);
	    } catch(Throwable t) {
	      // restart if there is any other error
	    	logger.log(Level.SEVERE, "error receiving data");
	    	restart("Error receiving data", t);
	    }
	  }
}