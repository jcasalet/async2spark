package asyncrepl;


//import com.mapr.fs.gateway.external.MapRExternalSink;
//import com.mapr.fs.gateway.external.MapRExternalSinkException;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import com.mapr.fs.gateway.external.MapRExternalSink;
import com.mapr.fs.gateway.external.MapRExternalSinkException;

public class ToSpark implements MapRExternalSink {
	private static final Logger logger = Logger.getLogger(MyReceiver.class.getName());
	File myLogFile, myConfigFile;
	BufferedWriter myLogWriter, mySocketWriter;
	BufferedReader myConfigReader;
	StringBuffer myDataBuffer;
	
	Socket mySocket;
	
	String myHost;
	int myPort;
	String myLogFileName=null;
	
	String sinkClassPath;
	String mapToString;
	 	  
	public  ToSpark() {
		myDataBuffer = new StringBuffer();
	}	
	
	
	 @Override
	public void loadConfig(String gatewayConfigFile) throws MapRExternalSinkException {
		 // 
		return;
	}
	
	 @Override
	  public String getDestinationName(Map myMap) throws MapRExternalSinkException {
		 return "maprdb2spark";
	 }
	 
	 @Override
	  public String getDestinationType(Map myMap) throws MapRExternalSinkException {
	    return "Spark";
	  }
	 
	 @Override
	public void verifyConfigMapSanity(Map sinkConfigMap) throws MapRExternalSinkException {
	  	// gets called when replica is added via maprcli
	  	return;
	}  
	
	@Override
	public void connect(String sinkConfigFile) throws MapRExternalSinkException {
    	// gets called every 2ms with stuff in buckets by default -- tunable?
		try {
			BufferedReader myConfigFileReader = new BufferedReader(new FileReader(sinkConfigFile));
			String line = null;
			while ((line = myConfigFileReader.readLine()) != null) {
				if(line.startsWith("sparksink.server.name:")) {
					myHost = line.split(": ")[1];
				}
				else if(line.startsWith("sparksink.server.port:")) {
					myPort = Integer.parseInt(line.split(": ")[1]);
				}
				else if(line.startsWith("sparksink.logfile.name:")) {
					myLogFileName = line.split(": ")[1];
				}
				
			}
			
			if(myLogFileName==null) {
				myLogFile = new File("/tmp/gateway-default.out");
			}
			else
				myLogFile = new File(myLogFileName);

			
			try {
				if (! myLogFile.exists()) 
					myLogFile.createNewFile();
				myLogWriter = new BufferedWriter(new FileWriter(myLogFile.getAbsoluteFile()));
			}
			catch (Exception e) {System.err.println(e.toString()); }
		}
		catch (Exception e) {
			System.err.println(e.toString());
			logger.log(Level.SEVERE, this.getClass().getName() + " failed to open file " + myLogFileName);
		}
		try {
			mySocket = new Socket(myHost, myPort);
			mySocketWriter = new BufferedWriter(new OutputStreamWriter(mySocket.getOutputStream()));
			myLogWriter.append("connected to host:port " + myHost + ":" + myPort);
			myLogWriter.flush();
		}
	    catch (Exception e) {
	    	System.err.println("connect " + e.toString());    
	    }
		return;  	
	}
	 
	@Override
	@SuppressWarnings("deprecation")
	public void update(Result result) throws MapRExternalSinkException {
		// called whenever hbase row is updated
		myDataBuffer.append("update:=" + "|");
		try {
			for(KeyValue keyValue : result.list()) {
				myDataBuffer.append("Key:" + new String(keyValue.getRow()) + "|");
				myDataBuffer.append("Family:" + new String(keyValue.getFamily()) + "|");
				myDataBuffer.append("Qualifier:" + new String(keyValue.getQualifier(), "UTF-8") + "|");
				myDataBuffer.append("Value:" + new String(keyValue.getValue(), "UTF-8") + "|");
				myDataBuffer.append("Timestamp:" + keyValue.getTimestamp() + "|");    
			}	
			myDataBuffer.append("\n");
			// serialize to log and socket
			myStreamWriter(myLogWriter, myDataBuffer);
			myStreamWriter(mySocketWriter, myDataBuffer);
			myDataBuffer.delete(0,  myDataBuffer.length());
		}
		catch (Exception e) {System.err.println("update " + e.toString());}
		return;
	}
	 
	@Override
	public void delete(String key) throws MapRExternalSinkException {
		// called whenever hbase row is deleted
		myDataBuffer.append("delete: " + key);
		try {
			myStreamWriter(myLogWriter, myDataBuffer);
			myStreamWriter(mySocketWriter, myDataBuffer);
		}
		catch (Exception e) {System.err.println("delete" + e.toString()); }	
		myDataBuffer.delete(0,  myDataBuffer.length());
		return;
	}
	 
	@Override
	public boolean compare(Result result) throws MapRExternalSinkException {
		// just return true for now (?)
		myDataBuffer = new StringBuffer("compare: " + result.getRow());
	    try {
			myStreamWriter(myLogWriter, myDataBuffer);
			myStreamWriter(mySocketWriter, myDataBuffer);
		}
		catch (Exception e) {System.err.println("compare " + e.toString()); }
		myDataBuffer.delete(0,  myDataBuffer.length());
	    return true;
	}
	 
	@Override
	public void deleteColumnFamily(String key, String family) throws MapRExternalSinkException {
		// called when all data for a family for a key got deleted
		myDataBuffer.append("deleteColumnFamily: " + key + " " + family);
	    try {
			myStreamWriter(myLogWriter, myDataBuffer);
			myStreamWriter(mySocketWriter, myDataBuffer);
		}
		catch (Exception e) {System.err.println("deleteColumnFamily" + e.toString()); }
		myDataBuffer.delete(0,  myDataBuffer.length());
	    return;
	}
	 
	@Override
	public void deleteColumn(String key, String family, String column) throws MapRExternalSinkException {
		// called when all data for a column got deleted
		myDataBuffer.append("deleteColumn: " + key + " " + family + " " + column);
	    try {
			myStreamWriter(myLogWriter, myDataBuffer);
			myStreamWriter(mySocketWriter, myDataBuffer);
		}
		catch (Exception e) {System.err.println("deleteColumn" + e.toString()); }
		myDataBuffer.delete(0,  myDataBuffer.length());
	    return;
	}
	 
	@Override
	public void flush() throws MapRExternalSinkException {
		// called periodically by gateway -- will flush everything in temp disk buffer 
		myDataBuffer.append("flush");

	    try {
	    	myStreamWriter(myLogWriter, myDataBuffer);
			myStreamWriter(mySocketWriter, myDataBuffer);
		}
		catch (Exception e) {System.err.println("flush " + e.toString()); }
		myDataBuffer.delete(0,  myDataBuffer.length());
	    return;
	}
	 
	@Override
	public void close() throws MapRExternalSinkException {
		// opposite of connect()
		myDataBuffer.append("close: ");

	    try {
	    	myStreamWriter(myLogWriter, myDataBuffer);
			myStreamWriter(mySocketWriter, myDataBuffer);
		}
		catch (Exception e) {System.err.println("close " + e.toString()); }
		myDataBuffer.delete(0,  myDataBuffer.length());
	    return;
	 }
	
	// my utility methods
	public void myStreamWriter(BufferedWriter myGenericWriter, StringBuffer myData) throws Exception {
		try {
			myGenericWriter.append(myData.toString());
			myGenericWriter.flush();
		}
		catch (Exception e) {System.err.println("myStreamWriter " + e.toString()); }
		return;
	}
	  
}
