/**
 * 
 */
package org.linear.s4.layer.accbalance;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;

import org.apache.s4.core.Stream;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.base.Event;
import org.linear.s4.events.PositionReportEvent;
import org.linear.s4.events.AccountBalanceEvent;
import org.linear.s4.events.TollCalculationEvent;
import org.linear.s4.events.OutAccountBalanceEvent;
import org.linear.s4.util.Constants;

import org.apache.s4.core.App;


/**
 * @author miyuru
 *
 */
public class AccountBalancePE extends ProcessingElement {
	private int peID = -1;
	transient Stream<Event> downStream;
    private HashMap<Integer, Integer> tollList;
	
    private long tupleCounter = 0;
    private int tupleCountingWindow = 5000;//This is in miliseconds
    private long previousTime = 0; //This is the time that the tuple measurement was taken previously
    private PrintWriter outLogger = null;
    private int dataRate = 0;
    private long currentTime = 0;
    private long expStartTime = 0;  
    
    public AccountBalancePE(App app){
    	super(app);
    }
    
	public void onEvent(Event event){
		if(getApp().getReceiver().getPartitionId() == peID){
			
			tupleCounter += 1;
			currentTime = System.currentTimeMillis();
			
			if (previousTime == 0){
				previousTime = System.currentTimeMillis();
	            expStartTime = previousTime;
			}
			
			if ((currentTime - previousTime) >= tupleCountingWindow){
				dataRate = Math.round((tupleCounter*1000)/(currentTime - previousTime));//need to multiply by thousand to compensate for ms time unit
				Date date = new Date(currentTime);
				DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
				outLogger.println(formatter.format(date) + "," + Math.round((currentTime - expStartTime)/1000) + "," + tupleCounter + "," + dataRate);
				outLogger.flush();
				tupleCounter = 0;
				previousTime = currentTime;
			}
			
			if(event instanceof AccountBalanceEvent){
			   process(((AccountBalanceEvent)event));
			}else if(event instanceof TollCalculationEvent){
			   TollCalculationEvent tollEvt = (TollCalculationEvent)event;
	    	   int key = tollEvt.vid;
	    	   int value = tollEvt.toll;
	    	   
	    	   Integer kkey = (Integer)tollList.get(key);
	    	   
	    	   //System.out.println("key : " + key + " value : " + value);
	    	   
	    	   if(kkey != null){
	    		   tollList.put(key, (kkey + value)); //If the car id is already in the hashmap we need to add the tool to the existing toll.
	    	   }else{
	    		   tollList.put(key, value);
	    	   }
			}
		}
	}

    public void process(AccountBalanceEvent evt){		    		
		if(evt != null){
			Integer item = (Integer)tollList.get(evt.vid);
			if(item != null){
				OutAccountBalanceEvent outAccBalEvt = new OutAccountBalanceEvent(evt.qid, item);
				//System.out.println("At AccountBalance --> " + outAccBalEvt.toString());
				downStream.put(outAccBalEvt);
			}
		}
    }
	
    @Override
    protected void onCreate() {
    	//System.out.println("On create alled on ACCBALANCE PE");
        Properties properties = new Properties();
        InputStream propertiesIS = AccountBalancePE.class.getClassLoader().getResourceAsStream(Constants.CONFIG_FILENAME);
        if (propertiesIS == null)
        {
            throw new RuntimeException("Properties file '" + Constants.CONFIG_FILENAME + "' not found in classpath");
        }
        
        try{
        	properties.load(propertiesIS);
        }catch(IOException e){
        	e.printStackTrace();
        }
        
        peID = Integer.parseInt(properties.getProperty(Constants.ACCBALANCE_PE_ID));   	
                
        this.tollList = new HashMap<Integer, Integer>();
        
        //Here we need to check whether we are at the correct PE to start logging the data rates
        //System.out.println("AccountBalance partition Id : " + getApp().getReceiver().getPartitionId() );
        if(getApp().getReceiver().getPartitionId() == peID){
			currentTime = System.currentTimeMillis();
			try {
				outLogger = new PrintWriter(new BufferedWriter(new FileWriter("/home/miyuru/projects/pamstream/experiments/s4-accbalance-rate.csv", true)));
	          outLogger.println("----- New Session -----<date-time>,<wall-clock-time(s)>,<physical-tuples-in-last-period>,<physical-tuples-data-rate>");
				outLogger.println("Date,Wall clock time (s),TTuples,Data rate (tuples/s)");
			} catch (IOException e1) {
				e1.printStackTrace();
			} 
        }
        
    }

    @Override
    protected void onRemove() {
    }
    
	public void setDownStream(Stream<Event> downEventStream){
		this.downStream = downEventStream;
	}
}
