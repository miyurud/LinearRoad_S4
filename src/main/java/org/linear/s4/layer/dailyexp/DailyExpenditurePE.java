/**
 * 
 */
package org.linear.s4.layer.dailyexp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.logging.Logger;
import java.net.InetAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.s4.base.Event;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;
import org.linear.s4.util.Utilities;
import org.linear.s4.input.HistoryLoadingNotifier;
import org.linear.s4.events.ExpenditureEvent;
import org.linear.s4.events.HistoryEvent;
import org.linear.s4.events.OutDailyExpenditureEvent;
import org.linear.s4.util.Constants;
import org.linear.s4.util.ZookeeperInterface;

import org.apache.s4.core.App;

/**
 * @author miyuru
 *
 */
//DailyExpenditure

public class DailyExpenditurePE extends ProcessingElement{
	private int peID = -1;
	transient Stream<Event> downStream;
    private LinkedList<ExpenditureEvent> expEvtList;
    private LinkedList<HistoryEvent> historyEvtList;
    //private static Log log = LogFactory.getLog(DailyExpensesListener.class);
    static Logger logger = Logger.getLogger(DailyExpenditurePE.class.getName());
    
    private long tupleCounter = 0;
    private int tupleCountingWindow = 5000;//This is in miliseconds
    private long previousTime = 0; //This is the time that the tuple measurement was taken previously
    private PrintWriter outLogger = null;
    private int dataRate = 0;
    private long currentTime = 0;
    private long expStartTime = 0; 

    public DailyExpenditurePE(App app){
    	super(app);
    	System.out.println("------> 1 ------>");
        Properties properties = new Properties();
        InputStream propertiesIS = DailyExpenditurePE.class.getClassLoader().getResourceAsStream(Constants.CONFIG_FILENAME);
        if (propertiesIS == null)
        {
            throw new RuntimeException("Properties file '" + Constants.CONFIG_FILENAME + "' not found in classpath");
        }
        
        try{
        	properties.load(propertiesIS);
        }catch(IOException e){
        	System.out.println(e.getMessage());
        }
        
        logger.info("------> 2 ------>");
        peID = Integer.parseInt(properties.getProperty(Constants.DAILY_EXP_PE_ID));
        expEvtList = new LinkedList<ExpenditureEvent>();
        historyEvtList = new LinkedList<HistoryEvent>();
        
        
        //The following line was originally commented out. But we uncommented to support the dat rate logging facility.
    	//if(getApp().getReceiver().getPartitionId() == peID){
        
        //Here we need to check whether we are at the correct PE to start logging the data rates
        if(getApp().getReceiver().getPartitionId() == peID){
			currentTime = System.currentTimeMillis();
			try {
				outLogger = new PrintWriter(new BufferedWriter(new FileWriter("/home/miyuru/projects/pamstream/experiments/s4-dailyexp-rate.csv", true)));
	          outLogger.println("----- New Session -----<date-time>,<wall-clock-time(s)>,<physical-tuples-in-last-period>,<physical-tuples-data-rate>");
				outLogger.println("Date,Wall clock time (s),TTuples,Data rate (tuples/s)");
			} catch (IOException e1) {
				e1.printStackTrace();
			} 
           
        
    		String historyFile = properties.getProperty(org.linear.s4.util.Constants.LINEAR_HISTORY);
    	logger.info("------> 3 ------>");
    		HistoryLoadingNotifier notifierObj = new HistoryLoadingNotifier(false); 
    		notifierObj.start();//The notification server starts at this point
    	logger.info("------> 4 ------>");
    		loadHistoricalInfo(historyFile);
    		notifierObj.setStatus(true);//At this moment we notify all the listeners that we have done loading the history data
    	logger.info("------> 5 ------>");
          	try{
        		String host = ZookeeperInterface.getValueAtPath("/lr/history/host");
        		logger.info("------> 6 ------>");
        		if(host == null){
        			ZookeeperInterface.createGroup("lr");
        			ZookeeperInterface.createGroup("lr/history");
        			ZookeeperInterface.createGroup("lr/history/host");
        		}    		
        		logger.info("------> 7 ------>");
        		ZookeeperInterface.setValueAtPath("/lr/history/host", InetAddress.getLocalHost().getHostAddress());
        		logger.info("------> 8 ------>");
        	}catch(Exception e){
        		logger.info(e.getMessage());
        	}
        logger.info("------> 9 ------>");

    	}
    	
//    	System.out.println("getApp() is null : " + (getApp()==null));
//    	System.out.println("getApp().getReceiver() is null : " + (getApp().getReceiver()==null));
    }
    
	public void onEvent(Event event){
		//System.out.println("DailyExp partition ID : " + getApp().getReceiver().getPartitionId());
		
		if(getApp().getReceiver().getPartitionId() == peID){
			
			//System.out.println("At Daily Expenditure --> " + event.toString());
			
			//for the data rate calculation, need to think whether we should check whether this is an expenditure event or not.
			//At the moment we check the type of the event, because we should think about the effective data rate, that contributes to the
			//final outcome of the application.
			if(event instanceof ExpenditureEvent){
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
				
				
				process(((ExpenditureEvent)event));
			}
		}
	}
	
    @Override
    public void onCreate() {
    	//System.out.println("On create alled on DAILYEXP PE");
        Properties properties = new Properties();
        InputStream propertiesIS = DailyExpenditurePE.class.getClassLoader().getResourceAsStream(Constants.CONFIG_FILENAME);
        if (propertiesIS == null)
        {
            throw new RuntimeException("Properties file '" + Constants.CONFIG_FILENAME + "' not found in classpath");
        }
        
        try{
        	properties.load(propertiesIS);
        }catch(IOException e){
        	e.printStackTrace();
        }
        
        peID = Integer.parseInt(properties.getProperty(Constants.DAILY_EXP_PE_ID));
        //System.out.println("DAILYEXP PEID : " + peID);
        
        /*-------------------------------------------*/
        

        
        /*----------------------------------------*/
        
        
        
        

        /*
		String historyFile = properties.getProperty(org.linear.s4.util.Constants.LINEAR_HISTORY);
		
		HistoryLoadingNotifier notifierObj = new HistoryLoadingNotifier(false); 
		System.out.println("Started loading history");
		notifierObj.start();//The notification server starts at this point
		
		loadHistoricalInfo(historyFile);
		
		notifierObj.setStatus(true);//At this moment we notify all the listeners that we have done loading the history data
		System.out.println("Done loading history");*/
    }
    
    public void process(ExpenditureEvent evt){    	
		int len = 0;
		
		Iterator<HistoryEvent> itr = historyEvtList.iterator();
		int sum = 0;
		while(itr.hasNext()){
			HistoryEvent histEvt = (HistoryEvent)itr.next();
			
			if((histEvt.carid == evt.vid) && (histEvt.x == evt.xWay) && (histEvt.d == evt.day)){
				sum += histEvt.daily_exp;
			}					
		}
		
		OutDailyExpenditureEvent outEvt = new OutDailyExpenditureEvent(evt.vid, sum);
		//System.out.println(outEvt.toString());
		downStream.put(outEvt);
		
				/*
				try{
				    bytesMessage = jmsCtx_output.getSession().createBytesMessage();

				    bytesMessage.writeBytes((Constants.DAILY_EXP_EVENT_TYPE + " " + evt.vid + " " + sum).getBytes());
				    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
				    producer_output.send(bytesMessage);
				}catch(JMSException e){
					e.printStackTrace();
				}
				*/
    }

	public void loadHistoricalInfo(String inputFileHistory) {
		BufferedReader in;
		try {
			in = new BufferedReader(new FileReader(inputFileHistory));
		
			String line;
			int counter = 0;
			int batchCounter = 0;
			int BATCH_LEN = 10000;//A batch size of 1000 to 10000 is usually OK		
			Statement stmt;
			StringBuilder builder = new StringBuilder();
					
			//log.info(Utilities.getTimeStamp() + " : Loading history data");
			System.out.println(Utilities.getTimeStamp() + " : Loading history data");
			while((line = in.readLine()) != null){			
				//#(1 8 0 55)
				/*
				0 - Car ID
				1 - day
				2 - x - Expressway number
				3 - daily expenditure
				*/
				
				String[] fields = line.split(" ");
				fields[0] = fields[0].substring(2);
				fields[3] = fields[3].substring(0, fields[3].length() - 1);
				
				historyEvtList.add(new HistoryEvent(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]), Integer.parseInt(fields[2]), Integer.parseInt(fields[3])));
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(Utilities.getTimeStamp() + " : Done Loading history data");
		//log.info(Utilities.getTimeStamp() + " : Done Loading history data");
		//Just notfy this to the input event injector so that it can start the data emission process
		try {
			PrintWriter writer = new PrintWriter("done.txt", "UTF-8");
			writer.println("\n");
			writer.flush();
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}
    
    @Override
    protected void onRemove() {
    }
    
	public void setDownStream(Stream<Event> downEventStream){
		this.downStream = downEventStream;
	}
}
