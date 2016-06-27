package org.linear.s4.input;

/**
 * This class first loads the historical data. Once the historical data is loaded we
 * start injecting tuples.
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.s4.base.Event;
import org.apache.s4.core.adapter.AdapterApp;
import org.apache.s4.core.RemoteStream;
import org.linear.s4.events.AccountBalanceEvent;
import org.linear.s4.events.ExpenditureEvent;
import org.linear.s4.events.PositionReportEvent;
import org.linear.s4.util.Constants;
import org.linear.s4.util.Utilities;

public class InputEventInjectorAdapter extends AdapterApp
{
	private static Log log = LogFactory.getLog(InputEventInjectorAdapter.class);
	
    private long tupleCounter = 0;
    private int tupleCountingWindow = 5000;//This is in miliseconds
    private long previousTime = 0; //This is the time that the tuple measurement was taken previously
    private PrintWriter outLogger = null;
    private int dataRate = 0;
    private long currentTime = 0;
    private long expStartTime = 0;  
	
    @Override
    protected void onStart(){
    	new Thread(new Runnable(){
    		@Override
    		public void run(){
    	   	    //First we get the input properties 
    			//System.out.println("AAAAAAAAAAAAAAAAA");
    			//log.info("AAAAAAAAAAAAAAAAA");
    	        Properties props = new Properties();
    	        //System.out.println("AAAAAAAAAAAAAAAAA222222");
    	        //log.info("AAAAAAAAAAAAAAAAA222222");
    	        InputStream propertiesIS = InputEventInjectorAdapter.class.getClassLoader().getResourceAsStream(org.linear.s4.util.Constants.CONFIG_FILENAME);
    	        //System.out.println("AAAAAAAAAAAAAAAAA33333333");
    	        //log.info("AAAAAAAAAAAAAAAAA33333333");
    	        if(propertiesIS == null){
    	        	throw new RuntimeException("Properties file '" + org.linear.s4.util.Constants.CONFIG_FILENAME + "' not found in the classpath");
    	        }
    	        //System.out.println("AAAAAAAAAAAAAAAAA444444444");
    	        try{
    	        	props.load(propertiesIS);
    	        }catch(IOException e){
    	        	//System.out.println("AAAAAAAAAAAAAAAAA555555");
    	        	//log.info("AAAAAAAAAAAAAAAAA555555");
    	        	e.printStackTrace();
    	        }
    	        
    	        //Next we wait until the history information loading gets completed
    	        int c = 0;
    	        while(!HistoryLoadingNotifierClient.isHistoryLoaded()){
    	        	try{
    	        		Thread.sleep(1000);//just wait one second and check again
    	        	}catch(InterruptedException e){
    	        		//Just ignore
    	        	}
    	        	System.out.println(c + " : isHistoryLoading...");
    	        	c++;
    	        }
    	        
    	        System.out.println("Done loading the history....");
    	        
    	        
    	        //System.out.println("BBBBBBB111");
    	        //log.info("BBBBBBB111");
    	    	String carDataFile = props.getProperty(org.linear.s4.util.Constants.LINEAR_CAR_DATA_POINTS);    			
    	    	//log.info(carDataFile);
    	    	//System.out.println("BBBBBBB2222");
    	    	//log.info("BBBBBBB2222");
    	    	emitTuples(carDataFile);
    		}
    	}).start();
    
		currentTime = System.currentTimeMillis();
		try {
			outLogger = new PrintWriter(new BufferedWriter(new FileWriter("/home/miyuru/projects/pamstream/experiments/s4-input-injector-rate.csv", true)));
			outLogger.println("----- New Session -----<date-time>,<wall-clock-time(s)>,<unique-tuples-in-last-period>,<physical-tuples-in-last-period>,<physical-tuples-data-rate>");
			outLogger.println("Date,Wall clock time (s),UTuples,TTuples,Data rate (tuples/s)");
		} catch (IOException e1) {
			e1.printStackTrace();
		}
    	
    }

    public void emitTuples(String carDataFile){
		try{
			BufferedReader in = new BufferedReader(new FileReader(carDataFile));
			String line;
			RemoteStream remoteStream = getRemoteStream();
			
			/*
			 
			 The input file has the following format
			 
			    Cardata points input tuple format
				0 - type of the packet (0=Position Report, 1=Account Balance, 2=Expenditure, 3=Travel Time [According to Richard's thesis. See Below...])
				1 - Seconds since start of simulation (i.e., time is measured in seconds)
				2 - Car ID (0..999,999)
				3 - An integer number of miles per hour (i.e., speed) (0..100)
				4 - Expressway number (0..9)
				5 - The lane number (There are 8 lanes altogether)(0=Ramp, 1=Left, 2=Middle, 3=Right)(4=Left, 5=Middle, 6=Right, 7=Ramp)
				6 - Direction (west = 0; East = 1)
				7 - Mile (This corresponds to the seg field in the original table) (0..99)
				8 - Distance from the last mile post (0..1759) (Arasu et al. Pos(0...527999) identifies the horizontal position of the vehicle as a meaure of number of feet
				    from the western most position on the expressway)
				9 - Query ID (0..999,999)
				10 - Starting milepost (m_init) (0..99)
				11 - Ending milepost (m_end) (0..99)
				12 - day of the week (dow) (0=Sunday, 1=Monday,...,6=Saturday) (in Arasu et al. 1...7. Probably this is wrong because 0 is available as DOW)
				13 - time of the day (tod) (0:00..23:59) (in Arasu et al. 0...1440)
				14 - day
				
				Notes
				* While Richard thesis explain the input tuple formats, it is probably not correct.
				* The correct type number would be 
				* 0=Position Report (Type=0, Time, VID, Spd, Xway, Lane, Dir, Seg, Pos)
				* 2=Account Balance Queries (because all and only all the 4 fields available on tuples with type 2) (Type=2, Time, VID, QID)
				* 3=Daily expenditure (Type=3, Time, VID, XWay, QID, Day). Here if day=1 it is yesterday. d=69 is 10 weeks ago. 
				* 4=Travel Time requests (Types=4, Time, VID, XWay, QID, Sinit, Send, DOW, TOD)
				
				history data input tuple format
				0 - Car ID
				1 - day
				2 - x - Expressway number
				3 - daily expenditure
				
				E.g.
				#(1 3 0 31)
				#(1 4 0 61)
				#(1 5 0 34)
				#(1 6 0 30)
				#(1 7 0 63)
				#(1 8 0 55)
				
				//Note that since the historical data table's key is made out of several fields it was decided to use a relation table 
				//instead of using a hashtable
			 
			 */
			
			
			while((line = in.readLine()) != null){	
				//System.out.println(line);
				
//				Long time = -1l;
//				Integer vid = -1;
//				Integer qid = -1;
//				Byte spd = -1;
//				Byte xway = -1;
//				Byte mile = -1;
//				Short ofst = -1;
//				Byte lane = -1;
//				Byte dir = -1;
//				Integer day = -1;
				
				line = line.substring(2, line.length() - 1);
				
				String[] fields = line.split(" ");
				byte typeField = Byte.parseByte(fields[0]); 
				
				String key = null;
				String value = null;
				
				//In the case of Storm it seems that we need not to send the type of the tuple through the network. It is because 
				//the event itself has some associated type. This seems to be an advantage of Storm compared to other message passing based solutions.
				switch(typeField){
					case 0:
						PositionReportEvent positionRpt = new PositionReportEvent(fields);
						remoteStream.put(positionRpt);
						tupleCounter += 3; //Although we are emitting just one tuple here, to be more accurate, and be equal with the rst of the evaluations we add three here.
						break;
					case 2:
						AccountBalanceEvent accBalanceEvt = new AccountBalanceEvent(fields);
						remoteStream.put(accBalanceEvt);
						tupleCounter += 1;
						break;
					case 3 : 
						ExpenditureEvent evt = new ExpenditureEvent(fields);
						remoteStream.put(evt);
						tupleCounter += 1;
						break;
					case 4:
						//This is a travel time report (Types=4, Time, VID, XWay, QID, Sinit, Send, DOW, TOD)
						break;
					case 5:
						System.out.println("Travel time query was issued : " + line);
						break;
				}
				
				currentTime = System.currentTimeMillis();
				
				if (previousTime == 0){
					previousTime = System.currentTimeMillis();
		            expStartTime = previousTime;
				}
				
				if ((currentTime - previousTime) >= tupleCountingWindow){
					dataRate = Math.round((tupleCounter*1000)/(currentTime - previousTime));//Need to multiple by thousand because the time is in ms
					Date date = new Date(currentTime);
					DateFormat formatter = new SimpleDateFormat("HH:mm:ss:SSS");
					outLogger.println(formatter.format(date) + "," + (currentTime - expStartTime) + "," + dataRate);
					outLogger.flush();
		            tupleCounter = 0;
		            previousTime = currentTime;
				}
			}
			System.out.println("Done emitting the input tuples...");
		}catch(IOException ec){
			ec.printStackTrace();
		}
    }
}