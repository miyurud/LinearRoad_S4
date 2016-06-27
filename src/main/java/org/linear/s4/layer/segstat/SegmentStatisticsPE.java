/**
 * 
 */
package org.linear.s4.layer.segstat;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Set;

import org.apache.s4.core.App;

import org.apache.s4.base.Event;
import org.apache.s4.core.Stream;
import org.apache.s4.core.ProcessingElement;
import org.linear.s4.events.AccidentEvent;
import org.linear.s4.events.LAVEvent;
import org.linear.s4.events.NOVEvent;
import org.linear.s4.events.PositionReportEvent;
import org.linear.s4.util.Constants;

/**
 * @author miyuru
 *
 */

public class SegmentStatisticsPE extends ProcessingElement{
	private int peID = -1;
	private long currentSecond;
	private LinkedList<PositionReportEvent> posEvtList;
	private ArrayList<PositionReportEvent> evtListNOV;
	private ArrayList<PositionReportEvent> evtListLAV;
	private byte minuteCounter;
	private int lavWindow = 5; //This is LAV window in minutes
	transient Stream<Event> downStream;
	
    private long tupleCounter = 0;
    private int tupleCountingWindow = 5000;//This is in miliseconds
    private long previousTime = 0; //This is the time that the tuple measurement was taken previously
    private PrintWriter outLogger = null;
    private int dataRate = 0;
    private long currentTime = 0;
    private long expStartTime = 0; 
	
    public SegmentStatisticsPE(App app){
    	super(app);
    }
    
	public void onEvent(Event event){
		//System.out.println("Segstat partition id : " + getApp().getReceiver().getPartitionId());
		if(getApp().getReceiver().getPartitionId() == peID){
			//System.out.println("At Segstat --> " + event.toString());
			/*
	        switch(typeField){
        	case 0:
        		//posEvtList.add(new PositionReportEvent(fields));
        		process(new PositionReportEvent(fields));
        		break;
        	case 2:
        		log.info("Account balance report");
        		break;
        	case 3:
        		log.info("Expenditure report");
        		break;
        	case 4:
        		log.info("Travel time report");
        		break;
        }
			*/
			
			if(event instanceof PositionReportEvent){

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
				
				process(((PositionReportEvent)event));
			}
		}
	}
	
    public void process(PositionReportEvent evt){       	
		if(currentSecond == -1){
			currentSecond = evt.time;
		}else{
			if((evt.time - currentSecond) > 60){
				calculateNOV();
				
				evtListNOV.clear();
				
				currentSecond = evt.time;
				minuteCounter++;
				
				if(minuteCounter >= lavWindow){
					calculateLAV(currentSecond);
						
					//LAV list cannot be cleared because it need to keep the data for 5 minutes prior to any time t
					//evtListLAV.clear();
					
					minuteCounter = 0;
				}
			}
		}
		evtListNOV.add(evt);
		evtListLAV.add(evt);
    }

	private void calculateLAV(long currentTime) {
		float result = -1;
		float avgVelocity = -1;
		
		ArrayList<Byte> segList = new ArrayList<Byte>();
		Hashtable<Byte, ArrayList<Integer> > htResult = new Hashtable<Byte, ArrayList<Integer> >();  

		//First identify the number of segments
		Iterator<PositionReportEvent> itr = evtListLAV.iterator();
		byte curID = -1;
		while(itr.hasNext()){
			curID = itr.next().mile;
			
			if(!segList.contains(curID)){
				segList.add(curID);
			}
		}
		
		ArrayList<PositionReportEvent> tmpEvtListLAV = new ArrayList<PositionReportEvent>(); 
		float lav = -1;
		
		for(byte i =0; i < 2; i++){ //We need to do this calculation for both directions (west = 0; East = 1)
			Iterator<Byte> segItr = segList.iterator();
			int vid = -1;
			byte mile = -1;
			ArrayList<Integer> tempList = null;
			PositionReportEvent evt = null;
			long totalSegmentVelocity = 0;
			long totalSegmentVehicles = 0;
			
			//We calculate LAV per segment
			while(segItr.hasNext()){
				mile = segItr.next();
				itr = evtListLAV.iterator();
				
				while(itr.hasNext()){
					evt = itr.next();
					
					if((Math.abs((evt.time - currentTime)) < 300)){
						if((evt.mile == mile) && (i == evt.dir)){ //Need only last 5 minutes data only
							vid = evt.vid;
							totalSegmentVelocity += evt.speed;
							totalSegmentVehicles++;
						}
						
						if(i == 1){//We need to add the events to the temp list only once. Because we iterate twice through the list
							tmpEvtListLAV.add(evt);//Because of the filtering in the previous if statement we do not accumulate events that are older than 5 minutes
						}
					}
				}
				
				lav = ((float)totalSegmentVelocity/totalSegmentVehicles);
				if(!Float.isNaN(lav)){	
					/*
					try{
					    bytesMessage = jmsCtx_toll.getSession().createBytesMessage();

					    bytesMessage.writeBytes((Constants.LAV_EVENT_TYPE + " " + mile + " " + lav + " " + i).getBytes());
					    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
					    producer_toll.send(bytesMessage);
					}catch(JMSException e){
						e.printStackTrace();
					}*/

					//Here we communicate with TollPE
					LAVEvent lavEvent = new LAVEvent(mile, lav, i);
					//System.out.println(lavEvent.toString());
					downStream.put(lavEvent);
					
					totalSegmentVelocity = 0;
					totalSegmentVehicles = 0;
				}
			}
		}
			
		//We assign the updated list here. We have discarded the events that are more than 5 minutes duration
		evtListLAV = tmpEvtListLAV;	
	}
	
	private void calculateNOV() {
		ArrayList<Byte> segList = new ArrayList<Byte>();
		Hashtable<Byte, ArrayList<Integer> > htResult = new Hashtable<Byte, ArrayList<Integer> >();  
		
		//Get the list of segments first
		Iterator<PositionReportEvent> itr = evtListNOV.iterator();
		byte curID = -1;
		while(itr.hasNext()){
			curID = itr.next().mile;
			
			if(!segList.contains(curID)){
				segList.add(curID);
			}
		}
		
		Iterator<Byte> segItr = segList.iterator();
		int vid = -1;
		byte mile = -1;
		ArrayList<Integer> tempList = null;
		PositionReportEvent evt = null;

		//For each segment		
		while(segItr.hasNext()){
			mile = segItr.next();
			itr = evtListNOV.iterator();
			while(itr.hasNext()){
				evt = itr.next();
				
				if(evt.mile == mile){
					vid = evt.vid;
					
					if(!htResult.containsKey(mile)){
						tempList = new ArrayList<Integer>();
						tempList.add(vid);
						htResult.put(mile, tempList);
					}else{
						tempList = htResult.get(mile);
						tempList.add(vid);
						
						htResult.put(mile, tempList);
					}
				}
			}
		}
		
		Set<Byte> keys = htResult.keySet();
		
		Iterator<Byte> itrKeys = keys.iterator();
		int numVehicles = -1;
		mile = -1;
		
		while(itrKeys.hasNext()){
			mile = itrKeys.next();
			numVehicles = htResult.get(mile).size();
			
			//Here we communicate with TollPE
			NOVEvent novEvt = new NOVEvent(((int)Math.floor(currentSecond/60)), mile, numVehicles);
			//System.out.println(novEvt.toString());
			downStream.put(novEvt);
		}
	}
	
    @Override
    protected void onCreate() {
    	//System.out.println("On create called on SEGSTAT PE");
        Properties properties = new Properties();
        InputStream propertiesIS = SegmentStatisticsPE.class.getClassLoader().getResourceAsStream(Constants.CONFIG_FILENAME);
        if (propertiesIS == null)
        {
            throw new RuntimeException("Properties file '" + Constants.CONFIG_FILENAME + "' not found in classpath");
        }
        
        try{
        	properties.load(propertiesIS);
        }catch(IOException e){
        	e.printStackTrace();
        }
        
        peID = Integer.parseInt(properties.getProperty(Constants.SEGSTAT_PE_ID));
        //System.out.println("SEGSTAT PEID : " + peID);
        
        posEvtList = new LinkedList<PositionReportEvent>();
        evtListNOV = new ArrayList<PositionReportEvent>();
        evtListLAV = new ArrayList<PositionReportEvent>();
        
        //Here we need to check whether we are at the correct PE to start logging the data rates
        if(getApp().getReceiver().getPartitionId() == peID){
        	//System.out.println("Segstat partition ID : " + getApp().getReceiver().getPartitionId());
			currentTime = System.currentTimeMillis();
			try {
				outLogger = new PrintWriter(new BufferedWriter(new FileWriter("/home/miyuru/projects/pamstream/experiments/s4-segstat-rate.csv", true)));
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
    
	public void setDownStream(Stream<Event> accidentEventStream){
		this.downStream = accidentEventStream;
	}
}
