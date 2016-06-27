/**
 * 
 */
package org.linear.s4.layer.accident;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.s4.base.Event;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;
import org.linear.s4.layer.toll.TollCalculationPE;
import org.linear.s4.events.NOVEvent;
import org.linear.s4.events.AccidentEvent;
import org.linear.s4.util.Constants;
import org.linear.s4.events.AccountBalanceEvent;
import org.linear.s4.events.PositionReportEvent;

import org.apache.s4.core.App;

/**
 * @author miyuru
 *
 */
public class AccidentDetectionPE extends ProcessingElement{
	transient Stream<Event> downStream;
	private int peID = -1;
	
    private long tupleCounter = 0;
    private int tupleCountingWindow = 5000;//This is in miliseconds
    private long previousTime = 0; //This is the time that the tuple measurement was taken previously
    private PrintWriter outLogger = null;
    private int dataRate = 0;
    private long currentTime = 0;
    private long expStartTime = 0; 
    
    private static Log log = LogFactory.getLog(AccidentDetectionPE.class);
	
    public AccidentDetectionPE(App app){
    	super(app);
    }
    
	public void setDownStream(Stream<Event> accidentEventStream){
		this.downStream = accidentEventStream;
	}
	
	public void onEvent(Event event){
		//System.out.println("AccidentDetect partitin ID : " + getApp().getReceiver().getPartitionId());
		//We do any processing in this node only if the PE ID is ACCIDENT_PE_ID
		if(getApp().getReceiver().getPartitionId() == peID){
			if(event instanceof PositionReportEvent){
				//System.out.println("At Accident detection --> " + event.toString());
				//log.info("At Accident detection --> " + event.toString());
				
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
				
				PositionReportEvent posEvt = (PositionReportEvent)event; //Since we konw it is PositionReport event
				Car c = new Car(posEvt.time, posEvt.vid, posEvt.speed,
						posEvt.xWay, posEvt.lane, posEvt.dir, posEvt.mile);
	
				detect(c);
			}
		}
	}
	
    public void detect(Car c) {	
		if (c.speed > 0) {
			remove_from_smashed_cars(c);
			remove_from_stopped_cars(c);
		} else if (c.speed == 0) {
			if (is_smashed_car(c) == false) {
				if (is_stopped_car(c) == true) {
					renew_stopped_car(c);
				} else {
					stopped_cars.add(c);
				}
				
				int flag = 0;
				for (int i = 0; i < stopped_cars.size() -1; i++) {
					Car t_car = (Car)stopped_cars.get(i);
					if ((t_car.carid != c.carid)&&(!t_car.notified)&&((c.time - t_car.time) <= 120) && (t_car.posReportID >= 3) &&
							((c.xway0 == t_car.xway0 && c.mile0 == t_car.mile0 && c.lane0 == t_car.lane0 && c.offset0 == t_car.offset0 && c.dir0 == t_car.dir0) &&
							(c.xway1 == t_car.xway1 && c.mile1 == t_car.mile1 && c.lane1 == t_car.lane1 && c.offset1 == t_car.offset1 && c.dir1 == t_car.dir1) &&
							(c.xway2 == t_car.xway2 && c.mile2 == t_car.mile2 && c.lane2 == t_car.lane2 && c.offset2 == t_car.offset2 && c.dir2 == t_car.dir2) &&
							(c.xway3 == t_car.xway3 && c.mile3 == t_car.mile3 && c.lane3 == t_car.lane3 && c.offset3 == t_car.offset3 && c.dir3 == t_car.dir3))) {
						
						if (flag == 0) {
							AccidentEvent a_event = new AccidentEvent();
							a_event.vid1 = c.carid;
							a_event.vid2 = t_car.carid;
							a_event.xway = c.xway0;
							a_event.mile = c.mile0;
							a_event.dir = c.dir0;
							a_event.time = t_car.time;
							
							//System.out.println(a_event.toString());
							
							downStream.put(a_event); //Here we communicate the event to TollPE
							
							//downStream
							
							/*
							try{
							    bytesMessage = jmsCtx_toll.getSession().createBytesMessage();

							    bytesMessage.writeBytes((Constants.NOV_EVENT_TYPE + " " + a_event.vid1 + " " + a_event.vid2 + " " + a_event.xway + " " + a_event.mile + " " + a_event.dir ).getBytes());
							    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
							    producer_toll.send(bytesMessage);
							}catch(JMSException e){
								e.printStackTrace();
							}*/
							
							
							
							t_car.notified = true;
							c.notified = true;
							flag = 1;
						}
						//The cars c and t_car have smashed with each other
						add_smashed_cars(c);
						add_smashed_cars(t_car);
						
						break;
					}
				}
			}
		}
	}
    
	public static LinkedList smashed_cars = new LinkedList();
	public static LinkedList stopped_cars = new LinkedList();
	public static LinkedList accidents = new LinkedList();
	
	public boolean is_smashed_car(Car car) {
		for (int i = 0; i < smashed_cars.size(); i++) {
			Car t_car = (Car)smashed_cars.get(i);

			if (((Car)smashed_cars.get(i)).carid == car.carid){
				return true;
			}
		}
		return false;
	}
	
	public void add_smashed_cars(Car c) {
		for (int i = 0; i < smashed_cars.size(); i++) {
			Car t_car = (Car)smashed_cars.get(i);
			if (c.carid == t_car.carid) {
				smashed_cars.remove(i);
			}
		}
		smashed_cars.add(c);
	}
	
	public boolean is_stopped_car(Car c) {
		for (int i = 0; i < stopped_cars.size(); i++) {
			Car t_car = (Car)stopped_cars.get(i);
			if (c.carid == t_car.carid) {
				return true;
			}
		}
		return false;
	}
	
	public void remove_from_smashed_cars(Car c) {
		for (int i = 0; i < smashed_cars.size(); i++) {
			Car t_car = (Car)smashed_cars.get(i);
			if (c.carid == t_car.carid) {
				smashed_cars.remove();
			}
		}
	}
	
	public void remove_from_stopped_cars(Car c) {
		for (int i = 0; i < stopped_cars.size(); i++) {
			Car t_car = (Car)stopped_cars.get(i);
			if (c.carid == t_car.carid) {
				stopped_cars.remove();
			}
		}
	}
	
	public void renew_stopped_car(Car c) {
		for (int i = 0; i < stopped_cars.size(); i++) {
			Car t_car = (Car)stopped_cars.get(i);
			if (c.carid == t_car.carid) {
				c.xway3 = t_car.xway2;
				c.xway2 = t_car.xway1;
				c.xway1 = t_car.xway0;
				c.mile3 = t_car.mile2;
				c.mile2 = t_car.mile1;
				c.mile1 = t_car.mile0;				
				c.lane3 = t_car.lane2;
				c.lane2 = t_car.lane1;
				c.lane1 = t_car.lane0;
				c.offset3 = t_car.offset2;
				c.offset2 = t_car.offset1;
				c.offset1 = t_car.offset0;				
				c.dir3 = t_car.dir2;
				c.dir2 = t_car.dir1;
				c.dir1 = t_car.dir0;
				c.notified = t_car.notified;
				c.posReportID = (byte)(t_car.posReportID + 1);
				
				stopped_cars.remove(i);
				stopped_cars.add(c);
				
				//Since we already found the car from the list we break at here
				break;
			}
		}
	}
	
	
    @Override
    protected void onCreate() {
    	//System.out.println("On create alled on ACCIDENT PE");
    	//log.info("On create alled on ACCIDENT PE");
        Properties properties = new Properties();
        InputStream propertiesIS = TollCalculationPE.class.getClassLoader().getResourceAsStream(Constants.CONFIG_FILENAME);
        if (propertiesIS == null)
        {
            throw new RuntimeException("Properties file '" + Constants.CONFIG_FILENAME + "' not found in classpath");
        }
        
        try{
        	properties.load(propertiesIS);
        }catch(IOException e){
        	e.printStackTrace();
        }
        
        peID = Integer.parseInt(properties.getProperty(Constants.ACCIDENT_PE_ID));
        System.out.println("ACCDETECT PEID : " + peID);
        
        //Here we need to check whether we are at the correct PE to start logging the data rates
        if(getApp().getReceiver().getPartitionId() == peID){
			currentTime = System.currentTimeMillis();
			try {
				outLogger = new PrintWriter(new BufferedWriter(new FileWriter("/home/miyuru/projects/pamstream/experiments/s4-accident-rate.csv", true)));
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
}
