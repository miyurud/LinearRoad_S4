/**
 * 
 */
package org.linear.s4.layer.toll;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;

import org.apache.s4.core.App;

import org.apache.s4.base.Event;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;
import org.linear.s4.events.AccidentEvent;
import org.linear.s4.events.LAVEvent;
import org.linear.s4.events.PositionReportEvent;
import org.linear.s4.events.TollCalculationEvent;
import org.linear.s4.events.NOVEvent;
import org.linear.s4.util.Constants;

/**
 * @author miyuru
 *
 */
public class TollCalculationPE extends ProcessingElement{
	int peID = -1;
	transient Stream<Event> downStream;
	LinkedList cars_list = new LinkedList();
	HashMap<Integer, Car> carMap = new HashMap<Integer, Car>(); 
	HashMap<Byte, AccNovLavTuple> segments = new HashMap<Byte, AccNovLavTuple>();
	byte NUM_SEG_DOWNSTREAM = 5; //Number of segments downstream to check whether an accident has happened or not
	int BASE_TOLL = 2; //This is a predefined constant (mentioned in Richard's thesis)
	private LinkedList<PositionReportEvent> posEvtList = new LinkedList<PositionReportEvent>();
	
    private long tupleCounter = 0;
    private int tupleCountingWindow = 5000;//This is in miliseconds
    private long previousTime = 0; //This is the time that the tuple measurement was taken previously
    private PrintWriter outLogger = null;
    private int dataRate = 0;
    private long currentTime = 0;
    private long expStartTime = 0; 
	
    public TollCalculationPE(App app){
    	super(app);
    }
    
	public void onEvent(Event event){	
		//System.out.println(event.toString());
		//We do any processing in this node only if the PE ID is TOLL_PE_ID
		//System.out.println("Toll Partition ID : " + getApp().getReceiver().getPartitionId());
		if(getApp().getReceiver().getPartitionId() == peID){
			//System.out.println("At Toll --->" + event.toString());
			
			/*
		       switch(typeField){
	       	   case Constants.POS_EVENT_TYPE:
	       		   process(new PositionReportEvent(fields, true));
	       		   break;
		       case Constants.LAV_EVENT_TYPE:
		    	   LAVEvent obj = new LAVEvent(Byte.parseByte(fields[1]), Float.parseFloat(fields[2]), Byte.parseByte(fields[3]));
		    	   lavEventOcurred(obj);
		    	   break;
		       case Constants.NOV_EVENT_TYPE:
		    	   //System.out.println("NOV Tuple : " + tuple);
		    	   try{
		    		   NOVEvent obj2 = new NOVEvent(Integer.parseInt(fields[1]), Byte.parseByte(fields[2]), Integer.parseInt(fields[3]));
		    		   novEventOccurred(obj2);
		    	   }catch(NumberFormatException e){
		    		   System.out.println("Not Number Format Exception for tuple : " + tuple);
		    	   }
		    	   break;
		       case Constants.ACCIDENT_EVENT_TYPE:
		    	   accidentEventOccurred(new AccidentEvent(Integer.parseInt(fields[1]), Integer.parseInt(fields[2]), Byte.parseByte(fields[3]), Byte.parseByte(fields[4]), Byte.parseByte(fields[5]), Long.parseLong(fields[6])));
		    	   break;
	       }*/
			
			
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
			
			if(event instanceof PositionReportEvent){
				process(((PositionReportEvent)event));
			}else if(event instanceof LAVEvent){
				lavEventOcurred(((LAVEvent)event));
			}else if(event instanceof NOVEvent){
				novEventOccurred(((NOVEvent)event));
			}
		}
	}
		
    @Override
    protected void onCreate() {
    	//System.out.println("On create alled on TOLL PE");
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
        
        peID = Integer.parseInt(properties.getProperty(Constants.TOLL_PE_ID));
        //System.out.println("TOLL PEID : " + peID);
        
        //Here we need to check whether we are at the correct PE to start logging the data rates
        if(getApp().getReceiver().getPartitionId() == peID){
			currentTime = System.currentTimeMillis();
			try {
				outLogger = new PrintWriter(new BufferedWriter(new FileWriter("/home/miyuru/projects/pamstream/experiments/s4-toll-rate.csv", true)));
	          outLogger.println("----- New Session -----<date-time>,<wall-clock-time(s)>,<physical-tuples-in-last-period>,<physical-tuples-data-rate>");
				outLogger.println("Date,Wall clock time (s),TTuples,Data rate (tuples/s)");
			} catch (IOException e1) {
				e1.printStackTrace();
			}
        }        
    }

	public void accidentEventOccurred(AccidentEvent accEvent) {
		//System.out.println("Accident Occurred :" + accEvent.toString());
		boolean flg = false;
		
		synchronized(this){
			flg = segments.containsKey(accEvent.mile);
		}
		
		if(!flg){
			AccNovLavTuple obj = new AccNovLavTuple();
			obj.isAcc = true;
			synchronized(this){
				segments.put(accEvent.mile, obj);
			}
		}else{
			synchronized(this){
				AccNovLavTuple obj = segments.get(accEvent.mile);
				obj.isAcc = true;
				segments.put(accEvent.mile, obj);
			}
		}
	}
    
    public void novEventOccurred(NOVEvent novEvent){
		boolean flg = false;

		flg = segments.containsKey(novEvent.segment);
	
		if(!flg){
			AccNovLavTuple obj = new AccNovLavTuple();
			obj.nov = novEvent.nov;
			segments.put(novEvent.segment, obj);
		}else{
			AccNovLavTuple obj = segments.get(novEvent.segment);
			obj.nov = novEvent.nov;

			segments.put(novEvent.segment, obj);
		}    	
    }
    
    public void lavEventOcurred(LAVEvent lavEvent){
		boolean flg = false;
		
		flg = segments.containsKey(lavEvent.segment); 
		
		if(!flg){
			AccNovLavTuple obj = new AccNovLavTuple();
			obj.lav = lavEvent.lav;
			segments.put(lavEvent.segment, obj);
		}else{
			AccNovLavTuple obj = segments.get(lavEvent.segment);
			obj.lav = lavEvent.lav;
			segments.put(lavEvent.segment, obj);
		}
    }

	public void process(PositionReportEvent evt){
		int len = 0;
		//BytesMessage bytesMessage = null;
				
		Iterator<Car> itr = cars_list.iterator();
	
		if(!carMap.containsKey(evt.vid)){
			Car c = new Car();
			c.carid = evt.vid;
			c.mile = evt.mile;
			carMap.put(evt.vid, c);
		}else{
			Car c = carMap.get(evt.vid);

				if(c.mile != evt.mile){ //Car is entering a new mile/new segment
					c.mile = evt.mile;
					carMap.put(evt.vid, c);

					if((evt.lane != 0)&&(evt.lane != 7)){ //This is to make sure that the car is not on an exit ramp
						AccNovLavTuple obj = null;
						
						obj = segments.get(evt.mile);

						if(obj != null){									
//							if(isInAccidentZone(evt)){
//								System.out.println("Its In AccidentZone");
//							}
							
							if(((obj.nov < 50)||(obj.lav > 40))||isInAccidentZone(evt)){
								TollCalculationEvent tollEvt = new TollCalculationEvent(); //In this case we set the toll to 0
								tollEvt.vid = evt.vid;
								tollEvt.segment = evt.mile;
																										
								//String msg = (Constants.TOLL_EVENT_TYPE + " " + tollEvt.toCompressedString());
								
								//This TollCalculationEvent event will be picked by both AccountBalancePE and OutputPE
								//System.out.println(tollEvt.toString());
								downStream.put(tollEvt);
								
								/*
								try{
								    bytesMessage = jmsCtx_output.getSession().createBytesMessage();
								    bytesMessage.writeBytes(msg.getBytes());
								    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
								    producer_output.send(bytesMessage);
								}catch(JMSException e){
									e.printStackTrace();
								}
								*/
								
								/*
								try{
							          bytesMessage = jmsCtx_account_balance.getSession().createBytesMessage();
							          bytesMessage.writeBytes(msg.getBytes());
							          bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
							          producer_account_balance.send(bytesMessage);
									} catch (JMSException e) {
										e.printStackTrace();
									}
									*/
							}else{
								TollCalculationEvent tollEvt = new TollCalculationEvent(); //In this case we need to calculate a toll
								tollEvt.vid = evt.vid;
								tollEvt.segment = evt.mile;
								
								if(segments.containsKey(evt.mile)){
									AccNovLavTuple tuple = null;
									
									synchronized(this){
										tuple = segments.get(evt.mile);
									}
																				
									tollEvt.toll = BASE_TOLL*(tuple.nov - 50)*(tuple.nov - 50);
																		
									//String msg = (Constants.TOLL_EVENT_TYPE + " " + tollEvt.toCompressedString());
									
									//This TollCalculationEvent event will be picked by both AccountBalancePE and OutputPE
									//System.out.println(tollEvt.toString());
									downStream.put(tollEvt);
									
									/*
									try{
									    bytesMessage = jmsCtx_output.getSession().createBytesMessage();

									    bytesMessage.writeBytes(msg.getBytes());
									    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
									    producer_output.send(bytesMessage);
									}catch(JMSException e){
										e.printStackTrace();
									}
									*/
									
									/*
									try{
								          bytesMessage = jmsCtx_account_balance.getSession().createBytesMessage();
								          bytesMessage.writeBytes(msg.getBytes());
								          bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
								          producer_account_balance.send(bytesMessage);
										} catch (JMSException e) {
											e.printStackTrace();
										}*/
								}
							}						
						}
					}
				}
		}
	}
    
	private boolean isInAccidentZone(PositionReportEvent evt) {
		byte mile = evt.mile;
		byte checkMile = (byte) (mile + NUM_SEG_DOWNSTREAM);
		
		while(mile < checkMile){
			if(segments.containsKey(mile)){
				AccNovLavTuple obj = segments.get(mile);
				
				if(Math.abs((evt.time - obj.time)) > 20){
					obj.isAcc = false;
					mile++;
					continue; //May be we remove the notification for a particular mile down the xway. But another mile still might have accident. Therefore, we cannot break here.
				}
				
				if(obj.isAcc){
					return true;
				}
			}
			mile++;
		}
		
		return false;
	}
    
    @Override
    protected void onRemove() {
    }
    
	public void setDownStream(Stream<Event> downEventStream){
		this.downStream = downEventStream;
	}
}
