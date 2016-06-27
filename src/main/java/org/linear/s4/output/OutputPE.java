/**************************************************************************************
 * Copyright (C) 2008 EsperTech, Inc. All rights reserved.                            *
 * http://esper.codehaus.org                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package org.linear.s4.output;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.s4.core.App;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.s4.core.ProcessingElement;
import org.apache.s4.base.Event;
import org.linear.s4.util.Constants;

public class OutputPE extends ProcessingElement {
	private int peID = -1;
	
    private long tupleCounter = 0;
    private int tupleCountingWindow = 5000;//This is in miliseconds
    private long previousTime = 0; //This is the time that the tuple measurement was taken previously
    private PrintWriter outLogger = null;
    private int dataRate = 0;
    private long currentTime = 0;
    private long expStartTime = 0; 
	
    public OutputPE(App app){
    	super(app);
    }
    
	public void onEvent(Event event){		
		//For the moment just print the content
		//System.out.println("Output partition id : " + getApp().getReceiver().getPartitionId());
		//System.out.println("At Output PE -->" + event.toString());
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
		}
	}
	
    @Override
    protected void onCreate() {
    	//System.out.println("On create alled on OUTPUT PE");
        Properties properties = new Properties();
        InputStream propertiesIS = OutputPE.class.getClassLoader().getResourceAsStream(Constants.CONFIG_FILENAME);
        if (propertiesIS == null)
        {
            throw new RuntimeException("Properties file '" + Constants.CONFIG_FILENAME + "' not found in classpath");
        }
        
        try{
        	properties.load(propertiesIS);
        }catch(IOException e){
        	e.printStackTrace();
        }
        
        peID = Integer.parseInt(properties.getProperty(Constants.OUTPUT_PE_ID));
        //System.out.println("OUTPUT PEID : " + peID);
        
        //Here we need to check whether we are at the correct PE to start logging the data rates
        if(getApp().getReceiver().getPartitionId() == peID){
			currentTime = System.currentTimeMillis();
			try {
				outLogger = new PrintWriter(new BufferedWriter(new FileWriter("/home/miyuru/projects/pamstream/experiments/s4-output-rate.csv", true)));
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
