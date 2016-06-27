/**
 * 
 */
package org.linear.s4.events;

import org.apache.s4.base.Event;

/**
 * @author miyuru
 *
 */
public class NOVEvent extends Event{
	public int minute; // Current Minute
	public byte segment; //A segement is in the range 0..99; It corresponds to a mile in the high way system
	public int nov; //Number of vehicles in this particular Segment
	
	public NOVEvent(int current_minute, byte mile, int numVehicles) {
		this.minute = current_minute;
		this.segment = mile;
		this.nov = numVehicles;
	}
	
	public NOVEvent(){
		
	}
	
	public String toString(){
		return "NOVEvent [minute=" + minute + ", segment=" + segment + ", nov=" + nov + "]";		
	}
}
