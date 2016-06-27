/**
 * 
 */
package org.linear.s4.events.keyfinder;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import org.apache.s4.base.KeyFinder;
import org.apache.s4.base.Event;
import org.linear.s4.events.PositionReportEvent;

/**
 * @author miyuru
 *
 */
public class PositionReportKeyFinder implements KeyFinder {
	@Override
	//public ArrayList<PositionReportEvent> get(Event event){
	public List<String> get(Event event){
		//return Arrays.asList(new String[] {event.get("name")});
		//System.out.println(event.toString());
		
		//ArrayList evtList = new ArrayList();
		//evtList.add(event);
		//return evtList;
		
		return Arrays.asList(new String[]{"name"});
	}
}
