package org.linear.s4.events.keyfinder;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

import org.apache.s4.base.KeyFinder;
import org.apache.s4.base.Event;
import org.linear.s4.events.AccidentEvent;

public class TollKeyFinder implements KeyFinder {
	@Override
	//public ArrayList<AccidentEvent> get(Event event){
	public List<String> get(Event event){
		//return Arrays.asList(new String[] {event.get("name")});
		//System.out.println(event.toString());
		
		//ArrayList evtList = new ArrayList();
		//evtList.add(event);
		
		return Arrays.asList(new String[]{"name"});
		
		//return evtList;
	}
}

