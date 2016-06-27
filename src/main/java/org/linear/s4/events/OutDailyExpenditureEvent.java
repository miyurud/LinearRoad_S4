/**
 * 
 */
package org.linear.s4.events;

import org.apache.s4.base.Event;

/**
 * @author miyuru
 *
 */
public class OutDailyExpenditureEvent extends Event {
	@Override
	public String toString() {
		return "OutDailyExpenditureEvent [qid=" + qid + ", exp=" + exp + "]";
	}

	public int qid; //Query ID
	public long exp; //Expenditure

	public OutDailyExpenditureEvent() {

	}
	
	public OutDailyExpenditureEvent(int qid, long exp) {	
		this.qid = qid;
		this.exp = exp;
	}
}
