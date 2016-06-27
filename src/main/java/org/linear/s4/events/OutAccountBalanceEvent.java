/**
 * 
 */
package org.linear.s4.events;

import org.apache.s4.base.Event;

/**
 * @author miyuru
 *
 */
public class OutAccountBalanceEvent extends Event {
	@Override
	public String toString() {
		return "OutAccountBalanceEvent [qid=" + qid + ", balance=" + balance
				+ "]";
	}

	public int qid; //Query ID
	public long balance; //The summation of all the toll records
	
	public OutAccountBalanceEvent() {

	}
	
	public OutAccountBalanceEvent(int qid, long balance) {
		this.qid = qid;
		this.balance = balance;
	}

}
