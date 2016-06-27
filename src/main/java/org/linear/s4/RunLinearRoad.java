/**
 * 
 */

package org.linear.s4;

import java.util.Arrays;
import java.util.List;

import org.apache.s4.core.App;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.base.Event;
import org.apache.s4.core.Stream;

import org.linear.s4.layer.accident.AccidentDetectionPE;
import org.linear.s4.layer.segstat.SegmentStatisticsPE;
import org.linear.s4.layer.toll.TollCalculationPE;
import org.linear.s4.layer.dailyexp.DailyExpenditurePE;
import org.linear.s4.layer.accbalance.AccountBalancePE;
import org.linear.s4.output.OutputPE;

import org.linear.s4.events.keyfinder.PositionReportKeyFinder;
import org.linear.s4.events.keyfinder.OutputKeyFinder;
import org.linear.s4.events.keyfinder.AccountBalanceKeyFinder;
import org.linear.s4.events.keyfinder.TollKeyFinder;

import org.linear.s4.events.PositionReportEvent;
import org.linear.s4.events.AccidentEvent;

/**
 * @author miyuru
 *
 */
public class RunLinearRoad extends App{
    @Override
    protected void onStart() {
    	System.out.println("The LR application started...");
    }

    @Override
    protected void onInit() {    	
    	/*
    	//We first declare all the Processing Elements.
    	AccidentDetectionPE accDetectPE = createPE(AccidentDetectionPE.class);//new AccidentDetectionPE();
    	SegmentStatisticsPE segstatPE = createPE(SegmentStatisticsPE.class);//new SegmentStatisticsPE();
    	TollCalculationPE tollPE = createPE(TollCalculationPE.class);//new TollCalculationPE();
    	DailyExpenditurePE dailyExp = createPE(DailyExpenditurePE.class);//new DailyExpenditurePE();
    	AccountBalancePE accBalPE = createPE(AccountBalancePE.class);//new AccountBalancePE();
    	OutputPE outputPE = createPE(OutputPE.class);//new OutputPE();
    	
    	//Next we create the streams. A stream can be defined with a KeyFinder.
    	Stream<Event> inputEventStream = createInputStream("inputstream", new PositionReportKeyFinder(), accDetectPE, segstatPE, tollPE, dailyExp, accBalPE);
    	//When an event is sent to the "position_reports" stream, its key will be identified through the KeyFinder implementation, hashed 
    	//and dispatched to the matching partition.

    	Stream<Event> tollStream = createInputStream("tollstream", new TollKeyFinder(), tollPE);
    	accDetectPE.setDownStream(tollStream);
    	segstatPE.setDownStream(tollStream);
    	
    	Stream<Event> accBalStream = createInputStream("accbalstream", new AccountBalanceKeyFinder(), accBalPE);
    	tollPE.setDownStream(accBalStream);

    	Stream<Event> outputStream = createInputStream("outputstream", new OutputKeyFinder(), outputPE);
    	tollPE.setDownStream(outputStream);
    	dailyExp.setDownStream(outputStream);
    	accBalPE.setDownStream(outputStream);*/

    	//We first declare all the Processing Elements.
    	AccidentDetectionPE accDetectPE = createPE(AccidentDetectionPE.class, "AccidentDetection");//new AccidentDetectionPE();
    	accDetectPE.setSingleton(true);
    	SegmentStatisticsPE segstatPE = createPE(SegmentStatisticsPE.class, "SegmentStatistics");//new SegmentStatisticsPE();
    	segstatPE.setSingleton(true);
    	TollCalculationPE tollPE = createPE(TollCalculationPE.class, "TollCalculation");//new TollCalculationPE();
    	tollPE.setSingleton(true);
    	DailyExpenditurePE dailyExp = createPE(DailyExpenditurePE.class, "DailyExpenditure");//new DailyExpenditurePE();
    	dailyExp.setSingleton(true);
    	AccountBalancePE accBalPE = createPE(AccountBalancePE.class, "AccountBalance");//new AccountBalancePE();
    	accBalPE.setSingleton(true);
    	OutputPE outputPE = createPE(OutputPE.class, "Output");//new OutputPE();
    	outputPE.setSingleton(true);
    	
    	//Next we create the streams. A stream can be defined with a KeyFinder.
    	Stream<Event> inputEventStream = createInputStream("inputstream", accDetectPE, segstatPE, tollPE, dailyExp, accBalPE);
    	//When an event is sent to the "position_reports" stream, its key will be identified through the KeyFinder implementation, hashed 
    	//and dispatched to the matching partition.

    	Stream<Event> tollStream = createStream("tollstream", tollPE);
    	accDetectPE.setDownStream(tollStream);
    	segstatPE.setDownStream(tollStream);
    	
    	Stream<Event> accBalStream = createStream("accbalstream", accBalPE);
    	tollPE.setDownStream(accBalStream);

    	Stream<Event> outputStream = createStream("outputstream", outputPE);
    	tollPE.setDownStream(outputStream);
    	dailyExp.setDownStream(outputStream);
    	accBalPE.setDownStream(outputStream);
    	
    }

    @Override
    protected void onClose() {
    }	
}
