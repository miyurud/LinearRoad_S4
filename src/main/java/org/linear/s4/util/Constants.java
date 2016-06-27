package org.linear.s4.util;


public class Constants
{
    public static final String CONFIG_FILENAME = "s4-LinearRoad.properties";
    
    public static final String LINEAR_HISTORY = "linear-history-file";
    public static final String LINEAR_CAR_DATA_POINTS = "linear-cardatapoints-file";
    
    public static final int POS_EVENT_TYPE = -1;
    public static final int ACC_BAL_EVENT_TYPE = -2;
    public static final int DAILY_EXP_EVENT_TYPE = -3;
    public static final int TRAVELTIME_EVENT_TYPE = -4;
    public static final int NOV_EVENT_TYPE = -5;
    public static final int LAV_EVENT_TYPE = -6;
    public static final int TOLL_EVENT_TYPE = -7;
    public static final int ACCIDENT_EVENT_TYPE = -8;
    
    public static final int HISTORY_LOADING_NOTIFIER_PORT = 2233;
    
    public static final String CLEAN_START = "clean-start";

    public static final String ACCIDENT_PE_ID = "accident-component-pe-id";
    public static final String ACCBALANCE_PE_ID = "accbalance-component-pe-id";
    public static final String SEGSTAT_PE_ID = "segstat-component-pe-id";
    public static final String TOLL_PE_ID = "toll-component-pe-id";
    public static final String DAILY_EXP_PE_ID = "dailyexp-component-pe-id";
    public static final String OUTPUT_PE_ID = "output-component-pe-id";

	public static final String ZOOKEEPER = "zookeeper.connect";
	public static final String NUM_THREADS = "num.threads";//This many threads will be created in segment statistics listener
	public static final String HISTORY_COMPONENT_HOST = "history.host";//This is the host where the history component is run
}
