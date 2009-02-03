package org.apache.hadoop.hive.hwi;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Vector;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.cli.OptionsProcessor;
import org.apache.hadoop.hive.cli.SetProcessor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.ExecDriver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.conf.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * HWISessionItem can be viewed as a wrapper for a Hive shell. With it the user has a  
 * session on the web server rather then in a console window. 
 *
 */
public class HWISessionItem implements Runnable, Comparable<HWISessionItem> {

	protected static final Log l4j = LogFactory.getLog( HWISessionItem.class.getName() );
	/**
	 * Represents the state a session item can be in.
	 *
	 */
	public enum WebSessionItemStatus{
		 NEW,QUERY_SET,QUERY_RUNNING,QUERY_COMPLETE,DESTROY,KILL_QUERY
	};
	
	private String sessionName;
	private HWISessionItem.WebSessionItemStatus status;
	
	
	private CliSessionState ss;
	private SetProcessor sp;
	private Driver qp;

	private String resultFile;
	private String errorFile;
	
	private String query;
	private int queryRet;
	
	HiveConf conf;
	Thread runnable;
	/**
	 * Creates an instance of WebSessionItem, sets status
	 * to NEW.
	 */
	protected HWISessionItem(){
		l4j.debug("HWISessionItem created");
		status=WebSessionItemStatus.NEW;
		queryRet=-40;
	}
	
	/**
	 * This is the initialization process that is carried out for each
	 * SessionItem this is a section of the code taken from the CLIDriver
	 */
	protected void itemInit() {
		l4j.debug("HWISessionItem itemInit start "+this.getSessionName() );
		OptionsProcessor oproc = new OptionsProcessor();
		
		if (System.getProperty("hwi-args")!=null){
			String [] parts = System.getProperty("hwi-args").split("\\s+");
		
			if (!oproc.process_stage1(parts)) {
			}
		}
		
		SessionState.initHiveLog4j();
		conf=new HiveConf(SessionState.class);
		ss = new CliSessionState(conf);
		SessionState.start(ss);

		sp = new SetProcessor();
		qp = new Driver();
		l4j.debug("HWISessionItem itemInit Complete "+this.getSessionName() );
	}

	/**
	* Set processor queries block for only a short amount of time the
	* client can issue these directly.
	* @param query This is a query in the form of SET THIS=THAT
	* @return chained call to setProcessor.run(String)
	*/
	public int runSetProcessorQuery(String query){
		return sp.run(query);
	}
	
	/**
	* The client does not start the thread themselves. They set their status to
	* QUERY_SET. 
	* 
	*/
	public void clientStart() throws HWIException {
		if (ss == null) {
			throw new HWIException("SessionState is null.");
		}

		if (query == null) {
			throw new HWIException("No Query was specified.");
		}

		if (this.status == WebSessionItemStatus.QUERY_RUNNING) {
			throw new HWIException("Query already running");
		}
		this.status = WebSessionItemStatus.QUERY_SET;
		l4j.debug(this.getSessionName()+" Query is set to start");
	}
	
	public void clientKill() throws HWIException {
		if (this.status != WebSessionItemStatus.QUERY_RUNNING){
			throw new HWIException("Can not kill that which is not running.");
		}
		this.status=WebSessionItemStatus.KILL_QUERY;
		l4j.debug(this.getSessionName()+" Query is set to KILL_QUERY");
	}
	/**
	 * This method returns A KILL command a non silent clisession would return 
	 * when invoking a HQL query from the CLI.
	 * @return a string URL showing the job in the job tracker
	 */
	public Collection<String> getKillCommand(){
		return ExecDriver.runningJobKillURIs.values();
		//Kill Command = /opt/hadoop/hadoop-0.19.0/bin/../bin/hadoop job  -Dmapred.job.tracker=hadoop1.jointhegrid.local:54311 -kill job_200812231617_0001
		//StringBuffer sb = new StringBuffer();
		//sb.append( conf.getVar(HiveConf.ConfVars.HADOOPBIN));
		//sb.append( " job ");
		//sb.append( " -Dmapred.job.tracker= ");
		//sb.append( conf.getVar(HiveConf.ConfVars.HADOOPJT));
		//sb.append( " -kill ");
		//sb.append( conf.getVar(HiveConf.ConfVars.HADOOPJOBNAME));
		//return sb.toString();
	}
	/**
	 * This method returns the URL a non silent clisession would return 
	 * when invoking a HQL query from the CLI.
	 * @return a string URL showing the job in the job tracker
	 */
	public Vector<String> getJobTrackerURI(){
		//is this my job?
		Vector<String> results = new Vector<String>();
		for (String id: ExecDriver.runningJobKillURIs.keySet() ){

			StringBuffer sb = new StringBuffer();
			sb.append("http://");
			//sb.append( this.conf.getVar( HiveConf.ConfVars.HADOOPJT ) );
			sb.append( conf.get("mapred.job.tracker.http.address") );
			sb.append("/jobdetails.jsp?jobid=");
			sb.append(id);
			results.add( sb.toString() );
		
		}
		return results;
		//http://hadoop1.jointhegrid.local:50030/jobdetails.jsp?jobid=
	}
	
	/** This method clears the private member variables. */
	public void clientRenew() throws HWIException {
		if (this.status == WebSessionItemStatus.QUERY_RUNNING) {
			throw new HWIException("Query already running");
		}

		this.query = null;
		this.resultFile = null;
		this.errorFile = null;
		this.status = WebSessionItemStatus.NEW;
		l4j.debug(this.getSessionName()+" Query is renewed to start");
	}

	/** 
	* This is a chained call to SessionState.setIsSilent(). Use this 
	* if you do not want the result file to have information status
	*/
	public void setSSIsSilent(boolean silent) throws HWIException{
		if (ss == null) 
			throw new HWIException("Session State is null");
		this.ss.setIsSilent(silent);
	}
	
	/** 
	* This is a chained call to SessionState.getIsSilent()
	*/
	public boolean getSSIsSilent() throws HWIException {
		if (ss == null) 
			throw new HWIException("Session State is null");
		return ss.getIsSilent();
	}

	 /**
	 * This is a callback style function used by the HiveSessionManager. The
	 * HiveSessionManager notices this and changes the state to QUERY_RUNNING
	 * then starts a thread.
	 */
	protected void startIt() {
		this.status = WebSessionItemStatus.QUERY_RUNNING;
		l4j.debug(this.getSessionName()+" Runnable is starting");
		runnable = new Thread(this);
		runnable.start();
		l4j.debug(this.getSessionName()+" Runnable has started");
	}
	/**
	 * This is a callback style function used by the HiveSessionManager. The
	 * HiveSessionManager notices this and attempts to stop the query. 
	 */
	protected void killIt(){
		l4j.debug(this.getSessionName()+" Attempting kill.");
		if ( this.runnable != null){
			try {
				this.runnable.join(1000);
				l4j.debug(this.getSessionName()+" Thread join complete");
			} catch (InterruptedException e) {
				l4j.error(this.getSessionName()+" killing session caused exception ",e );
			}
		}
	}
	/**
	 * Helper function to get configuration variables
	 * 
	 * @param wanted a ConfVar
	 * @return
	 */
	public String getHiveConfVar(HiveConf.ConfVars wanted) throws HWIException {
		String result = null;
		try {
			result = this.ss.getConf().getVar(wanted);
		} catch (Exception ex) {
			throw new HWIException(ex);
		}
		return result;
	}

	
	@Override
	/**
	* The run method will be called by HiveSessionManager. This method
	* reads the results from query processor completely when the query is done.
	* when completed this is set to HWISessionItemStatus.QUERY_COMPLETE.
	*/
	public void run() {
		l4j.debug(this.getSessionName()+" state is now QUERY_COMPLETE.");
		FileOutputStream fos = null;

		if (this.getResultFile() != null) {
			try {
				fos = new FileOutputStream(new File(this.resultFile));
				ss.out = new PrintStream(fos, true, "UTF-8");
			} catch (java.io.FileNotFoundException fex) {
				l4j.error(this.getSessionName()+" opening resultfile "+this.resultFile, fex);
			} catch (java.io.UnsupportedEncodingException uex) {
				l4j.error(this.getSessionName()+" opening resultfile "+this.resultFile, uex);
			}
		} else {
			l4j.debug(this.getSessionName()+" Output file was not specified");
		}
		
		queryRet = qp.run(this.query);
		Vector<String> res = new Vector<String>();
		while (qp.getResults(res)) {
			for (String row : res) {
				if (ss.out != null){
					ss.out.println(row);
				}
			}
			res.clear();
		}
		try {
			if (fos!= null){
				fos.close();
			}
		} catch (IOException ex) {
			l4j.error(this.getSessionName()+" closing result file "+this.getResultFile()+" caused exception." ,ex );
		}
		this.status = WebSessionItemStatus.QUERY_COMPLETE;
		l4j.debug(this.getSessionName()+" state is now QUERY_COMPLETE.");

	}

	public int compareTo(HWISessionItem other) {
		if (other == null)
			return -1;
		return this.getSessionName().compareTo(other.getSessionName());
	}

	/**
	 * Uses the sessionName property to compare to sessions
	 * @return true if sessionNames are equal false otherwise
	 */
	public boolean equals(Object other){
		if (other == null)
			return false;
		if (! (other instanceof HWISessionItem))
			return false;
		HWISessionItem o = (HWISessionItem) other;
		if (this.getSessionName().equals(o.getSessionName()) ){
			return true;
		} else {
			return false;
		}
	}
	
	protected void setQp(Driver qp) {
		this.qp = qp;
	}

	/**
	 * The query executed by Hive
	 * @return The query that this is executing or will be executed
	 */
	public String getQuery() {
		return query;
	}
	/**
	 * Use this function to set the query that Hive will run.
	 * @param query A query in Hive Query Language
	 */
	public void setQuery(String query) {
		this.query = query;
	}
	/**
	 * Used to determine the status of a query, possibly why it failed
	 * @return The result from Hive queryProcessor
	 */
	public int getQueryRet() {
		return queryRet;
	}

	protected void setQueryRet(int queryRet) {
		this.queryRet = queryRet;
	}

	public String getResultFile() {
		return resultFile;
	}

	public void setResultFile(String resultFile) {
		this.resultFile = resultFile;
	}
	/**
	 * The session name is an identifier to recognize the session
	 * @return the sessions name
	 */
	public String getSessionName() {
		return sessionName;
	}

	protected void setSessionName(String sessionName) {
		this.sessionName = sessionName;
	}

	protected SetProcessor getSp() {
		    return sp;
	}

	protected void setSp(SetProcessor sp) {
		this.sp = sp;
	}

	protected CliSessionState getSs() {
		return ss;
	}

	protected void setSs(CliSessionState ss) {
		this.ss = ss;
	}
	/**
	 * Used to represent to the user and other components what state the HWISessionItem 
	 * is in. Certain commands can only be run when the application is in certain states.
	 * @return the current status of the session
	 */
	public WebSessionItemStatus getStatus() {
		return status;
	}

	/**
	 * Currently unused
	 * @return a String with the full path to the error file.
	 */
	public String getErrorFile() {
		return errorFile;
	}

	/**
	 * Currently unused
	 * @param errorFile
	 *            the full path to the file for results.
	 */
	public void setErrorFile(String errorFile) {
		this.errorFile = errorFile;
	}

}
