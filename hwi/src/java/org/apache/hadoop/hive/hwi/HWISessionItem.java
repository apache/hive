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
import org.apache.hadoop.hive.ql.history.HiveHistoryViewer;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.conf.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * HWISessionItem can be viewed as a wrapper for a Hive shell. With it the user
 * has a session on the web server rather then in a console window.
 * 
 */
public class HWISessionItem implements Runnable, Comparable<HWISessionItem> {

	protected static final Log l4j = LogFactory.getLog(HWISessionItem.class
			.getName());

	/**
	 * Represents the state a session item can be in.
	 * 
	 */
	public enum WebSessionItemStatus {
		NEW, QUERY_SET, QUERY_RUNNING, QUERY_COMPLETE, DESTROY, KILL_QUERY
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
	HWIAuth auth;
	private String historyFile;

	/**
	 * Creates an instance of WebSessionItem, sets status to NEW.
	 */
	protected HWISessionItem() {
		l4j.debug("HWISessionItem created");
		status = WebSessionItemStatus.NEW;
		queryRet = -40;
		runnable = new Thread(this);
		runnable.start();
	}

	/**
	 * This is the initialization process that is carried out for each
	 * SessionItem. The goal is to emulate the startup of CLIDriver.
	 */
	protected void itemInit() {
		l4j.debug("HWISessionItem itemInit start " + this.getSessionName());
		OptionsProcessor oproc = new OptionsProcessor();

		if (System.getProperty("hwi-args") != null) {
			String[] parts = System.getProperty("hwi-args").split("\\s+");

			if (!oproc.process_stage1(parts)) {
			}
		}

		SessionState.initHiveLog4j();
		conf = new HiveConf(SessionState.class);
		ss = new CliSessionState(conf);
		SessionState.start(ss);
		sp = new SetProcessor();
		qp = new Driver();

		runSetProcessorQuery("hadoop.job.ugi=" + auth.getUser() + ","
				+ auth.getGroups()[0]);
		runSetProcessorQuery("user.name=" + auth.getUser());
		/*
		 * HiveHistoryFileName will not be accessible outside this thread. We must
		 * capture this now.
		 */
		this.historyFile = this.ss.get().getHiveHistory().getHistFileName();
		l4j.debug("HWISessionItem itemInit Complete " + this.getSessionName());
	}

	/**
	 * Set processor queries block for only a short amount of time. The client can
	 * issue these directly.
	 * 
	 * @param query
	 *          This is a query in the form of SET THIS=THAT
	 * @return chained call to setProcessor.run(String)
	 */
	public int runSetProcessorQuery(String query) {
		return sp.run(query);
	}

	/**
	 * HWISessionItem is a Runnable instance. Calling this method will change the
	 * status to QUERY_SET and notify(). The run method detects this and then
	 * continues processing.
	 */
	public void clientStart() throws HWIException {

		if (this.status == WebSessionItemStatus.QUERY_RUNNING) {
			throw new HWIException("Query already running");
		}
		this.status = WebSessionItemStatus.QUERY_SET;
		synchronized (this.runnable) {
			this.runnable.notifyAll();
		}
		l4j.debug(this.getSessionName() + " Query is set to start");
	}

	public void clientKill() throws HWIException {
		if (this.status != WebSessionItemStatus.QUERY_RUNNING) {
			throw new HWIException("Can not kill that which is not running.");
		}
		this.status = WebSessionItemStatus.KILL_QUERY;
		l4j.debug(this.getSessionName() + " Query is set to KILL_QUERY");
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
		this.resultFile = null;
		this.conf = null;
		this.ss = null;
		this.qp = null;
		this.sp = null;
		l4j.debug(this.getSessionName() + " Query is renewed to start");
	}

	/**
	 * This is a chained call to SessionState.setIsSilent(). Use this if you do
	 * not want the result file to have information status
	 */
	public void setSSIsSilent(boolean silent) throws HWIException {
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
	 * HiveSessionManager notices this and attempts to stop the query.
	 */
	protected void killIt() {
		l4j.debug(this.getSessionName() + " Attempting kill.");
		if (this.runnable != null) {
			try {
				this.runnable.join(1000);
				l4j.debug(this.getSessionName() + " Thread join complete");
			} catch (InterruptedException e) {
				l4j.error(this.getSessionName() + " killing session caused exception ",
						e);
			}
		}
	}

	/**
	 * Helper function to get configuration variables
	 * 
	 * @param wanted
	 *          a ConfVar
	 * @return Value of the configuration variable.
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
	
	public String getHiveConfVar(String s) throws HWIException{
		String result=null;
		try {
			result = conf.get(s);
		} catch (Exception ex) {
			throw new HWIException(ex);
		}
		return result;
	}
	/*
	 * mapred.job.tracker could be host:port or just local
	 * mapred.job.tracker.http.address could be host:port or just host
	 * In some configurations http.address is set to 0.0.0.0 we are combining the two
	 * variables to provide a url to the job tracker WUI if it exists. If hadoop chose
	 * the first available port for the JobTracker HTTP port will can not determine it.
	 */
	public String getJobTrackerURL(String jobid) throws HWIException{
		String jt = this.getHiveConfVar( "mapred.job.tracker" );
		String jth = this.getHiveConfVar( "mapred.job.tracker.http.address" );
		String [] jtparts = null; 
		String [] jthttpParts = null;
		if (jt.equalsIgnoreCase("local")){
			jtparts = new String [2];
			jtparts [0]="local";
			jtparts [1]="";
		} else {
			jtparts = jt.split(":");
		}
		if (jth.contains(":")) {
			jthttpParts = jth.split(":");
		} else {
			jthttpParts = new String [2];
			jthttpParts [0] = jth;
			jthttpParts [1] = "";
		}
		return jtparts[0]+":"+jthttpParts[1]+"/jobdetails.jsp?jobid="+jobid+"&refresh=30";
	}
	@Override
	/*
	 * HWISessionItem uses a wait() notify() system. If the thread detects conf to
	 * be null, control is transfered to initItem().A status of QUERY_SET causes
	 * control to transfer to the runQuery() method. DESTROY will cause the run
	 * loop to end permanently.
	 */
	public void run() {
		synchronized (this.runnable) {
			while (this.status != HWISessionItem.WebSessionItemStatus.DESTROY) {
				if (conf == null) {
					this.itemInit();
				}
				if (this.status == WebSessionItemStatus.QUERY_SET) {
					this.runQuery();
				}
				try {
					this.runnable.wait();
				} catch (InterruptedException e) {
					l4j.error("in wait() state ", e);
				}
			}
		}
	}

	/**
	 * This method calls the qp.run() method, writes the output to the result
	 * file, when finished the status will be QUERY_COMPLETE.
	 */
	public void runQuery() {

		FileOutputStream fos = null;

		if (this.getResultFile() != null) {
			try {
				fos = new FileOutputStream(new File(this.resultFile));
				ss.out = new PrintStream(fos, true, "UTF-8");
			} catch (java.io.FileNotFoundException fex) {
				l4j.error(this.getSessionName() + " opening resultfile "
						+ this.resultFile, fex);
			} catch (java.io.UnsupportedEncodingException uex) {
				l4j.error(this.getSessionName() + " opening resultfile "
						+ this.resultFile, uex);
			}
		} else {
			l4j.debug(this.getSessionName() + " Output file was not specified");
		}

		l4j.debug(this.getSessionName() + " state is now QUERY_RUNNING.");
		this.status = WebSessionItemStatus.QUERY_RUNNING;

		queryRet = qp.run(this.query);
		Vector<String> res = new Vector<String>();
		while (qp.getResults(res)) {
			for (String row : res) {
				if (ss.out != null) {
					ss.out.println(row);
				}
			}
			res.clear();
		}
		try {
			if (fos != null) {
				fos.close();
			}
		} catch (IOException ex) {
			l4j.error(this.getSessionName() + " closing result file "
					+ this.getResultFile() + " caused exception.", ex);
		}
		this.status = WebSessionItemStatus.QUERY_COMPLETE;
		l4j.debug(this.getSessionName() + " state is now QUERY_COMPLETE.");
	}

	public int compareTo(HWISessionItem other) {
		if (other == null)
			return -1;
		return this.getSessionName().compareTo(other.getSessionName());
	}

	/**
	 * 
	 * @return the HiveHistoryViewer for the session
	 * @throws HWIException
	 */
	public HiveHistoryViewer getHistoryViewer() throws HWIException {
		if (ss == null)
			throw new HWIException("Session state was null");
		/*
		 * we can not call this.ss.get().getHiveHistory().getHistFileName() directly
		 * as this call is made from a a Jetty thread and will return null
		 */
		HiveHistoryViewer hv = new HiveHistoryViewer(this.historyFile);
		return hv;
	}

	/**
	 * Uses the sessionName property to compare to sessions
	 * 
	 * @return true if sessionNames are equal false otherwise
	 */
	public boolean equals(Object other) {
		if (other == null)
			return false;
		if (!(other instanceof HWISessionItem))
			return false;
		HWISessionItem o = (HWISessionItem) other;
		if (this.getSessionName().equals(o.getSessionName())) {
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
	 * 
	 * @return The query that this is executing or will be executed
	 */
	public String getQuery() {
		return query;
	}

	/**
	 * Use this function to set the query that Hive will run.
	 * 
	 * @param query
	 *          A query in Hive Query Language
	 */
	public void setQuery(String query) {
		this.query = query;
	}

	/**
	 * Used to determine the status of a query, possibly why it failed
	 * 
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
	 * 
	 * @return the session's name
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
	 * Used to represent to the user and other components what state the
	 * HWISessionItem is in. Certain commands can only be run when the application
	 * is in certain states.
	 * 
	 * @return the current status of the session
	 */
	public WebSessionItemStatus getStatus() {
		return status;
	}

	/**
	 * Currently unused
	 * 
	 * @return a String with the full path to the error file.
	 */
	public String getErrorFile() {
		return errorFile;
	}

	/**
	 * Currently unused
	 * 
	 * @param errorFile
	 *          the full path to the file for results.
	 */
	public void setErrorFile(String errorFile) {
		this.errorFile = errorFile;
	}

	/**
	 * @return the auth
	 */
	public HWIAuth getAuth() {
		return auth;
	}

	/**
	 * @param auth the auth to set
	 */
	protected void setAuth(HWIAuth auth) {
		this.auth = auth;
	}

}
