package enlj.p106trading.mssqlv51.p10651basketball.sbteamhagoalpercent.logics;

import java.sql.*;
import org.w3c.dom.*;

import enlj.projenv.logics.*;
import enlj.webenv.logics.*;
import enlj.projenv.mssql.*;
import enlj.webenv.utils.*;
import enlj.p106trading.mssqlv51.resource.logics.*;
import enlj.p106trading.mssqlv51.resource.module.m10651.dleaguetable.anormaltable.logics.*;
import enlj.p106trading.mssqlv51.resource.module.m10651.dleaguetable.iteamod.logics.*;	

public class TeamsHAGoalPercentBL extends MSELogic
{
    /* Module Id */
    private final String kModuleId = "106510418";    	
    	   
	/* Task Ids */
	private final String kInitData	= "10651041801";
	private final String kSeasons	= "10651041804";
	private final String kTeams		= "10651041805";
	private final String kTeamOD	= "10651041806";	

	/* Server Row Names */
    private final String kSR_Leagues	= "sr1";
	private final String kSR_Seasons	= "sr2";
    private final String kSR_Teams		= "sr3";	

	/* Client Row Names */    	
	private final String kCR_LeagueId		= "cr1";
	private final String kCR_SeasonId		= "cr2";
	private final String kCR_League_FLTR	= "cr3";
	private final String kCR_TeamOD_FLTR	= "cr4";

	/* Status Ids */	
	private final String T1_FunctionPM  = "101";
	private final String T1_Leagues		= "102";

	private final String T4_Seasons		= "401";
	private final String T5_Teams		= "501";	

	/* Season Data Fields */
	private final int f_SLeagueId	= 0;

	/* Team Data Fields */
	private final int f_TLeagueId	= 0;
	private final int f_TSeasonId	= 1;
	
   	public TeamsHAGoalPercentBL ()
	{
		super ();		
	}
	
	/**
        A template method which has been extended from MSELogic.

        @see enlj.component.resource.logics.MSELogic#executeTask(Document oDocument, String oTaskId).
    */   
	public String executeTask (Document oDocument, String oTaskId)
	{		
		String oXMLString = "";
		setParams(oDocument);			

		if (oTaskId.equals (kInitData))
        {
            oXMLString = getInitData ();
        }
		else if (oTaskId.equals (kSeasons))
        {
            oXMLString = getSeasons ();
        }
		else if (oTaskId.equals (kTeams))
        {
            oXMLString = getTeams ();
        }				
		else if (oTaskId.equals (kTeamOD))
		{
			oXMLString = getTeamODData ();
		}

		return oXMLString;
	}
	
	private String getInitData ()
    {
        StringBuffer oBuffer = new StringBuffer ();
        oBuffer.append (getFunctionPM (getUserId (), kModuleId, T1_FunctionPM));
		oBuffer.append (BasketballUtil.getLeagues_Bbl (this, T1_Leagues, kSR_Leagues, ConstantsUtil.kFE_ChooseOne));

        return oBuffer.toString ();
    }	
	
	private String getSeasons ()
    {
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();

        String [] arrInfo = getParams (kCR_LeagueId);
		int nLeagueId = convertToInt (arrInfo [f_SLeagueId]);

        try
        {			
			String oSQL =
				" Select en_0651b06_seasoninfo_bbl.seasonid As id, " +
					" en_0651b06_seasoninfo_bbl.seasonname_" + getLanguage () + " As name, " +
					" Convert (varchar, en_0651b06_seasoninfo_bbl.startdate, 103) As startdate, " +
					" Convert (varchar, en_0651b06_seasoninfo_bbl.enddate, 103) As enddate, " +
					" en_0651b06_seasoninfo_bbl.startdate as orderdate, " +
					" 1 As orderid " +
				" From en_0651b06_seasoninfo_bbl, en_0651b01_leagueinfo_bbl " +
				" Where en_0651b06_seasoninfo_bbl.leagueid = en_0651b01_leagueinfo_bbl.leagueid And " +
					" en_0651b06_seasoninfo_bbl.seasonid > 0 And " +
					" en_0651b01_leagueinfo_bbl.leagueid = " + nLeagueId +
				" Union All " +
				" Select 0 As id, " +
					" en_0651z00_firstelement.name_" + getLanguage () + " As name, " +
					" Convert (varchar, GetDate (), 103) As startdate, " +
					" Convert (varchar, GetDate (), 103) As enddate, " +
					" GetDate () As orderdate, " +
					" 0 As orderid " +
				" From en_0651z00_firstelement " +
				" Where en_0651z00_firstelement.id = " + ConstantsUtil.kFE_ChooseOne +
				" Order By orderid, orderdate Desc, name ";

			oStatement = oConnector.getStatement ();

			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_Seasons)); 
				oBuffer.append (getStatusXML (T4_Seasons, 1, "TeamsHAGoalPercentBL:getSeasons:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T4_Seasons, -1, "TeamsHAGoalPercentBL:getSeasons:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T4_Seasons, -1, "TeamsHAGoalPercentBL:getSeasons:" + oException.toString ()));
            log (oBuffer.toString ());			
        }
        
        finally
        {
			try {oResultSet.close ();} catch (Exception oException) {oException.toString ();}
			oResultSet = null;

			try {oStatement.close ();} catch (Exception oException) {oException.toString ();}
			oStatement = null;
			
			oConnector.close ();
			oConnector = null;
        }

        return oBuffer.toString ();
    }

	private String getTeams ()
    {
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();

        String [] arrInfo = getParams (kCR_SeasonId);
		int nLeagueId = convertToInt (arrInfo [f_TLeagueId]);
		int nSeasonId = convertToInt (arrInfo [f_TSeasonId]);

        try
        {			
			String oSQL =
				" Select Distinct en_0651b03_teaminfo_bbl.teamid, " +
					" en_0651b03_teaminfo_bbl.teamname_" + getLanguage () +
				" From en_0651b08_scheduleinfo_bbl, en_0651b10_resultinfo_bbl, en_0651b03_teaminfo_bbl, en_0651b06_seasoninfo_bbl " +
				" Where en_0651b08_scheduleinfo_bbl.scheduleid = en_0651b10_resultinfo_bbl.scheduleid And " + 
					" (en_0651b08_scheduleinfo_bbl.ateamid = en_0651b03_teaminfo_bbl.teamid Or " +
						" en_0651b08_scheduleinfo_bbl.bteamid = en_0651b03_teaminfo_bbl.teamid) And " +
					" en_0651b08_scheduleinfo_bbl.leagueid = en_0651b06_seasoninfo_bbl.leagueid And " +	
					" en_0651b08_scheduleinfo_bbl.leagueid = " + nLeagueId + " And " +
					" en_0651b06_seasoninfo_bbl.seasonid = " + nSeasonId + " And " +
					" en_0651b10_resultinfo_bbl.resultstatusid = " + ConstantsUtil.kSRS_Settled + " And " +
					" en_0651b08_scheduleinfo_bbl.scheduledate Between " +
						" en_0651b06_seasoninfo_bbl.startdate And en_0651b06_seasoninfo_bbl.enddate " +
				" Order By teamname_" + getLanguage ();

			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_Teams)); 
				oBuffer.append (getStatusXML (T5_Teams, 1, "TeamsHAGoalPercentBL:getTeams:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T5_Teams, -1, "TeamsHAGoalPercentBL:getTeams:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T5_Teams, -1, "TeamsHAGoalPercentBL:getTeams:" + oException.toString ()));
            log (oBuffer.toString ());			
        }
        
        finally
        {
			try {oResultSet.close ();} catch (Exception oException) {oException.toString ();}
			oResultSet = null;

			try {oStatement.close ();} catch (Exception oException) {oException.toString ();}
			oStatement = null;
			
			oConnector.close ();
			oConnector = null;
        }

        return oBuffer.toString ();
    }	

	private String getTeamODData ()
	{
		StringBuffer oBuffer = new StringBuffer ();

        oBuffer.append (getTeamOD ());
		oBuffer.append (getLeagueTable ());

        return oBuffer.toString ();
	}

	private String getTeamOD ()
    {
		StringBuffer oBuffer = new StringBuffer ();
        
        int nSportId = SportUtil.kS_BasketBall;
            
		TeamODObj oLogic = new TeamODObj (getDocument ());
		NodeList arrNodes = getChildNodes (kCR_TeamOD_FLTR);

		for (int nIndex = 0; nIndex < arrNodes.getLength (); nIndex++)
		{
			Node oNode = arrNodes.item (nIndex);
			String [] arrInfo = getParams (oNode);

	        oBuffer.append (oLogic.getTeamOD (nSportId, arrInfo));
		}	
			    

        return oBuffer.toString ();
	}

	private String getLeagueTable ()
    {
		StringBuffer oBuffer = new StringBuffer ();

        int nSportId = SportUtil.kS_BasketBall;
                    
		String [] arrInfo = getParams (kCR_League_FLTR);				
		LeagueTableObj oLogic = new LeagueTableObj (getDocument ());

		oBuffer.append (oLogic.getLeagueTable (nSportId, arrInfo));		

        return oBuffer.toString ();
	}

	public void log (String oMessage)
	{
//		logMessage (oMessage);
	}
}