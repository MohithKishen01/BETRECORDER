package enlj.p106trading.mssqlv51.p10651basketball.sbleaguetable.logics;

import java.sql.*;
import org.w3c.dom.*;

import enlj.projenv.logics.*;
import enlj.webenv.logics.*;
import enlj.projenv.mssql.*;
import enlj.webenv.utils.*;
import enlj.p106trading.mssqlv51.resource.logics.*;
import enlj.p106trading.mssqlv51.resource.module.m10651.dleaguetable.anormaltable.logics.*;
import enlj.p106trading.mssqlv51.resource.module.m10651.dleaguetable.bnormaltableha.logics.*;

public class LeagueTableBL extends MSELogic
{
    /* Module Id */
    private final String kModuleId = "106510417";    	

	/* Task Ids */
	private final String kInitData		= "10651041701";
	private final String kSeasons		= "10651041704";
	private final String kTeams			= "10651041705";
	private final String kLeagueTable	= "10651041706";

	/* Server Row Names */
	private final String kSR_Leagues	= "sr1";
	private final String kSR_Seasons	= "sr2";
	private final String kSR_Teams		= "sr3";	

	/* Client Row Names */
	private final String kCR_League_FLTR			= "cr1";
	private final String kCR_Season_FLTR			= "cr2";
	private final String kCR_LeagueTable_FLTR		= "cr3";
	private final String kCR_SideLeague_FLTR		= "cr4";
	private final String kCR_LeagueTable_L6M_FLTR	= "cr5";
	private final String kCR_LeagueTable_L12M_FLTR	= "cr6";	

	/* Status Ids */		
	private final String T1_FunctionPM  = "101";
	private final String T1_Leagues		= "102";

	private final String T4_Seasons	= "401";
	private final String T5_Teams	= "501";

	/* League Season Field */
	private final int f_LeagueId	= 0;

	/* League Season Team Fields */
	private final int f_LeagueId_LST	= 0;
	private final int f_SeasonId_LST	= 1;

   	public LeagueTableBL ()
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
		else if (oTaskId.equals (kLeagueTable))
        {
            oXMLString = getLeagueTableList ();
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
		
		String [] arrInfo = getParams (kCR_League_FLTR);
		String oLeagueId = arrInfo [f_LeagueId];

        try
        {			
			String oSQL =
				" Select en_0651b06_seasoninfo_bbl.seasonid As id, " +
				   " en_0651b06_seasoninfo_bbl.seasonname_" + getLanguage () + " As name, " +
				   " Convert (nvarchar, en_0651b06_seasoninfo_bbl.startdate, 103) As startdate, " +
				   " Convert (nvarchar, en_0651b06_seasoninfo_bbl.enddate, 103) As enddate, " +
				   " en_0651b06_seasoninfo_bbl.startdate As orderdate, " +
				   " 1 As orderid " +
				" From en_0651b06_seasoninfo_bbl, en_0651b01_leagueinfo_bbl " +
				" Where en_0651b06_seasoninfo_bbl.leagueid = en_0651b01_leagueinfo_bbl.leagueid And " +				   
				   " en_0651b01_leagueinfo_bbl.leagueid = " + oLeagueId +
				" Union All " +
				" Select 0 As id, " +
				   " en_0651z00_firstelement.name_" + getLanguage () + " As name, " +
				   " Convert (nvarchar, GetDate (), 103) As startdate, " +
				   " Convert (nvarchar, GetDate (), 103) As enddate, " +
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
				oBuffer.append (getStatusXML (T4_Seasons, 1, "LeagueTableBL:getSeasons:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T4_Seasons, -1, "LeagueTableBL:getSeasons:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T4_Seasons, -1, "LeagueTableBL:getSeasons:" + oException.toString ()));
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
		
		String [] arrInfo = getParams (kCR_Season_FLTR);
		String oLeagueId = arrInfo [f_LeagueId_LST];
		String oSeasonId = arrInfo [f_SeasonId_LST];

        try
        {			
			String oSQL =
				" Select Distinct en_0651b03_teaminfo_bbl.teamid As id, " +
					" en_0651b03_teaminfo_bbl.teamname_" + getLanguage () + " As teamname_" + getLanguage () + " " +					
				" From en_0651b03_teaminfo_bbl, en_0651b08_scheduleinfo_bbl, en_0651b06_seasoninfo_bbl " +
				" Where (en_0651b08_scheduleinfo_bbl.ateamid = en_0651b03_teaminfo_bbl.teamid Or " +
						" en_0651b08_scheduleinfo_bbl.bteamid = en_0651b03_teaminfo_bbl.teamid) And " +
					" en_0651b08_scheduleinfo_bbl.leagueid = en_0651b06_seasoninfo_bbl.leagueid And " +
					" en_0651b08_scheduleinfo_bbl.leagueid = " + oLeagueId + " And " +
					" en_0651b06_seasoninfo_bbl.seasonid = " + oSeasonId + " And " +
					" en_0651b08_scheduleinfo_bbl.scheduledate Between " +					
						" en_0651b06_seasoninfo_bbl.startdate And en_0651b06_seasoninfo_bbl.enddate " +				
				" Order By teamname_" + getLanguage () + " ";

			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_Teams)); 
				oBuffer.append (getStatusXML (T5_Teams, 1, "LeagueTableBL:getTeams:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T5_Teams, -1, "LeagueTableBL:getTeams:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T5_Teams, -1, "LeagueTableBL:getTeams:" + oException.toString ()));
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

	private String getLeagueTableList ()
	{		
		StringBuffer oBuffer = new StringBuffer ();					
		
		oBuffer.append (getLeagueTable ());		
		oBuffer.append (getLeagueTableHA ());
		oBuffer.append (getLeagueTable_L6M ());
		oBuffer.append (getLeagueTable_L12M ());		

        return oBuffer.toString ();				
	}

	private String getLeagueTable ()
    {
		StringBuffer oBuffer = new StringBuffer ();
        
        int nSportId = SportUtil.kS_BasketBall;
                    
		String [] arrInfo = getParams (kCR_LeagueTable_FLTR);				
		LeagueTableObj oLogic = new LeagueTableObj (getDocument ());
		oBuffer.append (oLogic.getLeagueTable (nSportId, arrInfo));		

        return oBuffer.toString ();
	}
	
	private String getLeagueTableHA ()
    {
		StringBuffer oBuffer = new StringBuffer ();

        int nSportId = SportUtil.kS_BasketBall;
            
        SideLeagueTableObj oLogic = new SideLeagueTableObj (getDocument ());
		NodeList arrNodes = getChildNodes (kCR_SideLeague_FLTR);

		for (int nIndex = 0; nIndex < arrNodes.getLength (); nIndex++)
		{
			Node oNode = arrNodes.item (nIndex);
			String [] arrInfo = getParams (oNode);

	        oBuffer.append (oLogic.getSideLeagueTable (nSportId, arrInfo));
		}

        return oBuffer.toString ();
	}

	private String getLeagueTable_L6M ()
    {
		StringBuffer oBuffer = new StringBuffer ();
        
        int nSportId = SportUtil.kS_BasketBall;
                    
		String [] arrInfo = getParams (kCR_LeagueTable_L6M_FLTR);				
		LeagueTableObj oLogic = new LeagueTableObj (getDocument ());
		oBuffer.append (oLogic.getLeagueTable (nSportId, arrInfo));		

        return oBuffer.toString ();
	}

	private String getLeagueTable_L12M ()
    {
		StringBuffer oBuffer = new StringBuffer ();
        
        int nSportId = SportUtil.kS_BasketBall;
                    
		String [] arrInfo = getParams (kCR_LeagueTable_L12M_FLTR);				
		LeagueTableObj oLogic = new LeagueTableObj (getDocument ());
		oBuffer.append (oLogic.getLeagueTable (nSportId, arrInfo));		

        return oBuffer.toString ();
	}

	public void log (String oMessage)
	{
//		logMessage (oMessage);
	}
}