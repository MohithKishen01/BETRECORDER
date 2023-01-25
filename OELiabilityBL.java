package enlj.p106trading.mssqlv51.p10651basketball.rboeliability.logics;

import java.sql.*;
import org.w3c.dom.*;

import enlj.projenv.logics.*;
import enlj.webenv.logics.*;
import enlj.projenv.mssql.*;
import enlj.webenv.utils.*;
import enlj.p106trading.mssqlv51.resource.logics.*;
import enlj.p101admin.commonsv11.resource.logics.userpm.*;

public class OELiabilityBL extends MSELogic
{
    /* Module Id */
    private final String kModuleId = "106510405";    	
    
	/* Task Ids */
	private final String kInitData	    = "10651040501";
	private final String kBetTypes		= "10651040504";
	private final String kEvents        = "10651040505";
	private final String kSmartMaster   = "10651040506";
	private final String kSmartGroup    = "10651040507";
	private final String kLiability     = "10651040508";
	private final String kLiability_TH	= "10651040509";
 	 
	/* Server Row Names */
    private final String kSR_Events         = "sr1";
    private final String kSR_BetTypes		= "sr2";
    private final String kSR_SmartMaster    = "sr3";
    private final String kSR_SmartGroups    = "sr4";
    private final String kSR_TimeStamp		= "sr5";
    private final String kSR_Liability      = "sr6";    
    private final String kSR_Liability_TH   = "sr7";
    private final String kSR_CompanyUnits	= "sr8";
   
	/* Client Row Names */
	private final String kCR_CommonData  = "cr1";
    private final String kCR_Liability   = "cr2";
    
	/* Status Ids */
	private final String GT_TimeStamp   = "51";
	
	private final String T1_FunctionPM		= "101";
	private final String T1_CompanyUnits    = "102";
	private final String T4_BetTypes		= "401";	
	private final String T5_Events			= "501";
	private final String T6_SmartMaster		= "601";
	private final String T7_SmartGroups		= "701";
	private final String T8_Liability		= "801";
	private final String T9_Liability_TH	= "901";
    
    /* Common list Fields */
    private final int f_FromDate_CD	    = 0;
    private final int f_ToDate_CD       = 1;
    private final int f_CompanyUnitId_CD   = 2;
        
    /* Liability list Fields */
    private final int f_FromDate_LL          = 0;
    private final int f_ToDate_LL            = 1;
    private final int f_BetTypeIds_LL		 = 2;	
    private final int f_EventIds_LL          = 3;
    private final int f_SmartGroupIds_LL     = 4;
    private final int f_SmartMasterIds_LL    = 5;
    private final int f_CurrentTimeStamp_LL  = 6;
    private final int f_CompanyUnitId_LL     = 7;
        
   	public OELiabilityBL ()
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
        else if (oTaskId.equals (kBetTypes))
        {
			oXMLString = getBetTypes ();
        }
        else if (oTaskId.equals (kEvents))
        {
            oXMLString = getEvents ();
        }
        else if (oTaskId.equals (kSmartMaster))
        {
            oXMLString = getSmartMasters ();
        }
        else if (oTaskId.equals (kSmartGroup))
        {
            oXMLString = getSmartGroups ();
        }
        else if (oTaskId.equals (kLiability))
        {
            oXMLString = getLiability ();
        }
        else if (oTaskId.equals (kLiability_TH))
        {
            oXMLString = getLiability_TH ();
        }
       
		return oXMLString;
	}
	
	private String getInitData ()
    {
        StringBuffer oBuffer = new StringBuffer ();
        oBuffer.append (getFunctionPM (getUserId (), kModuleId, T1_FunctionPM));
        oBuffer.append (TradingUtil.getCompanyUnits (this, T1_CompanyUnits, kSR_CompanyUnits, ConstantsUtil.kFE_ChooseOne));
        
        return oBuffer.toString ();
    }          
    
    private String getLiability ()
    {
        DBConnector10651 oConnector = new DBConnector10651 ();
		Statement oStatement = null;
		ResultSet oResultSet = null;

        StringBuffer oBuffer = new StringBuffer ();

        try
        {  
            String [] arrInfo = getParams (kCR_Liability);          
            String oSQL = getLiabilitySQL (arrInfo, false);                        

			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			
            if (oResultSet != null)
            {
                oBuffer.append (toXMLString (oResultSet, kSR_Liability));
                oBuffer.append (getCurrentTimeStamp ());
                oBuffer.append (getStatusXML (T8_Liability, 1, "OELiabilityBL:getLiability:Successful"));
            }
            else
                oBuffer.append (getStatusXML (T8_Liability, -1, "OELiabilityBL:getLiability:Unsuccessful"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T8_Liability, -1, "OELiabilityBL:getLiability" + oException.toString ()));
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
    
    private String getLiability_TH ()
    {
        DBConnector10651 oConnector = new DBConnector10651 ();
		Statement oStatement = null;
		ResultSet oResultSet = null;

        StringBuffer oBuffer = new StringBuffer ();

        try
        {  
            String [] arrInfo = getParams (kCR_Liability);          
            String oSQL = getLiabilitySQL (arrInfo, true);                        

			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			
            if (oResultSet != null)
            {
                oBuffer.append (toXMLString (oResultSet, kSR_Liability_TH));
                oBuffer.append (getCurrentTimeStamp ());
                oBuffer.append (getStatusXML (T9_Liability_TH, 1, "OELiabilityBL:getLiability_TH:Successful"));
            }
            else
                oBuffer.append (getStatusXML (T9_Liability_TH, -1, "OELiabilityBL:getLiability_TH:Unsuccessful"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T9_Liability_TH, -1, "OELiabilityBL:getLiability_TH" + oException.toString ()));
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
    
    private String getLiabilitySQL (String [] arrInfo, boolean bThread)
    {
        String oFromDate = arrInfo [f_FromDate_LL] + " 00:00:00";
	    String oToDate = arrInfo [f_ToDate_LL] + " 23:59:59";
        
        String oSQL =             
            " Select en_0651c03_liabilityoe_bbl.smartid, " +
                " en_0651b04_smartgroupinfo.groupcode, " +
                " en_0651b04_smartgroupinfo.fgcolor, " +
                " en_0651b04_smartgroupinfo.bgcolor, " +
                " en_0151z00_currency.currencyid, " +
                " en_0151z00_currency.currencycode_" + getLanguage () + ", " +
                " group_currateinfo.currencyrate As group_currate, " +
                " en_0651c03_liabilityoe_bbl.scheduleid, " +
                " Convert (nvarchar, en_0651c02_betstatus_bbl.scheduledate, 103) As scheduledate, " +
                " Convert (nvarchar (5), en_0651c02_betstatus_bbl.scheduledate, 108) As scheduletime, " +
                " en_0651c03_liabilityoe_bbl.leagueid, " +
                " en_0651c03_liabilityoe_bbl.leaguename, " +
                " en_0651c03_liabilityoe_bbl.ateamname, " +
                " en_0651c03_liabilityoe_bbl.bteamname, " +
                " Sum (en_0651c03_liabilityoe_bbl.stake * account_currateinfo.currencyrate) As stake, " +
	            " Max (en_0651c02_betstatus_bbl.nonl_totalwinlose + en_0651c02_betstatus_bbl.live_totalwinlose) As totalwinlose, " +
	            " Max (en_0651c02_betstatus_bbl.nonl_totalturnover + en_0651c02_betstatus_bbl.live_totalturnover) As totalturnover, " +
                " Sum (en_0651c03_liabilityoe_bbl.odd * account_currateinfo.currencyrate) As oddliability, " +
                " Sum (en_0651c03_liabilityoe_bbl.even * account_currateinfo.currencyrate) As evenliability " +
           " From en_0651c03_liabilityoe_bbl, en_0651c02_betstatus_bbl, en_0651b04_smartgroupinfo, " +
	            " en_0151z00_currency, en_0651z00_currencyrates As group_currateinfo, " +  
	            " en_0651z00_currencyrates As account_currateinfo, en_0251z00_bettype WITH (NOLOCK) " +
            " Where en_0651c03_liabilityoe_bbl.betrefid = en_0651c02_betstatus_bbl.betrefid And " +
	            " en_0651c02_betstatus_bbl.smartid = en_0651b04_smartgroupinfo.smartid And " +
	            " en_0651b04_smartgroupinfo.currencyid = en_0151z00_currency.currencyid And " +
	            " group_currateinfo.currencyid = en_0651b04_smartgroupinfo.currencyid And " +
	            " account_currateinfo.currencyid = en_0651c03_liabilityoe_bbl.currencyid And " +    				
				" group_currateinfo.ratetypeid = " + ConstantsUtil.kRateT_HKD + " And " +
				" account_currateinfo.ratetypeid = " + ConstantsUtil.kRateT_HKD + " And " +
				" en_0251z00_bettype.bettypeid = en_0651c02_betstatus_bbl.bettypeid And " +
				" en_0651b04_smartgroupinfo.unitid = " + arrInfo [f_CompanyUnitId_LL] + " And " +
				" en_0251z00_bettype.mainbettypeid = " + SportUtil.kBMBT_OddEven +
				getDateCondition_PB (oFromDate, oToDate) +
				getIgnoreClientCondition () +
                getLiabilityCondition (arrInfo) + 
                getThreadCondition (arrInfo, bThread) +
            " Group By en_0651c03_liabilityoe_bbl.smartid, " +
                " en_0651b04_smartgroupinfo.groupcode, " +
                " en_0651b04_smartgroupinfo.fgcolor, " +
                " en_0651b04_smartgroupinfo.bgcolor, " +
                " en_0151z00_currency.currencyid, " +
                " en_0151z00_currency.currencycode_" + getLanguage () + ", " +
                " group_currateinfo.currencyrate, " +
                " en_0651c03_liabilityoe_bbl.scheduleid, " +
                " en_0651c02_betstatus_bbl.scheduledate, " +
                " en_0651c03_liabilityoe_bbl.leagueid, " +
                " en_0651c03_liabilityoe_bbl.leaguename, " +
                " en_0651c03_liabilityoe_bbl.ateamname, " +
                " en_0651c03_liabilityoe_bbl.bteamname " +
            " Order By en_0651b04_smartgroupinfo.groupcode, en_0651c03_liabilityoe_bbl.ateamname ";

        return oSQL;
	}
	
	private String getRefreshSmartIds (String [] arrInfo)
    {
		DBConnector10651 oConnector = new DBConnector10651 ();
		Statement oStatement = null;
		ResultSet oResultSet = null;
    
        StringBuffer oSmartIdsBuffer = new StringBuffer ("-1");
        
        try
        {
            String oFromDate = arrInfo [f_FromDate_LL] + " 00:00:00";
		    String oToDate = arrInfo [f_ToDate_LL] + " 23:59:59";
            String oCurrentTimeStamp = arrInfo [f_CurrentTimeStamp_LL];
            
            String oSQL =             
                " Select Distinct en_0651c03_liabilityoe_bbl.smartid " +	       
                " From en_0651c03_liabilityoe_bbl, en_0651b04_smartgroupinfo, en_0651c02_betstatus_bbl WITH (NOLOCK) " +
                " Where en_0651c03_liabilityoe_bbl.smartid = en_0651b04_smartgroupinfo.smartid And " +
	                " en_0651c02_betstatus_bbl.scheduleid = en_0651c03_liabilityoe_bbl.scheduleid And " +	
	                " en_0651b04_smartgroupinfo.unitid = " + arrInfo [f_CompanyUnitId_LL] + " And " +                
	                " en_0651c02_betstatus_bbl.createddate >= '" + oCurrentTimeStamp + "' " +
	                getDateCondition_PB (oFromDate, oToDate) +
	                getIgnoreClientCondition () +
	                getLiabilityCondition (arrInfo);

			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			
			if (oResultSet != null)
			{
				while (oResultSet.next ())   
					oSmartIdsBuffer.append ("," + oResultSet.getString ("smartid"));
			}
        }
        catch (Exception oException)
        {
            log ("OELiabilityBL:getRefreshSmartIds:" + oException.toString ());
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

        return oSmartIdsBuffer.toString ();
    }
    
    private String getLiabilityCondition (String [] arrInfo)
	{
		String oCondition = "";
		
		String oBetTypeIds = arrInfo [f_BetTypeIds_LL];
		if (oBetTypeIds.equals ("0") == false)
			oCondition += " And en_0651c03_liabilityoe_bbl.bettypeid In (" + oBetTypeIds + ") ";
				
		String oEventIds = arrInfo [f_EventIds_LL];
		if (oEventIds.equals ("0") == false)
			oCondition += " And en_0651c03_liabilityoe_bbl.scheduleid In (" + oEventIds + ") ";
			
		String oSmartGroupIds = arrInfo [f_SmartGroupIds_LL];
		if (oSmartGroupIds.equals ("0") == false)
			oCondition += " And en_0651b04_smartgroupinfo.smartid In (" + oSmartGroupIds + ") ";
			
		String oSmartMasterIds = arrInfo [f_SmartMasterIds_LL];
		if (oSmartMasterIds.equals ("0") == false)
			oCondition += " And en_0651b04_smartgroupinfo.mastergroupid In (" + oSmartMasterIds + ") ";
			
		UserSmartMasterPM_T1 objSmartMasterPM = new UserSmartMasterPM_T1 (getDocument ());
		String oSmartMasterIds_PM = objSmartMasterPM.getUserSmartMasterIds_PM (getUserId (), kModuleId, SportUtil.kS_BasketBall, convertToInt (arrInfo [f_CompanyUnitId_LL]));

		if (oSmartMasterIds_PM.equals ("0") == false)
		{
			oSmartMasterIds_PM = oSmartMasterIds_PM.replaceAll(", ", "_");
			oCondition += " And CHARINDEX ('_' + Convert (nvarchar, en_0651b04_smartgroupinfo.mastergroupid) + '_', Convert (nvarchar (max), '_" + oSmartMasterIds_PM + "_')) > 0 ";
		}

		UserSmartGroupPM_T1 objSmartGroupPM = new UserSmartGroupPM_T1 (getDocument ());
		String oSmartGroupIds_PM = objSmartGroupPM.getUserSmartGroupIds_PM (getUserId (), kModuleId, SportUtil.kS_BasketBall, convertToInt (arrInfo [f_CompanyUnitId_LL]));		

		if (oSmartGroupIds_PM.equals ("0") == false)
		{
			oSmartGroupIds_PM = oSmartGroupIds_PM.replaceAll(", ", "_");
			oCondition += " And CHARINDEX ('_' + Convert (nvarchar, en_0651b04_smartgroupinfo.smartid) + '_', Convert (nvarchar (max), '_" + oSmartGroupIds_PM + "_')) > 0 ";
		}

		return oCondition;
	}
    
    private String getThreadCondition (String [] arrInfo, boolean bThread)
	{
		String oCondition = "";
		
		if (bThread)
        {
		    String oCurrentTimeStamp = arrInfo [f_CurrentTimeStamp_LL];
		    oCurrentTimeStamp = oCurrentTimeStamp.equals ("notset") ? " GetDate () " : " '" + oCurrentTimeStamp + "' ";

            String oSmartIds = getRefreshSmartIds (arrInfo);
		    oCondition = " And en_0651c03_liabilityoe_bbl.smartid In (" + oSmartIds + ") ";
        } 
        
        return oCondition;
	}	    
    
    private String getCurrentTimeStamp ()
    {
        DBConnector10651 oConnector = new DBConnector10651 ();
		Statement oStatement = null;
		ResultSet oResultSet = null;		

        StringBuffer oBuffer = new StringBuffer ();
        try
        {
            String oSQL = 
                " Select Replace (Convert (nvarchar, GetDate (), 111), '/', '-') + ' ' + " + 
			        " Convert (nvarchar, GetDate (), 114) As timestampvalue ";
			        
			oStatement = oConnector.getStatement ();			
			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			
            if (oResultSet != null)
            {
                oBuffer.append (toXMLString (oResultSet, kSR_TimeStamp));
                oBuffer.append (getStatusXML (GT_TimeStamp, 1, "OELiabilityBL:getCurrentTimeStamp:Successful"));
            }
            else
                oBuffer.append (getStatusXML (GT_TimeStamp, -1, "OELiabilityBL:getCurrentTimeStamp:Unsuccessful"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (GT_TimeStamp, -1, "OELiabilityBL:getCurrentTimeStamp" + oException.toString ()));
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
    
    private String getBetTypes ()
    {
        DBConnector10651 oConnector = new DBConnector10651 ();
		Statement oStatement = null;
		ResultSet oResultSet = null;

        StringBuffer oBuffer = new StringBuffer ();

        try
        {           
            String [] arrInfo = getParams (kCR_CommonData);
            String oFromDate = arrInfo [f_FromDate_CD] + " 00:00:00";
		    String oToDate = arrInfo [f_ToDate_CD] + " 23:59:59";
		    
            String oSQL =             
                " Select Distinct en_0251z00_bettype.bettypeid, " +
	                " en_0251z00_bettype.bettype_" + getLanguage () +
                " From en_0651c03_liabilityoe_bbl, en_0651c02_betstatus_bbl, en_0251b02_accountinfo, en_0251z00_bettype WITH (NOLOCK) " +
                " Where en_0251z00_bettype.bettypeid = en_0651c03_liabilityoe_bbl.bettypeid And " +
                    " en_0651c03_liabilityoe_bbl.bettypeid = en_0651c02_betstatus_bbl.bettypeid And " +
                    " en_0651c02_betstatus_bbl.accountid = en_0251b02_accountinfo.accountid And " +
                    " en_0251b02_accountinfo.unitid = " + arrInfo [f_CompanyUnitId_CD] + " And " +
	                " en_0251z00_bettype.mainbettypeid = " + SportUtil.kBMBT_OddEven + " And " +
	                " en_0651c02_betstatus_bbl.scheduleid > 0 " +
	                getDateCondition_PB (oFromDate, oToDate) +
	                getIgnoreClientCondition () +
                " Order By bettypeid ";

			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			
            if (oResultSet != null)
            {
                oBuffer.append (toXMLString (oResultSet, kSR_BetTypes));
                oBuffer.append (getStatusXML (T4_BetTypes, 1, "OELiabilityBL:getBetTypes:Successful"));
            }
            else
                oBuffer.append (getStatusXML (T4_BetTypes, -1, "OELiabilityBL:getBetTypes:Unsuccessful"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T4_BetTypes, -1, "OELiabilityBL:getBetTypes" + oException.toString ()));
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
    
    private String getEvents ()
    {
        DBConnector10651 oConnector = new DBConnector10651 ();
		Statement oStatement = null;
		ResultSet oResultSet = null;

        StringBuffer oBuffer = new StringBuffer ();

        try
        {            
            String [] arrInfo = getParams (kCR_CommonData);
            String oFromDate = arrInfo [f_FromDate_CD] + " 00:00:00";
		    String oToDate = arrInfo [f_ToDate_CD] + " 23:59:59";
		    
            String oSQL = 
                " Select Distinct en_0651c02_betstatus_bbl.scheduleid, " +
	                " en_0651c02_betstatus_bbl.leagueid, " +
                    " en_0651c02_betstatus_bbl.leaguename, " +
                    " en_0651c02_betstatus_bbl.ateamname, " +
	                " en_0651c02_betstatus_bbl.bteamname " +	                
                " From en_0651c02_betstatus_bbl, en_0651c03_liabilityoe_bbl, en_0251b02_accountinfo, en_0251z00_bettype WITH (NOLOCK) " +
                " Where en_0651c02_betstatus_bbl.scheduleid = en_0651c03_liabilityoe_bbl.scheduleid And " +
					" en_0251z00_bettype.bettypeid = en_0651c02_betstatus_bbl.bettypeid And " +
					" en_0651c02_betstatus_bbl.accountid = en_0251b02_accountinfo.accountid And " +
                    " en_0251b02_accountinfo.unitid = " + arrInfo [f_CompanyUnitId_CD] + " And " +
					" en_0251z00_bettype.mainbettypeid = " + SportUtil.kBMBT_OddEven + " And " +
                    " en_0651c02_betstatus_bbl.scheduleid > 0 " +
                    getDateCondition_PB (oFromDate, oToDate) +
                    getIgnoreClientCondition () +
                " Order By leaguename ";

			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			
            if (oResultSet != null)
            {
                oBuffer.append (toXMLString (oResultSet, kSR_Events));
                oBuffer.append (getStatusXML (T5_Events, 1, "OELiabilityBL:getEvents:Successful"));
            }
            else
                oBuffer.append (getStatusXML (T5_Events, -1, "OELiabilityBL:getEvents:Unsuccessful"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T5_Events, -1, "OELiabilityBL:getEvents" + oException.toString ()));
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
    
    private String getSmartMasters ()
    {
        DBConnector10651 oConnector = new DBConnector10651 ();
		Statement oStatement = null;
		ResultSet oResultSet = null;

        StringBuffer oBuffer = new StringBuffer ();

        try
        {
            String [] arrInfo = getParams (kCR_CommonData);
            String oFromDate = arrInfo [f_FromDate_CD] + " 00:00:00";
		    String oToDate = arrInfo [f_ToDate_CD] + " 23:59:59";
		    String oConditionPM = "";

			UserSmartMasterPM_T1 objSmartMasterPM = new UserSmartMasterPM_T1 (getDocument ());
			String oSmartMasterIds_PM = objSmartMasterPM.getUserSmartMasterIds_PM (getUserId (), kModuleId, SportUtil.kS_BasketBall, convertToInt (arrInfo [f_CompanyUnitId_CD]));

			if (oSmartMasterIds_PM.equals ("0") == false)
			{
				oSmartMasterIds_PM = oSmartMasterIds_PM.replaceAll(", ", "_");
				oConditionPM += " And CHARINDEX ('_' + Convert (nvarchar, en_0651b04_smartgroupinfo.mastergroupid) + '_', Convert (nvarchar (max), '_" + oSmartMasterIds_PM + "_')) > 0 ";
			}

            String oSQL =
                " Select Distinct en_0651b04_smartmasterinfo.smartid, " +
                    " en_0651b04_smartmasterinfo.mastercode " +
                " From en_0651b04_smartmasterinfo, en_0651b04_smartgroupinfo, " +
                    " en_0651c02_betstatus_bbl, en_0651c03_liabilityoe_bbl, en_0251z00_bettype WITH (NOLOCK) " +
                " Where en_0651b04_smartmasterinfo.smartid = en_0651b04_smartgroupinfo.mastergroupid And " +
                    " en_0651b04_smartgroupinfo.smartid = en_0651c02_betstatus_bbl.smartid And " +
                    " en_0651b04_smartgroupinfo.smartid = en_0651c03_liabilityoe_bbl.smartid And " +
					" en_0251z00_bettype.bettypeid = en_0651c02_betstatus_bbl.bettypeid And " +
					" en_0651b04_smartmasterinfo.unitid = " + arrInfo [f_CompanyUnitId_CD] + " And " +
					" en_0251z00_bettype.mainbettypeid = " + SportUtil.kBMBT_OddEven + " And " +
                    " en_0651c02_betstatus_bbl.scheduleid > 0 " +
                    getDateCondition_PB (oFromDate, oToDate) +
                    getIgnoreClientCondition () +
                    oConditionPM +
                " Order By mastercode ";

			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			
            if (oResultSet != null)
            {
                oBuffer.append (toXMLString (oResultSet, kSR_SmartMaster));
                oBuffer.append (getStatusXML (T6_SmartMaster, 1, "OELiabilityBL:getSmartMasters:Successful"));
            }
            else
                oBuffer.append (getStatusXML (T6_SmartMaster, -1, "OELiabilityBL:getSmartMasters:Unsuccessful"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T6_SmartMaster, -1, "OELiabilityBL:getSmartMasters" + oException.toString ()));
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
    
    private String getSmartGroups ()
    {
        DBConnector10651 oConnector = new DBConnector10651 ();
		Statement oStatement = null;
		ResultSet oResultSet = null;

        StringBuffer oBuffer = new StringBuffer ();

        try
        {            
            String [] arrInfo = getParams (kCR_CommonData);
            String oFromDate = arrInfo [f_FromDate_CD] + " 00:00:00";
		    String oToDate = arrInfo [f_ToDate_CD] + " 23:59:59";
		    String oConditionPM = "";
		    
			UserSmartGroupPM_T1 objSmartGroupPM = new UserSmartGroupPM_T1 (getDocument ());
			String oSmartGroupIds_PM = objSmartGroupPM.getUserSmartGroupIds_PM (getUserId (), kModuleId, SportUtil.kS_BasketBall, convertToInt (arrInfo [f_CompanyUnitId_CD]));			

			if (oSmartGroupIds_PM.equals ("0") == false)
			{
				oSmartGroupIds_PM = oSmartGroupIds_PM.replaceAll(", ", "_");
				oConditionPM += " And CHARINDEX ('_' + Convert (nvarchar, en_0651b04_smartgroupinfo.smartid) + '_', Convert (nvarchar (max), '_" + oSmartGroupIds_PM + "_')) > 0 ";
			}

            String oSQL =             
                " Select Distinct en_0651b04_smartgroupinfo.smartid, " +
                    " en_0651b04_smartgroupinfo.groupcode " +
                " From en_0651b04_smartgroupinfo, en_0651c02_betstatus_bbl, en_0651c03_liabilityoe_bbl, " +
					" en_0251z00_bettype WITH (NOLOCK) " +
                " Where en_0651b04_smartgroupinfo.smartid = en_0651c02_betstatus_bbl.smartid And " +
                    " en_0651b04_smartgroupinfo.smartid = en_0651c03_liabilityoe_bbl.smartid And " +
					" en_0251z00_bettype.bettypeid = en_0651c02_betstatus_bbl.bettypeid And " +
					" en_0651b04_smartgroupinfo.unitid = " + arrInfo [f_CompanyUnitId_CD] + " And " +
					" en_0251z00_bettype.mainbettypeid = " + SportUtil.kBMBT_OddEven + " And " +
                    " en_0651c02_betstatus_bbl.scheduleid > 0 " +
                    getDateCondition_PB (oFromDate, oToDate) +
                    getIgnoreClientCondition () +
                    oConditionPM +
                " Order By groupcode ";

			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			
            if (oResultSet != null)
            {
                oBuffer.append (toXMLString (oResultSet, kSR_SmartGroups));
                oBuffer.append (getStatusXML (T7_SmartGroups, 1, "OELiabilityBL:getSmartGroups:Successful"));
            }
            else
                oBuffer.append (getStatusXML (T7_SmartGroups, -1, "OELiabilityBL:getSmartGroups:Unsuccessful"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T7_SmartGroups, -1, "OELiabilityBL:getSmartGroups" + oException.toString ()));
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

    private String getIgnoreClientCondition ()
    {
        String oCondition = " And en_0651c02_betstatus_bbl.clientid Not In ( " +
            " Select en_0651c91_ignoreclients_bbt.clientid From en_0651c91_ignoreclients_bbt) ";        
            
        return oCondition;
    }
    
    private String getDateCondition_PB (String oFromDate, String oToDate)
	{
		String oCondition = " And DateAdd (Minute, " + ConstantsUtil.kOffsetMinutes + ", en_0651c02_betstatus_bbl.scheduledate) " +
			" Between '" + oFromDate + "' And DateAdd (Minute, " + ConstantsUtil.kExtraMinutes + ", '" + oToDate + "') ";
		
		return oCondition;
	}
	
	public void log (String oMessage)
	{
//		logMessage (oMessage);
	}
}