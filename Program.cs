using System;
using System.Xml;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.SqlClient;
using IBM.Data.DB2;

namespace SAPExtractor
{
    static class Debug
    {
        static public void Run()
        {
#if DEBUG
            Console.WriteLine("DEBUG");
            Console.WriteLine(Data.index_names.IndexOf("pk"));
#endif
        }
        static public void End()
        {
#if DEBUG
            Console.WriteLine("");
            Console.WriteLine("Press any key to close ...");
            Console.ReadKey(true);
#endif
        }
    }

    static class Data
    {
        public static int batchsize;
        public static string profile, runid, md5_config, md5_profile;
        public static string sap_host, sap_port, sap_dbname, sap_schema, sap_username, sap_password, sap_query, sap_nullable;
        public static string dwh_host, dwh_port, dwh_dbname, dwh_schema, dwh_username, dwh_password, dwh_table, dwh_insertmode, dwh_finalization;
        public static string temptable, finaltable, backuptable, rowcount;
        public static List<string> field_names = new List<string>();
        public static List<string> field_types = new List<string>();
        public static List<string> index_names = new List<string>();
        public static List<string> index_unique = new List<string>();
        public static List<string> index_fields = new List<string>();
        public static List<string> param_names = new List<string>();
        public static List<string> param_sources = new List<string>();
        public static List<string> param_queries = new List<string>();
        public static List<string> param_values = new List<string>();
        
    }

    static class Program
    {
        static public string version = System.Reflection.Assembly.GetExecutingAssembly().GetName().Version.ToString();
        static public string pid = System.Diagnostics.Process.GetCurrentProcess().Id.ToString();
        static public string workdir = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
        static public string workname = typeof(Program).Assembly.GetName().Name;
        static int errorstate = 0;
        static DateTime startdate = DateTime.Now;
        static void Main(string[] args)
        {
            if (args.GetLength(0) == 0)
            {
                Log.Print("No parameter specified!");
                System.Environment.Exit(999);
            }
            if (args.GetLength(0) > 0)
            {
                Data.profile = args[0].ToUpper();
            }
            if (args.GetLength(0) > 1)
            {
                Data.runid = args[1].ToUpper();
            }
            else
            {
                Data.runid = startdate.ToString("yyyyMMddHHmmssff");
            }
            Program.Run();
        }
        static void Run()
        {
            Log.Init(workdir + @"\Logs\" + workname + "_" + startdate.ToString("yyyyMM") + ".log");
            Log.Print("<hr>");
            Log.Print("STARTING - Profile:[" + Data.profile + "] - RunID:[" + Data.runid + "] - PID:[" + pid + "]");
            Log.Print("  Version:[" + Program.version + "] - Host:[" + Environment.MachineName + "]");
            Program.ReadConfigXML(workdir + @"\" + workname + ".xml");
            Program.ReadProfileXML(workdir + @"\Profiles\" + Data.profile + ".xml");
            Program.InitializeDB();
            Program.CheckRunningProc();
            Program.PrepareExtraction();
            Program.QueryParameters();
            Program.ExtractData();
            Program.TransformData();
            Program.Finalization();
            Program.Cleanup();

            TimeSpan runtime = DateTime.Now - startdate;
            Log.Print("FINISHED - Profile:[" + Data.profile + "] - RunID:[" + Data.runid + "] - Duration:[" + Math.Floor(runtime.TotalHours).ToString("##00") + ":" + runtime.Minutes.ToString("00") + ":" + runtime.Seconds.ToString("00") + "]");
            Log.Print("<hr>");
            Log.Close();
            Debug.Run();
            
            Program.Exit(-1);
        }
        static void ReadConfigXML(string filepath)
        {
            try
            {
                XML xml = new XML(filepath);
                Log.Print("MD5 of ConfigXML: [" + xml.md5 + "]");
                Data.md5_config = xml.md5;
                Data.sap_host = xml.GetValue("/config/sap/host");
                Data.sap_port = xml.GetValue("/config/sap/port");
                Data.sap_dbname = xml.GetValue("/config/sap/dbname");
                Data.sap_schema = xml.GetValue("/config/sap/schema");
                Data.sap_username = xml.GetValue("/config/sap/username");
                Data.sap_password = xml.GetValue("/config/sap/password");
                Data.dwh_host = xml.GetValue("/config/dwh/host");
                Data.dwh_dbname = xml.GetValue("/config/dwh/dbname");
                Data.dwh_schema = xml.GetValue("/config/dwh/schema");
                Data.dwh_username = xml.GetValue("/config/dwh/username");
                Data.dwh_password = xml.GetValue("/config/dwh/password");
                Data.batchsize = Convert.ToInt32(xml.GetValue("/config/program/batchsize"));
                Program.Exit(0);
            }
            catch (Exception expt)
            {
                Log.Print("Error reading config XML : " + expt.Message);
                Program.Exit(19);
            }
        }
        static void ReadProfileXML(string filepath)
        {
            try
            {
                XML xml = new XML(filepath);
                Log.Print("MD5 of ProfileXML: [" + xml.md5 + "]");
                Data.md5_profile = xml.md5;
                Data.sap_query = xml.GetValue("/profile/sap/query");
                Data.sap_nullable = xml.GetValue("/profile/sap/query/@nullable", true);
                Data.dwh_table = xml.GetValue("/profile/dwh/tablename");
                Data.dwh_insertmode = xml.GetValue("/profile/dwh/insertmode").ToLower();
                Data.dwh_finalization = xml.GetValue("/profile/dwh/finalization", true);

                for (int i = 1; i <= xml.GetCount("/profile/dwh/fieldtypes/field"); i++)
                {
                    Data.field_names.Add(xml.GetValue("/profile/dwh/fieldtypes/field[" + i + "]/@name"));
                    Data.field_types.Add(xml.GetValue("/profile/dwh/fieldtypes/field[" + i + "]/@type"));
                }
                for (int i = 1; i <= xml.GetCount("/profile/dwh/indexes/index", true); i++)
                {
                    Data.index_names.Add(xml.GetValue("/profile/dwh/indexes/index[" + i + "]/@name").ToLower());
                    Data.index_unique.Add(xml.GetValue("/profile/dwh/indexes/index[" + i + "]/@unique").ToLower());
                    Data.index_fields.Add(xml.GetValue("/profile/dwh/indexes/index[" + i + "]/@fields"));
                }
                for (int i = 1; i <= xml.GetCount("/profile/parameters/param", true); i++)
                {
                    Data.param_names.Add(xml.GetValue("/profile/parameters/param[" + i + "]/@name"));
                    Data.param_sources.Add(xml.GetValue("/profile/parameters/param[" + i + "]/@source").ToLower());
                    Data.param_queries.Add(xml.GetValue("/profile/parameters/param[" + i + "]"));
                    Data.param_values.Add("");
                }
                Data.temptable = "[" + Data.dwh_dbname + "].[" + Data.dwh_schema + "].[SAPExtractor_tmp_" + Data.profile + "]";
                Data.finaltable = "[" + Data.dwh_dbname + "].[" + Data.dwh_schema + "].[" + Data.dwh_table + "]";
                Data.backuptable = "[" + Data.dwh_dbname + "].[" + Data.dwh_schema + "].[SAPExtractor_bak_" + Data.profile + "]";
                if (Data.dwh_insertmode != "append" && Data.dwh_insertmode != "truncate" && Data.dwh_insertmode != "merge" && Data.dwh_insertmode != "manual")
                {
                    Log.Print("Invalid insertmode in profile: " + Data.dwh_insertmode);
                    Program.Exit(5);
                }
                if (Data.sap_nullable == null) Data.sap_nullable = "n";
                if (Data.sap_nullable != "y" && Data.sap_nullable != "n")
                {
                    Log.Print("Invalid nullable value in profile: " + Data.sap_nullable);
                    Program.Exit(6);
                }
                if (Data.dwh_insertmode == "merge" && Data.index_names.IndexOf("pk") < 0)
                {
                    Log.Print("Insertmode 'merge' requires a unique index named 'pk'!");
                    Program.Exit(6);
                }
                Program.Exit(0);
            }
            catch (Exception expt)
            {
                Log.Print("Error reading profile XML : " + expt.Message);
                Program.Exit(18);
            }
        }
        static void InitializeDB()
        {
            try
            {
                Log.Print("Initializing connections");
                DB.Initialize("dwh", Data.dwh_host, Data.dwh_port, Data.dwh_username, Data.dwh_password, Data.dwh_dbname, Data.dwh_schema);
                DB.Initialize("sap", Data.sap_host, Data.sap_port, Data.sap_username, Data.sap_password, Data.sap_dbname, Data.sap_schema);
            }
            catch (Exception expt)
            {
                Log.Print("Error during initializing DB: " + expt.Message);
                Program.Exit(7);
            }
        }
        static void CheckRunningProc()
        {
            try
            {
                Log.Print("Check for other running extractions");
                string oldpid = DB.QueryValue("dwh", "select processid from [" + Data.dwh_dbname + "].[" + Data.dwh_schema + "].[SAPExtractor_log] where substring(status,1,1) not in ('C','E','9') and profile = '" + Data.profile + "' and runid != '" + Data.runid + "' order by startdate desc, runid desc;", true);
                if (oldpid != null && pid != oldpid)
                {
                    System.Diagnostics.Process[] proc = System.Diagnostics.Process.GetProcesses();
                    for (int i = 0; i < proc.Count(); i++)
                    {
                        if (proc[i].Id.ToString() == oldpid && proc[i].ProcessName == System.Diagnostics.Process.GetCurrentProcess().ProcessName)
                        {
                            Log.Print("Stoping this RunID due to other still running ProcessID [" + oldpid + "]");
                            Program.SetExtractLog("InsertNew", "status='E0', remark='Old PID [" + oldpid + "] still running!'");
                            Program.Exit(8);
                        }
                    }
                }
                Program.SetExtractLog("CancelOld", "status='0'");
            }
            catch (Exception expt)
            {
                Log.Print("Error checking for running extractions: " + expt.Message);
                Program.SetExtractLog("Update", "status='E'+status, remark='Error checking for running extractions'");
                Program.Exit(9);
            }
        }
        static void PrepareExtraction()
        {
            string sql = "";
            try
            {
                Log.Print("Preparing extraction");
                //Deleting Temp-Table
                DB.Execute("dwh", "begin try drop table " + Data.temptable + " end try begin catch end catch");

                //Create Temp-Table according to defined Fieldtypes, only Dates are stored as VarChar.
                sql = "create table " + Data.temptable + "(";
                for (int i = 0; i < Data.field_types.Count; i++)
                {
                    if (Data.field_types[i].ToLower().Contains("date")) sql += " [" + Data.field_names[i] + "] varchar(14),";
                    else sql += " [" + Data.field_names[i] + "] " + Data.field_types[i] + ",";
                }
                sql = sql.Trim(',') + ")";
                DB.Execute("dwh", sql);

                //Create PK-Index if available
                //if (Data.index_names.IndexOf("pk") >= 0) DB.Execute("dwh", "create unique nonclustered index " + Data.dwh_table + "_tmpPK on " + Data.temptable + " (" + Data.index_fields[Data.index_names.IndexOf("pk")] + ")");

                Program.SetExtractLog("Update", "status='1'");
            }
            catch (Exception expt)
            {
                Log.Print("Error preparing extraction: " + expt.Message);
                Program.SetExtractLog("Update", "status='E'+status, remark='Error preparing extraction'");
                Program.Exit(10);
            }
        }
        static void QueryParameters()
        {
            string param = "";
            try
            {
                Log.Print("Querying predefined parameters");

                Program.ReplaceParameter("DestinationTable", Data.dwh_table);
                Program.ReplaceParameter("ThisProfile", Data.profile);
                Program.ReplaceParameter("ThisRunID", Data.runid);

                param = DB.QueryValue("dwh", "select runid from [" + Data.dwh_dbname + "].[" + Data.dwh_schema + "].[SAPExtractor_log] where profile = '" + Data.profile + "' and runid != '" + Data.runid + "' order by startdate desc, runid desc;", true);
                Log.Print("  LastRunID: [" + param + "]");
                Program.ReplaceParameter("LastRunID", param);

                param = DB.QueryValue("dwh", "select runid from [" + Data.dwh_dbname + "].[" + Data.dwh_schema + "].[SAPExtractor_log] where profile = '" + Data.profile + "' and runid != '" + Data.runid + "' and status = '9' order by startdate desc, runid desc;", true);
                Log.Print("  LastFinishedRunID: [" + param + "]");
                Program.ReplaceParameter("LastFinishedRunID", param);

                Program.SetExtractLog("Update", "status='2'");

                param = "";
                int cnt = Data.param_names.Count;
                if (cnt > 0)
                {
                    Log.Print("Querying " + cnt + " custom parameters");
                    param = "";
                    for (int i = 0; i < cnt; i++)
                    {
                        if (Data.param_names[i] == "ThisProfile" || Data.param_names[i] == "ThisRunID" || Data.param_names[i] == "LastRunID" || Data.param_names[i] == "LastFinishedRunID" || Data.param_names[i] == "DestinationTable")
                        {
                            Log.Print("  " + Data.param_names[i] + ": Predefined parameter can not be changed!");
                        }
                        else if (Data.sap_query.Contains("{{" + Data.param_names[i] + "}}") || Data.dwh_finalization.Contains("{{" + Data.param_names[i] + "}}"))
                        {
                            Data.param_values[i] = DB.QueryValue(Data.param_sources[i], Data.param_queries[i]);
                            Program.ReplaceParameter(Data.param_names[i], Data.param_values[i]);
                            Log.Print("  " + Data.param_names[i] + ": [" + Data.param_values[i] + "]");
                            param += Data.param_names[i] + ": [" + Data.param_values[i] + "]; ";
                        }
                        else
                        {
                            Log.Print("  " + Data.param_names[i] + ": Skipped because it is not used!");
                        }
                    }
                    if (param != "") param = param.Substring(0, param.Length - 2).Replace('\'', '"');
                }
                Program.SetExtractLog("Update", "status='3', parameters='" + param + "'");
            }
            catch (Exception expt)
            {
                Log.Print("Error querying parameters: " + expt.Message);
                Program.SetExtractLog("Update", "status='E'+status, remark='Error querying parameters'");
                Program.Exit(11);
            }
        }
        static void ReplaceParameter(string name, string value)
        {
            Data.sap_query = Data.sap_query.Replace("{{" + name + "}}", value);
            Data.dwh_finalization = Data.dwh_finalization.Replace("{{" + name + "}}", value);
        }
        static void ExtractData()
        {
            try
            {
                Log.Print("Extracting data from SAP");
                DB.BulkExtract();
                Program.SetExtractLog("Update", "status='5', affectedrows='" + Data.rowcount + "'");
            }
            catch (Exception expt)
            {
                Log.Print("Error extracting data: " + expt.Message);
                Program.SetExtractLog("Update", "status='E'+status, remark='Error extracting data'");
                Program.Exit(12);
            }
        }
        static void TransformData()
        {
            string sql = "";
            string pk = "";
            try
            {
                if (Data.dwh_insertmode == "manual")
                {
                    Log.Print("Only manual transformation");
                }
                else
                {
                    Log.Print("Preparing data transformation");

                    if (DB.QueryValue("dwh", "select count(object_id('" + Data.finaltable + "','U'))", true) == "0")
                    {
                        Log.Print("  Creating destination table");
                        sql = "create table " + Data.finaltable + "(";
                        int j = Data.index_names.IndexOf("pk");
                        if (j >= 0) pk = "," + Data.index_fields[j].ToLower().Replace(" ", "") + ",";
                        for (int i = 0; i < Data.field_types.Count; i++)
                        {
                            if (pk.Contains("," + Data.field_names[i].ToLower() + ",")) sql += " [" + Data.field_names[i] + "] " + Data.field_types[i] + " not null,";
                            else sql += " [" + Data.field_names[i] + "] " + Data.field_types[i] + ",";
                        }
                        sql = sql.TrimEnd(',') + ")";
                        DB.Execute("dwh", sql);

                        if (Data.index_names.Count > 0) Log.Print("  Creating destination indexes");
                        for (int i = 0; i < Data.index_names.Count; i++)
                        {
                            if (Data.index_names[i] == "pk")
                            {
                                //sql = "create unique clustered index " + Data.dwh_table + "_" + Data.index_names[i].ToUpper().Replace(" ", "") + " on " + Data.finaltable + " (" + Data.index_fields[i] + ")";
                                sql = "alter table " + Data.finaltable + " add constraint " + Data.dwh_table + "_PK primary key clustered (" + Data.index_fields[i] + ")";
                            }
                            else if (Data.index_unique[i] == "y")
                            {
                                sql = "create unique nonclustered index " + Data.dwh_table + "_" + Data.index_names[i].ToUpper().Replace(" ", "") + " on " + Data.finaltable + " (" + Data.index_fields[i] + ")";
                            }
                            else
                            {
                                sql = "create nonclustered index " + Data.dwh_table + "_" + Data.index_names[i].ToUpper().Replace(" ", "") + " on " + Data.finaltable + " (" + Data.index_fields[i] + ")";
                            }
                            DB.Execute("dwh", sql);
                        }
                    }
                    else if (Data.dwh_insertmode == "truncate")
                    {
                        Log.Print("  Truncating destination table");
                        DB.Execute("dwh", "begin try drop table " + Data.backuptable + " end try begin catch end catch");
                        DB.Execute("dwh", "select * into " + Data.backuptable + " from " + Data.finaltable);
                        DB.Execute("dwh", "truncate table " + Data.finaltable);
                    }
                    Program.SetExtractLog("Update", "status='6'");

                    if (Data.dwh_insertmode == "merge")
                    {
                        sql = "merge " + Data.finaltable + " as d using (select ";
                    }
                    else if (Data.dwh_insertmode == "truncate" || Data.dwh_insertmode == "append")
                    {
                        sql = "insert into " + Data.finaltable + " (";
                        for (int i = 0; i < Data.field_names.Count(); i++) sql += "[" + Data.field_names[i] + "],";
                        sql = sql.TrimEnd(',') + ") select ";
                    }
                    for (int i = 0; i < Data.field_names.Count(); i++)
                    {
                        sql += DB.ConvertType("dwh", Data.field_types[i], Data.field_names[i]) + " as [" + Data.field_names[i] + "],";
                    }
                    sql = sql.TrimEnd(',') + " from " + Data.temptable;
                    if (Data.dwh_insertmode == "merge")
                    {
                        sql += ") as x on ";
                        string[] columns = Data.index_fields[Data.index_names.IndexOf("pk")].Split(',');
                        for (int i = 0; i < columns.Count(); i++)
                        {
                            if (i > 0) sql += "and ";
                            sql += "x.[" + columns[i] + "] = d.[" + columns[i] + "] ";
                        }
                        sql += "when not matched then insert (";
                        for (int i = 0; i < Data.field_names.Count(); i++) sql += "[" + Data.field_names[i] + "],";
                        sql = sql.TrimEnd(',') + ") values (";
                        for (int i = 0; i < Data.field_names.Count(); i++) sql += "x.[" + Data.field_names[i] + "],";
                        sql = sql.TrimEnd(',') + ") when matched then update set ";
                        for (int i = 0; i < Data.field_names.Count(); i++) sql += "d.[" + Data.field_names[i] + "] = x.[" + Data.field_names[i] + "],";
                        sql = sql.TrimEnd(',') + ";";
                    }

                    Log.Print("Transforming data into DWH");
                    DB.Execute("dwh", sql);
                }
                Program.SetExtractLog("Update", "status='7'");
            }
            catch (Exception expt)
            {
                Log.Print("Error transforming data: " + expt.Message);
                Program.SetExtractLog("Update", "status='E'+status, remark='Error transforming data'");
                Program.Exit(13);
            }
        }
        static void Finalization()
        {
            try
            {
                if (Data.dwh_finalization.Trim().Length == 0)
                {
                    Log.Print("Skipping manual finalization");
                }
                else
                {
                    Log.Print("Executing manual finalization");
                    DB.Execute("dwh", Data.dwh_finalization);
                }
                Program.SetExtractLog("Update", "status='8'");
            }
            catch (Exception expt)
            {
                Log.Print("Error executing finalization: " + expt.Message);
                Program.SetExtractLog("Update", "status='E'+status, remark='Error executing finalization'");
                Program.Exit(14);
            }
        }
        static void Cleanup()
        {
            try
            {
                Log.Print("Cleaning up workspace");
                DB.Execute("dwh", "drop table " + Data.temptable);
                Program.SetExtractLog("Update", "status='9', enddate=getdate()");
                DB.Close();
            }
            catch (Exception expt)
            {
                Log.Print("Error cleaning up workspace: " + expt.Message);
                Program.SetExtractLog("Update", "status='E'+status, remark='Error during cleanup'");
                Program.Exit(15);
            }
        }
        static void SetExtractLog(string mode, string text = null)
        {
            try
            {
                //Set old unfinished RunIDs on C%
                if (mode == "CancelOld") DB.Execute("dwh", "update [" + Data.dwh_dbname + "].[" + Data.dwh_schema + "].[SAPExtractor_log] set status = 'C'+status where substring(status,1,1) not in ('C','E','9') and profile = '" + Data.profile + "';");
                //Insert new RunID
                if (mode == "InsertNew" || mode == "CancelOld") DB.Execute("dwh", "insert into [" + Data.dwh_dbname + "].[" + Data.dwh_schema + "].[SAPExtractor_log] (profile, runid, startdate, status, processid, configmd5, profilemd5, appversion) values ('" + Data.profile + "','" + Data.runid + "',convert(datetime,'" + Program.startdate.ToString("yyyy-MM-dd HH:mm:ss") + "',121),'0'," + Program.pid + ",'" + Data.md5_config + "','" + Data.md5_profile + "','" + Program.version + "');");
                //Update current RunID
                if (text.Length > 0) DB.Execute("dwh", "update [" + Data.dwh_dbname + "].[" + Data.dwh_schema + "].[SAPExtractor_log] set " + text + " where profile = '" + Data.profile + "' and runid = '" + Data.runid + "';");
            }
            catch (Exception expt)
            {
                Log.Print("Error with " + mode + " into Log-Table [" + Log.Trunc(text) + "]: " + expt.Message);
                Program.Exit(16);
            }
        }
        public static void SetError(int errorcode)
        {
            if (errorcode > 0) errorstate = errorstate | (int)Math.Pow(2, errorcode - 1); //Store ErrorCodes with BinaryOr into ErrorState
        }
        public static void Exit(int errorcode = 0)
        {
            if (errorcode > 0 || errorstate > 0)
            {
                Program.SetError(errorcode);
                Log.Print("ABORTING - Profile:[" + Data.profile + "] - RunID:[" + Data.runid + "] - PID:[" + pid + "] - ErrorCode:[" + errorstate.ToString() + "]");
                Log.Print("<hr>");
            }
            if (errorcode != 0 || errorstate != 0)
            {
                Debug.End();
                Log.Close();
                System.Environment.Exit(errorstate);
            }
        }
    }

    static class DB
    {
        static DB2Connection sap_conn = new DB2Connection();
        static SqlConnection dwh_conn = new SqlConnection();
        public static void Initialize(string db, string host, string port, string username, string password, string dbname, string schema)
        {
            try
            {
                if (db.ToLower() == "sap") sap_conn.ConnectionString = "server=" + host + ":" + port + ";uid=" + username + ";pwd=" + password + ";database=" + dbname + ";currentschema=" + schema + ";";
                else if (db.ToLower() == "dwh") dwh_conn.ConnectionString = "server=" + host + ";user id=" + username + ";password=" + password + ";database=" + dbname + ";connection timeout=30;trusted_connection=no;";
                else throw new Exception("No valid connectionstring for [" + db + "]");
            }
            catch (Exception expt)
            {
                throw expt;
            }
        }
        static dynamic Connection(string db)
        {
            try
            {
                if (sap_conn.ConnectionString == null || dwh_conn.ConnectionString == null) throw new Exception("Missing connection strings!");
                else if (db.ToLower() == "sap") return sap_conn;
                else if (db.ToLower() == "dwh") return dwh_conn;
                else throw new Exception("No valid connection defined for [" + db + "]");
            }
            catch (Exception expt)
            {
                throw expt;
            }
        }
        static string State(string db)
        {
            try
            {
                dynamic conn = Connection(db);
                if (conn.State == ConnectionState.Open) return "Open";
                else if (conn.State == ConnectionState.Closed) return "Closed";
                else if (conn.State == ConnectionState.Broken) return "Broken";
                else if (conn.State == ConnectionState.Fetching) return "Fetching";
                else if (conn.State == ConnectionState.Executing) return "Executing";
                else if (conn.State == ConnectionState.Connecting) return "Connecting";
            }
            catch (Exception expt)
            {
                Log.Print("Error checking state of [" + db + "]: "+expt.Message);
                throw expt;
            }
            return null;
        }
        static void Open(string db)
        {
            try
            {
                if (State(db) == "Closed" || State(db) == "Broken") Connection(db).Open();
            }
            catch (Exception expt)
            {
                Log.Print("Error connecting to [" + db + "]: " + expt.Message);
                throw expt;
            }
        }
        public static void Execute(string db, string sql)
        {
            try
            {
                Open(db);
                dynamic cmd = Connection(db).CreateCommand();
                cmd.CommandTimeout = 0;
                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();
            }
            catch (Exception expt)
            {
                Log.Print("Error executing SQL [" + Log.Trunc(sql) + "] on [" + db + "]:" + expt.Message);
                throw expt;
            }
        }
        public static string QueryValue(string db, string sql, bool allownull = false)
        {
            try
            {
                Open(db);
                dynamic cmd = Connection(db).CreateCommand();
                cmd.CommandTimeout = 0;
                cmd.CommandText = sql;
                dynamic reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    string output = reader.GetValue(0).ToString();
                    reader.Close();
                    return output;
                }
                else if (allownull)
                {
                    reader.Close();
                    return null;
                }
                else
                {
                    reader.Close();
                    throw new Exception("No data found!");
                }
            }
            catch (Exception expt)
            {
                Log.Print("Error querying value [" + Log.Trunc(sql) + "] on [" + db + "]: " + expt.Message);
                throw expt;
            }
            return null;
        }
        public static void BulkExtract()
        {
            try
            {
                Open("sap");
                DB2Command cmd = Connection("sap").CreateCommand();
                cmd.CommandText = Data.sap_query;
                cmd.CommandTimeout = 0;
                DB2DataReader reader = cmd.ExecuteReader();

                if (Data.field_types.Count() != reader.VisibleFieldCount) throw new Exception("Amount of columns does not match to Specifications!");
                bool haserror = false;
                for (int i = 0; i < reader.VisibleFieldCount; i++)
                {
                    if (Data.field_names[i].ToLower() != reader.GetName(i).ToLower())
                    {
                        haserror = true;
                        Log.Print("  Column" + (i + 1) + ": " + Data.field_names[i].ToLower() + " <> " + reader.GetName(i).ToLower());
                    }
                }
                if (haserror) throw new Exception("Name of columns does not match to Specifications!");

                if (Data.sap_nullable == "n" && !reader.HasRows)
                {
                    throw new Exception("No data found!");
                }
                else if (Data.sap_nullable == "y" && !reader.HasRows)
                {
                    Data.rowcount = "0";
                }
                else if (reader.HasRows)
                {
                    Open("dwh");
                    SqlBulkCopy sqlbulk = new SqlBulkCopy(Connection("dwh"));
                    sqlbulk.DestinationTableName = Data.temptable;
                    sqlbulk.BulkCopyTimeout = 0;
                    sqlbulk.BatchSize = Data.batchsize;
                    sqlbulk.NotifyAfter = Data.batchsize;
                    sqlbulk.SqlRowsCopied += new SqlRowsCopiedEventHandler(BulkEvent);
                    sqlbulk.WriteToServer(reader);
                    Data.rowcount = QueryValue("dwh", "select count(0) from " + Data.temptable);
                }
                Log.Print("  " + Data.rowcount + " rows extracted");
            }
            catch (Exception expt)
            {
                throw expt;
            }
        }
        public static void BulkEvent(object sender, SqlRowsCopiedEventArgs args)
        {
            Log.Print("  " + args.RowsCopied + " rows");
        }
        public static void Close(string db = null)
        {
            try
            {
                if (db == null)
                {
                    Close("sap");
                    Close("dwh");
                }
                else
                {
                    if (State(db) != "Closed") Connection(db).Close();
                }
            }
            catch (Exception expt)
            {
                Log.Print("Error closing connection of [" + db + "]: " + expt.Message);
                throw expt;
            }
        }
        public static string ConvertType(string db, string type, string value)
        {
            if (db == "dwh" && type.ToLower().Contains("date"))
            {
                return "convert(" + type.ToLower() + ", case when [" + value + "] = '00000000' then null when [" + value + "] > '20790101' then '20790101' when [" + value + "] < '19000101' then '19000101' else [" + value + "] end,112)";
            }
            else if (db == "dwh" && type.ToLower().Contains("varchar"))
            {
                return "ltrim(rtrim([" + value + "]))";
            }
            else
            {
                return "[" + value + "]";
            }
        }
    }

    class XML
    {
        public string md5;
        public string filepath;
        string content;
        XmlDocument doc;
        XmlElement xml;
        public XML(string filepath)
        {
            try
            {
                this.filepath = filepath;
                if (System.IO.File.Exists(filepath))
                { 
                    CalculateMD5();
                    FetchContent();
                    BackupFile();
                }
                else
                {
                    throw new Exception("File not found!");
                }
            }
            catch (Exception expt)
            {
                Log.Print("Error opening XML [" + filepath + "]: " + expt.Message);
                Program.Exit(17);
            }
        }
        void CalculateMD5()
        {
            var checksum = System.Security.Cryptography.MD5.Create();
            try
            {
                md5 = BitConverter.ToString(checksum.ComputeHash(System.IO.File.OpenRead(filepath))).Replace("-", "").ToLower();
            }
            catch (Exception expt)
            {
                Log.Print("Error calculating MD5 of [" + filepath + "]: " + expt.Message);
                throw expt;
            }
        }
        void FetchContent()
        {
            try
            {
                System.IO.StreamReader reader = new System.IO.StreamReader(filepath);
                string text = reader.ReadToEnd().Replace("&amp;", "&").Replace("&lt;", "<").Replace("&gt;", ">").Replace("&apos;", "'").Replace("&quot;", "\"");
                System.Text.RegularExpressions.Regex regex = new System.Text.RegularExpressions.Regex("(<\\/?[a-z0-9_]+)( +[a-z0-9_]+=\"[^\"]*\")*( ?\\/)?(>)|(<!--)|(-->)"); //RegEx:(<\/?[a-z0-9_]+)( +[a-z0-9_]+="[^"]*")*( ?\/)?(>)|(<!--)|(-->)
                System.Text.RegularExpressions.MatchCollection matches = regex.Matches(text);
                int i = 0;
                foreach (System.Text.RegularExpressions.Match match in matches)
                {
                    content += text.Substring(i, match.Index-i).Replace("&", "&amp;").Replace("<", "&lt;").Replace(">", "&gt;").Replace("'", "&apos;").Replace("\"", "&quot;") + match.Value;
                    i = match.Index + match.Length;
                }
                content += text.Substring(i);
                reader.Close();
                doc = new XmlDocument();
                doc.LoadXml(content);
                xml = doc.DocumentElement;
            }
            catch (Exception expt)
            {
                Log.Print("Error fetching XML [" + filepath + "]: " + expt.Message);
                throw expt;
            }
        }
        void BackupFile()
        {
            try
            {
                string dir = Program.workdir + @"\Backup";
                System.IO.Directory.CreateDirectory(dir);
                System.IO.File.Copy(filepath, dir + @"\" + System.IO.Path.GetFileNameWithoutExtension(filepath) + "_" + md5 + ".xml", true);
            }
            catch (Exception expt)
            {
                Log.Print("  Could not store backup of XML");
            }
        }
        public string GetValue(string element, bool allownull = false)
        {
            try
            {
                XmlNode node = xml.SelectSingleNode(element);
                if (node != null) return node.InnerText.Trim();
                else if (node == null && allownull) return null;
                else
                {
                    Log.Print("Missing XML value for [" + element + "]");
                    Program.SetError(1);
                }
            }
            catch (Exception expt)
            {
                Log.Print("Error fetching value of [" + element + "]: " + expt.Message);
                Program.SetError(2);
            }
            return null;
        }
        public int GetCount(string element, bool allownull = false)
        {
            try
            {
                int output = xml.SelectNodes(element).Count;
                if (output != 0 || allownull) return output;
                else
                {
                    Log.Print("Missing XML count for [" + element + "]");
                    Program.SetError(3);
                    return 0;
                }
            }
            catch (Exception expt)
            {
                Log.Print("Error counting value of [" + element + "]: " + expt.Message);
                Program.SetError(4);
                return 0;
            }
        }
    }

    static class Log
    {
        static System.IO.StreamWriter logstream;
        public static void Init(string filepath)
        {
            try
            {
                System.IO.Directory.CreateDirectory(System.IO.Path.GetDirectoryName(filepath));
                logstream = new System.IO.StreamWriter(filepath, true);
                logstream.AutoFlush = true;
            }
            catch (Exception expt)
            {
                Log.Print("Error initializing LOG [" + filepath + "]: " + expt.Message);
                throw expt;
            }
        }
        public static void Print(string text)
        {
            string output;
            if (text == "<hr>") output = string.Concat(System.Linq.Enumerable.Repeat('-', 85));
            else output = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ff") + ":  " + text;
            Console.WriteLine(output);
            if (logstream != null) logstream.WriteLine(output);
        }
        public static string Trunc(string text, int length = 40)
        {
            if (text.Trim().Length > length) return text.Trim().Substring(0, length).Trim() + "...";
            return text;
        }
        public static void Close()
        {
            if (logstream != null) logstream.Close();
        }
    }
}
