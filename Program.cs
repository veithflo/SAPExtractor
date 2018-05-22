using System;
using System.Xml;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Data;
using System.Data.SqlClient;

namespace SAPExtractor
{
    class Debug
    {
        public static void Run()
        {
            Debug.Print("Debug.Run");

        }
        public static void End()
        {
#if DEBUG
            Console.WriteLine("");
            Console.WriteLine("Press any Key to close ...");
            Console.ReadKey();
#endif
        }
        public static void Print(string text)
        {
#if DEBUG
            Log.Print("    " + text);
#endif
        }
    }

    class Data
    {
        public class Source
        {
            public static string table;
            public static string filter;
            public static string module;
            public static string[] fields;
            public static Dictionary<string, string> datatypes = new Dictionary<string, string>();
            public static Dictionary<string, string> datalengths = new Dictionary<string, string>();
            public static Dictionary<string, string> decimals = new Dictionary<string, string>();
        }
        public class Destination
        {
            public static string mode;
            public static string dbname;
            public static string table;
        }
    }

    class Log
    {
        public static void Print(string text)
        {
            if (text == "<hr>") Console.WriteLine(string.Concat(Enumerable.Repeat('-', 100)));
            else Console.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ff") + ":  " + text);
        }
    }

    class SQL
    {
        public static string host;
        public static string user;
        public static string password;
        public static string dbname;
        static SqlConnection conn;

        public static string State()
        {
            if (conn == null) return "Closed";
            if (conn.State == ConnectionState.Open) return "Open";
            if (conn.State == ConnectionState.Closed) return "Closed";
            if (conn.State == ConnectionState.Broken) return "Broken";
            if (conn.State == ConnectionState.Fetching) return "Fetching";
            if (conn.State == ConnectionState.Executing) return "Executing";
            if (conn.State == ConnectionState.Connecting) return "Connecting";
            return "Closed";
        }
        public static bool Connect()
        {
            Debug.Print("SQL.Connect");
            try
            {
                if (State() == "Closed")
                {
                    conn = new SqlConnection("server=" + host + ";trusted_connection=no;" + "user id=" + user + ";password=" + password + ";database=" + dbname + ";connection timeout=30");
                    conn.Open();
                }
            }
            catch (Exception expt)
            {
                Log.Print("Error connecting to [" + host + "]: " + expt.Message);
                return false;
            }
            return true;
        }
        public static bool Close()
        {
            Debug.Print("SQL.Close");
            if (State() != "Closed")
            {
                conn.Close();
            }
            return true;
        }
        public static bool Execute(string sql)
        {
            Debug.Print("SQL.Execute");
            if (State() != "Open")
            {
                Log.Print("No open connection to [" + host + "]");
                return false;
            }
            try
            { 
                SqlCommand cmd = new SqlCommand(sql, conn);
                cmd.ExecuteNonQuery();
                conn.Close();
            }
            catch (Exception expt)
            {
                Log.Print("Error executing CMD on [" + host + "]: " + expt.Message);
                return false;
            }
            return true;
        }
    }

    class SAP
    {
        public static string host;
        public static string user;
        public static string password;
        public static string sysnum;
        public static string language;
        public static string client;
        static ERPConnect.R3Connection conn;
        static Int32 threadid = 0;
        static Int32 threadcount = 0;
        static Int64 threadrows = 0;
        public static string State ()
        {
            if (conn != null) return "Open";
            if (conn == null) return "Closed";
            return "Null";
        }
        public static bool Connect()
        {
            Debug.Print("SAP.Connect");
            try
            {
                if (State() == "Closed")
                {
                    ERPConnect.LIC.SetLic("826DZD4CY8-17655");
                    conn = new ERPConnect.R3Connection(SAP.host, Convert.ToInt32(SAP.sysnum), SAP.user, SAP.password, SAP.language, SAP.client);
                    conn.Open();
                }
            }
            catch (Exception expt)
            {
                Log.Print("Error connecting to [" + host + "]: " + expt.Message);
                return false;
            }
            return true;
        }
        public static bool Close()
        {
            Debug.Print("SAP.Close");
            if (State() != "Closed")
            {
                conn.Close();
            }
            conn = null;
            return true;
        }
        public static bool GetMeta()
        {
            Debug.Print("SAP.GetMeta");
            if (State() != "Open")
            {
                Log.Print("No open connection to [" + host + "]");
                return false;
            }
            try
            {
                ERPConnect.Utils.ReadTable query;
                query = new ERPConnect.Utils.ReadTable(conn);
                query.SetCustomFunctionName("Z_XTRACT_IS_TABLE");
                query.TableName = "DD03L";
                query.AddField("fieldname");
                query.AddField("datatype");
                query.AddField("leng");
                query.AddField("decimals");
                query.AddField("keyflag");
                query.AddField("notnull");
                query.WhereClause = "tabname = '" + Data.Source.table + "' and fieldname in (";
                foreach (string field in Data.Source.fields) query.WhereClause = query.WhereClause + "'" + field + "',";
                query.WhereClause = query.WhereClause.Trim(',') + ")";
                query.PackageSize = 0;
                query.RaiseIncomingPackageEvent = false;
                query.Run();

                for (int i = 0; i < query.Result.Rows.Count; i++)
                {
                    Debug.Print(query.Result.Rows[i]["fieldname"].ToString() + " as " + query.Result.Rows[i]["datatype"].ToString() + "(" + query.Result.Rows[i]["leng"].ToString().TrimStart('0') + ")");
                    Data.Source.datatypes.Add(query.Result.Rows[i]["fieldname"].ToString(), query.Result.Rows[i]["datatype"].ToString());
                    Data.Source.datalengths.Add(query.Result.Rows[i]["fieldname"].ToString(), query.Result.Rows[i]["leng"].ToString().TrimStart('0'));
                    Data.Source.decimals.Add(query.Result.Rows[i]["fieldname"].ToString(), query.Result.Rows[i]["decimals"].ToString().TrimStart('0'));
                }
                if (Data.Source.fields.Count() != Data.Source.datatypes.Count)
                {
                    string output = "";
                    foreach (string field in Data.Source.fields) if (!Data.Source.datatypes.ContainsKey(field)) output += "," + field;
                    Log.Print("No metadata for columns [" + output.Trim(',') + "]");
                    return false;
                }
            }
            catch (Exception expt)
            {
                Log.Print("Error executing CMD on [" + host + "]: " + expt.Message);
                return false;
            }
            return true;
        }
        public static bool Query()
        {
            Debug.Print("SAP.Query");
            if (State() != "Open")
            {
                Log.Print("No open connection to [" + host + "]");
                return false;
            }
            try
            {
                //ERPConnect.RFCFunction func = conn.CreateFunction(Data.Source.module); //Reason for this line?
                ERPConnect.Utils.ReadTable query;
                query = new ERPConnect.Utils.ReadTable(conn);

                query.SetCustomFunctionName(Data.Source.module);
                query.TableName = Data.Source.table;
                query.WhereClause = Data.Source.filter;
                foreach (string field in Data.Source.fields) query.AddField(field);

                ThreadPool.SetMinThreads(1, 1);
                ThreadPool.SetMaxThreads(Program.maxthreads, Program.maxthreads);
                query.PackageSize = Program.packagesize;
                query.RaiseIncomingPackageEvent = true;
                query.IncomingPackage += new ERPConnect.Utils.ReadTable.OnIncomingPackage(PackageReader);

                Log.Print("Processing chunks of " + Program.packagesize.ToString() + " rows in " + Program.maxthreads.ToString() + " threads");
                lock ("ThreadCount") threadcount++;
                query.Run();
                lock ("ThreadCount") threadcount--;
                while (threadcount > 0) Thread.Sleep(1000);
                Log.Print("Extraction of " + threadrows + " rows completed");
            }
            catch (Exception expt)
            {
                Log.Print("Error executing CMD on [" + host + "]: " + expt.Message);
                return false;
            }
            return true;
        }
        private static void PackageReader(ERPConnect.Utils.ReadTable sender, DataTable result)
        {
            if (result.Rows.Count > 0)
            {
                lock ("ThreadCount")
                {
                    threadcount++;
                    ThreadPool.QueueUserWorkItem(new WaitCallback(PackageThread), result.Copy());
                }
            }
        }
        private static void PackageThread(object datatable)
        {
            int thisthreadid;
            lock ("ThreadId")
            {
                threadid++;
                thisthreadid = threadid;
            }
            DataTable result = (DataTable)datatable;
            Log.Print("Starting thread #" + thisthreadid.ToString("000") + " with " + result.Rows.Count + " Rows");




            


            //Debug.Print(Data.Source.fields[0] + " " + Data.Source.fields[1] + " " + Data.Source.fields[2]);
            //Debug.Print(result.Rows[0][Data.Source.fields[0]].ToString() + " " + result.Rows[0][Data.Source.fields[1]].ToString() + " " + result.Rows[0][Data.Source.fields[2]].ToString());
            //Debug.Print(result.Rows[1][Data.Source.fields[0]].ToString() + " " + result.Rows[1][Data.Source.fields[1]].ToString() + " " + result.Rows[1][Data.Source.fields[2]].ToString());


            lock ("ThreadCount")
            {
                threadrows += result.Rows.Count;
                threadcount--;
            }
        }
    }

    class Program
    {
        public static int packagesize;
        public static int maxthreads;
        static string profile;
        static string runid;
        static string workdir = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
        static DateTime startdate = DateTime.Now;
        static bool noerror = true;

        static string GetFileMD5(string path)
        {
            var md5 = System.Security.Cryptography.MD5.Create();
            var file = System.IO.File.OpenRead(path);
            return BitConverter.ToString(md5.ComputeHash(file)).Replace("-", "").ToLower();
        }

        static string GetXmlNode(XmlElement xml, string path, string nullval = null)
        {
            XmlNode node = xml.SelectSingleNode(path);
            if (node == null)
            {
                if (nullval == null)
                {
                    Log.Print("Missing XML value for [" + path + "]");
                    noerror = false;
                    return null;
                }
                else
                {
                    return nullval;
                }
            }
            else
            {
                return node.InnerText;
            }
        }

        static bool ReadConfigFile()
        {
            noerror = true;
            string path = workdir + @"\SAPExtractor.xml";
            try
            {
                Log.Print("MD5 of config file: [" + GetFileMD5(path) + "]");

                XmlDocument xmldoc = new XmlDocument();
                xmldoc.Load(path);
                XmlElement xml = xmldoc.DocumentElement;

                Program.packagesize = int.Parse(GetXmlNode(xml, "/settings/program/packagesize"));
                Program.maxthreads = int.Parse(GetXmlNode(xml, "/settings/program/maxthreads"));

                SAP.host = GetXmlNode(xml, "/settings/sap/host");
                SAP.user = GetXmlNode(xml, "/settings/sap/user");
                SAP.password = GetXmlNode(xml, "/settings/sap/password");
                SAP.sysnum = GetXmlNode(xml, "/settings/sap/sysnum");
                SAP.language = GetXmlNode(xml, "/settings/sap/language");
                SAP.client = GetXmlNode(xml, "/settings/sap/client");

                SQL.host = GetXmlNode(xml, "/settings/sql/host");
                SQL.user = GetXmlNode(xml, "/settings/sql/user");
                SQL.password = GetXmlNode(xml, "/settings/sql/password");
                SQL.dbname = GetXmlNode(xml, "/settings/sql/dbname");
            }
            catch (Exception expt)
            {
                Log.Print("Error reading config file: " + expt.Message);
                noerror = false;
            }
            return noerror;
        }

        static bool ReadProfileFile()
        {
            noerror = true;
            string path = workdir + @"\Profiles\" + profile + ".xml";
            try
            {
                Log.Print("MD5 of profile file: [" + GetFileMD5(path) + "]");

                XmlDocument xmldoc = new XmlDocument();
                xmldoc.Load(path);
                XmlElement xml = xmldoc.DocumentElement;

                Data.Source.table = GetXmlNode(xml, "/profile/source/table").ToUpper();
                Data.Source.fields = GetXmlNode(xml, "/profile/source/fields").ToUpper().Replace(" ", "").Split(',').Distinct().ToArray();
                Data.Source.filter = GetXmlNode(xml, "/profile/source/filter");
                Data.Source.module = GetXmlNode(xml, "/profile/source/module", "Z_XTRACT_IS_TABLE").ToUpper();
                Data.Destination.mode = GetXmlNode(xml, "/profile/destination/mode", "append");
                Data.Destination.dbname = GetXmlNode(xml, "/profile/destination/dbname", "Export_DB");
                Data.Destination.table = GetXmlNode(xml, "/profile/destination/table");
            }
            catch (Exception expt)
            {
                Log.Print("Error reading profile file: " + expt.Message);
                noerror = false;
            }
            return noerror;
        }

        static void Run()
        {
            Exit(!ReadConfigFile());
            Exit(!ReadProfileFile());

            Exit(!SQL.Connect());
            Exit(!SQL.Execute("BEGIN TRY DROP TABLE " + Data.Destination.dbname + ".dbo.temp_extract_" + Data.Destination.table + " END TRY BEGIN CATCH END CATCH"));
            Exit(!SQL.Close());

            Exit(!SAP.Connect());
            Exit(!SAP.GetMeta());
            Exit(!SAP.Query());
            Exit(!SAP.Close());

            Debug.Run();
        }

        static void Main(string[] args)
        {
            if (args.GetLength(0) == 0)
            {
                Log.Print("No parameter specified!");
                return;
            }
            if (args.GetLength(0) > 0)
            {
                profile = args[0].ToUpper();
            }
            if (args.GetLength(0) > 1)
            {
                runid = args[1].ToUpper();
            }
            else
            {
                runid = startdate.ToString("yyyyMMddHHmmssff");
            }
            Log.Print("<hr>");
            Log.Print("STARTING - Profile: [" + profile + "] - RunID: [" + runid + "] - PID: [" + System.Diagnostics.Process.GetCurrentProcess().Id + "]");
            Run();
            Log.Print("FINISHED - Profile: [" + profile + "] - RunID: [" + runid + "] - PID: [" + System.Diagnostics.Process.GetCurrentProcess().Id + "]");
            Log.Print("<hr>");

            Debug.End();
        }
        static void Exit(bool exit = true)
        {
            if (exit)
            {
                Log.Print("ABORTING - Profile: [" + profile + "] - RunID: [" + runid + "] - PID: [" + System.Diagnostics.Process.GetCurrentProcess().Id + "]");
                Log.Print("<hr>");
                Debug.End();
                System.Environment.Exit(1);
            }
        }
    }
}
