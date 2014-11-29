using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;

namespace sql2json
{
    class Program
    {
        public static string[] LoadConfig()
        {
            List<string> settings = new List<string>();

            try
            {
                Configuration config = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);

                foreach (string key in ConfigurationManager.AppSettings)
                {
                    settings.Add(ConfigurationManager.AppSettings[key]);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }

            return settings.ToArray();
        }

        public static string GetTableName(string table, out int numItems)
        {
            string result = String.Empty;
            numItems = 0;

            try
            {
                if (table.Contains("="))
                {
                    string[] parts = table.Split('=');

                    try
                    {
                        numItems = (!parts[1].Contains(",")) ? Convert.ToInt32(parts[1]) : -1;
                    }
                    catch (Exception ex) {
                        numItems = -1;
                        Console.WriteLine(ex.ToString());
                    }

                    result = parts[0];
                }
                else
                    result = table;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }

            return result;
        }

        public static int GetNumItems(string table, out string[] items)
        {
            int result = 0;
            items = null;

            try
            {
                if (table.Contains("="))
                {
                    string[] parts = table.Split('=');
                    items = parts[1].Split(',');
                    result = Convert.ToInt32(items[0]);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }

            return result;
        }

        public static bool ExistsinFields(string ky, string[] arr, out bool last)
        {
            bool result = false;
            last = false;

            try
            {
                int i = 1;

                foreach (string a in arr)
                {
                    last = (i == arr.Length) ? true : false;
                    
                    if (ky.ToLower() == a.ToLower())
                    {
                        result = true;
                        break;
                    }
                    
                    i++;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }

            return result;
        }

        public static void ExecuteDataTransfer(string[] settings)
        {
            int numItems = 0;

            try
            {
                Console.WriteLine("ExecuteDataTransfer started");

                if (settings.Length == 3)
                {
                    if (settings[1].Contains("|"))
                    {
                        string[] tables = settings[1].Split('|');

                        if (tables.Length > 0)
                        {
                            Console.WriteLine("ExecuteDataTransfer tables.Length: " + tables.Length.ToString());

                            using (SqlConnection mySqlConnection = new SqlConnection(settings[0]))
                            {
                                Console.WriteLine("ExecuteDataTransfer mySqlConnection: " + mySqlConnection.ConnectionString);

                                foreach (string table in tables)
                                {
                                    string tn = GetTableName(table, out numItems);

                                    Console.WriteLine("ExecuteDataTransfer processing table: " + tn);
                                    
                                    using (SqlCommand mySqlCommand = new SqlCommand("SELECT * FROM " + tn + ";"))
                                    {
                                        Console.WriteLine("ExecuteDataTransfer mySqlCommand: " + mySqlCommand.CommandText);
                                        
                                        mySqlCommand.Connection = mySqlConnection;

                                        mySqlConnection.Open();

                                        using (SqlDataReader rdr = mySqlCommand.ExecuteReader())
                                        {
                                            if (rdr.HasRows)
                                            {
                                                List<string> rows = new List<string>();

                                                int idRow = 1;
                                                while (rdr.Read())
                                                {
                                                    string jsonStr = String.Empty;
                                                    tn = GetTableName(table, out numItems);

                                                    if (numItems == -1)
                                                    {
                                                        string[] fields = null;
                                                        numItems = GetNumItems(table, out fields);

                                                        //Console.WriteLine("ExecuteDataTransfer numItems: " + numItems.ToString());

                                                        if (fields != null)
                                                        {
                                                            for (int i = -1; i <= numItems - 1; i++)
                                                            {
                                                                string value = String.Empty;
                                                                string key = String.Empty;

                                                                if (i >= 0)
                                                                {
                                                                    value = (rdr.GetValue(i) as object).ToString().Trim();
                                                                    key = (rdr.GetName(i) as object).ToString().Trim();

                                                                    bool last = false;

                                                                    if (ExistsinFields(key, fields, out last))
                                                                    {
                                                                        jsonStr = AddJsonLine(jsonStr, key, value, last);

                                                                        if (last) break;
                                                                    }
                                                                }
                                                                else
                                                                {
                                                                    key = "id";
                                                                    value = idRow.ToString();

                                                                    jsonStr = AddJsonLine(jsonStr, key, value, (i == (numItems - 1)) ? true : false);
                                                                }
                                                            }
                                                        }
                                                    }
                                                    else
                                                    {
                                                        for (int i = -1; i <= numItems - 1; i++)
                                                        {
                                                            string value = String.Empty;
                                                            string key = String.Empty;

                                                            if (i >= 0)
                                                            {
                                                                value = (rdr.GetValue(i) as object).ToString().Trim();
                                                                key = (rdr.GetName(i) as object).ToString().Trim();
                                                            }
                                                            else
                                                            {
                                                                key = "id";
                                                                value = idRow.ToString();
                                                            }

                                                            jsonStr = AddJsonLine(jsonStr, key, value, (i == (numItems - 1)) ? true : false);
                                                        }
                                                    }

                                                    if (jsonStr != String.Empty)
                                                    {
                                                        rows.Add(jsonStr);
                                                    }

                                                    idRow++;
                                                }

                                                rdr.Close();

                                                Console.WriteLine("ExecuteDataTransfer  rdr.Close()");

                                                WriteJson(rows.ToArray(), tn, settings[2]);
                                            }
                                            else
                                            {
                                                rdr.Close();

                                                Console.WriteLine("ExecuteDataTransfer  rdr.Close()");
                                            }
                                        }

                                        mySqlConnection.Close();

                                        Console.WriteLine("ExecuteDataTransfer mySqlConnection.Close()");
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }

        public static string AddJsonLine(string jsonStr, string key, string value, bool ll)
        {
            string result = String.Empty;

            try
            {
                jsonStr += (!ll) ? "\"" + key + "\"" + ":" + "\"" + value + "\"" + ", " : "\"" + key + "\"" + ":" + "\"" + value + "\"";
                result = jsonStr;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }

            return result;
        }

        public static void WriteJson(string[] rows, string tn, string folder)
        {
            List<string> nr = new List<string>();

            Console.WriteLine("WriteJson started");

            try
            {
                int len = rows.Length, i = 1;

                foreach (string row in rows)
                {
                    string il = (i == 1) ? "[" : String.Empty;
                    string ll = (i == len) ? "]" : ",";

                    string rw = il + "{" + row + "}" + ll;

                    nr.Add(rw);
                    i++;
                }

                Console.WriteLine("WriteJson folder is: " + folder + " and exists: " + Directory.Exists(folder).ToString());
                Console.WriteLine("WriteJson nr.Count() is: " + nr.Count.ToString());

                if (nr.Count > 0 && Directory.Exists(folder))
                {
                    string ffn = Path.Combine(folder, tn + ".json");

                    File.WriteAllLines(ffn, nr.ToArray());

                    Console.WriteLine("WriteJson: " + ffn);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }
        
        static void Main(string[] args)
        {
            string[] settings = LoadConfig();
            ExecuteDataTransfer(settings);
        }
    }
}
