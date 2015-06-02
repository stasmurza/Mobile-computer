using System;
using System.Collections.Generic;
using System.Resources;
using System.Reflection;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ImportFromSQLCompact
{
    static public class DBConnectionSettings
    {
        public static string DestinationConnectionString
        {           
            get {
                ImportedDates Obj = new ImportedDates();
                Assembly assembly = Assembly.GetAssembly(Obj.GetType());
                ResourceManager rm = new ResourceManager("ImportFromSQLCompact.Resources", assembly);
                string DestinationServerName = rm.GetString("DestinationServerName");
                string StringDestinationConnection = String.Format("Persist Security Info=False;User ID=UserName;Password=UserPassword;Server={0};database=DataCollectionTerminal;", DestinationServerName);

                
                return StringDestinationConnection; }
            set { ; }
        }
    }
}
