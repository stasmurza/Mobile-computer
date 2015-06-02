using System;
using System.Resources;
using System.Reflection;
using System.Data;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ImportFromSQLCompact
{
    class ImportedDates
    {

        public DateTime[] GetImportedDates(DateTime StartRange, DateTime EndRange)
        {

            DateTime[] DatesArray = null;
            using (SqlConnection DBConnection = new SqlConnection(DBConnectionSettings.DestinationConnectionString))
            {

                string queryString = @"
                SELECT CarDate 
                FROM dbo.CarsState
                WHERE ((CarDate >= @StartRange) AND (CarDate <= @EndRange))
                GROUP BY CarDate;";

                DBConnection.Open();

                SqlDataAdapter DAdapter = new SqlDataAdapter(queryString, DBConnection);

                SqlCommand command = new SqlCommand(queryString, DBConnection);

                SqlParameter ParStartRange = new SqlParameter("@StartRange", SqlDbType.DateTime);
                ParStartRange.Value = StartRange;
                command.Parameters.Add(ParStartRange);

                SqlParameter ParEndRange = new SqlParameter("@EndRange", SqlDbType.DateTime);
                ParEndRange.Value = EndRange;
                command.Parameters.Add(ParEndRange);
               
                DAdapter.SelectCommand = command;

                DataSet DSet = new DataSet();

                DAdapter.Fill(DSet);

                DBConnection.Close();

                DataTable CarsState = DSet.Tables[0];
                int NumberOfRows = CarsState.Rows.Count;

                for (int i = 0; i < NumberOfRows; i++)
                {

                    DataRow RowImport = CarsState.Rows[i];

                    if (DatesArray == null)
                    {
                        DatesArray = new DateTime[NumberOfRows];
                    }

                    DatesArray[i] = Convert.ToDateTime(RowImport["CarDate"]);

                }
            }

            if (DatesArray == null)
                DatesArray = new DateTime[0];
            return DatesArray;
        }        
    }
}
