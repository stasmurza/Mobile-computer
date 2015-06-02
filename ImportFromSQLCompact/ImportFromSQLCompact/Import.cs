using System;
using System.Resources;
using System.Reflection;
using System.Linq;
using System.Data;
using System.Data.SqlServerCe;
using System.Data.SqlClient;
using System.Collections;
using System.Collections.Generic;



namespace ImportFromSQLCompact
{
    class ImportFromSQLCE
    {
        string StringSourceConnection;
        public string ExceptionMessage;
        public bool CompletedSuccesfully;        
        
        //Current progress
        public int CurrentProgress;
        private decimal CountOfInsertingStrings;
        private decimal NumberOfInsertingString;

        public delegate void MyEventHandler();
        public event MyEventHandler EndOfImportEvent;
        public event MyEventHandler UpdateProgressBarPercentageEvent;


        //ImportFromSQLCE
        //
        public ImportFromSQLCE(string ParSqlCompactFile)
        {
            StringSourceConnection = @"data source=" + ParSqlCompactFile;

            CurrentProgress = 0;
            CountOfInsertingStrings = 0;
            NumberOfInsertingString = 0;
        }


        //get data from destination data base
        //
        private void GetDataFromDestinationDataBase(ref DataTable CarsDestDB)
        {

            //Get Cars from destination data base
            using (SqlConnection DBConnection = new SqlConnection(DBConnectionSettings.DestinationConnectionString))
            {

                string queryString = @"
                SELECT CarId, CarNumber 
                FROM dbo.Cars;";

                DBConnection.Open();

                SqlDataAdapter DAdapter = new SqlDataAdapter(queryString, DBConnection);

                SqlCommand command = new SqlCommand(queryString, DBConnection);
                DAdapter.SelectCommand = command;

                DataSet DSet = new DataSet();

                DAdapter.Fill(DSet);

                DBConnection.Close();

                DataTable ImportFromDestDB = DSet.Tables[0];
                int NumberOfRows = ImportFromDestDB.Rows.Count;

                for (int i = 0; i < NumberOfRows; i++)
                {

                    DataRow RowImport = ImportFromDestDB.Rows[i];

                    DataRow CarRow = CarsDestDB.NewRow();   
                    CarRow["CarId"] = Convert.ToInt32(RowImport["CarId"]);
                    CarRow["CarNumber"] = Convert.ToString(RowImport["CarNumber"]);
                    CarsDestDB.Rows.Add(CarRow);

                }
            }
        }


        //Get cars id from destination data base
        //
        private DataTable GetTableCarsId()
        {
            DataTable CarsId = new DataTable();
            CarsId.Columns.Add("CarId", System.Type.GetType("System.Int32"));
            CarsId.Columns.Add("CarNumber", System.Type.GetType("System.String"));

            using (SqlConnection DBConnection = new SqlConnection(DBConnectionSettings.DestinationConnectionString))
            {
                string queryString =
                "SELECT CarId, CarNumber FROM dbo.Cars;";

                SqlDataAdapter DAdapter = new SqlDataAdapter(queryString, DBConnection);
                SqlCommand command = new SqlCommand(queryString, DBConnection);
                DAdapter.SelectCommand = command;

                DataSet DSet = new DataSet();
                DBConnection.Open();
                DAdapter.Fill(DSet);
                DBConnection.Close();

                DataTable ImportFromTerminal = DSet.Tables[0];
                int NumberOfRows = ImportFromTerminal.Rows.Count;

                for (int i = 0; i < NumberOfRows; i++)
                {
                    DataRow RowImport = ImportFromTerminal.Rows[i];
        
                    DataRow CarsIdRow = CarsId.NewRow();
                    CarsIdRow["CarId"] = Convert.ToInt32(RowImport["CarId"]);
                    CarsIdRow["CarNumber"] = Convert.ToString(RowImport["CarNumber"]);
                    CarsId.Rows.Add(CarsIdRow);
                }                
            }

            return CarsId;
        }


        //get data from data collection terminal
        //        
        private void GetDataFromSQLCompactFile(ref DataTable Cars, ref DataTable CarsState, ref DataTable GoodsDetails)
        {

            //Get cars id from destination data base
            DataTable CarsId = GetTableCarsId();     

            using (SqlCeConnection DBConnection = new SqlCeConnection(StringSourceConnection))
            {
                string queryString =
                "SELECT t_doc_details.id_good, t_doc_details.id_doc, t_doc_details.count_doc, t_doc_details.count_real, t_goods.good_name, t_goods.good_code, " +
                " t_doc_head.doc_date, t_doc_head.doc_number, t_doc_head.doc_state " +
                "FROM t_doc_details " +
                "INNER JOIN t_doc_head " +
                "ON t_doc_details.id_doc = t_doc_head.id_doc " +
                "INNER JOIN t_goods " +
                "ON t_doc_details.id_good = t_goods.id_good;";

                SqlCeDataAdapter DAdapter = new SqlCeDataAdapter(queryString, DBConnection);
                SqlCeCommand command = new SqlCeCommand(queryString, DBConnection);
                DAdapter.SelectCommand = command;

                DataSet DSet = new DataSet();
                DBConnection.Open();
                DAdapter.Fill(DSet);
                DBConnection.Close();

                DataTable ImportFromTerminal = DSet.Tables[0];
                int NumberOfRows = ImportFromTerminal.Rows.Count;

                for (int i = 0; i < NumberOfRows; i++)
                {

                    DataRow RowImport = ImportFromTerminal.Rows[i];

                    //CarsId
                    string CarNumber = Convert.ToString(RowImport["doc_number"]);

                    IEnumerable<DataRow> queryCarsId =
                    from c in CarsId.AsEnumerable()
                    where String.Compare(c.Field<String>("CarNumber"), CarNumber, StringComparison.InvariantCultureIgnoreCase) == 0
                    select c;

                    int CarId = 0;
                    if (queryCarsId.Count() != 0)
                    {
                        DataTable FindCarId = queryCarsId.CopyToDataTable<DataRow>();
                        CarId = (int)FindCarId.Rows[0]["CarId"];
                    }
                    else
                    {
                        int RowsCount = CarsId.Rows.Count;
                        for (int NumberOfRow = 0; NumberOfRow < RowsCount; NumberOfRow++)
                        {
                            CarId = Math.Max(CarId, (int)CarsId.Rows[NumberOfRow]["CarId"]);
                        }

                        CarId++;

                        DataRow CarsIdRow = CarsId.NewRow();
                        CarsIdRow["CarId"] = CarId;
                        CarsIdRow["CarNumber"] = CarNumber;
                        CarsId.Rows.Add(CarsIdRow);
                    }

                    //Cars
                    IEnumerable<DataRow> queryCars =
                    from c in Cars.AsEnumerable()
                    where c.Field<int>("CarId") == CarId
                    select c;

                    if (queryCars.Count() == 0)
                    {
                        DataRow CarRow = Cars.NewRow();
                        CarRow["CarId"] = CarId;
                        CarRow["CarNumber"] = CarNumber;
                        Cars.Rows.Add(CarRow);
                    }

                    //CarsState                    
                    DateTime DocDate = Convert.ToDateTime(RowImport["doc_date"]).Date;                    
                    long CarStateId = Convert.ToInt64(DocDate.ToString("yyyyMMdd") + Convert.ToString(CarId));                    
                    
                    IEnumerable<DataRow> queryCarState =
                    from c in CarsState.AsEnumerable()
                    where c.Field<long>("Id") == CarStateId
                    select c;

                    if (queryCarState.Count() == 0)
                    {
                        DataRow CarStateRow = CarsState.NewRow();                        
                        CarStateRow["Id"] = CarStateId;
                        CarStateRow["CarId"] = CarId;
                        CarStateRow["CarDate"] = DocDate;
                        CarStateRow["CarState"] = Convert.ToInt16(RowImport["doc_state"]);
                        CarsState.Rows.Add(CarStateRow);
                    }

                    //GoodsDetails                    
                    long IdGood = Convert.ToInt32(RowImport["id_good"]);
                    string OrderCode = Convert.ToString(RowImport["good_code"]).Trim();

                    string OrderCodeForID = null;
                    if (OrderCode.Length > 0)
                    {
                        OrderCodeForID = OrderCode.Length > 10 ? OrderCode.Substring(0, 10) : OrderCode;
                    }

                    //DocDate 8; IdGood 10; CarId 5; OrderCode 10;
                    String GoodsDetailsId = DocDate.ToString("yyyyMMdd") + IdGood.ToString("D10") + CarId.ToString("D5") + OrderCodeForID.PadLeft(10, '0'); //nvarchar(33)
                     
                    IEnumerable<DataRow> queryGoodsDetails =
                    from c in GoodsDetails.AsEnumerable()
                    where c.Field<string>("Id") == GoodsDetailsId
                    select c;

                    DataRow GoodRow;
                    if (queryGoodsDetails.Count() == 0)
                    {
                        GoodRow = GoodsDetails.NewRow();                        
                    }
                    else
                    {
                        GoodRow = queryGoodsDetails.FirstOrDefault();
                    }

                    GoodRow["Id"] = GoodsDetailsId;
                    GoodRow["IdGood"] = IdGood;
                    GoodRow["GoodName"] = Convert.ToString(RowImport["good_name"]);
                    GoodRow["CarDate"] = DocDate;
                    GoodRow["CarId"] = CarId;
                    GoodRow["OrderCode"] = OrderCode;
                    
                    if (GoodRow.IsNull("CountPlanned"))
                    {
                        GoodRow["CountPlanned"] = Convert.ToDecimal(RowImport["count_doc"]);
                    }
                    else
                    {
                        GoodRow["CountPlanned"] = Convert.ToDecimal(GoodRow["CountPlanned"]) +  Convert.ToDecimal(RowImport["count_doc"]);
                    }
                    
                    if (GoodRow.IsNull("CountReal"))
                    {
                        GoodRow["CountReal"] = Convert.ToDecimal(RowImport["count_doc"]);
                    }
                    else
                    {
                        GoodRow["CountReal"] = Convert.ToDecimal(GoodRow["CountReal"]) + Convert.ToDecimal(RowImport["count_real"]);
                    }

                    GoodsDetails.Rows.Add(GoodRow);

                }
            }
        }
        

        //Вызывает событие обновления прогрес бара
        //
        void UpdateProgress()
        {
            if (CountOfInsertingStrings == 0)
                return;

            int PreviousCurrentProgress = CurrentProgress;
            
            decimal curProgress = (NumberOfInsertingString * 100) / (CountOfInsertingStrings);
            CurrentProgress = (int)Math.Round(curProgress);

            if (PreviousCurrentProgress != CurrentProgress)
            {
                UpdateProgressBarPercentageEvent();
            }
        }


        //Get all DateTime from source table CarsState
        //
        private DateTime[] GetCarsDate(DataTable CarsStateSourceDB)
        {
            IEnumerable<DateTime> query = from cs in CarsStateSourceDB.AsEnumerable()
                                          group cs by new { CarDate = cs.Field<DateTime>("CarDate") } into grp
                                          select grp.Key.CarDate;

            int CountOfItems = query.Count();
            DateTime[] ArrCarsDate = new DateTime[CountOfItems];
            int IAr = 0;
            foreach (DateTime CurDate in query)
            {
                ArrCarsDate[IAr] = CurDate;
                IAr++;
            }

            return ArrCarsDate;
        }


        //Delete strings from destination tables before bulkInsert
        //where DateTime in array ArrCarsDate
        //
        private void DeleteStringByDatesInDestinationDataBase(DateTime[] ArrCarsDate)
        {
            using (SqlConnection DestinationConnection = new SqlConnection(DBConnectionSettings.DestinationConnectionString))
            {
                string queryString = @"
                    DELETE FROM [dbo].[CarsState] WHERE CarDate = @CarDate
                    DELETE FROM [dbo].[GoodsDetails] WHERE CarDate = @CarDate;"
                    ;

                DestinationConnection.Open();

                int NumberOfRows = ArrCarsDate.Count();
                for (int i = 0; i < NumberOfRows; i++)
                {
                    SqlCommand command = new SqlCommand(queryString, DestinationConnection);

                    SqlParameter ParCarDate = new SqlParameter("@CarDate", SqlDbType.DateTime);

                    ParCarDate.Value = ArrCarsDate[i];

                    command.Parameters.Add(ParCarDate);

                    command.ExecuteNonQuery();
                }
                                               
                DestinationConnection.Close();
            }
        }


        //Get new tables without string from destination database
        //
        private void GetNewTables(DataTable CarsDestDB, DataTable CarsSourceDB, ref DataTable NewCars)
        {
            //New cars
            int NumberOfRows = CarsSourceDB.Rows.Count;
            for (int i = 0; i < NumberOfRows; i++)
            {
                DataRow RowSource = CarsSourceDB.Rows[i];

                int CarId = Convert.ToInt32(RowSource["CarId"]);

                IEnumerable<DataRow> query =
                from c in CarsDestDB.AsEnumerable()
                where c.Field<int>("CarId") == CarId
                select c;

                if (query.Count() != 0)
                    continue;

                DataRow CarsRow = NewCars.NewRow();
                CarsRow["CarId"] = Convert.ToInt32(RowSource["CarId"]);
                CarsRow["CarNumber"] = Convert.ToString(RowSource["CarNumber"]);
                NewCars.Rows.Add(CarsRow);
            }
        }


        // Insert new strings from parameter TableWithNewStrings
        // into SQL table from parameter SQLTableName
        //
        private bool InsertTableIntoSQLTable(DataTable TableWithNewStrings, string SQLTableName)
        {
            if (TableWithNewStrings.Rows.Count == 0)
                return true;

            // Open a connection to the database. 
            using (SqlConnection connection =
                       new SqlConnection(DBConnectionSettings.DestinationConnectionString))
            {
                connection.Open();

                // Perform an initial count on the destination table.
                string query = "SELECT COUNT(*) FROM " + "dbo." + SQLTableName + ";";

                SqlCommand commandRowCount = new SqlCommand(query, connection);

                long countStart = System.Convert.ToInt64(
                    commandRowCount.ExecuteScalar());                

                // Create the SqlBulkCopy object.  
                // Note that the column positions in the source DataTable  
                // match the column positions in the destination table so  
                // there is no need to map columns.  
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = "dbo." + SQLTableName;

                    try
                    {                        
                        // Write from the source to the destination.
                        bulkCopy.WriteToServer(TableWithNewStrings);
                    }
                    catch (Exception ex)
                    {
                        ExceptionMessage = ex.Message;
                        return false;
                    }
                }

                // Perform a final count on the destination  
                // table to see how many rows were added. 
                long countEnd = System.Convert.ToInt64(
                    commandRowCount.ExecuteScalar());

                long CountOfInsertedString = countEnd - countStart;
                if (CountOfInsertedString != TableWithNewStrings.Rows.Count)
                {
                    ExceptionMessage = @"Не удалось заполнить таблицу " + SQLTableName + "!";
                    return false;
                }

                NumberOfInsertingString += CountOfInsertedString;
                UpdateProgress();
            }

            return true;
        }


        //insert data to destination tables
        //
        private bool InsertDataToDestinationDataBase(DataTable NewCars, DataTable NewCarsState, DataTable NewGoodsDetails)
        {
            bool Succesfull;

            Succesfull = InsertTableIntoSQLTable(NewCars, "Cars");

            if (Succesfull == false)
                return false;

            Succesfull = InsertTableIntoSQLTable(NewCarsState, "CarsState");

            if (Succesfull == false)
                return false;

            Succesfull = InsertTableIntoSQLTable(NewGoodsDetails, "GoodsDetails");

            if (Succesfull == false)
                return false;

            return true;
        }


        //Do import from SQL
        //
        public void DoImportFromSQL()
        {
            DataTable Cars, CarsState, GoodsDetails;

            //Cars
            DataColumn CarsColumnCarId = new DataColumn();
            CarsColumnCarId.DataType = System.Type.GetType("System.Int32");
            CarsColumnCarId.ColumnName = "CarId";
            CarsColumnCarId.Unique = true;
            
            Cars = new DataTable();
            Cars.Columns.Add(CarsColumnCarId);
            Cars.Columns.Add("CarNumber", System.Type.GetType("System.String"));
            Cars.PrimaryKey = new DataColumn[] { CarsColumnCarId };


            //CarsState
            DataColumn CarsStateColumnId = new DataColumn();
            CarsStateColumnId.DataType = System.Type.GetType("System.Int64");
            CarsStateColumnId.ColumnName = "Id";
            CarsStateColumnId.Unique = true;            
            
            CarsState = new DataTable();
            CarsState.Columns.Add(CarsStateColumnId);
            CarsState.Columns.Add("CarId", System.Type.GetType("System.Int16"));
            CarsState.Columns.Add("CarDate", System.Type.GetType("System.DateTime"));
            CarsState.Columns.Add("CarState", System.Type.GetType("System.Int16"));
            CarsState.PrimaryKey = new DataColumn[] { CarsStateColumnId };


            //GoodsDetails                        
            DataColumn GoodsDetailsColumnId = new DataColumn();
            GoodsDetailsColumnId.DataType = System.Type.GetType("System.String");
            GoodsDetailsColumnId.ColumnName = "Id";
            GoodsDetailsColumnId.Unique = true;            

            GoodsDetails = new DataTable();
            GoodsDetails.Columns.Add(GoodsDetailsColumnId);
            GoodsDetails.Columns.Add("IdGood", System.Type.GetType("System.Int32"));
            GoodsDetails.Columns.Add("GoodName", System.Type.GetType("System.String"));
            GoodsDetails.Columns.Add("CarDate", System.Type.GetType("System.DateTime"));
            GoodsDetails.Columns.Add("CarID", System.Type.GetType("System.Int16"));
            GoodsDetails.Columns.Add("OrderCode", System.Type.GetType("System.String"));
            GoodsDetails.Columns.Add("CountPlanned", System.Type.GetType("System.Decimal"));
            GoodsDetails.Columns.Add("CountReal", System.Type.GetType("System.Decimal"));
            GoodsDetails.PrimaryKey = new DataColumn[] { GoodsDetailsColumnId };


            DataTable CarsDestDB = Cars.Clone();
            DataTable CarsStateDestDB = CarsState.Clone();
            DataTable GoodsDetailsDestDB = GoodsDetails.Clone();

            DataTable CarsSourceDB = Cars.Clone();
            DataTable CarsStateSourceDB = CarsState.Clone();
            DataTable GoodsDetailsSourceDB = GoodsDetails.Clone();

            DataTable NewCars = Cars.Clone();
            DataTable NewCarsState = CarsState.Clone();
            DataTable NewGoodsDetails = GoodsDetails.Clone();


            try
            {
                GetDataFromDestinationDataBase(ref CarsDestDB);

                GetDataFromSQLCompactFile(ref CarsSourceDB, ref CarsStateSourceDB, ref GoodsDetailsSourceDB);
                
                DateTime[] ArrCarsDate = GetCarsDate(CarsStateSourceDB);

                DeleteStringByDatesInDestinationDataBase(ArrCarsDate);

                GetNewTables(CarsDestDB, CarsSourceDB, ref NewCars);

                CompletedSuccesfully = InsertDataToDestinationDataBase(NewCars, CarsStateSourceDB, GoodsDetailsSourceDB);
            }
            catch (Exception ex)
            {
                CompletedSuccesfully = false;
                ExceptionMessage = ex.Message + " " + ex.ToString();
            }        

            EndOfImportEvent();
        }
    }
}