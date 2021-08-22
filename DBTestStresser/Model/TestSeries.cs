using DBTestStresser.Util;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBTestStresser.Model {
    public class TestSeries {
        public int Id { get; set; }
        public EntityDBMS DBMS { get; set; }
        public string Connection_String { get; set; }
        public string OperationType { get; set; }

        private int[] ConcurrencyAmounts { get; set; }

        public string CSV { get; set; }

        private string DIR_CSV_URL = @".\CSV";

        public TestSeries() {
            //ConcurrencyAmounts = new int[] {1, 5, 10, 50, 100, 300, 500, 1000, 5000, 10000, 50000 };//, 50000 };//, 100000 }; //500000, 1000000};
            ConcurrencyAmounts = new int[] {10, 100, 500, 1000, 5000, 10000, 50000, 100000 };//, 50000 };//, 100000 }; //500000, 1000000};
            CSV = "sep=;\nConcurrency amount; Average thread execution time (ms); Max thread execution time (ms)\n";
                //"Average query execution time (ms); Total threads executions times / thread number (ms)\n";
        }
        public void Execute() {
            Test t;

            for (int i = 0; i < ConcurrencyAmounts.Length; i++) {
                int amount = ConcurrencyAmounts[i];
                string[] queries = null;
                switch (OperationType) {
                    case "Read":
                        queries = DBMS.GenerateRandomReadQueries(amount);

                        //queries = RandomDB.GenerateRandomSQLReadQueries(amount);
                    break;
                    case "Write":
                        queries = DBMS.GenerateRandomWriteQueries(amount);                        
                        //queries = RandomDB.GenerateRandomSQLWriteQueries(amount);
                        break;
                    case "Update":
                        queries = DBMS.GenerateRandomUpdateQueries(amount);
                        break;
                }

                t = new Test();
                t.ConcurrencyAmount = amount;
                t.Serie = this;
                t.Queries = queries;

                if (DBMS.Name == "Cassandra") {
                    t.ExecuteCassandraTest();
                } else {
                    t.Execute();
                }
                
            }

            GUI.Log("======== END =========");

            // Writing CSV FILE
            if (!Directory.Exists(DIR_CSV_URL)) {
                Directory.CreateDirectory(DIR_CSV_URL);
            }

            string csvFilename = this.DBMS.Name + "" + OperationType + "-" 
                + DateTime.Now.ToString("yyyyMMdd_hhmmss") + ".csv";
            string csvUrl = DIR_CSV_URL + "\\" + csvFilename;
            if (File.Exists(csvUrl)) {
                
                int i = 1;
                string originalCsvUrl = csvUrl;

                do {
                    csvUrl = originalCsvUrl + "_" + i;
                    i++;
                } while (File.Exists(csvUrl));

            }

            //var stream = File.Create(csvUrl);
            //stream.Close();
            File.WriteAllText(csvUrl,CSV);
        }
    }
}
