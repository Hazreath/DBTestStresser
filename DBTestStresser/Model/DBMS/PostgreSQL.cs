using DBTestStresser.Util;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBTestStresser.Model.DBMS {
    public class PostgreSQL : EntityDBMS {

        private PostgreSQL() {

        }
        public PostgreSQL(string ip, string port) {
            this.Ip = ip;
            this.Port = !String.IsNullOrEmpty(port) ? port : "5432";
            this.Name = "PostgreSQL";
        }
        public override string BuildConnectionString() {
            string cnx_str = String.Format(
                        " Server = {0};" +
                        " Port = {1};" +
                        " Database = {2};" +
                        " User Id = {3};" +
                        " Password = {4};" +
                        " Command Timeout=0;",
                        Ip, Port, EntityDBMS.DB_NAME, EntityDBMS.USER_NAME, EntityDBMS.PASSWORD
                    );

            return cnx_str;
        }

        public override DatabaseConnection GetConnection() {
            return new DatabaseConnection((DbConnection) new NpgsqlConnection(BuildConnectionString()));
        }
        public override void ReadQuery(DatabaseConnection cnx, string query) {
            var cmd = new NpgsqlCommand(query, (NpgsqlConnection)cnx.GetConnectionInstance());
            //object r = cmd.ExecuteScalar();   
            Stopwatch s = new Stopwatch();
            s.Start();
            var reader = cmd.ExecuteReader();
            var p = new Product();
            var b = new Brand();
            
            while (reader.Read()) {
                p.Id = reader.GetInt32(0);
                p.Name = reader.GetString(1);
                p.Price = reader.GetDouble(2);
                p.Stock = reader.GetInt32(3);
            }
            s.Stop();
            Console.WriteLine("mesure ds query : " + s.ElapsedMilliseconds + "ms");
        }

        public override void WriteQuery(DatabaseConnection cnx, string query) {
            var cmd = new NpgsqlCommand(query, (NpgsqlConnection)cnx.GetConnectionInstance());
            cmd.ExecuteNonQuery();
        }

        public override void PopulateDB() {
            var cnx = GetConnection();

            string wipe = "DELETE FROM orders; DELETE FROM products; DELETE FROM customers;" +
                "DELETE FROM brands;";
            string seq_reset = "ALTER SEQUENCE brands_id_seq RESTART;" +
                "ALTER SEQUENCE products_id_seq RESTART;" +
                "ALTER SEQUENCE customers_id_seq RESTART;" +
                "ALTER SEQUENCE orders_id_seq RESTART;";
            //+
            //"TRUNCATE TABLE orders; TRUNCATE TABLE customers; TRUNCATE TABLE customers;" +
            //"TRUNCATE TABLE orders";
            ;
            //string resetSequences = "ALTER SEQUENCE products_id_seq RESTART WITH 1;" +
            //                   "ALTER SEQUENCE brands_id_seq RESTART WITH 1;" +
            //                   "ALTER SEQUENCE customers_id_seq RESTART WITH 1; ";
            GUI.Log("Connection to DB...");
            cnx.Open();
            GUI.Log("Connected !");
            GUI.Log("Wiping existing data...");
            WriteQuery(cnx, wipe);
            

            GUI.Log("Resetting sequences...");
            WriteQuery(cnx, seq_reset);
            //string reset = "ALTER TABLE brands AUTO_INCREMENT = 1;" +
            //    "ALTER TABLE products AUTO_INCREMENT = 1;" +
            //    "ALTER TABLE customers AUTO_INCREMENT = 1; ";
            //WriteQuery(cnx, reset);

            // Brands
            GUI.Log("BRANDS - Generating query");
            List<string> types = new List<string> { "string" };
            string insert = "INSERT INTO brands(name) ";
            insert += RandomDB.BuildInsertIntoValues(types, N_BRANDS) + ";";
            GUI.Log("BRANDS - Executing query");
            WriteQuery(cnx, insert);

            // Customers
            GUI.Log("CUSTOMERS - Generating query");
            types = new List<string> { "string", "string", "string", "string" };
            insert = "INSERT INTO customers(surname,name,city,email)";
            insert += RandomDB.BuildInsertIntoValues(types, N_CUSTOMERS) + ";";
            GUI.Log("CUSTOMERS - Executing query");
            WriteQuery(cnx, insert);

            // Products
            GUI.Log("PRODUCTS - Generating query");
            types = new List<string> { "string", "double", "int", "int" };
            var intMins = new int[] { 100, 1, 1 };
            var intMaxs = new int[] { 400, N_BRANDS, 100 };
            insert = "INSERT INTO products(name,price,brand_id,stock)";
            insert += RandomDB.BuildInsertIntoValues(types, N_PRODUCTS, intMins, intMaxs) + ";";
            GUI.Log("PRODUCTS - Executing query");
            WriteQuery(cnx, insert);

            // Orders
            GUI.Log("ORDERS - Generating query");
            types = new List<string> { "string", "int", "int" };
            intMins = new int[] { 1, 1 };
            intMaxs = new int[] { N_CUSTOMERS, N_PRODUCTS };
            insert = "INSERT INTO orders(date,customer_id,product_id)";
            insert += RandomDB.BuildInsertIntoValues(types, N_ORDERS, intMins, intMaxs) + ";";
            GUI.Log("ORDERS - Executing query");
            WriteQuery(cnx, insert);

            GUI.Log("===== END ======");
            cnx.Close();
        }

        public override string[] GenerateRandomReadQueries(int amount) {
            return RandomDB.GenerateRandomSQLReadQueries(amount);
        }

        public override string[] GenerateRandomWriteQueries(int amount) {
            return RandomDB.GenerateRandomSQLWriteQueries(amount);
        }

        public override string TestConnection() {
            string ret = "Connection successful !";
            try {
                var cnx = GetConnection();
                cnx.Open();
                cnx.Close();
            } catch (Exception e) {
                ret = "Connexion error : " + e.Message;
            }

            return ret;
        }

        public override void UpdateQuery(DatabaseConnection cnx, string query) {
            var cmd = new NpgsqlCommand(query, (NpgsqlConnection) cnx.GetConnectionInstance());
            cmd.ExecuteNonQuery();
        }

        public override string[] GenerateRandomUpdateQueries(int amount) {
            return RandomDB.GenerateRandomSQLUpdateQueries(amount);
        }
    }
}
