using DBTestStresser.Util;
using Neo4j.Driver;
using Nito.AsyncEx;
using Nito.AsyncEx.Synchronous;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBTestStresser.Model.DBMS {
    public class Neo4J : EntityDBMS {

        public Neo4J (string ip, string port) {
            this.Ip = ip;
            this.Port = !String.IsNullOrEmpty(port) ? port : "7687";
            this.Name = "Neo4j";
        }
        public override string BuildConnectionString() {
            return "bolt://" + Ip + ":" + Port;
        }

        public override DatabaseConnection GetConnection() {
            
            return new DatabaseConnection(GraphDatabase.Driver(
                BuildConnectionString(),AuthTokens.Basic(USER_NAME,PASSWORD)));
        }

        public override void PopulateDB() {
            var driver = (IDriver)GetConnection().GetConnectionInstance();
            var sess = driver.Session();
            

            // Delete all nodes & relationships
            var wipeNodesWithRelations = "match (a) -[r] -> () delete a, r";
            var wipeSinglesNodes = "match (a) delete a";
            sess.Run(wipeNodesWithRelations);
            sess.Run(wipeSinglesNodes);

            // Brands
            GUI.Log("Generating brands...");
            string json;
            for (int i = 0; i < N_BRANDS; i++) {
                json = Brand.GenerateRandom(i).ToCypherJsonString();
                sess.Run("CREATE (b:Brand " + json + ")");
            }

            GUI.Log("Generating customers...");
            for (int i = 0; i < N_CUSTOMERS; i++) {
                json = Customer.GenerateRandom(i).ToCypherJsonString();
                sess.Run("CREATE (c:Customer " + json + ")");
            }

            Product p;
            string cyph;
            GUI.Log("Generating products...");
            for (int i = 0; i < N_PRODUCTS; i++) {
                p = Product.GenerateRandom(N_BRANDS, i);
                cyph = "MATCH (b:Brand {Id:" + p.Brand.Id + "}) " +
                "CREATE (p:Product " + p.ToCypherJsonString() + ") -[:OWNED_BY]-> (b)";
                
                sess.Run(cyph);
            }

            Order o;

            GUI.Log("Generating orders...");
            for (int i = 0; i < N_ORDERS; i++) {
                o = Order.GenerateRandom(N_PRODUCTS, N_CUSTOMERS, N_BRANDS, i);
                cyph =
                "MATCH (c:Customer {Id:" + o.Customer.Id + "}) " +
                "MATCH (p:Product {Id:" + o.Product.Id + "}) " +
                "CREATE (o:Order " + o.ToCypherJsonString() + ") -[:CREATED_FOR]-> (c) -[:BOUGHT]-> (p)";

                sess.Run(cyph);
            }

            GUI.Log("Complete !");



        }


        public override string[] GenerateRandomReadQueries(int amount) {
            var queries = new string[amount];

            string cyph = "MATCH (p:Product) WHERE p.Id > {0} AND p.Id < {1} return p";

            int greater, lower;
            int nbProductsPerPage = 100;
            for (int i = 0; i < amount; i++) {
                greater = RandomDB.GenerateRandomInt(0, N_PRODUCTS - nbProductsPerPage);
                lower = RandomDB.GenerateRandomInt(greater, greater + nbProductsPerPage);
                queries[i] = String.Format(cyph, greater, lower);
            }

            return queries;
        }

        public override string[] GenerateRandomWriteQueries(int amount) {
            Order o;
            string[] queries = new string[amount];
            string cyph = "CREATE (o:Order {0})";
            for (int i = 0; i < amount; i++) {
                o = Order.GenerateRandom(N_PRODUCTS,N_CUSTOMERS,N_BRANDS,-i);
                queries[i] = String.Format(cyph, o.ToCypherJsonString());
            }
            return queries;
        }

        

        public override void ReadQuery(DatabaseConnection cnx, string query) {
            RunNeo4JQuery(cnx, query);
        }
        public override void WriteQuery(DatabaseConnection cnx, string query) {
            RunNeo4JQuery(cnx, query);
        }
        public override void UpdateQuery(DatabaseConnection cnx, string query) {
            RunNeo4JQuery(cnx, query);
        }

        private void RunNeo4JQuery(DatabaseConnection cnx, string query) {
            var c = (IDriver) cnx.GetConnectionInstance();
            //var session = c.Session();
            var asess = c.AsyncSession();
            var cmd = asess.RunAsync(query);
            var r = cmd.WaitAsync(new System.Threading.CancellationToken());
            

            //session.Run(query);
        }
        public override string TestConnection() {
            var r = "Connected successfully !";
            try {
                var c = (IDriver) GetConnection().GetConnectionInstance();
                var sess = c.Session();
                sess.Run("match (a) -> () RETURN a");
                
            } catch (Exception e) {
                r = "Connexion error : " + e.Message;
            }

            return r;
            
        }

        

        

        public override string[] GenerateRandomUpdateQueries(int amount) {
            string[] queries = new string[amount];
            for (int i = 0; i < amount; i++) {
                queries[i] = String.Format("MATCH (p:Product) WHERE p.Id = {0} " +
                    "SET p.Stock = p.Stock + 1",RandomDB.GenerateRandomInt(0,N_PRODUCTS));
            }

            return queries;
        }
    }
}
