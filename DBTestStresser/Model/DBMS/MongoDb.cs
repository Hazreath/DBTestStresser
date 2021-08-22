using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using DBTestStresser.Model.ExampleStore;
using DBTestStresser.Util;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace DBTestStresser.Model.DBMS {
    public class MongoDb : EntityDBMS {
        private MongoDb() {

        }
        public MongoDb(string ip, string port) {
            this.Ip = ip;
            this.Port = !String.IsNullOrEmpty(port) ? port : "27017";
            
            this.Name = "MongoDB";
        }
        public override string BuildConnectionString() {
            return "mongodb://" + Ip + ":" + Port;
        }

        public override DatabaseConnection GetConnection() {
            return new DatabaseConnection(
                new MongoClient(BuildConnectionString())
            );
            //return null;
        }

        public override void PopulateDB() {
            MongoClient dbClient = new MongoClient(BuildConnectionString());
            var db = dbClient.GetDatabase(DB_NAME);

            GUI.Log("Generating brands...");
            var cl_brands = db.GetCollection<ModelExampleStore>(C_BRANDS);
            PopulateCollection<Brand>(cl_brands,N_BRANDS);

            GUI.Log("Generating customers...");
            var cl_customers = db.GetCollection<ModelExampleStore>(C_CUSTOMERS);
            PopulateCollection<Customer>(cl_customers, N_CUSTOMERS);

            GUI.Log("Generating products...");
            var cl_products = db.GetCollection<ModelExampleStore>(C_PRODUCTS);
            PopulateCollection<Product>(cl_products, N_PRODUCTS);

            GUI.Log("Generating orders...");
            var cl_orders = db.GetCollection<ModelExampleStore>(C_ORDERS);
            PopulateCollection<Order>(cl_orders, N_ORDERS);
            GUI.Log("Done !");
        }

        public void PopulateCollection<T>(IMongoCollection<ModelExampleStore> collection,
            int amount) {
            
            collection.DeleteMany(Builders<ModelExampleStore>.Filter.Empty);

            ModelExampleStore[] toInsert = new ModelExampleStore[amount];
            for (int i = 0; i < amount; i++) {
                
                if (typeof(T) == typeof(Brand)) {
                    toInsert[i] = Brand.GenerateRandom(i + 1);
                } else if (typeof(T) == typeof(Product)) {
                    toInsert[i] = Product.GenerateRandom(N_BRANDS, i + 1);
                } else if (typeof(T) == typeof(Customer)) {
                    toInsert[i] = Customer.GenerateRandom(i + 1);
                } else if (typeof(T) == typeof(Order)) {
                    toInsert[i] = Order.GenerateRandom(N_PRODUCTS,N_CUSTOMERS,N_BRANDS, i + 1);
                }

                //Console.WriteLine(Brand.GenerateRandom(i));
            }
            collection.InsertMany(toInsert);
        }

        public override void ReadQuery(DatabaseConnection cnx, string filter) {
            var client = cnx.MongoClient;
            var db = client.GetDatabase(DB_NAME);
            var s = new Stopwatch();
            
            var products = db.GetCollection<Product>(C_PRODUCTS).Find(filter).ToList();
        }

        public override void WriteQuery(DatabaseConnection cnx, string json) {
            var client = new MongoClient(BuildConnectionString());
            var db = client.GetDatabase(DB_NAME);
            var orders = db.GetCollection<BsonDocument>(C_ORDERS);
            // Serialisation takes less than > 1ms in most cases :
            // In average, 0.9% of thoses serialisations take more than 0 ms
            // <=> neglected
            var bson = BsonSerializer.Deserialize<BsonDocument>(json);
            
            orders.InsertOne(bson);
            
        }

        public override string[] GenerateRandomReadQueries(int amount) {
            return RandomDB.GenerateRandomMongoSHFilters(amount);
        }

        public override string[] GenerateRandomWriteQueries(int amount) {
            return RandomDB.GenerateRandomMongoJsons(amount);
        }

        public override string TestConnection() {
            string ret = "Connection successful !";
            
            try {
                var client = GetConnection().MongoClient;
                var db = client.GetDatabase(DB_NAME);
                Stopwatch s = new Stopwatch();
                
                var brands = db.GetCollection<Product>(C_BRANDS).Find("{}").FirstOrDefault();
                // Default mongo timeout : 30s
            } catch (Exception e) {
                ret = "Connexion error : " + e.Message;
            }

            return ret;
        }

        public override void UpdateQuery(DatabaseConnection cnx, string json) {
            var client = new MongoClient(BuildConnectionString());
            var db = client.GetDatabase(DB_NAME);
            var products = db.GetCollection<BsonDocument>(C_PRODUCTS);
            // Serialisation takes less than > 1ms in most cases :
            // In average, 0.9% of thoses serialisations take more than 0 ms
            // <=> neglected
            var bson = BsonSerializer.Deserialize<BsonDocument>(json);
            string filter = "{" +
                String.Format("_id:{0}", RandomDB.GenerateRandomInt(0, N_PRODUCTS))
                + "}";
            
            products.UpdateOne(filter,bson);
        }

        public override string[] GenerateRandomUpdateQueries(int amount) {
            string[] queries = new string[amount];
            string incrStockByOne = "{$inc: {stock:1}}";
            for (int i = 0; i < amount; i++) {
                queries[i] = incrStockByOne;
            }

            return queries;
        }
    }
}
