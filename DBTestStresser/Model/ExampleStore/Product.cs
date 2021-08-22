using DBTestStresser.Util;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace DBTestStresser.Model {
    public class Product : ExampleStore.ModelExampleStore {
        public int Id { get; set; }
        public Brand Brand { get; set; }
        public string Name { get; set; }

        public double Price { get; set; }

        public int Stock { get; set; }
        

        public static Product GenerateRandom(int maxBrandId, int id=-1) {
            Product p = new Product();
            p.Id = id;
            p.Name = RandomDB.GenerateRandomString(15);
            p.Price = (double) RandomDB.GenerateRandomInt(0, 500);
            p.Stock = RandomDB.GenerateRandomInt(0, 15);
            int brand_id = RandomDB.GenerateRandomInt(0, maxBrandId);
            p.Brand = Brand.GenerateRandom(brand_id);

            return p;
        }

        public JsonDocument ToJSON() {
            string content = JsonConvert.SerializeObject(this);
            Console.WriteLine(content);
            var json = JsonDocument.Parse(content);
            return json;
        }
        public string ToCypherJsonString() {
            // Avoid serializing Brand in graph
            string json = "{" + String.Format("Id:{0},Name:\"{1}\",Price:{2},Stock:{3}",
                Id,Name,Price,Stock) + "}";
            return json;
        }
        public override string ToString() {
            return String.Format("{0}:{1},{2},{3}({4})",Id,Brand,Name,Price,Stock);
        }
    }
}
