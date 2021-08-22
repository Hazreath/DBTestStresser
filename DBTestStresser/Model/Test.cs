using Cassandra;
using DBTestStresser.Util;
using DiagnosticsUtils;
using Npgsql;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DBTestStresser.Model {
    public class Test {
        public int Id { get; set; }
        public TestSeries Serie { get; set; }

        public int ConcurrencyAmount { get; set; }

        public string[] Queries { get; set; }
        public void Execute() {
            List<Task> tasks = new List<Task>();
            Task t;
            string query = "";
            ConcurrentBag<long> threadTimes = new ConcurrentBag<long>();
            ConcurrentBag<double> queriesTimes = new ConcurrentBag<double>();
            ConcurrentBag<string> outputs = new ConcurrentBag<string>();
            
            Stopwatch chrono = new Stopwatch();
            GUI.Log("Running " + ConcurrencyAmount + " concurrents jobs...");
            chrono.Start();
            for (int i = 0; i < ConcurrencyAmount; i++) {
                query = Queries[i];
                t = Task.Run(
                    () => ExecuteThread(this.Serie.DBMS,threadTimes, queriesTimes,outputs, query)
                );
                tasks.Add(t);
            }
            try {
                Task.WaitAll(tasks.ToArray());
                chrono.Stop();

                TestSummary(threadTimes);
                
            } catch (Exception e) {
                chrono.Stop();
                GUI.Log("Threads join failed : " + e.Message + "\n" + e.StackTrace);
                GUI.Log("Outputs =============");
                foreach (var err in outputs) {
                    if (!String.IsNullOrEmpty(err)) {
                        GUI.Log(err);
                    }
                }
            }
        }

        public void TestSummary(ConcurrentBag<long> threadTimes) {
            // Mean query execution time
            string debug = "COLLECTION SIZE: " + threadTimes.Count + " : {";
            double threadMean = 0.0;
            long maxExecTime = 0L;
            foreach (long ms in threadTimes) {
                debug += ms + ',';
                threadMean += (double) ms;
                maxExecTime = (ms > maxExecTime) ? ms : maxExecTime;
            }
            
            if (threadTimes.Count > 0) {
                threadMean = threadMean / threadTimes.Count;
            }
            debug += "}";
            
            GUI.Log("All jobs returned : ");
            GUI.Log("average thread execution time : " + threadMean + " ms");
            GUI.Log("Max thread execution time : " + maxExecTime + " ms");
            
            GUI.Log("");
            this.Serie.CSV += ConcurrencyAmount + ";" 
                + threadMean + ";" + maxExecTime + ";\n";
        }

        public void ExecuteCassandraTest() {
            List<Task> tasks = new List<Task>();
            Task t;
            string query = "";
            ConcurrentBag<long> threadTimes = new ConcurrentBag<long>();
            ConcurrentBag<double> queriesTimes = new ConcurrentBag<double>();
            ConcurrentBag<string> outputs = new ConcurrentBag<string>();

            Stopwatch chrono = new Stopwatch();
            Stopwatch thread = new Stopwatch();
            GUI.Log("Running " + ConcurrencyAmount + " concurrents jobs...");
            var dbms = this.Serie.DBMS;

            chrono.Start();
            var cnx = (Cluster)dbms.GetConnection().GetConnectionInstance();
            var session = cnx.Connect();
            PreparedStatement prep;
            query = Queries[0];

            

            
            var op = ConcurrentUtils.ConcurrentUtils.Times(ConcurrencyAmount, 512, _ =>
                    ExecuteCassandraThread(threadTimes, session, Queries));
            //for (int i = 0; i < ConcurrencyAmount; i++) {
            //    //query = Queries[i];
            //    //t = Task.Run(
            //    //    () => ExecuteThread(this.Serie.DBMS, threadTimes, queriesTimes, outputs, query)
            //    //);
            //    //tasks.Add(t);
                
            //    //op.Wait();
            //}
            try {
                Task.WaitAll(tasks.ToArray());
                chrono.Stop();

                TestSummary(threadTimes);
            } catch (Exception e) {
                chrono.Stop();
                GUI.Log("Threads join failed : " + e.Message + "\n" + e.StackTrace);
                GUI.Log("Outputs =============");
                foreach (var err in outputs) {
                    if (!String.IsNullOrEmpty(err)) {
                        GUI.Log(err);
                    }
                }
            }
        }

        public Task ExecuteCassandraThread(ConcurrentBag<long> threadTimes,ISession session,
            string[] queries) {
            Stopwatch s = new Stopwatch();
            string query = queries[threadTimes.Count];
            PreparedStatement p = session.Prepare(query);

            s.Start();
            var op = session.ExecuteAsync(p.Bind());
            op.Wait();
            s.Stop();

            threadTimes.Add(s.ElapsedMilliseconds);
            return op;
        }
        public void ExecuteThread(EntityDBMS dbms,ConcurrentBag<long> threadTimes,
            ConcurrentBag<double> queryTimes,
            ConcurrentBag<string> outputs, string query) {

            Stopwatch threadChrono = new Stopwatch();
            
            ExecutionStopwatch queryChrono = new ExecutionStopwatch();
            var cnx = dbms.GetConnection();
            string operation = this.Serie.OperationType;
            try {

                threadChrono.Start();

                if (cnx.NeedConnexionOpening)
                    cnx.Open();
                
                if (dbms.Name == "Cassandra") {

                } else {

                }
                if (operation == "Read") {
                    dbms.ReadQuery(cnx, query);
                } else if (operation == "Write"){
                    
                    dbms.WriteQuery(cnx, query);
                } else if (operation == "Update") {
                    dbms.UpdateQuery(cnx, query);
                }

                if (cnx.NeedConnexionOpening)
                    cnx.Close();

                threadChrono.Stop();
            } catch (Exception e) {
                outputs.Add("ERROR : " + e.Message + "\n" + e.StackTrace);
                if (cnx.NeedConnexionOpening 
                    && cnx.GetState() == System.Data.ConnectionState.Open) {
                    cnx.Close();
                }
                //if (threadChrono.IsRunning) {
                //    threadChrono.Stop();
                //}
                //if (queryChrono.IsRunning) {
                //    queryChrono.Stop();
                //}
            }

            // Thread end
            threadTimes.Add(threadChrono.ElapsedMilliseconds);
            //queryTimes.Add(queryChrono.Elapsed.TotalMilliseconds);

            //Console.WriteLine("thread : " + threadChrono.ElapsedMilliseconds + "; query : "
            //    + queryChrono.Elapsed.ToString());
        }
    }
}
