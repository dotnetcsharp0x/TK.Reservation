using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Data;
using System.Drawing.Text;
using System.Xml;
using TK.Reservation.Actions;
using TK.Reservation.Models;

namespace TK.Reservation
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var builder = new HostBuilder();
            XmlReaderSettings readerSettings = new XmlReaderSettings();
            readerSettings.IgnoreComments = true;
            #region XML connect - settings.xml
            XmlDocument xApi = new XmlDocument();
            xApi.Load("settings.xml");
            XmlElement? xRootApi = xApi.DocumentElement;
            int timing = 0;
            bool debug = true;
            int PredReserveRoute = -1;
            string logpath = "";
            int logclearperiod = 0;
            #endregion

            #region ReadXml
            if (xRootApi != null)
            {
                foreach (XmlElement xnode in xRootApi)
                {
                    if (xnode.Name == "Timing")
                    {
                        timing = Convert.ToInt32(xnode.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                    }
                    if (xnode.Name == "LogPath")
                    {
                        logpath = Convert.ToString(xnode.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                    }
                    if (xnode.Name == "LogClearPeriod")
                    {
                        logclearperiod = Convert.ToInt32(xnode.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                    }
                    if (xnode.Name == "PredReserveRoute")
                    {
                        PredReserveRoute = Convert.ToInt32(xnode.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                    }
                    if (xnode.Name == "Debug")
                    {
                        if (Convert.ToInt32(xnode.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", "")) == 1)
                        {
                            debug = true;
                        }
                        else
                        {
                            debug = false;
                        }
                    }
                    if (xnode.Name == "Reservation")
                    {
                        foreach (XmlElement a in xnode)
                        {
                            #region PredReserve
                            if (a.Name == "PredReserve")
                            {
                                Params prm = new Params();
                                prm.Timing = timing;
                                prm.Debug = debug;
                                prm.LogPath = logpath;
                                prm.LogClearPeriod = logclearperiod;
                                prm.DaysBefore = 0;
                                prm.DaysAfter = 0;
                                prm.kst_lst = new List<double>();
                                prm.kst_lst2 = new List<double>();

                                for (int i = 0; i < a.ChildNodes.Count; i++)
                                {
                                    XmlNode a2 = a.ChildNodes[i];
                                    if (a2.Name == "PoolNr")
                                    {
                                        prm.pool_nr = Convert.ToDouble(a2.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                                    }
                                    if (a2.Name == "RouteNr")
                                    {
                                        prm.tour_nr = Convert.ToInt32(a2.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                                    }
                                    if (a2.Name == "MainKstPred")
                                    {
                                        prm.MainKstPred = Convert.ToDouble(a2.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                                    }
                                    if (a2.Name == "DaysBefore")
                                    {
                                        prm.DaysBefore = Convert.ToInt32(a2.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                                    }
                                    if (a2.Name == "DaysAfter")
                                    {
                                        prm.DaysAfter = Convert.ToInt32(a2.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                                    }
                                    if (a2.Name == "HoursFrom")
                                    {
                                        prm.HoursFrom = Convert.ToInt32(a2.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                                    }
                                    if (a2.Name == "HoursTo")
                                    {
                                        prm.HoursTo = Convert.ToInt32(a2.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                                    }
                                    if (a2.Name == "Ksts")
                                    {
                                        foreach (XmlElement a3 in a2)
                                        {
                                            if (a3.Name == "OhlKor")
                                            {
                                                prm.OhlKor = Convert.ToDouble(a3.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                                            }
                                            if (a3.Name == "OhlSht")
                                            {
                                                prm.OhlSht = Convert.ToDouble(a3.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                                            }
                                            if (a3.Name == "ZamKor")
                                            {
                                                prm.ZamKor = Convert.ToDouble(a3.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                                            }
                                            if (a3.Name == "ZamSht")
                                            {
                                                prm.ZamSht = Convert.ToDouble(a3.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                                            }
                                            prm.kst_lst.Add(Convert.ToDouble(a3.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", "")));
                                            prm.kst_lst2.Add(Convert.ToDouble(a3.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", "")));
                                        }
                                    }
                                }
                                if(prm.tour_nr == null)
                                {
                                    prm.PredReserveRoute = PredReserveRoute;
                                    prm.tour_nr = prm.PredReserveRoute;
                                }
                                prm.verladedatum = Convert.ToInt32(DateTime.Now.AddDays(0).ToString("yyyyMMdd"));
                                prm.DaysBeforeS = Convert.ToInt32(DateTime.Now.AddDays(-prm.DaysBefore).ToString("yyyyMMdd"));
                                prm.DaysAfterS = Convert.ToInt32(DateTime.Now.AddDays(prm.DaysAfter).ToString("yyyyMMdd"));
                                prm.reserve_type = 1;
                                Thread thread = new Thread(() => TkRunReservation(prm));
                                thread.Start();
                            }
                            #endregion
                            #region Routes
                            if (a.Name == "Routes")
                            {
                                Params? prm = new Params();
                                var das = new List<double>() { };
                                prm.kst_lst = new List<double>() { };
                                prm.kst_lst2 = new List<double>() { };
                                prm.Timing = timing;
                                prm.Debug = debug;
                                prm.LogPath = logpath;
                                prm.LogClearPeriod = logclearperiod;
                                prm.DaysBefore = 0;
                                prm.DaysAfter = 0;
                                for (int i = 0; i < a.ChildNodes.Count; i++)
                                {
                                    XmlNode a2 = a.ChildNodes[i];
                                    if (a2.Name == "PoolNr")
                                    {
                                        prm.pool_nr = Convert.ToDouble(a2.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                                    }
                                    if (a2.Name == "RouteNr")
                                    {
                                        prm.tour_nr = Convert.ToInt32(a2.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                                    }
                                    if (a2.Name == "MainKstZam")
                                    {
                                        prm.MainKstZam = Convert.ToDouble(a2.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                                    }
                                    if (a2.Name == "MainKstOhl")
                                    {
                                        prm.MainKstOhl = Convert.ToDouble(a2.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                                    }

                                    if (a2.Name == "DaysBefore")
                                    {
                                        prm.DaysBefore = Convert.ToInt32(a2.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                                    }
                                    if (a2.Name == "DaysAfter")
                                    {
                                        prm.DaysAfter = Convert.ToInt32(a2.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                                    }
                                    if (a2.Name == "HoursFrom")
                                    {
                                        prm.HoursFrom = Convert.ToInt32(a2.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                                    }
                                    if (a2.Name == "HoursTo")
                                    {
                                        prm.HoursTo = Convert.ToInt32(a2.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                                    }
                                    if (a2.Name == "Ksts")
                                    {
                                        foreach (XmlElement a3 in a2)
                                        {
                                            if (a3.Name == "OhlKor")
                                            {
                                                prm.OhlKor = Convert.ToDouble(a3.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                                            }
                                            if (a3.Name == "OhlSht")
                                            {
                                                prm.OhlSht = Convert.ToDouble(a3.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                                            }
                                            if (a3.Name == "ZamKor")
                                            {
                                                prm.ZamKor = Convert.ToDouble(a3.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                                            }
                                            if (a3.Name == "ZamSht")
                                            {
                                                prm.ZamSht = Convert.ToDouble(a3.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", ""));
                                            }
                                            prm.kst_lst.Add(Convert.ToDouble(a3.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", "")));
                                            prm.kst_lst2.Add(Convert.ToDouble(a3.InnerText.Replace("\r", "").Replace("\n", "").Replace("\t", "")));
                                        }
                                    }
                                }
                                if(prm.tour_nr == null)
                                {
                                    prm.tour_nr = -1;
                                }
                                prm.PredReserveRoute = PredReserveRoute;
                                prm.DaysBeforeS = Convert.ToInt32(DateTime.Now.AddDays(-prm.DaysBefore).ToString("yyyyMMdd"));
                                prm.DaysAfterS = Convert.ToInt32(DateTime.Now.AddDays(prm.DaysAfter).ToString("yyyyMMdd"));
                                prm.reserve_type = 2;
                                Thread thread = new Thread(() => TkRunReservation(prm));
                                thread.Start();
                            }
                            #endregion
                        }
                    }
                }
            }
            #endregion
            
            await builder.RunConsoleAsync();
        }

        #region Background service worker
        public class MySpecialService : BackgroundService
        {
            private Params _prm = new Params();
            public MySpecialService(Params prm)
            {
                _prm = prm;
            }
            protected override async Task ExecuteAsync(CancellationToken stoppingToken)
            {
                RunReservation rr = new RunReservation();
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        if (Convert.ToInt32(DateTime.Now.ToString("HHmmss")) > _prm.HoursFrom && Convert.ToInt32(DateTime.Now.ToString("HHmmss")) < _prm.HoursTo)
                        {
                            string subPath = _prm.LogPath; // Your code goes here

                            bool exists = System.IO.Directory.Exists(_prm.LogPath);

                            if (!exists)
                            {
                                System.IO.Directory.CreateDirectory(_prm.LogPath);
                            }
                            string[] files = Directory.GetFiles(_prm.LogPath);
                            foreach (string file in files)
                            {
                                if (DateTime.Now - File.GetCreationTime(file) > TimeSpan.FromDays(_prm.LogClearPeriod))
                                {
                                    File.Delete(file);
                                }
                            }
                            await new RunReservation().MakeSnapShotFromDatabase(_prm);
                        }
                        else
                        {
                            Console.WriteLine($"Работа анализа {_prm.pool_nr} не началась. Запуск возможен с {_prm.HoursFrom} по {_prm.HoursTo}");
                            await rr.WriteToFile(_prm.LogPath + DateTime.Now.ToString("yyyy-MM-dd-HH-") + _prm.pool_nr + ".txt", $"{DateTime.Now}:"
                                + $" Работа анализа {_prm.pool_nr} не началась. Запуск возможен с {_prm.HoursFrom} по {_prm.HoursTo}"
                                + $" - {_prm.pool_nr + Environment.NewLine}");
                        }
                        await Task.Delay(TimeSpan.FromSeconds(_prm.Timing));
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                    }
                }
            }
        }
        #endregion

        #region TkRunReservation function fromThread
        static private async void TkRunReservation(Params prm)
        {
            var builder = new HostBuilder()
                .ConfigureAppConfiguration((hostingContext, config) =>
                {
                })
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddOptions();
                    services.AddSingleton(prm);
                    services.AddSingleton<IHostedService, MySpecialService>();
                });
            await builder.RunConsoleAsync();
        }
        #endregion
    }
}