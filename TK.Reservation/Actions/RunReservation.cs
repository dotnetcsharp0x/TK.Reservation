using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TK.Reservation.Data;
using TK.Reservation.Models;

namespace TK.Reservation.Actions
{
    public class RunReservation : IDisposable
    {
        private bool debug_mode = true; // Если true, то записи в базу данных не будет
        private bool decrease = true;
        public RunReservation() { }
        public async Task WriteToFile(string path,string text)
        {
            await File.AppendAllTextAsync(path, text);
        }

        public async Task<double?> MakeSnapShotFromDatabase(Params? prm)
        {
            debug_mode = prm.Debug;
            #region CreateMainObject
            List<FA3901_00101>? FA3901 = new List<FA3901_00101>();
            List<FA0078_00112>? FA0078 = new List<FA0078_00112>();
            List<FA0077_00114>? FA0077 = new List<FA0077_00114>();
            List<LA0054_00107>? LA0054 = new List<LA0054_00107>();
            List<SY0012_00110>? SY0012 = new List<SY0012_00110>();
            List<SY8249_00104>? SY8249 = new List<SY8249_00104>();
            List<SY8081_00104>? SY8081 = new List<SY8081_00104>();
            List<LA2259_00103>? LA2259 = new List<LA2259_00103>();

            //List<double>? kst_lst = prm.kst_lst2;
            List<double>? kst_lst = [.. prm.kst_lst2];
            #endregion

            #region MakeSnapshotFromDatabase
            #region Dapper
            var builder = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory());
            builder.AddJsonFile("appsettings.json", optional: false);
            var configuration = builder.Build();
            var connectionString = configuration.GetConnectionString("csbContext").ToString();
            using (IDbConnection db = new OracleConnection(connectionString))
            {
                Console.WriteLine($"Создается слепок анализа 3901: {prm.pool_nr}");
                await WriteToFile(prm.LogPath + DateTime.Now.ToString("yyyy-MM-dd-HH-") + prm.pool_nr + ".txt", $"{DateTime.Now}: Создается слепок анализа 3901: {prm.pool_nr + Environment.NewLine}");
                FA3901 = db.Query<FA3901_00101>("SELECT FA3901_BS_NR, FA3901_POOL_NR, FA3901_FREIGABE FROM tkmiratorg.FA3901_00101 WHERE FA3901_POOL_NR = " + prm.pool_nr + " AND FA3901_FREIGABE = 1 AND FA3901_BS_NR = 128303").ToList();
            }
            #endregion
            if (FA3901.Count > 0)
            {
                #region EntityFrameworkCore
                await using (AppDbContext _context = new AppDbContext())
                {
                    prm.DaysBeforeS = Convert.ToInt32(DateTime.Now.AddDays(-prm.DaysBefore).ToString("yyyyMMdd"));
                    prm.DaysAfterS = Convert.ToInt32(DateTime.Now.AddDays(prm.DaysAfter).ToString("yyyyMMdd"));
                    Console.WriteLine($"Начинаю поиск заказов по дате от: {prm.DaysBeforeS} до даты: {prm.DaysAfterS}");
                    await WriteToFile(prm.LogPath + DateTime.Now.ToString("yyyy-MM-dd-HH-") + prm.pool_nr + ".txt", $"{DateTime.Now}: Начинаю поиск заказов по дате от: {prm.DaysBeforeS} до даты: {prm.DaysAfterS} - {prm.pool_nr + Environment.NewLine}");
                    //FA3901 = (from i in _context.FA3901_00101 
                    //                where i.FA3901_POOL_NR == prm.pool_nr && i.FA3901_FREIGABE == 1
                    //                select i).AsNoTracking().ToList();
                    Console.WriteLine("Создается слепок заказов 78");
                    await WriteToFile(prm.LogPath + DateTime.Now.ToString("yyyy-MM-dd-HH-") + prm.pool_nr + ".txt", $"{DateTime.Now}: Создается слепок заказов 78 - {prm.pool_nr + Environment.NewLine}");
                    List<double> list_main_kst = new List<double>();
                    if (prm.reserve_type == 1)
                    {
                        list_main_kst.Add(prm.MainKstPred);
                        FA0078 = (from i in _context.FA0078_00112
                                  where i.FA078_VERLADEDATUM >= prm.DaysBeforeS && i.FA078_VERLADEDATUM <= prm.DaysAfterS && i.FA078_TOUR_NR == prm.tour_nr && i.FA078_STATUS_2 != 60
                                  select i).Where(x => FA3901.Select(x => x.FA3901_BS_NR).Contains(x.FA078_BS_NR)).Where(x => list_main_kst.Contains(x.FA078_VERS_ST)).AsNoTracking().ToList();
                    }
                    else
                    {
                        list_main_kst.Add(prm.MainKstZam);
                        list_main_kst.Add(prm.MainKstOhl);
                        if (prm.tour_nr == -1)
                        {
                            Console.WriteLine($"Маршрут не указан. Ищу все заказы по маршруту кроме маршрута {prm.PredReserveRoute}");
                            await WriteToFile(prm.LogPath + DateTime.Now.ToString("yyyy-MM-dd-HH-") + prm.pool_nr + ".txt", $"{DateTime.Now}:"
                                +$" Маршрут не указан. Ищу все заказы по маршруту кроме маршрута {prm.PredReserveRoute}"
                                +$" - {prm.pool_nr + Environment.NewLine}");
                            FA0078 = (from i in _context.FA0078_00112
                                      where i.FA078_VERLADEDATUM >= prm.DaysBeforeS && i.FA078_VERLADEDATUM <= prm.DaysAfterS && i.FA078_STATUS_2 != 60 && i.FA078_TOUR_NR != prm.PredReserveRoute
                                      select i).Where(x => FA3901.Select(x => x.FA3901_BS_NR).Contains(x.FA078_BS_NR)).Where(x => list_main_kst.Contains(x.FA078_VERS_ST)).AsNoTracking().ToList();
                        }
                        else
                        {
                            Console.WriteLine($"Маршрут указан. Ищу все заказы по маршруту {prm.tour_nr}");
                            await WriteToFile(prm.LogPath + DateTime.Now.ToString("yyyy-MM-dd-HH-") + prm.pool_nr + ".txt", $"{DateTime.Now}:"
                                + $" Маршрут указан. Ищу все заказы по маршруту {prm.tour_nr}"
                                + $" - {prm.pool_nr + Environment.NewLine}");
                            FA0078 = (from i in _context.FA0078_00112
                                      where i.FA078_VERLADEDATUM >= prm.DaysBeforeS && i.FA078_VERLADEDATUM <= prm.DaysAfterS && i.FA078_STATUS_2 != 60 && i.FA078_TOUR_NR == prm.tour_nr
                                      select i).Where(x => FA3901.Select(x => x.FA3901_BS_NR).Contains(x.FA078_BS_NR)).Where(x => list_main_kst.Contains(x.FA078_VERS_ST)).AsNoTracking().ToList();
                        }
                    }
                    List<LA2259_00103> cust = new List<LA2259_00103>();
                    if (FA0078.Count > 0)
                    {
                        Console.WriteLine("Создается слепок артикулов 77");
                        await WriteToFile(prm.LogPath + DateTime.Now.ToString("yyyy-MM-dd-HH-") + prm.pool_nr + ".txt", $"{DateTime.Now}:"
                                + $" Создается слепок артикулов 77"
                                + $" - {prm.pool_nr + Environment.NewLine}");
                        FA0077 = (from i in _context.FA0077_00114
                                  select i).Where(x => FA0078.Select(x => x.FA078_BS_NR).Contains(x.FA077_BS_NR)).AsNoTracking().ToList();
                        Console.WriteLine("Создается слепок остатков 54");
                        await WriteToFile(prm.LogPath + DateTime.Now.ToString("yyyy-MM-dd-HH-") + prm.pool_nr + ".txt", $"{DateTime.Now}:"
                                + $" Создается слепок остатков 54"
                                + $" - {prm.pool_nr + Environment.NewLine}");
                        LA0054 = (from i in _context.LA0054_00107
                                  where i.LA0054_ETAGE != 99 && i.LA0054_LA_STATUS < 2
                                  select i).Where(x => FA0077.Select(x => x.FA077_ART_NR).Contains(x.LA0054_ART_NR)).Where(x => kst_lst.Contains(x.LA0054_KST_NR)).AsNoTracking().ToList();
                        Console.WriteLine("Создается слепок остатков 12");
                        await WriteToFile(prm.LogPath + DateTime.Now.ToString("yyyy-MM-dd-HH-") + prm.pool_nr + ".txt", $"{DateTime.Now}:"
                                + $" Создается слепок остатков 12"
                                + $" - {prm.pool_nr + Environment.NewLine}");
                        SY0012 = (from i in _context.SY0012_00110
                                  select i).Where(x => FA0077.Select(x => x.FA077_ART_NR).Contains(x.SY0012_NR)).AsNoTracking().ToList();
                        Console.WriteLine("Создается слепок остатков 8249");
                        await WriteToFile(prm.LogPath + DateTime.Now.ToString("yyyy-MM-dd-HH-") + prm.pool_nr + ".txt", $"{DateTime.Now}:"
                                + $" Создается слепок остатков 8249"
                                + $" - {prm.pool_nr + Environment.NewLine}");
                        SY8249 = (from i in _context.SY8249_00104
                                  where i.SY8249_EINHEIT == "вл"
                                  select i).Where(x => FA0077.Select(x => x.FA077_ART_NR).Contains(x.SY8249_ART_NR)).AsNoTracking().ToList();
                        Console.WriteLine("Создается слепок 8081");
                        await WriteToFile(prm.LogPath + DateTime.Now.ToString("yyyy-MM-dd-HH-") + prm.pool_nr + ".txt", $"{DateTime.Now}:"
                                + $" Создается слепок 8081"
                                + $" - {prm.pool_nr + Environment.NewLine}");
                        SY8081 = (from i in _context.SY8081_00104
                                  where i.SY8081_SORTGR_TITEL == 1
                                  select i).Where(x => FA0077.Select(x => x.FA077_ART_NR).Contains(x.SY8081_ART_NR)).AsNoTracking().ToList();
                        Console.WriteLine("Получение данных для удаления прошлых резервов");
                        await WriteToFile(prm.LogPath + DateTime.Now.ToString("yyyy-MM-dd-HH-") + prm.pool_nr + ".txt", $"{DateTime.Now}:"
                                + $" Получение данных для удаления прошлых резервов"
                                + $" - {prm.pool_nr + Environment.NewLine}");
                        cust = (from i in _context.LA2259_00103 select i).Where(x => FA0077.Select(d => d.FA077_BS_NR).Contains(x.LA2259_AUFTRAG)).AsNoTracking().ToList();
                    }
                    else
                    {
                        Console.WriteLine($"Задания по маршрутам для анализа {prm.pool_nr} не найдены!");
                        await WriteToFile(prm.LogPath + DateTime.Now.ToString("yyyy-MM-dd-HH-") + prm.pool_nr + ".txt", $"{DateTime.Now}:"
                                + $" Задания по маршрутам для анализа {prm.pool_nr} не найдены!"
                                + $" - {prm.pool_nr + Environment.NewLine}");
                    }
                    #endregion
                    #endregion

                    #region DeleteOldReserve
                    if (cust.Count() > 0)
                    {
                        foreach (var to_delete in cust)
                        {
                            Console.WriteLine($"Удаление прошлого резерва под заказ {to_delete.LA2259_AUFTRAG}");
                            await WriteToFile(prm.LogPath + DateTime.Now.ToString("yyyy-MM-dd-HH-") + prm.pool_nr + ".txt", $"{DateTime.Now}:"
                                + $" Удаление прошлого резерва под заказ {to_delete.LA2259_AUFTRAG}"
                                + $" - {prm.pool_nr + Environment.NewLine}");
                            _context.Remove(to_delete);
                        }
                    }
                    if (!debug_mode)
                    {
                        _context.SaveChanges();
                    }
                    #endregion

                    foreach (var fa078_bs_nr in FA0078)
                    {
                        foreach (var fa077_bs_nr in FA0077)
                        {
                            if (fa077_bs_nr.FA077_NR == 6447085)
                            {

                            }
                            
                            decrease = true;
                            var sy8081 = (from i in SY8081 where i.SY8081_ART_NR == fa077_bs_nr.FA077_ART_NR select i).FirstOrDefault();

                            #region ChangePositionLagerOnSortGrpParameters
                            if (sy8081 == null && fa078_bs_nr.FA078_VERS_ST == prm.MainKstPred || fa078_bs_nr.FA078_VERS_ST == prm.MainKstZam || fa078_bs_nr.FA078_VERS_ST == prm.MainKstOhl)
                            {
                                fa077_bs_nr.FA077_LAGER = prm.ZamKor;
                            }
                            else
                            {
                                if (sy8081.SY8081_SORTGR_NR == 5 && fa078_bs_nr.FA078_VERS_ST == prm.MainKstPred && fa077_bs_nr.FA077_LS_RET_GRUND == 0)
                                {
                                    fa077_bs_nr.FA077_LAGER = prm.OhlSht;
                                }
                                if (sy8081.SY8081_SORTGR_NR == 8 && fa078_bs_nr.FA078_VERS_ST == prm.MainKstPred && fa077_bs_nr.FA077_LS_RET_GRUND == 0)
                                {
                                    fa077_bs_nr.FA077_LAGER = prm.OhlSht;
                                }
                                if (sy8081.SY8081_SORTGR_NR == 3 && fa078_bs_nr.FA078_VERS_ST == prm.MainKstPred && fa077_bs_nr.FA077_LS_RET_GRUND == 0)
                                {
                                    fa077_bs_nr.FA077_LAGER = prm.ZamSht;
                                }
                                if (sy8081.SY8081_SORTGR_NR == 5 && fa078_bs_nr.FA078_VERS_ST == prm.MainKstPred && fa077_bs_nr.FA077_LS_RET_GRUND == 1)
                                {
                                    fa077_bs_nr.FA077_LAGER = prm.OhlKor;
                                }
                                if (sy8081.SY8081_SORTGR_NR == 3 && fa078_bs_nr.FA078_VERS_ST == prm.MainKstPred && fa077_bs_nr.FA077_LS_RET_GRUND == 1)
                                {
                                    fa077_bs_nr.FA077_LAGER = prm.ZamKor;
                                }
                                if (sy8081.SY8081_SORTGR_NR == 8 && fa078_bs_nr.FA078_VERS_ST == prm.MainKstPred && fa077_bs_nr.FA077_LS_RET_GRUND == 1)
                                {
                                    fa077_bs_nr.FA077_LAGER = prm.OhlKor;
                                }
                            }
                            #endregion

                            if (fa077_bs_nr.FA077_LS_MENGE > 0 && fa077_bs_nr.FA077_ZURUECK == 0)
                            {

                            }
                            else
                            {
                                if (fa077_bs_nr.FA077_BS_NR == fa078_bs_nr.FA078_BS_NR)
                                {
                                    double kst_find = 0;
                                    double kst_find_kr = 0;

                                    #region ChangePositionLagerOnTypeOfCollection
                                    if (prm.reserve_type == 2)
                                    {
                                        if (fa078_bs_nr.FA078_VERS_ST == prm.MainKstOhl && fa077_bs_nr.FA077_LS_RET_GRUND == 1)
                                        {
                                            kst_find = prm.OhlKor;
                                            kst_find_kr = prm.OhlKor;
                                        }
                                        if (fa078_bs_nr.FA078_VERS_ST == prm.MainKstZam && fa077_bs_nr.FA077_LS_RET_GRUND == 1)
                                        {
                                            kst_find = prm.ZamKor;
                                            kst_find_kr = prm.ZamKor;
                                        }
                                        if (fa078_bs_nr.FA078_VERS_ST == prm.MainKstOhl && fa077_bs_nr.FA077_LS_RET_GRUND == 0)
                                        {
                                            kst_find = prm.OhlSht;
                                            kst_find_kr = prm.OhlKor;
                                        }
                                        if (fa078_bs_nr.FA078_VERS_ST == prm.MainKstZam && fa077_bs_nr.FA077_LS_RET_GRUND == 0)
                                        {
                                            kst_find = prm.ZamSht;
                                            kst_find_kr = prm.ZamKor;
                                        }
                                    }
                                    if (prm.reserve_type == 1)
                                    {
                                        if (fa078_bs_nr.FA078_VERS_ST == prm.MainKstPred && fa077_bs_nr.FA077_LAGER == prm.OhlSht)
                                        {
                                            kst_find = fa077_bs_nr.FA077_LAGER;
                                            kst_find_kr = prm.OhlKor;
                                        }
                                        if (fa078_bs_nr.FA078_VERS_ST == prm.MainKstPred && fa077_bs_nr.FA077_LAGER == prm.ZamSht)
                                        {
                                            kst_find = fa077_bs_nr.FA077_LAGER;
                                            kst_find_kr = prm.ZamKor;
                                        }
                                        if (fa078_bs_nr.FA078_VERS_ST == prm.MainKstPred && fa077_bs_nr.FA077_LAGER == prm.ZamKor)
                                        {
                                            kst_find = fa077_bs_nr.FA077_LAGER;
                                            kst_find_kr = prm.ZamKor;
                                        }
                                        if (fa078_bs_nr.FA078_VERS_ST == prm.MainKstPred && fa077_bs_nr.FA077_LAGER == prm.OhlKor)
                                        {
                                            kst_find = fa077_bs_nr.FA077_LAGER;
                                            kst_find_kr = prm.OhlKor;
                                        }
                                    }
                                    #endregion


                                    double KAT_GRP_NR = 0;
                                    if (fa077_bs_nr.FA077_KAT_GRP_NR == 19000101) // IF max_date == 0 THEN default date ELSE date
                                    {
                                        KAT_GRP_NR = 99999999;
                                    }
                                    else
                                    {
                                        KAT_GRP_NR = fa077_bs_nr.FA077_KAT_GRP_NR;
                                    }
                                find_rest:
                                    List<double> ksts_to_find = [..prm.kst_lst];
                                    if (fa077_bs_nr.FA077_LS_RET_GRUND == 1)
                                    {
                                        ksts_to_find.Remove(prm.OhlSht);
                                        ksts_to_find.Remove(prm.ZamSht);
                                    }
                                    else
                                    {
                                        ksts_to_find.Add(prm.OhlSht);
                                        ksts_to_find.Add(prm.ZamSht);
                                    }
                                    if (fa077_bs_nr.FA077_NR == 2533697)
                                    {

                                    }
                                    if (fa077_bs_nr.FA077_ART_NR == 1010026900)
                                    {

                                    }
                                    Console.WriteLine($"Поиск остатков для позиции {fa078_bs_nr.FA078_BS_NR}-{fa077_bs_nr.FA077_ART_NR}-{fa077_bs_nr.FA077_NR}-{fa077_bs_nr.FA077_HBK_DATUM}-{KAT_GRP_NR}");
                                    await WriteToFile(prm.LogPath + DateTime.Now.ToString("yyyy-MM-dd-HH-") + prm.pool_nr + ".txt", $"{DateTime.Now}:"
                                + $" Поиск остатков для позиции {fa078_bs_nr.FA078_BS_NR}-{fa077_bs_nr.FA077_ART_NR}-{fa077_bs_nr.FA077_NR}-{fa077_bs_nr.FA077_HBK_DATUM}-{KAT_GRP_NR}"
                                + $" - {prm.pool_nr + Environment.NewLine}");
                                    if (fa078_bs_nr.FA078_BS_NR == 8026128795)
                                    {
                                        Console.WriteLine(fa078_bs_nr.FA078_BS_NR);
                                    }
                                    var t = (from i in LA0054 where i.LA0054_ART_NR == fa077_bs_nr.FA077_ART_NR select i);
                                    //var resp_la0054_2 = (from i in LA0054
                                    //                   where i.LA0054_ART_NR == fa077_bs_nr.FA077_ART_NR
                                    //                   && i.LA0054_LAGERTYP_DATUM >= fa077_bs_nr.FA077_HBK_DATUM
                                    //                   && i.LA0054_LAGERTYP_DATUM <= KAT_GRP_NR
                                    //                   orderby i.LA0054_LAGERTYP_DATUM ascending, i.LA0054_KST_NR descending, i.LA0054_MENGE_LE ascending, i.LA0054_PRIO ascending
                                    //                   select i).Where(x => ksts_to_find.Contains(x.LA0054_KST_NR))
                                                 
                                    //                   ;
                                    var resp_la0054 = (from i in LA0054
                                                       where i.LA0054_ART_NR == fa077_bs_nr.FA077_ART_NR
                                                       && i.LA0054_LAGERTYP_DATUM >= fa077_bs_nr.FA077_HBK_DATUM
                                                       && i.LA0054_LAGERTYP_DATUM <= KAT_GRP_NR
                                                       orderby i.LA0054_LAGERTYP_DATUM ascending, i.LA0054_KST_NR descending, i.LA0054_MENGE_LE ascending, i.LA0054_PRIO ascending
                                                       select i).Where(x => ksts_to_find.Contains(x.LA0054_KST_NR))
                                                       //.OrderBy(x=>x.LA0054_LAGERTYP_DATUM)
                                                       //.OrderByDescending(x=>x.LA0054_KST_NR)
                                                       //.OrderBy(x => x.LA0054_MENGE_LE)
                                                       //.OrderBy(x => x.LA0054_PRIO)
                                                       .FirstOrDefault()
                                                       ;
                                    
                                    if (resp_la0054 != null)
                                    {
                                        if(resp_la0054.LA0054_NVE == 332000000018207714)
                                        {
                                            Console.WriteLine("332000000018207714");
                                        }
                                        if(resp_la0054.LA0054_LAGERPLATZ == 20244)
                                        {
                                            Console.WriteLine("332000000018207714");
                                        }
                                        double kontrakt = 0;
                                        if (resp_la0054.LA0054_GANG < 2 && fa077_bs_nr.FA077_LS_RET_GRUND == 1)
                                        {
                                            kontrakt = Convert.ToDouble(fa078_bs_nr.FA078_TOUR_NR.ToString() + 5.ToString());
                                        }
                                        if (resp_la0054.LA0054_KST_NR != prm.OhlSht && resp_la0054.LA0054_KST_NR != prm.ZamSht && fa077_bs_nr.FA077_LS_RET_GRUND == 0)
                                        {
                                            kontrakt = Convert.ToDouble(fa078_bs_nr.FA078_TOUR_NR.ToString() + 1.ToString());
                                        }
                                        //-----------------------[РЕЗЕРВИРОВАНИЕ]-------------------------------------

                                        #region IF ek == SE
                                        //-----------------------[ЕСЛИ ЗАКАЗ В СКЛАДСКИХ ЕДИНИЦАХ]--------------------
                                        LA2259_00103 la2259 = new LA2259_00103();
                                        if (fa077_bs_nr.FA077_BE_KZ == 2)
                                        {
                                            if (decrease)
                                            {
                                                fa077_bs_nr.FA077_BS_MENGE = fa077_bs_nr.FA077_BS_MENGE - fa077_bs_nr.FA077_LA_MENGE; // От объема заказа отнимаем отгруженный объем
                                            }

                                            #region IF bin quantity less or equal position order quantity
                                            //------------------------[ЕСЛИ В ЯЧЕЙКЕ ОБЪЕМ МЕНЬШЕ ИЛИ РАВЕН ЗАКАЗУ]------------
                                            if (resp_la0054.LA0054_MENGE_LE <= fa077_bs_nr.FA077_BS_MENGE && fa077_bs_nr.FA077_BS_MENGE > 0 && resp_la0054.LA0054_ART_NR > 0)
                                            {
                                                Console.WriteLine($"[В ячейке меньше или равен заказу. Заказ в СЕ]: Резервирование заказа {fa078_bs_nr.FA078_BS_NR}, артикула {fa077_bs_nr.FA077_ART_NR}-{fa077_bs_nr.FA077_NR}. Объем СЕ: {resp_la0054.LA0054_MENGE_LE}, объем ЦЕ: {resp_la0054.LA0054_MENGE_PE}. Контракт: {kontrakt}. Склад: {resp_la0054.LA0054_KST_NR}. Ячейка: {resp_la0054.LA0054_LAGERPLATZ}");
                                                await WriteToFile(prm.LogPath + DateTime.Now.ToString("yyyy-MM-dd-HH-") + prm.pool_nr + ".txt", $"{DateTime.Now}:"
                                + $" [В ячейке меньше или равен заказу. Заказ в СЕ]: Резервирование заказа {fa078_bs_nr.FA078_BS_NR}, артикула {fa077_bs_nr.FA077_ART_NR}-{fa077_bs_nr.FA077_NR}. Объем СЕ: {resp_la0054.LA0054_MENGE_LE}, объем ЦЕ: {resp_la0054.LA0054_MENGE_PE}. Контракт: {kontrakt}. Склад: {resp_la0054.LA0054_KST_NR}. Ячейка: {resp_la0054.LA0054_LAGERPLATZ}"
                                + $" - {prm.pool_nr + Environment.NewLine}");
                                                la2259.LA2259_KST_NR = resp_la0054.LA0054_KST_NR;
                                                la2259.LA2259_ART_NR = resp_la0054.LA0054_ART_NR;
                                                la2259.LA2259_LAGERTYP_NR = resp_la0054.LA0054_LAGERTYP_NR;
                                                la2259.LA2259_LAGERTYP_DATUM = resp_la0054.LA0054_LAGERTYP_DATUM;
                                                la2259.LA2259_LAGERTYP_LOS = resp_la0054.LA0054_LAGERTYP_LOS;
                                                la2259.LA2259_LAGERPLATZ = resp_la0054.LA0054_LAGERPLATZ;
                                                la2259.LA2259_LAGERPLATZ_SUB = resp_la0054.LA0054_LAGERPLATZ_SUB;
                                                la2259.LA2259_NVE = resp_la0054.LA0054_NVE;
                                                la2259.LA2259_REF_NR = resp_la0054.LA0054_REF_NR;
                                                la2259.LA2259_HERKUNFT = 1;
                                                la2259.LA2259_KUNDE = fa077_bs_nr.FA077_ADR_NR;
                                                la2259.LA2259_AUFTRAG = fa077_bs_nr.FA077_BS_NR;
                                                la2259.LA2259_KONTRAKT = kontrakt;
                                                la2259.LA2259_POSTEN_NR = fa077_bs_nr.FA077_NR;
                                                la2259.LA2259_ANL_DATUM = 20231019;
                                                la2259.LA2259_ANL_USER = 8900;
                                                la2259.LA2259_ANL_STATION = 26;
                                                la2259.LA2259_ANL_PROG = 2258;
                                                la2259.LA2259_ANL_FKT = 0;
                                                la2259.LA2259_UPD_DATUM = 20231019;
                                                la2259.LA2259_UPD_USER = 8900;
                                                la2259.LA2259_UPD_STATION = 26;
                                                la2259.LA2259_UPD_PROG = 2258;
                                                la2259.LA2259_UPD_FKT = 0;
                                                la2259.LA2259_VATER_PROGRAMM = 3130;
                                                la2259.LA2259_KONTO_ID = 0;
                                                la2259.LA2259_KONTO_INV = 0;
                                                la2259.LA2259_RESERV_MENGE_LE = resp_la0054.LA0054_MENGE_LE;
                                                la2259.LA2259_RESERV_MENGE_PE = resp_la0054.LA0054_MENGE_PE;
                                                la2259.LA2259_RESERV_TARA = 0;
                                                la2259.LA2259_RESERV_ABW_MENGE_LE = 0;
                                                la2259.LA2259_RESERV_ABW_EINHEIT = "-";
                                                la2259.LA2259_RESERV_DATUM = 200101+resp_la0054.LA0054_ETAGE;
                                                la2259.LA2259_RESERV_ZEIT = 105050;
                                                la2259.LA2259_LIEFERDATUM = fa078_bs_nr.FA078_VERLADEDATUM;
                                                la2259.LA2259_PLAN_DATUM = 0;
                                                la2259.LA2259_TOUR_NR = fa078_bs_nr.FA078_TOUR_NR;
                                                la2259.LA2259_EMPFST_NR = 0;
                                                la2259.LA2259_ST3810_LFD_NR = 0;
                                                la2259.LA2259_VIRTU_BESTAND = 0;
                                                la2259.LA2259_GEDRUCKT = 0;
                                                la2259.LA2259_U_ART_NR = 0;
                                                la2259.LA2259_RESERV_MODUS = 0;
                                                la2259.LA2259_RESERV_ANTEIL = 0;
                                                la2259.LA2259_EINGABE_MENGE = 0;
                                                la2259.LA2259_AUS_KONTRAKT = 0;
                                                la2259.LA2259_USE_SOLL_LA_STAT = 0;

                                                //-----------------------------[УДАЛЯЕМ ЯЧЕЙКУ ЕСЛИ ЗАРЕЗЕРВИРОВАЛИ ЕЕ ПОЛНОСТЬЮ]--------------------
                                                resp_la0054.LA0054_ART_NR = 0;
                                                if (resp_la0054.LA0054_MENGE_LE < fa077_bs_nr.FA077_BS_MENGE)
                                                {
                                                    fa077_bs_nr.FA077_BS_MENGE = fa077_bs_nr.FA077_BS_MENGE - resp_la0054.LA0054_MENGE_LE;
                                                    decrease = false;
                                                }
                                                if (resp_la0054.LA0054_MENGE_LE == fa077_bs_nr.FA077_BS_MENGE)
                                                {
                                                    fa077_bs_nr.FA077_BS_MENGE = 0;
                                                }
                                            }
                                            #endregion
                                            #region IF bin quantity greater than position order quantity
                                            //------------------------[ЕСЛИ В ЯЧЕЙКЕ ОБЪЕМ БОЛЬШЕ ЗАКАЗА]----------------------
                                            if (resp_la0054.LA0054_MENGE_LE > fa077_bs_nr.FA077_BS_MENGE && fa077_bs_nr.FA077_BS_MENGE > 0 && resp_la0054.LA0054_ART_NR > 0)
                                            {
                                                Console.WriteLine($"[В ячейке больше заказа. Заказ в СЕ]: Резервирование заказа {fa078_bs_nr.FA078_BS_NR}, артикула {fa077_bs_nr.FA077_ART_NR}-{fa077_bs_nr.FA077_NR}. Объем СЕ: {fa077_bs_nr.FA077_BS_MENGE}, объем ЦЕ: {fa077_bs_nr.FA077_BS_MENGE * fa077_bs_nr.FA077_UMR_FAKTOR_3}. Контракт: {kontrakt}. Склад: {resp_la0054.LA0054_KST_NR}. Ячейка: {resp_la0054.LA0054_LAGERPLATZ}");
                                                await WriteToFile(prm.LogPath + DateTime.Now.ToString("yyyy-MM-dd-HH-") + prm.pool_nr + ".txt", $"{DateTime.Now}:"
                                + $" [В ячейке больше заказа. Заказ в СЕ]: Резервирование заказа {fa078_bs_nr.FA078_BS_NR}, артикула {fa077_bs_nr.FA077_ART_NR}-{fa077_bs_nr.FA077_NR}. Объем СЕ: {fa077_bs_nr.FA077_BS_MENGE}, объем ЦЕ: {fa077_bs_nr.FA077_BS_MENGE * fa077_bs_nr.FA077_UMR_FAKTOR_3}. Контракт: {kontrakt}. Склад: {resp_la0054.LA0054_KST_NR}. Ячейка: {resp_la0054.LA0054_LAGERPLATZ}"
                                + $" - {prm.pool_nr + Environment.NewLine}");
                                                la2259.LA2259_KST_NR = resp_la0054.LA0054_KST_NR;
                                                la2259.LA2259_ART_NR = resp_la0054.LA0054_ART_NR;
                                                la2259.LA2259_LAGERTYP_NR = resp_la0054.LA0054_LAGERTYP_NR;
                                                la2259.LA2259_LAGERTYP_DATUM = resp_la0054.LA0054_LAGERTYP_DATUM;
                                                la2259.LA2259_LAGERTYP_LOS = resp_la0054.LA0054_LAGERTYP_LOS;
                                                la2259.LA2259_LAGERPLATZ = resp_la0054.LA0054_LAGERPLATZ;
                                                la2259.LA2259_LAGERPLATZ_SUB = resp_la0054.LA0054_LAGERPLATZ_SUB;
                                                la2259.LA2259_NVE = resp_la0054.LA0054_NVE;
                                                la2259.LA2259_REF_NR = resp_la0054.LA0054_REF_NR;
                                                la2259.LA2259_HERKUNFT = 1;
                                                la2259.LA2259_KUNDE = fa077_bs_nr.FA077_ADR_NR;
                                                la2259.LA2259_AUFTRAG = fa077_bs_nr.FA077_BS_NR;
                                                la2259.LA2259_KONTRAKT = kontrakt;
                                                la2259.LA2259_POSTEN_NR = fa077_bs_nr.FA077_NR;
                                                la2259.LA2259_ANL_DATUM = 20231019;
                                                la2259.LA2259_ANL_USER = 8900;
                                                la2259.LA2259_ANL_STATION = 26;
                                                la2259.LA2259_ANL_PROG = 2258;
                                                la2259.LA2259_ANL_FKT = 0;
                                                la2259.LA2259_UPD_DATUM = 20231019;
                                                la2259.LA2259_UPD_USER = 8900;
                                                la2259.LA2259_UPD_STATION = 26;
                                                la2259.LA2259_UPD_PROG = 2258;
                                                la2259.LA2259_UPD_FKT = 0;
                                                la2259.LA2259_VATER_PROGRAMM = 3130;
                                                la2259.LA2259_KONTO_ID = 0;
                                                la2259.LA2259_KONTO_INV = 0;
                                                la2259.LA2259_RESERV_MENGE_LE = fa077_bs_nr.FA077_BS_MENGE;
                                                la2259.LA2259_RESERV_MENGE_PE = fa077_bs_nr.FA077_BS_MENGE * fa077_bs_nr.FA077_UMR_FAKTOR_3;
                                                la2259.LA2259_RESERV_TARA = 0;
                                                la2259.LA2259_RESERV_ABW_MENGE_LE = 0;
                                                la2259.LA2259_RESERV_ABW_EINHEIT = "-";
                                                la2259.LA2259_RESERV_DATUM = 200101 + resp_la0054.LA0054_ETAGE;
                                                la2259.LA2259_RESERV_ZEIT = 105050;
                                                la2259.LA2259_LIEFERDATUM = fa078_bs_nr.FA078_VERLADEDATUM;
                                                la2259.LA2259_PLAN_DATUM = 0;
                                                la2259.LA2259_TOUR_NR = fa078_bs_nr.FA078_TOUR_NR;
                                                la2259.LA2259_EMPFST_NR = 0;
                                                la2259.LA2259_ST3810_LFD_NR = 0;
                                                la2259.LA2259_VIRTU_BESTAND = 0;
                                                la2259.LA2259_GEDRUCKT = 0;
                                                la2259.LA2259_U_ART_NR = 0;
                                                la2259.LA2259_RESERV_MODUS = 0;
                                                la2259.LA2259_RESERV_ANTEIL = 0;
                                                la2259.LA2259_EINGABE_MENGE = 0;
                                                la2259.LA2259_AUS_KONTRAKT = 0;
                                                la2259.LA2259_USE_SOLL_LA_STAT = 0;

                                                //----------------------------- [ОБНОВЛЯЕМ ВЕС В ЯЧЕЙКЕ]--------------------
                                                resp_la0054.LA0054_MENGE_PE = resp_la0054.LA0054_MENGE_PE - (fa077_bs_nr.FA077_BS_MENGE * fa077_bs_nr.FA077_UMR_FAKTOR_3);
                                                resp_la0054.LA0054_MENGE_LE = resp_la0054.LA0054_MENGE_LE - fa077_bs_nr.FA077_BS_MENGE;
                                                fa077_bs_nr.FA077_BS_MENGE = 0;
                                            }
                                            #endregion
                                        }
                                        #endregion
                                        #region IF ek == CE
                                        //-----------------------[ЕСЛИ ЗАКАЗ В ЦЕНОВЫХ ЕДИНИЦАХ]--------------------
                                        if (fa077_bs_nr.FA077_BE_KZ == 1 || fa077_bs_nr.FA077_BE_KZ == 3)
                                        {
                                            if (fa077_bs_nr.FA077_ME_1 == "кг")
                                            {
                                                if (fa077_bs_nr.FA077_LS_MENGE > 0)
                                                {
                                                    fa077_bs_nr.FA077_BS_MENGE = fa077_bs_nr.FA077_BS_MENGE - fa077_bs_nr.FA077_KG_MENGE;
                                                    decrease = false;
                                                }
                                            }
                                            else
                                            {
                                                fa077_bs_nr.FA077_BS_MENGE = fa077_bs_nr.FA077_BS_MENGE - fa077_bs_nr.FA077_LS_MENGE;
                                            }
                                            double resp_minus = 0;
                                            #region IF bin quantity less or equal position order quantity
                                            var sy0012_ek = (from i in SY0012 where i.SY0012_NR == fa077_bs_nr.FA077_ART_NR select i).FirstOrDefault();
                                             var sy8429_ek = (from i in SY8249 where i.SY8249_ART_NR == fa077_bs_nr.FA077_ART_NR select i).FirstOrDefault();
                                            if (sy0012_ek != null && sy8429_ek != null)
                                            {
                                                resp_minus = sy0012_ek.SY0012_LA_KGME / 2;
                                                if (fa077_bs_nr.FA077_LS_RET_GRUND == 0 && fa077_bs_nr.FA077_ME_1 == "кг")
                                                {
                                                    resp_minus = (sy0012_ek.SY0012_LA_KGME / sy8429_ek.SY8249_EK_MEBE) / 2;
                                                }
                                                if (fa077_bs_nr.FA077_ME_1 != "кг")
                                                {
                                                    resp_minus = 0;
                                                }
                                            }
                                            //------------------------[ЕСЛИ В ЯЧЕЙКЕ ОБЪЕМ МЕНЬШЕ ИЛИ РАВЕН ЗАКАЗУ]------------
                                            if (resp_la0054.LA0054_MENGE_PE <= (fa077_bs_nr.FA077_BS_MENGE-resp_minus) && (fa077_bs_nr.FA077_BS_MENGE - resp_minus) > 0 && resp_la0054.LA0054_ART_NR > 0)
                                            {
                                                Console.WriteLine($"[В ячейке меньше или равен заказу. Заказ в ЦЕ]: Резервирование заказа {fa078_bs_nr.FA078_BS_NR}, артикула {fa077_bs_nr.FA077_ART_NR}-{fa077_bs_nr.FA077_NR}. Объем СЕ: {resp_la0054.LA0054_MENGE_LE}, объем ЦЕ: {resp_la0054.LA0054_MENGE_PE}. Контракт: {kontrakt}. Склад: {resp_la0054.LA0054_KST_NR}. Ячейка: {resp_la0054.LA0054_LAGERPLATZ}");
                                                await WriteToFile(prm.LogPath + DateTime.Now.ToString("yyyy-MM-dd-HH-") + prm.pool_nr + ".txt", $"{DateTime.Now}:"
                                + $" [В ячейке меньше или равен заказу. Заказ в ЦЕ]: Резервирование заказа {fa078_bs_nr.FA078_BS_NR}, артикула {fa077_bs_nr.FA077_ART_NR}-{fa077_bs_nr.FA077_NR}. Объем СЕ: {resp_la0054.LA0054_MENGE_LE}, объем ЦЕ: {resp_la0054.LA0054_MENGE_PE}. Контракт: {kontrakt}. Склад: {resp_la0054.LA0054_KST_NR}. Ячейка: {resp_la0054.LA0054_LAGERPLATZ}"
                                + $" - {prm.pool_nr + Environment.NewLine}");
                                                la2259.LA2259_KST_NR = resp_la0054.LA0054_KST_NR;
                                                la2259.LA2259_ART_NR = resp_la0054.LA0054_ART_NR;
                                                la2259.LA2259_LAGERTYP_NR = resp_la0054.LA0054_LAGERTYP_NR;
                                                la2259.LA2259_LAGERTYP_DATUM = resp_la0054.LA0054_LAGERTYP_DATUM;
                                                la2259.LA2259_LAGERTYP_LOS = resp_la0054.LA0054_LAGERTYP_LOS;
                                                la2259.LA2259_LAGERPLATZ = resp_la0054.LA0054_LAGERPLATZ;
                                                la2259.LA2259_LAGERPLATZ_SUB = resp_la0054.LA0054_LAGERPLATZ_SUB;
                                                la2259.LA2259_NVE = resp_la0054.LA0054_NVE;
                                                la2259.LA2259_REF_NR = resp_la0054.LA0054_REF_NR;
                                                la2259.LA2259_HERKUNFT = 1;
                                                la2259.LA2259_KUNDE = fa077_bs_nr.FA077_ADR_NR;
                                                la2259.LA2259_AUFTRAG = fa077_bs_nr.FA077_BS_NR;
                                                la2259.LA2259_KONTRAKT = kontrakt;
                                                la2259.LA2259_POSTEN_NR = fa077_bs_nr.FA077_NR;
                                                la2259.LA2259_ANL_DATUM = 20231019;
                                                la2259.LA2259_ANL_USER = 8900;
                                                la2259.LA2259_ANL_STATION = 26;
                                                la2259.LA2259_ANL_PROG = 2258;
                                                la2259.LA2259_ANL_FKT = 0;
                                                la2259.LA2259_UPD_DATUM = 20231019;
                                                la2259.LA2259_UPD_USER = 8900;
                                                la2259.LA2259_UPD_STATION = 26;
                                                la2259.LA2259_UPD_PROG = 2258;
                                                la2259.LA2259_UPD_FKT = 0;
                                                la2259.LA2259_VATER_PROGRAMM = 3130;
                                                la2259.LA2259_KONTO_ID = 0;
                                                la2259.LA2259_KONTO_INV = 0;
                                                la2259.LA2259_RESERV_MENGE_LE = resp_la0054.LA0054_MENGE_LE;
                                                la2259.LA2259_RESERV_MENGE_PE = resp_la0054.LA0054_MENGE_PE;
                                                la2259.LA2259_RESERV_TARA = 0;
                                                la2259.LA2259_RESERV_ABW_MENGE_LE = 0;
                                                la2259.LA2259_RESERV_ABW_EINHEIT = "-";
                                                la2259.LA2259_RESERV_DATUM = 200101 + resp_la0054.LA0054_ETAGE;
                                                la2259.LA2259_RESERV_ZEIT = 105050;
                                                la2259.LA2259_LIEFERDATUM = fa078_bs_nr.FA078_VERLADEDATUM;
                                                la2259.LA2259_PLAN_DATUM = 0;
                                                la2259.LA2259_TOUR_NR = fa078_bs_nr.FA078_TOUR_NR;
                                                la2259.LA2259_EMPFST_NR = 0;
                                                la2259.LA2259_ST3810_LFD_NR = 0;
                                                la2259.LA2259_VIRTU_BESTAND = 0;
                                                la2259.LA2259_GEDRUCKT = 0;
                                                la2259.LA2259_U_ART_NR = 0;
                                                la2259.LA2259_RESERV_MODUS = 0;
                                                la2259.LA2259_RESERV_ANTEIL = 0;
                                                la2259.LA2259_EINGABE_MENGE = 0;
                                                la2259.LA2259_AUS_KONTRAKT = 0;
                                                la2259.LA2259_USE_SOLL_LA_STAT = 0;

                                                //-----------------------------[УДАЛЯЕМ ЯЧЕЙКУ ЕСЛИ ЗАРЕЗЕРВИРОВАЛИ ЕЕ ПОЛНОСТЬЮ]--------------------
                                                resp_la0054.LA0054_ART_NR = 0;
                                                if (resp_la0054.LA0054_MENGE_PE < fa077_bs_nr.FA077_BS_MENGE)
                                                {
                                                    fa077_bs_nr.FA077_BS_MENGE = fa077_bs_nr.FA077_BS_MENGE - resp_la0054.LA0054_MENGE_PE;
                                                    decrease = false;
                                                }
                                                if (resp_la0054.LA0054_MENGE_PE == fa077_bs_nr.FA077_BS_MENGE)
                                                {
                                                    fa077_bs_nr.FA077_BS_MENGE = 0;
                                                }
                                            }
                                            #endregion
                                            #region IF bin quantity greater than position order quantity
                                            Console.WriteLine(resp_la0054.LA0054_MENGE_PE);
                                            Console.WriteLine(fa077_bs_nr.FA077_BS_MENGE - resp_minus);
                                            Console.WriteLine(fa077_bs_nr.FA077_BS_MENGE);
                                            Console.WriteLine(resp_minus);
                                            Console.WriteLine(resp_la0054.LA0054_ART_NR);
                                            //------------------------[ЕСЛИ В ЯЧЕЙКЕ ОБЪЕМ БОЛЬШЕ ЗАКАЗА]----------------------
                                            if (resp_la0054.LA0054_MENGE_PE > (fa077_bs_nr.FA077_BS_MENGE - resp_minus) && (fa077_bs_nr.FA077_BS_MENGE - resp_minus) > 0 && resp_la0054.LA0054_ART_NR > 0)
                                            {
                                                Console.WriteLine($"[В ячейке больше заказа. Заказ в ЦЕ]: Резервирование заказа {fa078_bs_nr.FA078_BS_NR}, артикула {fa077_bs_nr.FA077_ART_NR}-{fa077_bs_nr.FA077_NR}. Объем СЕ: {fa077_bs_nr.FA077_BS_MENGE / fa077_bs_nr.FA077_UMR_FAKTOR_3}, объем ЦЕ: {fa077_bs_nr.FA077_BS_MENGE}. Контракт: {kontrakt}. Склад: {resp_la0054.LA0054_KST_NR}. Ячейка: {resp_la0054.LA0054_LAGERPLATZ}");
                                                await WriteToFile(prm.LogPath + DateTime.Now.ToString("yyyy-MM-dd-HH-") + prm.pool_nr + ".txt", $"{DateTime.Now}:"
                                + $" [В ячейке больше заказа. Заказ в ЦЕ]: Резервирование заказа {fa078_bs_nr.FA078_BS_NR}, артикула {fa077_bs_nr.FA077_ART_NR}-{fa077_bs_nr.FA077_NR}. Объем СЕ: {fa077_bs_nr.FA077_BS_MENGE / fa077_bs_nr.FA077_UMR_FAKTOR_3}, объем ЦЕ: {fa077_bs_nr.FA077_BS_MENGE}. Контракт: {kontrakt}. Склад: {resp_la0054.LA0054_KST_NR}. Ячейка: {resp_la0054.LA0054_LAGERPLATZ}"
                                + $" - {prm.pool_nr + Environment.NewLine}");
                                                la2259.LA2259_KST_NR = resp_la0054.LA0054_KST_NR;
                                                la2259.LA2259_ART_NR = resp_la0054.LA0054_ART_NR;
                                                la2259.LA2259_LAGERTYP_NR = resp_la0054.LA0054_LAGERTYP_NR;
                                                la2259.LA2259_LAGERTYP_DATUM = resp_la0054.LA0054_LAGERTYP_DATUM;
                                                la2259.LA2259_LAGERTYP_LOS = resp_la0054.LA0054_LAGERTYP_LOS;
                                                la2259.LA2259_LAGERPLATZ = resp_la0054.LA0054_LAGERPLATZ;
                                                la2259.LA2259_LAGERPLATZ_SUB = resp_la0054.LA0054_LAGERPLATZ_SUB;
                                                la2259.LA2259_NVE = resp_la0054.LA0054_NVE;
                                                la2259.LA2259_REF_NR = resp_la0054.LA0054_REF_NR;
                                                la2259.LA2259_HERKUNFT = 1;
                                                la2259.LA2259_KUNDE = fa077_bs_nr.FA077_ADR_NR;
                                                la2259.LA2259_AUFTRAG = fa077_bs_nr.FA077_BS_NR;
                                                la2259.LA2259_KONTRAKT = kontrakt;
                                                la2259.LA2259_POSTEN_NR = fa077_bs_nr.FA077_NR;
                                                la2259.LA2259_ANL_DATUM = 20231019;
                                                la2259.LA2259_ANL_USER = 8900;
                                                la2259.LA2259_ANL_STATION = 26;
                                                la2259.LA2259_ANL_PROG = 2258;
                                                la2259.LA2259_ANL_FKT = 0;
                                                la2259.LA2259_UPD_DATUM = 20231019;
                                                la2259.LA2259_UPD_USER = 8900;
                                                la2259.LA2259_UPD_STATION = 26;
                                                la2259.LA2259_UPD_PROG = 2258;
                                                la2259.LA2259_UPD_FKT = 0;
                                                la2259.LA2259_VATER_PROGRAMM = 3130;
                                                la2259.LA2259_KONTO_ID = 0;
                                                la2259.LA2259_KONTO_INV = 0;
                                                la2259.LA2259_RESERV_MENGE_LE = fa077_bs_nr.FA077_BS_MENGE / fa077_bs_nr.FA077_UMR_FAKTOR_3;
                                                la2259.LA2259_RESERV_MENGE_PE = fa077_bs_nr.FA077_BS_MENGE;
                                                la2259.LA2259_RESERV_TARA = 0;
                                                la2259.LA2259_RESERV_ABW_MENGE_LE = 0;
                                                la2259.LA2259_RESERV_ABW_EINHEIT = "-";
                                                la2259.LA2259_RESERV_DATUM = 200101 + resp_la0054.LA0054_ETAGE;
                                                la2259.LA2259_RESERV_ZEIT = 105050;
                                                la2259.LA2259_LIEFERDATUM = fa078_bs_nr.FA078_VERLADEDATUM;
                                                la2259.LA2259_PLAN_DATUM = 0;
                                                la2259.LA2259_TOUR_NR = fa078_bs_nr.FA078_TOUR_NR;
                                                la2259.LA2259_EMPFST_NR = 0;
                                                la2259.LA2259_ST3810_LFD_NR = 0;
                                                la2259.LA2259_VIRTU_BESTAND = 0;
                                                la2259.LA2259_GEDRUCKT = 0;
                                                la2259.LA2259_U_ART_NR = 0;
                                                la2259.LA2259_RESERV_MODUS = 0;
                                                la2259.LA2259_RESERV_ANTEIL = 0;
                                                la2259.LA2259_EINGABE_MENGE = 0;
                                                la2259.LA2259_AUS_KONTRAKT = 0;
                                                la2259.LA2259_USE_SOLL_LA_STAT = 0;

                                                //----------------------------- [ОБНОВЛЯЕМ ВЕС В ЯЧЕЙКЕ]--------------------
                                                resp_la0054.LA0054_MENGE_PE = resp_la0054.LA0054_MENGE_PE - fa077_bs_nr.FA077_BS_MENGE;
                                                resp_la0054.LA0054_MENGE_LE = resp_la0054.LA0054_MENGE_LE - (fa077_bs_nr.FA077_BS_MENGE / fa077_bs_nr.FA077_UMR_FAKTOR_3);
                                                fa077_bs_nr.FA077_BS_MENGE = 0;
                                            }
                                            #endregion
                                        }
                                        #endregion

                                        if (la2259.LA2259_LAGERTYP_LOS == null)
                                        {
                                            la2259.LA2259_LAGERTYP_LOS = "";
                                        }
                                        if (la2259.LA2259_ART_NR > 0)
                                        {
                                            _context.Add(la2259);
                                        }
                                        else
                                        {
                                            fa077_bs_nr.FA077_BS_MENGE = 0;
                                        }
                                        //LA2259.Add(la2259);
                                        if (fa077_bs_nr.FA077_BS_MENGE > 0)
                                        {
                                            goto find_rest;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    #region Database save changes
                    if (!debug_mode)
                    {
                        _context.SaveChanges();
                    }
                    #endregion
                    if (prm.reserve_type == 1)
                    {
                        Console.WriteLine($"Шаг предрезерва завершен {DateTime.Now}");
                        await WriteToFile(prm.LogPath + DateTime.Now.ToString("yyyy-MM-dd-HH-") + prm.pool_nr + ".txt", $"{DateTime.Now}:"
                                + $" Шаг предрезерва завершен"
                                + $" - {prm.pool_nr + Environment.NewLine}");
                    }
                    else
                    {
                        Console.WriteLine($"Шаг резерва по маршрутам завершен {DateTime.Now}");
                        await WriteToFile(prm.LogPath + DateTime.Now.ToString("yyyy-MM-dd-HH-") + prm.pool_nr + ".txt", $"{DateTime.Now}:"
                                + $" Шаг резерва по маршрутам завершен"
                                + $" - {prm.pool_nr + Environment.NewLine}");
                    }
                }
                FA3901 = null;
                FA0078 = null;
                FA0077 = null;
                LA0054 = null;
                SY8081 = null;
                LA2259 = null;
                kst_lst = null;
                builder = null;
                configuration = null;
                return 1;
            }
            else
            {
                FA3901 = null;
                FA0078 = null;
                FA0077 = null;
                LA0054 = null;
                SY8081 = null;
                LA2259 = null;
                kst_lst = null;
                builder = null;
                configuration = null;
                Console.WriteLine($"По анализу {prm.pool_nr} не найдено заданий в 3901");
                await WriteToFile(prm.LogPath + DateTime.Now.ToString("yyyy-MM-dd-HH-") + prm.pool_nr + ".txt", $"{DateTime.Now}:"
                                + $" По анализу {prm.pool_nr} не найдено заданий в 3901"
                                + $" - {prm.pool_nr + Environment.NewLine}");
                return 0;
            }
        }

        #region Dispose
        public void Dispose()
        {
            
        }
        ~RunReservation()
        {

        }
        #endregion
    }
}
