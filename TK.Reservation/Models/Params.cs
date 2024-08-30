using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TK.Reservation.Models
{
    public class Params
    {
        public Params() {
            
        }
        public int verladedatum {  get; set; }
        public int? tour_nr { get; set; }
        public double pool_nr { get; set; }
        public List<double>? kst_lst {  get; set; }
        public List<double>? kst_lst2 {  get; set; }
        public double OhlKor { get; set; }
        public double OhlSht {  get; set; }
        public double ZamKor { get; set; }
        public double ZamSht { get; set; }
        public double MainKstZam {  get; set; }
        public double MainKstOhl { get; set; }
        public double MainKstPred { get; set; }
        public int reserve_type { get; set; }
        public int Timing { get; set; }
        public string? LogPath { get; set; }
        public int LogClearPeriod {  get; set; }
        public bool Debug {  get; set; }
        public int PredReserveRoute {  get; set; }
        public int DaysBefore {  get; set; }
        public int DaysBeforeS {  get; set; }
        public int DaysAfter {  get; set; }
        public int DaysAfterS {  get; set; }
        public int HoursFrom { get; set; }
        public int HoursTo { get; set; }
    }
}
