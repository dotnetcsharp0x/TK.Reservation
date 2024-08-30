using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TK.Reservation.Models
{
    public class FA3901_00101
    {
        public double FA3901_BS_NR {  get; set; }
        public double FA3901_POOL_NR { get; set; }
        public double FA3901_FREIGABE { get; set; }
    }
}
