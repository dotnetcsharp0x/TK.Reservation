using TK.Reservation.Models;
using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;

namespace TK.Reservation.Data
{
    public class AppDbContext : DbContext
    {
        private string connectionString;
        public AppDbContext()
        {
            var builder = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory());
            builder.AddJsonFile("appsettings.json", optional: false);
            var configuration = builder.Build();
            connectionString = configuration.GetConnectionString("csbContext").ToString();
        }
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseOracle(connectionString);
        }
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<LA2259_00103>().HasKey(u => new { u.LA2259_KST_NR, u.LA2259_ART_NR, u.LA2259_LAGERTYP_NR,u.LA2259_LAGERTYP_DATUM,u.LA2259_LAGERTYP_LOS,u.LA2259_LAGERPLATZ,u.LA2259_LAGERPLATZ_SUB,u.LA2259_NVE,u.LA2259_REF_NR,u.LA2259_HERKUNFT,u.LA2259_KUNDE,u.LA2259_AUFTRAG,u.LA2259_KONTRAKT,u.LA2259_POSTEN_NR });
            modelBuilder.Entity<FA0078_00112>().HasNoKey();
            modelBuilder.Entity<FA3901_00101>().HasNoKey();
            modelBuilder.Entity<FA0077_00114>().HasNoKey();
            modelBuilder.Entity<LA0054_00107>().HasNoKey();
            modelBuilder.Entity<SY8081_00104>().HasNoKey();
            modelBuilder.Entity<SY0012_00110>().HasNoKey();
            modelBuilder.Entity<SY8249_00104>().HasNoKey();
            modelBuilder.Entity<SY8581_00106>().HasKey(u => new { u.SY8581_NVE });
        }
        public DbSet<LA2259_00103> LA2259_00103 { get; set; }
        public DbSet<FA0078_00112> FA0078_00112 { get; set; }
        public DbSet<FA3901_00101> FA3901_00101 { get; set; }
        public DbSet<FA0077_00114> FA0077_00114 { get; set; }
        public DbSet<LA0054_00107> LA0054_00107 { get; set; }
        public DbSet<SY8081_00104> SY8081_00104 { get; set; }
        public DbSet<SY0012_00110> SY0012_00110 { get; set; }
        public DbSet<SY8249_00104> SY8249_00104 { get; set; }
        public DbSet<SY8581_00106> SY8581_00106 { get; set; }

        ~AppDbContext()
        {
        }
    }
}
