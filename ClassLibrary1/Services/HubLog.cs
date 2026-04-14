using System;
using System.Data;
using System.Data.SqlClient;

namespace Vendita.HubMisureEE.Services
{
    internal class HubLog
    {
        public static void SaveLog2DB(string tipo, string dove, string messaggio, string stringConnessione)
        {
            using (SqlConnection conn = new SqlConnection(stringConnessione))
            {
                conn.Open();
                string query = "Insert into HubLog(DataIns, Tipo, Dove, Messaggio) values(@DataIns, @Tipo, @Dove, @Messaggio)";
                SqlCommand com = new SqlCommand(query, conn);
                com.Parameters.Add("@DataIns", SqlDbType.DateTime).Value = DateTime.Now;
                com.Parameters.Add("@Tipo", SqlDbType.VarChar).Value = tipo;
                com.Parameters.Add("@Dove", SqlDbType.VarChar).Value = dove;
                com.Parameters.Add("@Messaggio", SqlDbType.VarChar).Value = messaggio;

                com.ExecuteNonQuery();
            }
        }
        public static void SaveLog2DB(string tipo, string dove, string messaggio, SqlConnection connessione)
        {
            string query = "Insert into HubLog(DataIns, Tipo, Dove, Messaggio) values(@DataIns, @Tipo, @Dove, @Messaggio)";
            SqlCommand com = new SqlCommand(query, connessione);
            com.Parameters.Add("@DataIns", SqlDbType.DateTime).Value = DateTime.Now;
            com.Parameters.Add("@Tipo", SqlDbType.VarChar).Value = tipo;
            com.Parameters.Add("@Dove", SqlDbType.VarChar).Value = dove;
            com.Parameters.Add("@Messaggio", SqlDbType.VarChar).Value = messaggio;

            com.ExecuteNonQuery();
        }
    }
}