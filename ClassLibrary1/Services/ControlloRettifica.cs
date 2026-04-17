using System;
using System.Data;
using System.Data.SqlClient;

namespace Vendita.HubMisureEE.Services
{

    internal class ControllaRettifica
    {
        public static bool IsRettificato(SqlConnection connessione, string PIvaUtente, string PIvaDistributore, string Pod, DateTime DataMisura)
        {
            int Id = 0;
            int IdFileXml = 0;


            string query = "SELECT  IdLetture FROM Letture WHERE(PIvaUtente=@PIvaUtente AND PIvaDistributore=@PIvaDistributore AND Pod=@Pod AND DataMisura=@DataMisura)";
            using (SqlCommand com = new SqlCommand(query, connessione))
            {

                com.Parameters.Add("@PIvaUtente", SqlDbType.VarChar).Value = PIvaUtente;
                com.Parameters.Add("@PIvaDistributore", SqlDbType.VarChar).Value = PIvaDistributore;
                com.Parameters.Add("@Pod", SqlDbType.VarChar).Value = Pod;
                com.Parameters.Add("@DataMisura", SqlDbType.DateTime).Value = DataMisura;
                //com.ExecuteNonQuery();

                IdFileXml = (int)com.ExecuteScalar();
            }
            if (IdFileXml != 0)
            {
                Rettifica("Letture", IdFileXml, connessione);
                Rettifica("FileXml", IdFileXml, connessione);
                Rettifica("Curve", IdFileXml, connessione);
                return true;
            }


            return false;
        }

        private static void Rettifica(String NomeTabella, int Id, SqlConnection connessione)
        {



            string query = $"UPDATE {NomeTabella} SET Rettificato=@Rettificato WHERE IdLetture=@IdLetture";
            using (SqlCommand com = new SqlCommand(query, connessione))
            {

                com.Parameters.Add("@IdLetture", SqlDbType.Int).Value = Id;
                com.Parameters.Add("@Rettificato", SqlDbType.Bit).Value = true;


                com.ExecuteNonQuery();

            }



        }
    }
}