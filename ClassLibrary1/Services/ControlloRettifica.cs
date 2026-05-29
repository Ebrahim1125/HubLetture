using System;
using System.Data;
using System.Data.SqlClient;
using Vendita.HubMisureEE.Models.Rettifica;


namespace Vendita.HubMisureEE.Services
{
    // La classe ControllaRettifica contiene metodi per verificare se una lettura è stata rettificata e per aggiornare lo stato di rettifica nei database.
    internal class ControllaRettifica
    {
        public static bool IsRettificato(SqlConnection connessione, string PIvaUtente, string PIvaDistributore, string CodiceMisuratore, string DataMisure, string nameXml)
        {
            int IdFileXml = 0;
            bool esito = false;

            if (DataMisure.GetType() == typeof(Models.Rettifica.FlussoMisure))
            {
                // La query SQL seleziona l'IdFile dalla tabella Letture, unendo la tabella Curve,
                // filtrando per i parametri specificati (PIvaUtente, PIvaDistributore, CodPdr, DataMisura) e verificando se il CodFlusso inizia con 'T'.

                try
                {
                    IdFileXml = QueryIdRettificare(connessione, PIvaUtente, PIvaDistributore, CodiceMisuratore,
                        DataMisure, nameXml, "CodPdr");



                    if (IdFileXml != 0)
                    {
                        Rettifica("Gas", IdFileXml, connessione);
                        Rettifica("FileXml", IdFileXml, connessione);

                        HubLog.SaveLog2DB("Info", "Gas.ControllaRettifica.IsRettificato",
                            $"Lettura: {nameXml} , trovato file da rettificare", connessione);
                        esito = true;


                    }

                    HubLog.SaveLog2DB("Warning", "Gas.ControllaRettifica.IsRettificato(PeriodicoNonTrovato)",
                        $"Lettura: {nameXml}- Nessuna lettura da rettificare", connessione);
                    esito = false;
                }
                catch (Exception ex)
                {
                    HubLog.SaveLog2DB("Error", "Gas.ControllaRettifica.IsRettificato(PeriodicoNonTrovato)",
                        $"{nameXml}-{ex.Message}", connessione);
                    esito = false;
                }

            }

            return esito;

        }

        // Il metodo Rettifica aggiorna il campo Rettificato a true per un record specifico identificato da IdFile nella tabella specificata.
        private static void Rettifica(string NomeTabella, int Id, SqlConnection connessione)
        {
            try
            {
                string query = $"UPDATE {NomeTabella} SET Rettificato=@Rettificato WHERE IdFile=@IdFile";
                using (SqlCommand com = new SqlCommand(query, connessione))
                {
                    com.Parameters.Add("@IdFile", SqlDbType.Int).Value = Id;
                    com.Parameters.Add("@Rettificato", SqlDbType.Bit).Value = true;

                    com.ExecuteNonQuery();
                }
            }
            catch (SqlException ex)
            {
                HubLog.SaveLog2DB("Error", "ControllaRettifica.Rettifica", ex.Message, connessione);
            }
            catch (Exception ex)
            {
                HubLog.SaveLog2DB("Error", "ControllaRettifica.Rettifica", ex.Message, connessione);
            }
        }

        private static int QueryIdRettificare(SqlConnection connessione, string PIvaUtente, string PIvaDistributore, string CodiceMisuratore, object DataMisure, string nameXml, string NumMisuratore)
        {
            int IdFileXml = 0;
            string query = $@"SELECT l.IdFile 
                                FROM LettureEE l
                                    LEFT JOIN Curve c ON l.Id = c.IdLetture 
                                WHERE l.PIvaUtente = @PIvaUtente
                                    AND l.PIvaDistributore = @PIvaDistributore
                                    AND l.{NumMisuratore} = @Pod AND l.CodFlusso LIKE 'P%'
                                    AND ((@DataMisura = DATEFROMPARTS(YEAR(l.MeseAnno), MONTH(l.MeseAnno), c.Giorno) 
                                    OR @DataMisura = l.DataMisura))";

            using (SqlCommand com = new SqlCommand(query, connessione))
            {
                com.Parameters.Add("@PIvaUtente", SqlDbType.VarChar).Value = PIvaUtente;
                com.Parameters.Add("@PIvaDistributore", SqlDbType.VarChar).Value = PIvaDistributore;
                com.Parameters.Add("@Pod", SqlDbType.VarChar).Value = NumMisuratore;
                com.Parameters.Add("@DataMisura", SqlDbType.Date).Value = DataMisure;
                IdFileXml = Convert.ToInt32(com.ExecuteScalar());
            }

            return IdFileXml;
        }
    }
}
