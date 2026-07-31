using System;
using System.Data;
using System.Data.SqlClient;
using log4net;

namespace Vendita.HubMisureEE.Services
{
    // La classe ControllaRettifica contiene metodi per verificare se una lettura è stata rettificata e per aggiornare lo stato di rettifica nei database.
    internal class ControllaRettifica
    {
        public static bool IsRettificato(string nomeDB, SqlConnection connessione, string piVaUtente, string piVaDistributore, string Pod, object DataMisure, ILog log)
        {
            int IdFileXml = 0;

            // La query SQL seleziona l'IdFile dalla tabella Letture, unendo con la tabella Curve,
            // filtrando per i parametri specificati (PIvaUtente, PIvaDistributore, Pod, DataMisura)
            try
            {
                string query = $@"SELECT l.IdFile FROM {nomeDB}.dbo.LettureEE l
                     LEFT JOIN {nomeDB}.dbo.CurveEE c ON l.Id = c.IdLetture 
                     WHERE l.PIvaUtente = @PIvaUtente
                     AND l.PIvaDistributore = @PIvaDistributore
                     AND l.Pod = @Pod 
                     AND l.CodFlusso NOT LIKE 'SMIS'
                     AND l.CodFlusso NOT LIKE 'SOS'
                     AND l.CodFlusso NOT LIKE 'S2G'
                     AND l.CodFlusso NOT LIKE 'SNS'
                     AND l.CodFlusso NOT LIKE 'SOF'
                     AND l.CodFlusso NOT LIKE 'SNF'
                     AND l.CodFlusso NOT LIKE 'F2G'
                     AND ((@DataMisura = DATEFROMPARTS(YEAR(l.MeseAnno), MONTH(l.MeseAnno), c.Giorno) 
                     OR  @DataMisura = l.DataMisura))";
                using (SqlCommand com = new SqlCommand(query, connessione))
                {
                    com.Parameters.Add("@PIvaUtente", SqlDbType.VarChar).Value = piVaUtente;
                    com.Parameters.Add("@PIvaDistributore", SqlDbType.VarChar).Value = piVaDistributore;
                    com.Parameters.Add("@Pod", SqlDbType.VarChar).Value = Pod;
                    com.Parameters.Add("@DataMisura", SqlDbType.Date).Value = DataMisure;
                    IdFileXml = Convert.ToInt32(com.ExecuteScalar());
                }
                if (IdFileXml != 0)
                {
                    Rettifica($"{nomeDB}.dbo.LettureEE", IdFileXml, connessione, log);
                    Rettifica($"{nomeDB}.dbo.FileXmlEE", IdFileXml, connessione, log);
                    Rettifica($"{nomeDB}.dbo.CurveEE", IdFileXml, connessione, log);
                    log.Info("EE.ControllaRettifica.IsRettificato -- Trovato file da rettificare");
                    return true;
                }
                else
                {
                    log.Info("EE.ControllaRettifica.IsRettificato--Nessun file da rettificare trovato per i parametri forniti.");
                }
                return false;
            }
            catch (Exception ex)
            {
                //HubLog.SaveLog2DB("Error", "ControllaRettifica.IsRettificato(PeriodicoNonTrovato)", ex.Message, connessione);
                log.Error("EE.ControllaRettifica.IsRettificato -- Errore ricerca file da rettificare -- " + ex.Message);

                return false;
            }
        }

        // Il metodo Rettifica aggiorna il campo Rettificato a true per un record specifico identificato da IdFile nella tabella specificata.
        private static void Rettifica(string NomeTabella, int Id, SqlConnection connessione, ILog log)
        {
            try
            {
                string query = $"UPDATE {NomeTabella} SET Valido=@Valido WHERE IdFile=@IdFile";
                using (SqlCommand com = new SqlCommand(query, connessione))
                {
                    com.Parameters.Add("@IdFile", SqlDbType.Int).Value = Id;
                    com.Parameters.Add("@Valido", SqlDbType.Bit).Value = false;

                    com.ExecuteNonQuery();
                }
            }
            catch (SqlException ex)
            {
                //HubLog.SaveLog2DB("Error", "ControllaRettifica.Rettifica", ex.Message, connessione);
                log.Error($"EE.ControllaRettifica.Rettifica -- Errore SQL nell'aggiornamento della validita per " + ex.Message);
            }
            catch (Exception ex)
            {
                //HubLog.SaveLog2DB("Error", "ControllaRettifica.Rettifica", ex.Message, connessione);
                log.Error($"EE.ControllaRettifica.Rettifica -- Errore nell'aggiornamento della validita " + ex.Message);
            }
        }
    }
}