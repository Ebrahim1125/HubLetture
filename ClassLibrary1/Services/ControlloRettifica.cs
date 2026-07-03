using System;
using System.Data;
using System.Data.SqlClient;
using log4net;

namespace Vendita.HubMisureEE.Services
{
    // La classe ControllaRettifica contiene metodi per verificare se una lettura è stata rettificata e per aggiornare lo stato di rettifica nei database.
    internal class ControllaRettifica
    {
        public static bool IsRettificato(SqlConnection connessione, string piVaUtente, string piVaDistributore, string Pod, object DataMisure, ILog log)
        {
            int IdFileXml = 0;

           
            if (DataMisure.GetType() == FlussoMisureGas.Rettifica)
            {
                string query = @"SELECT l.IdFile FROM Letture l
                     LEFT JOIN Curve c ON l.Id = c.IdLetture WHERE l.PIvaUtente = @PIvaUtente
                     AND l.PIvaDistributore = @PIvaDistributore
                     AND l.Pod = @Pod AND l.CodFlusso LIKE 'P%'
                     AND l.CodFlusso NOT LIKE 'SMIS'
                     AND ((@DataMisura = DATEFROMPARTS(YEAR(l.MeseAnno), MONTH(l.MeseAnno), c.Giorno) 
                     OR  @DataMisura = l.DataMisura))";
                using (SqlCommand com = new SqlCommand(query, connessione))
                {
                   IdFileXml = QueryIdRettificare( connessione,  PIvaUtente,  PIvaDistributore,  CodiceMisuratore,  DataMisure,  nameXml, "CodPdr")

                   
                        
                    if (IdFileXml != 0)
                        {
                            Rettifica("Gas", IdFileXml, connessione);
                            Rettifica("FileXml", IdFileXml, connessione);

                            HubLog.SaveLog2DB("Info", "Gas.ControllaRettifica.IsRettificato", $"Lettura: {nameXml} , trovato file da rettificare", connessione);
                            return true;
                            
                        }
                        HubLog.SaveLog2DB("Warning", "Gas.ControllaRettifica.IsRettificato(PeriodicoNonTrovato)", $"Lettura: {nameXml}- Nessuna lettura da rettificare", connessione);
                    return false;
                }
                catch (Exception ex)
                {
                    Rettifica("Letture", IdFileXml, connessione, log);
                    Rettifica("FileXml", IdFileXml, connessione, log);
                    Rettifica("Curve", IdFileXml, connessione, log);
                    log.Info("ControllaRettifica.IsRettificato--Trovato file da rettificare--" );
                    return true;
                }
                else
                {
                    log.Info("ControllaRettifica.IsRettificato--Nessun file da rettificare trovato per i parametri forniti.");
                }
                return false;
            }
            catch (Exception ex)
            {
                //HubLog.SaveLog2DB("Error", "ControllaRettifica.IsRettificato(PeriodicoNonTrovato)", ex.Message, connessione);
                log.Error("ControllaRettifica.IsRettificato--Errore ricerca file da rettificare--" + ex.Message);

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
                    com.Parameters.Add("@Valdio", SqlDbType.Bit).Value = false;

                    com.ExecuteNonQuery();
                }
            }
            catch (SqlException ex)
            {
                //HubLog.SaveLog2DB("Error", "ControllaRettifica.Rettifica", ex.Message, connessione);
                log.Error($"ControllaRettifica.Rettifica--Errore SQL nell'aggiornamento della validita per " + ex.Message);
            }
            catch (Exception ex)
            {
                //HubLog.SaveLog2DB("Error", "ControllaRettifica.Rettifica", ex.Message, connessione);
                log.Error($"ControllaRettifica.Rettifica--Errore nell'aggiornamento della validita" + ex.Message);
            }
        }

        private static int QueryIdRettificare(SqlConnection connessione, string PIvaUtente, string PIvaDistributore, string CodiceMisuratore, object DataMisure, string nameXml, string TipoMisuratore)
        {
            int IdFileXml=0;
            string query = $@"SELECT l.IdFile 
                                FROM LettureEE l
                                    LEFT JOIN Curve c ON l.Id = c.IdLetture 
                                WHERE l.PIvaUtente = @PIvaUtente
                                    AND l.PIvaDistributore = @PIvaDistributore
                                    AND l.{TipoMisuratore} = @Pod AND l.CodFlusso LIKE 'P%'
                                    AND ((@DataMisura = DATEFROMPARTS(YEAR(l.MeseAnno), MONTH(l.MeseAnno), c.Giorno) 
                                    OR @DataMisura = l.DataMisura))";

            using (SqlCommand com = new SqlCommand(query, connessione))
                {
                    com.Parameters.Add("@PIvaUtente", SqlDbType.VarChar).Value = PIvaUtente;
                    com.Parameters.Add("@PIvaDistributore", SqlDbType.VarChar).Value = PIvaDistributore;
                    com.Parameters.Add("@Pod", SqlDbType.VarChar).Value = Pod;
                    com.Parameters.Add("@DataMisura", SqlDbType.Date).Value = DataMisure;
                    IdFileXml = Convert.ToInt32(com.ExecuteScalar());
                }

            return IdFileXml;
    }
    }
}

