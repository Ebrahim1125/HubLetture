using System;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Xml;
using System.Xml.Serialization;

namespace Vendita.HubMisureEE.Services
{
    public class CaricaXML
    {
        private static bool IsRettifica(string fileName)
        {
            string[] sigleRettifica =
            {
                "RFO2G", "RNO2G", "RIN2G", "RNV2G", "RSN2G",
                "SMR2G", "RTR2G", "DSR2G", "AVR2G", "VPR2G",
                "RFO", "RNO", "RIN", "RNV", "RSN",
                "SMR", "RTR", "DSR", "AVR", "VPR", "INTR"
            };

            return sigleRettifica.Any(s => fileName.Contains(s));
        }
        public static void LoadXml(XmlDocument Doc, string connectionString, string FolderLavoro, int IdLetture) { 
            if (Doc == null)
            {
                HubLog.SaveLog2DB("Error", "CaricaXml.LoadXml", "XmlDocument is null", connectionString);
                return;
            }

            if (string.IsNullOrWhiteSpace(connectionString))
            {
                return;
            }

            try
            {
                using (SqlConnection connessione = new SqlConnection(connectionString))
                {
                    connessione.Open();

                    string fileName = Path.GetFileName(Doc.BaseURI) ?? string.Empty;
                    fileName = fileName.ToUpper();



                    bool isPeriodica = !IsRettifica(fileName);
                    Type tipoDaUsare = isPeriodica ? typeof(Models.Periodico.FlussoMisure) : typeof(Models.Rettifica.FlussoMisure);

                    XmlSerializer serializer = new XmlSerializer(tipoDaUsare);

                    object flussoGenerico;

                    try
                    {
                        using (XmlReader reader = new XmlNodeReader(Doc))
                        {
                            flussoGenerico = serializer.Deserialize(reader);
                        }
                    }
                    catch (Exception ex)
                    {
                        HubLog.SaveLog2DB("Error", "CaricaXml.Deserialize", $"Errore durante la deserializzazione del file {fileName}: {ex}", connessione);
                        return;
                    }

                    if (flussoGenerico == null)
                    {
                        HubLog.SaveLog2DB("Warning", "CaricaXml.Deserialize", $"Deserializzazione nulla per il file {fileName}", connessione);
                        return;
                    }

                    try
                    {
                        if (isPeriodica)
                        {
                            SaveFlusso.SaveFlusso2DB((Models.Periodico.FlussoMisure)flussoGenerico, connessione, FolderLavoro, IdLetture, fileName);
                        }
                        else
                        {
                            SaveFlusso.SaveFlusso2DB((Models.Rettifica.FlussoMisure)flussoGenerico, connessione, FolderLavoro, IdLetture, fileName);
                        }
                    }
                    catch (Exception ex)
                    {
                        HubLog.SaveLog2DB("Error", "CaricaXml.SaveFlusso2DB", $"Errore durante il salvataggio del file {fileName}: {ex}", connessione);
                        return;
                    }
                }
            }
            catch (Exception ex)
            {
                HubLog.SaveLog2DB("Error", "CaricaXml.LoadXml", ex.ToString(), connectionString);
            }
        }
    }
}
