using System;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Xml;
using System.Xml.Serialization;
using Vendita.HubMisureEE.Models.Rettifica;
using Vendita.HubMisureEE.Models.Periodico;

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
        public static void LoadXml(XmlDocument Doc, string connectionString, string FolderLavoro, int IdFile)
        {
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
                    string fileName = string.Empty;
                    try
                    {
                        fileName = Path.GetFileName(Doc.BaseURI) ?? string.Empty;
                        fileName = fileName.ToUpper();
                    }
                    catch (FileNotFoundException fn)
                    {
                        HubLog.SaveLog2DB("Error", "CaricaXML.LoadXml(FileNotFound)", fn.Message, connessione);
                    }
                    catch (FileLoadException fl)
                    {
                        HubLog.SaveLog2DB("Error", "CaricaXML.LoadXml(FileLoad)", fl.Message, connessione);
                    }
                    catch (FileFormatException ff)
                    {
                        HubLog.SaveLog2DB("Error", "CaricaXML.LoadXml(FileFormat)", ff.Message, connessione);
                    }
                    catch (Exception ex)
                    {
                        HubLog.SaveLog2DB("Error", "CaricaXML.LoadXml(UnknownError)", ex.Message, connessione);
                    }

                    string[] arName = fileName.Split('_');
                    string tipoPrat = arName[3];

                    bool isPeriodica = !IsRettifica(tipoPrat);
                    Type tipoDaUsare = isPeriodica ? typeof(Models.Periodico.FlussoMisure) : typeof(Models.Rettifica.FlussoMisure);


                    XmlSerializer serializer = new XmlSerializer(tipoDaUsare);

                    dynamic flussoGenerico;

                    try
                    {
                        using (XmlReader reader = new XmlNodeReader(Doc))
                        {
                            flussoGenerico = serializer.Deserialize(reader);
                        }
                    }
                    catch (SerializationException se)
                    {
                        HubLog.SaveLog2DB("Error", "CaricaXml.Deserialize", $"Errore durante la deserializzazione del file {fileName}: {se}", connessione);
                        return;
                    }
                    catch (Exception ex)
                    {
                        HubLog.SaveLog2DB("Error", "CaricaXml.Deserialize", $"Errore durante la deserializzazione del file {fileName}: {ex}", connessione);
                        return;
                    }

                    if (flussoGenerico == null)
                    {
                        HubLog.SaveLog2DB("Warning", "CaricaXml.Deserialize", $"FLusso = null - {fileName}", connessione);
                        return;
                    }

                    try
                    {
                        SaveFlusso.SaveFlusso2DB(flussoGenerico, connessione, FolderLavoro, IdFile, fileName);
                    }
                    catch (Exception ex)
                    {
                        HubLog.SaveLog2DB("Error", "CaricaXml.SaveFlusso2DB", $"Errore durante la lavorazione del file {fileName}: {ex}", connessione);
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