using System;
using System.CodeDom;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Xml;
using System.Xml.Serialization;
using log4net;


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


        public static void LoadXml(XmlDocument Doc, string connectionString, string FolderLavoro, int IdFile, ILog log)
        {
            if (Doc == null)
            {
                //HubLog.SaveLog2DB("Error", "CaricaXml.LoadXml", "XmlDocument is null", connectionString);
                log.Error("CaricaXml.LoadXml, XmlDocument is null");
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
                        //HubLog.SaveLog2DB("Error", "CaricaXML.LoadXml(FileNotFound)", fn.Message, connessione);
                        log.Error("CaricaXML.LoadXml(FileNotFound)" + fn.Message);
                    }
                    catch (FileLoadException fl)
                    {
                        //HubLog.SaveLog2DB("Error", "CaricaXML.LoadXml(FileLoad)", fl.Message, connessione);
                        log.Error("CaricaXML.LoadXml(FileLoad)" + fl.Message);
                    }
                    catch (FileFormatException ff)
                    {
                        //HubLog.SaveLog2DB("Error", "CaricaXML.LoadXml(FileFormat)", ff.Message, connessione);
                        log.Error("CaricaXML.LoadXml(FileFormat)" + ff.Message);
                    }
                    catch (Exception ex)
                    {
                        //HubLog.SaveLog2DB("Error", "CaricaXML.LoadXml(UnknownError)", ex.Message, connessione);
                        log.Error("CaricaXML.LoadXml(UnknownError)" + ex.Message);
                    }

                    bool isSmis = IsSmis(fileName);
                    bool isRettifica = IsRettifica(fileName);
                    bool isPeriodica = IsPeriodico(fileName);

                    Type tipoDaUsare;

                    if (IsSmis(fileName))
                    {
                        tipoDaUsare = typeof(Models.Smis.FlussoMisure);
                    }
                    else if (IsRettifica(fileName))
                    {
                        tipoDaUsare = typeof(Models.Rettifica.FlussoMisure);
                    }
                    else
                    {
                        tipoDaUsare = typeof(Models.Periodico.FlussoMisure);
                    }

                    XmlSerializer serializer = new XmlSerializer(tipoDaUsare);

                    object flussoGenerico;

                    try
                    {
                        using (XmlReader reader = new XmlNodeReader(Doc))
                        {
                            flussoGenerico = serializer.Deserialize(reader);
                        }
                    }
                    catch (SerializationException se)
                    {
                        //HubLog.SaveLog2DB("Error", "CaricaXml.Deserialize", $"Errore durante la deserializzazione del file {fileName}: {se}", connessione);
                        log.Error($"Errore durante la deserializzazione del file {fileName}: {se}");
                        return;
                    }
                    catch (Exception ex)
                    {
                        //HubLog.SaveLog2DB("Error", "CaricaXml.Deserialize", $"Errore durante la deserializzazione del file {fileName}: {ex}", connessione);
                        log.Error($"Errore durante la deserializzazione del file {fileName}: {ex}");
                        return;
                    }

                    if (flussoGenerico == null)
                    {
                        //HubLog.SaveLog2DB("Warning", "CaricaXml.Deserialize", $"Deserializzazione nulla per il file {fileName}", connessione);
                        log.Error($"Deserializzazione nulla per il file {fileName}");
                        return;
                    }

                    try
                    {
                        if (isPeriodica)
                        {
                            SaveFlusso.SaveFlusso2DB((Models.Periodico.FlussoMisure)flussoGenerico, connessione, FolderLavoro, IdFile, fileName, log);
                        }
                        else if (isRettifica)
                        {
                            SaveFlusso.SaveFlusso2DB((Models.Rettifica.FlussoMisure)flussoGenerico, connessione, FolderLavoro, IdFile, fileName, log);
                        }

                        else
                        {
                            SaveFlusso.SaveFlusso2DB((Models.Smis.FlussoMisure)flussoGenerico, connessione, FolderLavoro, IdFile, fileName, log);
                        }
                    }
                    catch (Exception ex)
                    {
                        //HubLog.SaveLog2DB("Error", "CaricaXml.SaveFlusso2DB", $"Errore durante la lavorazione del file {fileName}: {ex}", connessione);
                        log.Error($"Errore durante la lavorazione del file {fileName}: {ex}");
                        return;
                    }
                }
            }
            catch (Exception ex)
            {
                //HubLog.SaveLog2DB("Error", "CaricaXml.LoadXml", ex.ToString(), connectionString);
                log.Error("CaricaXml.LoadXml" + ex.ToString());
            }
        }

        private static bool IsPeriodico(string filename)
        {
            string[] siglePeriodico = { "PDO", "PDO2G", "PNO", "PNO2G", "VNO", "VNO2G", "SNM", "SNM2G", "EIN",
                "EIN2G", "SM", "SM2G", "RT", "RT2G", "DS", "DS2G", "AV", "AV2G", "VP", "VP2G", "INT" };
            return siglePeriodico.Any(s => filename.Contains(s));
        }
        private static bool IsSmis(string filename)
        {
            string[] sigleSmis = { "SMIS" };
            return sigleSmis.Any(s => filename.Contains(s));
        }
    }


}