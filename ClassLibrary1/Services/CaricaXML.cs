using log4net;
using System;
using System.CodeDom;
using System.Data.SqlClient;
using System.Diagnostics.Eventing.Reader;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.Serialization;
using System.Xml;
using System.Xml.Serialization;


namespace Vendita.HubMisureEE.Services
{
    public class CaricaXML
    {

        public static void LoadXml(XmlDocument Doc, string DBNameEString, string FolderLavoro, int IdFile, ILog log)
        {
            if (Doc == null)
            {
                //HubLog.SaveLog2DB("Error", "CaricaXml.LoadXml", "XmlDocument is null", connectionString);
                log.Error("EE.CaricaXml.LoadXml, XmlDocument is null");
                return;
            }

            if (string.IsNullOrWhiteSpace(DBNameEString))
            {
                return;
            }

            try
            {
                string[] elementDB = DBNameEString.Split('_');
                using (SqlConnection connessione = new SqlConnection(elementDB[1]))
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
                        log.Error("EE.CaricaXML.LoadXml(FileNotFound)" + fn.Message);
                    }
                    catch (FileLoadException fl)
                    {
                        //HubLog.SaveLog2DB("Error", "CaricaXML.LoadXml(FileLoad)", fl.Message, connessione);
                        log.Error("EE.CaricaXML.LoadXml(FileLoad)" + fl.Message);
                    }
                    catch (FileFormatException ff)
                    {
                        //HubLog.SaveLog2DB("Error", "CaricaXML.LoadXml(FileFormat)", ff.Message, connessione);
                        log.Error("EE.CaricaXML.LoadXml(FileFormat)" + ff.Message);
                    }
                    catch (Exception ex)
                    {
                        //HubLog.SaveLog2DB("Error", "CaricaXML.LoadXml(UnknownError)", ex.Message, connessione);
                        log.Error("EE.CaricaXML.LoadXml(UnknownError)" + ex.Message);
                    }

                    string[] arName = fileName.Split('_');
                    string tipoPrat = arName[3];

                    bool isSmis = IsSmis(tipoPrat);
                    bool isRettifica = IsRettifica(tipoPrat);
                    bool isPeriodica = IsPeriodico(tipoPrat);
                    bool isFlussoS = IsFlussoS(tipoPrat);
                    bool isFlussoF = IsFlussoF(tipoPrat);

                    Type tipoDaUsare = null;

                    if (isSmis)
                    {
                        tipoDaUsare = typeof(Models.Smis.FlussoMisure);
                    }
                    else if (isRettifica)
                    {
                        tipoDaUsare = typeof(Models.Rettifica.FlussoMisure);
                    }
                    else if (isPeriodica)
                    {
                        tipoDaUsare = typeof(Models.Periodico.FlussoMisure);
                    }
                    else if (isFlussoS)
                    {
                        tipoDaUsare = typeof(Models.FlussoS.FlussoMisure);
                    }
                    else if (isFlussoF)
                    {
                        tipoDaUsare = typeof(Models.FlussoF.FlussoMisure);
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
                        log.Error($"EE.Errore durante la deserializzazione del file {fileName}: {se}");
                        return;
                    }
                    catch (Exception ex)
                    {
                        //HubLog.SaveLog2DB("Error", "CaricaXml.Deserialize", $"Errore durante la deserializzazione del file {fileName}: {ex}", connessione);
                        log.Error($"EE.Errore durante la deserializzazione del file {fileName}: {ex}");
                        return;
                    }

                    if (flussoGenerico == null)
                    {
                        //HubLog.SaveLog2DB("Warning", "CaricaXml.Deserialize", $"Deserializzazione nulla per il file {fileName}", connessione);
                        log.Error($"EE.Deserializzazione nulla per il file {fileName}");
                        return;
                    }

                    try
                    {
                        if (isPeriodica)
                        {
                            SaveFlusso.SaveFlusso2DB((Models.Periodico.FlussoMisure)flussoGenerico, elementDB[0], connessione, FolderLavoro, IdFile, fileName, log);
                        }
                        else if (isRettifica)
                        {
                            SaveFlusso.SaveFlusso2DB((Models.Rettifica.FlussoMisure)flussoGenerico, elementDB[0], connessione, FolderLavoro, IdFile, fileName, log);
                        }
                        else if(isSmis)
                        {
                            SaveFlusso.SaveFlusso2DB((Models.Smis.FlussoMisure)flussoGenerico, elementDB[0], connessione, FolderLavoro, IdFile, fileName, log);
                        }
                        else if (isFlussoF)
                        {
                            SaveFlusso.SaveFlusso2DB((Models.FlussoF.FlussoMisure)flussoGenerico, elementDB[0], connessione, FolderLavoro, IdFile, fileName, log);
                        }
                        else if (isFlussoS)
                        {
                            SaveFlusso.SaveFlusso2DB((Models.FlussoS.FlussoMisure)flussoGenerico, elementDB[0], connessione, FolderLavoro, IdFile, fileName, log);
                        }
                    }
                    catch (Exception ex)
                    {
                        //HubLog.SaveLog2DB("Error", "CaricaXml.SaveFlusso2DB", $"Errore durante la lavorazione del file {fileName}: {ex}", connessione);
                        log.Error($"EE.Errore durante la lavorazione del file {fileName}: {ex}");
                        return;
                    }
                }
            }
            catch (Exception ex)
            {
                //HubLog.SaveLog2DB("Error", "CaricaXml.LoadXml", ex.ToString(), connectionString);
                log.Error("EE.CaricaXml.LoadXml" + ex.ToString());
            }
        }

        private static bool IsPeriodico(string codFlusso)
        {
            string[] siglePeriodico = { "PDO", "PDO2G", "PNO", "PNO2G", "VNO", "VNO2G", "SNM", "SNM2G", "EIN",
                "EIN2G", "SM", "SM2G", "RT", "RT2G", "DS", "DS2G", "AV", "AV2G", "VP", "VP2G", "INT" };
  
            return siglePeriodico.Contains(codFlusso);
                

        }
        private static bool IsRettifica(string codFlusso)
        {

            string[] sigleRettifica =
            {
                "RFO2G", "RNO2G", "RIN2G", "RNV2G", "RSN2G",
                "SMR2G", "RTR2G", "DSR2G", "AVR2G", "VPR2G",
                "RFO", "RNO", "RIN", "RNV", "RSN",
                "SMR", "RTR", "DSR", "AVR", "VPR", "INTR"
            };

            return sigleRettifica.Contains(codFlusso); ;

        }
        private static bool IsSmis(string codFlusso)
        {
            string[] sigleSmis = { "SMIS" };
            return sigleSmis.Contains(codFlusso);
        }
        private static bool IsFlussoS(string codFlusso)
        {
            string[] singleFlussoS = { "SOS", "S2G", "SNS" };
            return singleFlussoS.Contains(codFlusso);
        }
        private static bool IsFlussoF(string codFlusso)
        {
            string[] singleFlussoF = { "SOF", "SNF", "F2G" };
            return singleFlussoF.Contains(codFlusso);
        }
    }


}