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
        public static void LoadXml(XmlDocument doc, string connectionString, string FolderLavoro, int IdFileXml)
        {
            try
            {
                using (SqlConnection connessione = new SqlConnection(connectionString))
                // creo l'oggetto che mi permettera' la connessione al database
                {
                    connessione.Open();
                    // eseguo con il metodo Open() la connessione vera e propria


                    //Per ogni file documentoxml analizza il nome del file e se riconosce nel nome valori come RR ossia di 
                    //rettifica allora per la creazione degli oggetti utilizza la classe adatta

                    string fileName = Path.GetFileName(Doc.BaseURI) ?? "";
                    fileName = fileName.ToUpper();



                    bool isPeriodica = !IsRettifica(fileName);
                    Type tipoDaUsare = isPeriodica ? typeof(Models.Periodico.FlussoMisure) : typeof(Models.Rettifica.FlussoMisure);


                    XmlSerializer serializer = new XmlSerializer(tipoDaUsare);
                    //Per poter lavorare con gli xml devo creare un oggetto di tipo XmlSerializer
                    //che serve a trasformare un XML in un oggetto C# (deserializzazione)
                    // Creo e preparo un convertitore di XML
                    int idFileXml = 0;
                    using (XmlReader reader = new XmlNodeReader(Doc))

                    // creo l oggetto reader che serve proprio a leggere tutto il contenuto dell xml
                    // senza il reader non potro leggere all interno del file
                    {
                        object FlussoGenerico = serializer.Deserialize(reader);
                        //Deserialize scansiona l’XML nodo per nodo e riempie un’istanza della classe che hai passato a XmlSerializer (tipoDaUsare)
                        //con i valori trovati.

                        if (FlussoGenerico == null)
                            return;
                        //Controlla se oggetto è nullo (null).
                        //Se sì, salta all’iterazione successiva del ciclo foreach.
                        // Questa parte evita errori di tipo NullReferenceException
                        if (isPeriodica)
                        {
                            SaveFlusso.SaveFlusso2DB((Models.Periodico.FlussoMisure)FlussoGenerico, connessione, FolderLavoro, idFileXml, fileName);
                        }
                        else
                        {
                            SaveFlusso.SaveFlusso2DB((Models.Rettifica.FlussoMisure)FlussoGenerico, connessione, FolderLavoro, idFileXml, fileName);

                        }
                        // Gestione tipo STANDARD
                    }
                }
            }
            catch (Exception ex)
            {
                HubLog.SaveLog2DB("Error", "CaricaXml.cs", ex.Message, connectionString);
            }
        }
    }
}
