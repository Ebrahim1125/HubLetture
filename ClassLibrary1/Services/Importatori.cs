using System;
using System.Collections.Generic;
using System.IO;
using System.Xml;

namespace Vendita.HubMisureEE.Services
{
    public static class Importatori
    {
        public static void Importa(string folderSorgente, string folderLavoro, string stringaConnessione)
        {
            /// Importa file XML a partire da uno o più archivi ZIP presenti in una cartella sorgente./// 
            /// Flusso operativo:/// 
            /// 1. Estrae i file ZIP dalla cartella sorgente in una cartella di lavoro.///
            /// 2. Ottiene la lista dei file XML estratti.///
            /// 3. Per ogni file XML:///
            /// - Carica il contenuto in memoria./// 
            /// - Passa il documento a un metodo di elaborazione che lo salva/elabora nel database.///
            /// 4. In caso di errore, registra il problema su database.

            try
            {
                int IdFile = 0;

                List<string> flusso = ZipExtractorService.UnloadZip(folderSorgente, folderLavoro, stringaConnessione, out IdFile);
                foreach (string Doc in flusso)

                {
                    XmlDocument doc = new XmlDocument();
                    doc.Load(Path.Combine(folderLavoro, Doc));
                    CaricaXML.LoadXml(doc, stringaConnessione, folderLavoro, IdFile++);
                }
            }
            catch (Exception e)
            {
                HubLog.SaveLog2DB("Error", "Importatori.cs", e.Message, stringaConnessione);
            }
        }
    }
}