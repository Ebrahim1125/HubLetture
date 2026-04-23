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