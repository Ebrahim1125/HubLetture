using log4net;
using System.Collections.Generic;
using System.IO;
using System.Xml;

namespace Vendita.HubMisureEE.Services
{
    public static class Importatori
    {
        public static void Importa(string folderSorgente, string folderLavoro, string stringaConnessione)
        {
            int IdLetture = 0;
            List<string> flusso = ZipExtractorService.UnloadZip(folderSorgente, folderLavoro, stringaConnessione, out IdLetture);

            foreach (string Doc in flusso)
            {
                XmlDocument doc = new XmlDocument();
               
                doc.Load(Path.Combine(folderLavoro, Doc));      
                
                CaricaXML.LoadXml(doc, stringaConnessione, folderLavoro, IdLetture++);
            }
        }
    }
}
