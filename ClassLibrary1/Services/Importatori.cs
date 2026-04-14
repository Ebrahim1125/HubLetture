using System.Collections.Generic;
using System.Xml;

namespace Vendita.HubMisureEE.Services
{
    public static class Importatori
    {
        public static void Importa(string folderSorgente, string folderLavoro, string stringaConnessione)
        {
            int IdFileXml = 0;
            List<XmlDocument> flusso = ZipExtractorService.UnloadZip(folderSorgente, folderLavoro, stringaConnessione, out IdFileXml);

            foreach (XmlDocument Doc in flusso)
            {
                CaricaXML.LoadXml(Doc, stringaConnessione, folderLavoro, IdFileXml++);
            }
        }
    }
}