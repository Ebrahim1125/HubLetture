using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Xml;

namespace Vendita.HubMisureEE.Services
{
    public class ZipExtractorService
    {
        public static List<XmlDocument> UnloadZip(string inFile, string outFile, string stringConnect, out int IdFIleXml)
        {
            string[] zipFiles = null;
            string[] xmlFiles = null;
            string[] allFiles = null;
            List<XmlDocument> flusso = new List<XmlDocument>();
            try
            {
                zipFiles = Directory.GetFiles(inFile, "*.zip");
                xmlFiles = Directory.GetFiles(inFile, "*.xml");
                allFiles = zipFiles.Union(xmlFiles).ToArray();


            }
            catch (DirectoryNotFoundException ex)
            {
                HubLog.SaveLog2DB("Error", "ZipExtractorService.cs", ex.Message, stringConnect);
            }
            catch (IOException ex)
            {
                HubLog.SaveLog2DB("Error", "ZipExtractorService.cs", ex.Message, stringConnect);
            }


            DataTable dt = new DataTable();
            IdFIleXml = 0;
            try
            {
                using (SqlConnection conn = new SqlConnection(stringConnect))
                {
                    conn.Open();

                    using (var da = new SqlDataAdapter("SELECT Id, NomeFile, Lavorato FROM FileXml", conn))
                    {
                        da.Fill(dt);
                        IdFIleXml = dt.Rows.Count;
                    }

                }
            }
            catch (SqlException ex)
            {
                HubLog.SaveLog2DB("Error", "ZipExtractorService.cs", ex.Message, stringConnect);
            }


            foreach (string item in allFiles)
            {
                try
                {
                    if (item.EndsWith(".zip"))
                    {


                        using (FileStream fileStream = new FileStream(item, FileMode.Open, FileAccess.Read))
                        using (ZipStorer zipfile = ZipStorer.Open(fileStream, FileAccess.Read))
                        {

                            foreach (var file in zipfile.ReadCentralDir())
                            {

                                int fileCheck = dt.Select($"NomeFile = '{file.FilenameInZip}'").Count();

                                if (file.FilenameInZip.EndsWith(".xml") && fileCheck == 0 && ControlloNomeFile(file.FilenameInZip))
                                {
                                    string extract = Path.Combine(outFile, file.FilenameInZip);

                                    XmlDocument xmlDoc = new XmlDocument();
                                    zipfile.ExtractFile(file, Path.Combine(outFile, file.FilenameInZip));
                                    xmlDoc.Load(extract);

                                    flusso.Add(xmlDoc);
                                }
                            }
                        }
                    }
                    else if (item.EndsWith(".xml"))
                    {
                        int fileCheck = dt.Select($"NomeFile = '{Path.GetFileName(item)}'").Count();
                        if (fileCheck == 0 && ControlloNomeFile(Path.GetFileName(item)))
                        {
                            File.Copy(item, Path.Combine(outFile, Path.GetFileName(item)));
                            string extract = Path.Combine(outFile, Path.GetFileName(item));


                            XmlDocument xmlDoc = new XmlDocument();

                            xmlDoc.Load(extract);
                            flusso.Add(xmlDoc);
                        }

                    }


                }
                catch (Exception ex)
                {
                    HubLog.SaveLog2DB("Error", "ZipExtractorService.cs", ex.Message, stringConnect);
                }
            }
            return flusso;
        }


        private static bool ControlloNomeFile(string FileName)
        {
            string[] parti = FileName.Split('_');
            string[] endName = parti[6].Split('.');

            if (endName[1] == "xml" && parti[0].Length == 11 && parti[1].Length == 11 && parti[2].Length == 6 && parti[4].Length == 14 && parti[5].Length == 7)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}