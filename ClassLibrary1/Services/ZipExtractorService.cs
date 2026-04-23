using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;

namespace Vendita.HubMisureEE.Services
{
    public class ZipExtractorService
    {
        public static List<string> UnloadZip(string inFile, string outFile, string stringConnect, out int IdFile)
        {
            string[] zipFiles = null;
            string[] xmlFiles = null;
            string[] allFiles = null;

            List<string> flusso = new List<string>();

            try
            {
                zipFiles = Directory.GetFiles(inFile, "*.zip");
                xmlFiles = Directory.GetFiles(inFile, "*.xml");
                allFiles = zipFiles.Union(xmlFiles).ToArray();

            }
            catch (FileLoadException ex)
            {
                HubLog.SaveLog2DB("Error", "ZipExtractorService.cs/UnloadZip", ex.Message, stringConnect);
            }
            catch (DirectoryNotFoundException ex)
            {
                HubLog.SaveLog2DB("Error", "ZipExtractorService.cs/UnloadZip", ex.Message, stringConnect);
            }
            catch (IOException ex)
            {
                HubLog.SaveLog2DB("Error", "ZipExtractorService.cs/UnloadZip", ex.Message, stringConnect);
            }
            catch (UnauthorizedAccessException ex)
            {
                HubLog.SaveLog2DB("Error", "ZipExtractorService.cs/UnloadZip", ex.Message, stringConnect);
            }

            DataTable FileXml = new DataTable();

            IdFile = 0;
            try
            {
                using (SqlConnection conn = new SqlConnection(stringConnect))
                {
                    conn.Open();

                    using (var FileXmlDb = new SqlDataAdapter("SELECT IdFile, NomeFile, Lavorato FROM FileXml", conn))
                    {
                        FileXmlDb.Fill(FileXml);
                        IdFile = FileXml.Rows.Count + 1;
                    }

                }
            }
            catch (SqlException ex)
            {
                HubLog.SaveLog2DB("Error", "ZipExtractorService/Query FileXml", ex.Message, stringConnect);
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

                            foreach (ZipFileEntry file in zipfile.ReadCentralDir())
                            {

                                int fileCheck = FileXml.Select($"NomeFile = '{file.FilenameInZip}'").Count();

                                if (file.FilenameInZip.ToLower().EndsWith(".xml") && ControlloNomeFile(Path.GetFileName(file.FilenameInZip)) && fileCheck == 0)
                                {

                                    zipfile.ExtractFile(file, Path.Combine(outFile, file.FilenameInZip));


                                    flusso.Add(Path.GetFileName(file.FilenameInZip));
                                }
                            }
                        }
                    }
                    else if (item.EndsWith(".xml"))
                    {
                        int fileCheck = FileXml.Select($"NomeFile = '{Path.GetFileName(item)}'").Count();
                        if (ControlloNomeFile(Path.GetFileName(item)) && fileCheck == 0)
                        {
                            File.Copy(item, Path.Combine(outFile, Path.GetFileName(item)));

                            flusso.Add(Path.GetFileName(item));
                        }

                    }

                }
                catch (Exception ex)
                {
                    HubLog.SaveLog2DB("Error", "ZipExtractorService.cs/Inserimento nomi nel flusso", ex.Message, stringConnect);
                }
            }
            return flusso;
        }

        private static bool ControlloNomeFile(string FileName)
        {
            string[] parti = FileName.Split('_');

            string[] endName = parti[6].Split('.');
         
            if (endName[1].ToLower() == "xml" && parti[0].Length == 11 && parti[1].Length == 11 && parti[2].Length == 6 && parti[4].Length == 14 && parti[5].Length == 7)
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
