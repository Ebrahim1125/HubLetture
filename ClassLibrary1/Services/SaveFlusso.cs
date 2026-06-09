using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Text;
using System.IO;
using System.Reflection;
using System.Xml.Serialization;
using Vendita.HubMisureEE.Models.Periodico;
using Vendita.HubMisureEE.Models.Rettifica;
using Vendita.HubMisureEE.Models.Smis;

namespace Vendita.HubMisureEE.Services
{
    internal class SaveFlusso
    {
        // Lavorazione del flusso Periodico e salvataggio su DB
        public static void SaveFlusso2DB(Models.Periodico.FlussoMisure FlussoMisura, SqlConnection connessione, string FolderLavoro, int IdFile, string fileName)
        {
            if (FlussoMisura == null || FlussoMisura.DatiPod == null || FlussoMisura.DatiPod.Length == 0)
                return;

            //ESTRAZIONE TIMESTAMP DAL NOME FILE
            string[] arrName = fileName.Split('_');
            string timeStamp = arrName[4];

            //PREPARAZIONE DATATABLE
            DataTable dtLetture = new DataTable();

            dtLetture.Columns.Add("Id", typeof(int));
            dtLetture.Columns.Add("CodFlusso", typeof(string));
            dtLetture.Columns.Add("PIvaUtente", typeof(string));
            dtLetture.Columns.Add("PIvaDistributore", typeof(string));
            dtLetture.Columns.Add("CodContrDisp", typeof(string));
            dtLetture.Columns.Add("Pod", typeof(string));
            dtLetture.Columns.Add("MeseAnno", typeof(DateTime));
            dtLetture.Columns.Add("DataMisura", typeof(DateTime));
            dtLetture.Columns.Add("DataRilevazione", typeof(DateTime));
            dtLetture.Columns.Add("Motivazione", typeof(string));
            dtLetture.Columns.Add("TipoRettifica", typeof(string));
            dtLetture.Columns.Add("DataPrest", typeof(DateTime));
            dtLetture.Columns.Add("CodPrat_SII", typeof(string));
            dtLetture.Columns.Add("MisuraRaccolta", typeof(string));
            dtLetture.Columns.Add("MisuraTipoDato", typeof(string));
            dtLetture.Columns.Add("MisuraTipoCp", typeof(string));
            dtLetture.Columns.Add("MisuraCausaOstativa", typeof(string));
            dtLetture.Columns.Add("MisuraValidato", typeof(string));
            dtLetture.Columns.Add("Trattamento", typeof(string));
            dtLetture.Columns.Add("Tensione", typeof(int));
            dtLetture.Columns.Add("Forfait", typeof(string));
            dtLetture.Columns.Add("GruppoMis", typeof(string));
            dtLetture.Columns.Add("Ka", typeof(decimal));
            dtLetture.Columns.Add("Kr", typeof(decimal));
            dtLetture.Columns.Add("Kp", typeof(decimal));
            dtLetture.Columns.Add("MisuraPotMax", typeof(decimal));
            dtLetture.Columns.Add("MisuraEaF1", typeof(decimal));
            dtLetture.Columns.Add("MisuraEaF2", typeof(decimal));
            dtLetture.Columns.Add("MisuraEaF3", typeof(decimal));
            dtLetture.Columns.Add("MisuraEaF4", typeof(decimal));
            dtLetture.Columns.Add("MisuraEaF5", typeof(decimal));
            dtLetture.Columns.Add("MisuraEaF6", typeof(decimal));
            dtLetture.Columns.Add("MisuraErF1", typeof(decimal));
            dtLetture.Columns.Add("MisuraErF2", typeof(decimal));
            dtLetture.Columns.Add("MisuraErF3", typeof(decimal));
            dtLetture.Columns.Add("MisuraErF4", typeof(decimal));
            dtLetture.Columns.Add("MisuraErF5", typeof(decimal));
            dtLetture.Columns.Add("MisuraErF6", typeof(decimal));
            dtLetture.Columns.Add("MisuraPotF1", typeof(decimal));
            dtLetture.Columns.Add("MisuraPotF2", typeof(decimal));
            dtLetture.Columns.Add("MisuraPotF3", typeof(decimal));
            dtLetture.Columns.Add("MisuraPotF4", typeof(decimal));
            dtLetture.Columns.Add("MisuraPotF5", typeof(decimal));
            dtLetture.Columns.Add("MisuraPotF6", typeof(decimal));
            dtLetture.Columns.Add("MisuraEaM", typeof(decimal));
            dtLetture.Columns.Add("MisuraErM", typeof(decimal));
            dtLetture.Columns.Add("MisuraPotM", typeof(decimal));
            dtLetture.Columns.Add("MisuraErcF1", typeof(decimal));
            dtLetture.Columns.Add("MisuraErcF2", typeof(decimal));
            dtLetture.Columns.Add("MisuraErcF3", typeof(decimal));
            dtLetture.Columns.Add("MisuraErcF4", typeof(decimal));
            dtLetture.Columns.Add("MisuraErcF5", typeof(decimal));
            dtLetture.Columns.Add("MisuraErcF6", typeof(decimal));
            dtLetture.Columns.Add("MisuraErcM", typeof(decimal));
            dtLetture.Columns.Add("MisuraEriF1", typeof(decimal));
            dtLetture.Columns.Add("MisuraEriF2", typeof(decimal));
            dtLetture.Columns.Add("MisuraEriF3", typeof(decimal));
            dtLetture.Columns.Add("MisuraEriF4", typeof(decimal));
            dtLetture.Columns.Add("MisuraEriF5", typeof(decimal));
            dtLetture.Columns.Add("MisuraEriF6", typeof(decimal));
            dtLetture.Columns.Add("MisuraEriM", typeof(decimal));
            dtLetture.Columns.Add("ConsumoDataInizioPeriodo", typeof(DateTime));
            dtLetture.Columns.Add("ConsumoEaF1", typeof(decimal));
            dtLetture.Columns.Add("ConsumoEaF2", typeof(decimal));
            dtLetture.Columns.Add("ConsumoEaF3", typeof(decimal));
            dtLetture.Columns.Add("ConsumoErF1", typeof(decimal));
            dtLetture.Columns.Add("ConsumoErF2", typeof(decimal));
            dtLetture.Columns.Add("ConsumoErF3", typeof(decimal));
            dtLetture.Columns.Add("ConsumoPotF1", typeof(decimal));
            dtLetture.Columns.Add("ConsumoPotF2", typeof(decimal));
            dtLetture.Columns.Add("ConsumoPotF3", typeof(decimal));
            dtLetture.Columns.Add("ConsumoEaM", typeof(decimal));
            dtLetture.Columns.Add("ConsumoErM", typeof(decimal));
            dtLetture.Columns.Add("ConsumoPotM", typeof(decimal));
            dtLetture.Columns.Add("ConsumoErcF1", typeof(decimal));
            dtLetture.Columns.Add("ConsumoErcF2", typeof(decimal));
            dtLetture.Columns.Add("ConsumoErcF3", typeof(decimal));
            dtLetture.Columns.Add("ConsumoErcF4", typeof(decimal));
            dtLetture.Columns.Add("ConsumoErcF5", typeof(decimal));
            dtLetture.Columns.Add("ConsumoErcF6", typeof(decimal));
            dtLetture.Columns.Add("ConsumoErcM", typeof(decimal));
            dtLetture.Columns.Add("ConsumoEriF1", typeof(decimal));
            dtLetture.Columns.Add("ConsumoEriF2", typeof(decimal));
            dtLetture.Columns.Add("ConsumoEriF3", typeof(decimal));
            dtLetture.Columns.Add("ConsumoEriF4", typeof(decimal));
            dtLetture.Columns.Add("ConsumoEriF5", typeof(decimal));
            dtLetture.Columns.Add("ConsumoEriF6", typeof(decimal));
            dtLetture.Columns.Add("ConsumoEriM", typeof(decimal));
            dtLetture.Columns.Add("Valido", typeof(bool));
            dtLetture.Columns.Add("IdFile", typeof(int));
            dtLetture.Columns.Add("TimeStamp", typeof(string));
            dtLetture.PrimaryKey = new DataColumn[] { dtLetture.Columns["Id"] };

            //PRELIEVO ULTIMO ID LETTURA
            int IdLettura = 0;
            string queryId = @"SELECT IDENT_CURRENT('Letture') AS IdLettura";

            using (SqlCommand cmd = new SqlCommand(queryId, connessione))
            {
                object result = cmd.ExecuteScalar();
                IdLettura = Convert.ToInt32(result);
            }

            //Quartini
            DataTable QE;
            QE = new DataTable();
            QE.Columns.Add("Id", typeof(int)).AutoIncrement = true;
            QE.Columns.Add("IdLetture", typeof(int));
            QE.Columns.Add("IdFile", typeof(int));
            QE.Columns.Add("Giorno", typeof(int));
            QE.Columns.Add("Tipo", typeof(string)).MaxLength = 105;
            for (int i = 1; i <= 100; i++)
            {
                DataColumn col = new DataColumn($"E{i}", typeof(decimal))
                {
                    DefaultValue = 0m,
                    AllowDBNull = false
                };

                QE.Columns.Add(col);
            }
            QE.PrimaryKey = new DataColumn[] { QE.Columns["Id"] };

            //scrittura per Tabella FileXml
            DataTable FileXml = new DataTable();
            FileXml.Columns.Add("IdFile", typeof(int)).AutoIncrement = true;
            FileXml.Columns.Add("DataIns", typeof(DateTime));
            FileXml.Columns.Add("NomeFile", typeof(string)).MaxLength = 250;
            FileXml.Columns.Add("FileXml", typeof(string));
            FileXml.Columns.Add("Lavorato", typeof(bool));

            FileXml.PrimaryKey = new DataColumn[] { FileXml.Columns["Id"] };

            string piVaUtente = "";
            string piVaDistributore = "";
            string codContrDisp = "";
            string codiceFlusso = FlussoMisura.CodFlusso.ToString();

            for (int i = 0; i < FlussoMisura.IdentificativiFlusso.Items.Length; i++)
            {
                var name = FlussoMisura.IdentificativiFlusso.ItemsElementName[i];
                string value = FlussoMisura.IdentificativiFlusso.Items[i];
                if (name == Models.Periodico.ItemsChoiceType.PIvaUtente) piVaUtente = value;
                else if (name == Models.Periodico.ItemsChoiceType.PIvaDistributore) piVaDistributore = value;
                else if (name == Models.Periodico.ItemsChoiceType.CodContrDisp) codContrDisp = value;
            }

            // CICLO POD (Riempimento DataRow)
            for (int j = 0; j < FlussoMisura.DatiPod.Length; j++)
            {
                Models.Periodico.FlussoMisureDatiPod pod = FlussoMisura.DatiPod[j];

                // --- VARIABILE DINAMICA - mi sono vista costretta ad usare una variabile dinamica perchè 
                // le classi da prendere erano tante e con nomi diversi, in questo modo prendo tutto da una sola variabile 
                dynamic d = pod.Item;
                DettaglioConsumoV2Type consumo = d as DettaglioConsumoV2Type;
                IdLettura++;

                // CREAZIONE DATAROW LETTURE
                try
                {
                    DataRow dr = dtLetture.NewRow();
                    dr["Id"] = IdLettura;
                    dr["CodFlusso"] = codiceFlusso;
                    dr["PIvaUtente"] = piVaUtente;
                    dr["PIvaDistributore"] = piVaDistributore;
                    dr["CodContrDisp"] = codContrDisp;
                    dr["Pod"] = pod.Pod;
                    dr["MeseAnno"] = ParseMonthYearOrDbNull(pod.MeseAnno);
                    dr["DataMisura"] = ParseDateOrDbNull(pod?.DataMisura);
                    dr["DataPrest"] = ParseDateOrDbNull(pod?.DataPrest);
                    dr["CodPrat_SII"] = pod.CodPrat_SII ?? "";

                    //Campi Periodico
                    dr["TipoRettifica"] = (d.GetType().GetProperty("TipoRettifica") != null) ? d?.TipoRettifica : DBNull.Value;
                    dr["DataRilevazione"] = (d.GetType().GetProperty("DataRilevazione") != null) ? d?.DataRilevazione : DBNull.Value;
                    dr["Motivazione"] = /* (d.GetType().GetProperty("Motivazione") != null) ? d?.Motivazione : */ DBNull.Value;
                    dr["MisuraRaccolta"] = d?.Raccolta ?? DBNull.Value;
                    dr["MisuraTipoDato"] = d?.TipoDato ?? DBNull.Value;
                    dr["MisuraTipoCp"] = d?.TipoCp ?? DBNull.Value;
                    dr["MisuraCausaOstativa"] = d?.CausaOstativa ?? DBNull.Value;
                    dr["MisuraValidato"] = d?.Validato ?? DBNull.Value;
                    dr["Trattamento"] = (object)pod.DatiPdp?.Trattamento ?? DBNull.Value;
                    dr["Tensione"] = (object)pod.DatiPdp?.Tensione ?? DBNull.Value;
                    dr["Forfait"] = (object)pod.DatiPdp?.Forfait ?? DBNull.Value;
                    dr["GruppoMis"] = (object)pod.DatiPdp?.GruppoMis ?? DBNull.Value;

                    dr["Ka"] = ParseDecimalOrDbNull(pod.DatiPdp?.Ka);
                    dr["Kr"] = ParseDecimalOrDbNull(pod.DatiPdp?.Kr);
                    dr["Kp"] = ParseDecimalOrDbNull(pod.DatiPdp?.Kp);
                    dr["MisuraPotMax"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "PotMax")?.ToString());
                    dr["MisuraEaF1"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EaF1")?.ToString());
                    dr["MisuraEaF2"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EaF2")?.ToString());
                    dr["MisuraEaF3"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EaF3")?.ToString());
                    dr["MisuraEaF4"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EaF4")?.ToString());
                    dr["MisuraEaF5"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EaF5")?.ToString());
                    dr["MisuraEaF6"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EaF6")?.ToString());
                    dr["MisuraErF1"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErF1")?.ToString());
                    dr["MisuraErF2"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErF2")?.ToString());
                    dr["MisuraErF3"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErF3")?.ToString());
                    dr["MisuraErF4"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErF4")?.ToString());
                    dr["MisuraErF5"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErF5")?.ToString());
                    dr["MisuraErF6"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErF6")?.ToString());
                    dr["MisuraPotF1"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "PotF1")?.ToString());
                    dr["MisuraPotF2"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "PotF2")?.ToString());
                    dr["MisuraPotF3"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "PotF3")?.ToString());
                    dr["MisuraPotF4"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "PotF4")?.ToString());
                    dr["MisuraPotF5"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "PotF5")?.ToString());
                    dr["MisuraPotF6"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "PotF6")?.ToString());
                    dr["MisuraEaM"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EaM")?.ToString());
                    dr["MisuraPotM"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "PotM")?.ToString());
                    dr["MisuraErcF1"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErcF1")?.ToString());
                    dr["MisuraErcF2"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErcF2")?.ToString());
                    dr["MisuraErcF3"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErcF3")?.ToString());
                    dr["MisuraErcF4"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErcF4")?.ToString());
                    dr["MisuraErcF5"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErcF5")?.ToString());
                    dr["MisuraErcF6"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErcF6")?.ToString());
                    dr["MisuraErcM"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErcM")?.ToString());
                    dr["MisuraEriF1"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EriF1")?.ToString());
                    dr["MisuraEriF2"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EriF2")?.ToString());
                    dr["MisuraEriF3"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EriF3")?.ToString());
                    dr["MisuraEriF4"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EriF4")?.ToString());
                    dr["MisuraEriF5"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EriF5")?.ToString());
                    dr["MisuraEriF6"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EriF6")?.ToString());
                    dr["MisuraEriM"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EriM")?.ToString());
                    dr["MisuraErM"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErM")?.ToString());
                    dr["ConsumoDataInizioPeriodo"] = (object)consumo?.DataInizioPeriodo ?? DBNull.Value;
                    dr["ConsumoEaF1"] = DBNull.Value;
                    dr["ConsumoEaF2"] = DBNull.Value;
                    dr["ConsumoEaF3"] = DBNull.Value;
                    dr["ConsumoErF1"] = DBNull.Value;
                    dr["ConsumoErF2"] = DBNull.Value;
                    dr["ConsumoErF3"] = DBNull.Value;
                    dr["ConsumoPotF1"] = DBNull.Value;
                    dr["ConsumoPotF2"] = DBNull.Value;
                    dr["ConsumoPotF3"] = DBNull.Value;
                    dr["ConsumoEaM"] = ParseDecimalOrDbNull(GetPropOrDbNull(consumo, "EaM")?.ToString());
                    dr["ConsumoPotM"] = ParseDecimalOrDbNull(GetPropOrDbNull(consumo, "PotM")?.ToString());
                    dr["ConsumoErcF1"] = ParseDecimalOrDbNull(GetPropOrDbNull(consumo, "ErcF1")?.ToString());
                    dr["ConsumoErcF2"] = ParseDecimalOrDbNull(GetPropOrDbNull(consumo, "ErcF2")?.ToString());
                    dr["ConsumoErcF3"] = ParseDecimalOrDbNull(GetPropOrDbNull(consumo, "ErcF3")?.ToString());
                    dr["ConsumoErcF4"] = ParseDecimalOrDbNull(GetPropOrDbNull(consumo, "ErcF4")?.ToString());
                    dr["ConsumoErcF5"] = ParseDecimalOrDbNull(GetPropOrDbNull(consumo, "ErcF5")?.ToString());
                    dr["ConsumoErcF6"] = ParseDecimalOrDbNull(GetPropOrDbNull(consumo, "ErcF6")?.ToString());
                    dr["ConsumoErcM"] = ParseDecimalOrDbNull(GetPropOrDbNull(consumo, "ErcM")?.ToString());
                    dr["ConsumoEriF1"] = ParseDecimalOrDbNull(GetPropOrDbNull(consumo, "EriF1")?.ToString());
                    dr["ConsumoEriF2"] = ParseDecimalOrDbNull(GetPropOrDbNull(consumo, "EriF2")?.ToString());
                    dr["ConsumoEriF3"] = ParseDecimalOrDbNull(GetPropOrDbNull(consumo, "EriF3")?.ToString());
                    dr["ConsumoEriF4"] = ParseDecimalOrDbNull(GetPropOrDbNull(consumo, "EriF4")?.ToString());
                    dr["ConsumoEriF5"] = ParseDecimalOrDbNull(GetPropOrDbNull(consumo, "EriF5")?.ToString());
                    dr["ConsumoEriF6"] = ParseDecimalOrDbNull(GetPropOrDbNull(consumo, "EriF6")?.ToString());
                    dr["ConsumoEriM"] = ParseDecimalOrDbNull(GetPropOrDbNull(consumo, "EriM")?.ToString());
                    dr["ConsumoErM"] = ParseDecimalOrDbNull(GetPropOrDbNull(consumo, "ErM")?.ToString());
                    dr["TimeStamp"] = timeStamp;

                    dr["Valido"] = true;
                    dr["IdFile"] = IdFile;

                    dtLetture.Rows.Add(dr);
                }
                catch (Exception ex)
                {
                    HubLog.SaveLog2DB("Error", "SaveFlusso2DB", $"Errore durante la creazione della DataRow letture per il POD {pod.Pod}: {ex.Message}", connessione);
                }

                // CREAZIONE DATAROW QUARTINI
                try
                {
                    if (pod.Item is DettaglioMisuraPDOv2Type pdo)
                    {
                        MappaQuartini(QE, IdFile, IdLettura, "Ea", pdo.Ea);
                        MappaQuartini(QE, IdFile, IdLettura, "Er", pdo.Er);
                        MappaQuartini(QE, IdFile, IdLettura, "Erc", pdo.Erc);
                        MappaQuartini(QE, IdFile, IdLettura, "Eri", pdo.Eri);
                    }
                    else if (pod.Item is DettaglioMisuraPeriodico2GORType gor)
                    {
                        MappaQuartini(QE, IdFile, IdLettura, "Ea", gor.Ea);
                        MappaQuartini(QE, IdFile, IdLettura, "Er", gor.Er);
                        MappaQuartini(QE, IdFile, IdLettura, "Erc", gor.Erc);
                        MappaQuartini(QE, IdFile, IdLettura, "Eri", gor.Eri);
                    }
                    else if (pod.Item is DettaglioMisuraPeriodico2GRType gr)
                    {
                        MappaQuartini(QE, IdFile, IdLettura, "Ea", gr.Ea);
                        MappaQuartini(QE, IdFile, IdLettura, "Er", gr.Er);
                        MappaQuartini(QE, IdFile, IdLettura, "Erc", gr.Erc);
                        MappaQuartini(QE, IdFile, IdLettura, "Eri", gr.Eri);
                    }
                    else if (pod.Item is DettaglioMisuraPeriodicoIntType pint)
                    {
                        MappaQuartini(QE, IdFile, IdLettura, "Ea", pint.Ea);
                        MappaQuartini(QE, IdFile, IdLettura, "Er", pint.Er);
                        MappaQuartini(QE, IdFile, IdLettura, "Erc", pint.Erc);
                        MappaQuartini(QE, IdFile, IdLettura, "Eri", pint.Eri);

                    }
                }
                catch (Exception ex)
                {
                    HubLog.SaveLog2DB("Error", "SaveFlusso2DB", $"Errore durante la creazione della DataRow quartini per il POD {pod.Pod}: {ex.Message}", connessione);
                }
            }

            try
            {
                DataRow drFile = FileXml.NewRow();
                drFile["DataIns"] = DateTime.Now;
                drFile["NomeFile"] = fileName;
                drFile["FileXml"] = "";
                drFile["Lavorato"] = true;
                FileXml.Rows.Add(drFile);
            }
            catch (Exception ex)
            {
                HubLog.SaveLog2DB("Error", "SaveFlusso2DB", $"Errore durante la creazione della DataRow FileXml per il file {fileName}: {ex.Message}", connessione);
            }
            //Scrittura col bulk
            Bulk2DB(QE, "Curve", connessione);
            Bulk2DB(dtLetture, "Letture", connessione);
            Bulk2DB(FileXml, "FileXml", connessione);
            FileLavorato(FolderLavoro, fileName, connessione);
        }

        // Lavorazione del flusso Rettifico e salvataggio su DB
        public static void SaveFlusso2DB(Models.Rettifica.FlussoMisure FlussoRettifica, SqlConnection connessione, string FolderLavoro, int IdFile, string fileName)
        {
            if (FlussoRettifica == null || FlussoRettifica.DatiPod == null || FlussoRettifica.DatiPod.Length == 0)
                return;

            string[] arrName = fileName.Split('_');
            string timeStamp = arrName[4];

            //PREPARAZIONE DATATABLE
            DataTable dtLetture = new DataTable();

            dtLetture.Columns.Add("Id", typeof(int)).AutoIncrement = true;
            dtLetture.Columns.Add("CodFlusso", typeof(string));
            dtLetture.Columns.Add("PIvaUtente", typeof(string));
            dtLetture.Columns.Add("PIvaDistributore", typeof(string));
            dtLetture.Columns.Add("CodContrDisp", typeof(string));
            dtLetture.Columns.Add("Pod", typeof(string));
            dtLetture.Columns.Add("MeseAnno", typeof(DateTime));
            dtLetture.Columns.Add("DataMisura", typeof(DateTime));
            dtLetture.Columns.Add("TipoRettifica", typeof(string));
            dtLetture.Columns.Add("DataRilevazione", typeof(DateTime));
            dtLetture.Columns.Add("Motivazione", typeof(string));
            dtLetture.Columns.Add("DataPrest", typeof(DateTime));
            dtLetture.Columns.Add("CodPrat_SII", typeof(string));
            dtLetture.Columns.Add("MisuraRaccolta", typeof(string));
            dtLetture.Columns.Add("MisuraTipoDato", typeof(string));
            dtLetture.Columns.Add("MisuraTipoCp", typeof(string));
            dtLetture.Columns.Add("MisuraCausaOstativa", typeof(string));
            dtLetture.Columns.Add("MisuraValidato", typeof(string));
            dtLetture.Columns.Add("Trattamento", typeof(string));
            dtLetture.Columns.Add("Tensione", typeof(int));
            dtLetture.Columns.Add("Forfait", typeof(string));
            dtLetture.Columns.Add("GruppoMis", typeof(string));
            dtLetture.Columns.Add("Ka", typeof(decimal));
            dtLetture.Columns.Add("Kr", typeof(decimal));
            dtLetture.Columns.Add("Kp", typeof(decimal));
            dtLetture.Columns.Add("MisuraPotMax", typeof(decimal));
            dtLetture.Columns.Add("MisuraEaF1", typeof(decimal));
            dtLetture.Columns.Add("MisuraEaF2", typeof(decimal));
            dtLetture.Columns.Add("MisuraEaF3", typeof(decimal));
            dtLetture.Columns.Add("MisuraEaF4", typeof(decimal));
            dtLetture.Columns.Add("MisuraEaF5", typeof(decimal));
            dtLetture.Columns.Add("MisuraEaF6", typeof(decimal));
            dtLetture.Columns.Add("MisuraErF1", typeof(decimal));
            dtLetture.Columns.Add("MisuraErF2", typeof(decimal));
            dtLetture.Columns.Add("MisuraErF3", typeof(decimal));
            dtLetture.Columns.Add("MisuraErF4", typeof(decimal));
            dtLetture.Columns.Add("MisuraErF5", typeof(decimal));
            dtLetture.Columns.Add("MisuraErF6", typeof(decimal));
            dtLetture.Columns.Add("MisuraPotF1", typeof(decimal));
            dtLetture.Columns.Add("MisuraPotF2", typeof(decimal));
            dtLetture.Columns.Add("MisuraPotF3", typeof(decimal));
            dtLetture.Columns.Add("MisuraPotF4", typeof(decimal));
            dtLetture.Columns.Add("MisuraPotF5", typeof(decimal));
            dtLetture.Columns.Add("MisuraPotF6", typeof(decimal));
            dtLetture.Columns.Add("MisuraEaM", typeof(decimal));
            dtLetture.Columns.Add("MisuraErM", typeof(decimal));
            dtLetture.Columns.Add("MisuraPotM", typeof(decimal));
            dtLetture.Columns.Add("MisuraErcF1", typeof(decimal));
            dtLetture.Columns.Add("MisuraErcF2", typeof(decimal));
            dtLetture.Columns.Add("MisuraErcF3", typeof(decimal));
            dtLetture.Columns.Add("MisuraErcF4", typeof(decimal));
            dtLetture.Columns.Add("MisuraErcF5", typeof(decimal));
            dtLetture.Columns.Add("MisuraErcF6", typeof(decimal));
            dtLetture.Columns.Add("MisuraErcM", typeof(decimal));
            dtLetture.Columns.Add("MisuraEriF1", typeof(decimal));
            dtLetture.Columns.Add("MisuraEriF2", typeof(decimal));
            dtLetture.Columns.Add("MisuraEriF3", typeof(decimal));
            dtLetture.Columns.Add("MisuraEriF4", typeof(decimal));
            dtLetture.Columns.Add("MisuraEriF5", typeof(decimal));
            dtLetture.Columns.Add("MisuraEriF6", typeof(decimal));
            dtLetture.Columns.Add("MisuraEriM", typeof(decimal));
            dtLetture.Columns.Add("ConsumoDataInizioPeriodo", typeof(DateTime));
            dtLetture.Columns.Add("ConsumoEaF1", typeof(decimal));
            dtLetture.Columns.Add("ConsumoEaF2", typeof(decimal));
            dtLetture.Columns.Add("ConsumoEaF3", typeof(decimal));
            dtLetture.Columns.Add("ConsumoErF1", typeof(decimal));
            dtLetture.Columns.Add("ConsumoErF2", typeof(decimal));
            dtLetture.Columns.Add("ConsumoErF3", typeof(decimal));
            dtLetture.Columns.Add("ConsumoPotF1", typeof(decimal));
            dtLetture.Columns.Add("ConsumoPotF2", typeof(decimal));
            dtLetture.Columns.Add("ConsumoPotF3", typeof(decimal));
            dtLetture.Columns.Add("ConsumoEaM", typeof(decimal));
            dtLetture.Columns.Add("ConsumoErM", typeof(decimal));
            dtLetture.Columns.Add("ConsumoPotM", typeof(decimal));
            dtLetture.Columns.Add("ConsumoErcF1", typeof(decimal));
            dtLetture.Columns.Add("ConsumoErcF2", typeof(decimal));
            dtLetture.Columns.Add("ConsumoErcF3", typeof(decimal));
            dtLetture.Columns.Add("ConsumoErcF4", typeof(decimal));
            dtLetture.Columns.Add("ConsumoErcF5", typeof(decimal));
            dtLetture.Columns.Add("ConsumoErcF6", typeof(decimal));
            dtLetture.Columns.Add("ConsumoErcM", typeof(decimal));
            dtLetture.Columns.Add("ConsumoEriF1", typeof(decimal));
            dtLetture.Columns.Add("ConsumoEriF2", typeof(decimal));
            dtLetture.Columns.Add("ConsumoEriF3", typeof(decimal));
            dtLetture.Columns.Add("ConsumoEriF4", typeof(decimal));
            dtLetture.Columns.Add("ConsumoEriF5", typeof(decimal));
            dtLetture.Columns.Add("ConsumoEriF6", typeof(decimal));
            dtLetture.Columns.Add("ConsumoEriM", typeof(decimal));
            dtLetture.Columns.Add("Valido", typeof(bool));
            dtLetture.Columns.Add("IdFile", typeof(string));
            dtLetture.Columns.Add("TimeStamp", typeof(string));
            dtLetture.PrimaryKey = new DataColumn[] { dtLetture.Columns["Id"] };


            //PRELIEVO ULTIMO ID LETTURA
            int IdLettura = 0;

            string queryId = @"SELECT IDENT_CURRENT('Letture') AS IdLettura";

            using (SqlCommand cmd = new SqlCommand(queryId, connessione))
            {
                object result = cmd.ExecuteScalar();
                IdLettura = Convert.ToInt32(result);
            }

            //tabella CURVE
            DataTable QE;
            QE = new DataTable();
            QE.Columns.Add("Id", typeof(int)).AutoIncrement = true;
            QE.Columns.Add("IdLetture", typeof(int));
            QE.Columns.Add("IdFile", typeof(int));
            QE.Columns.Add("Giorno", typeof(int));
            QE.Columns.Add("Tipo", typeof(string)).MaxLength = 105;
            for (int i = 1; i <= 100; i++)
            {
                DataColumn col = new DataColumn($"E{i}", typeof(decimal))
                {
                    DefaultValue = 0m,
                    AllowDBNull = false
                };
                QE.Columns.Add(col);
            }

            QE.PrimaryKey = new DataColumn[] { QE.Columns["Id"] };

            //scrittura per Tabella Xml
            DataTable FileXml = new DataTable();
            FileXml.Columns.Add("IdFile", typeof(int)).AutoIncrement = true;
            FileXml.Columns.Add("DataIns", typeof(DateTime));
            FileXml.Columns.Add("NomeFile", typeof(string)).MaxLength = 250;
            FileXml.Columns.Add("FileXml", typeof(string));
            FileXml.Columns.Add("Lavorato", typeof(bool));

            string piVaUtente = "";
            string piVaDistributore = "";
            string codContrDisp = "";
            string codiceFlusso = FlussoRettifica.CodFlusso.ToString();
            string nPod = "";
            object DataMisure = new object();

            // PRELIEVO IDENTIFICATIVI FLUSSO (PIvaUtente, PIvaDistributore, CodContrDisp)
            for (int i = 0; i < FlussoRettifica.IdentificativiFlusso.Items.Length; i++)
            {
                var name = FlussoRettifica.IdentificativiFlusso.ItemsElementName[i];
                string value = FlussoRettifica.IdentificativiFlusso.Items[i];
                if (name == Models.Rettifica.ItemsChoiceType.PIvaUtente) piVaUtente = value;
                else if (name == Models.Rettifica.ItemsChoiceType.PIvaDistributore) piVaDistributore = value;
                else if (name == Models.Rettifica.ItemsChoiceType.CodContrDisp) codContrDisp = value;
            }

            // CICLO POD (Riempimento DataRow)
            for (int j = 0; j < FlussoRettifica.DatiPod.Length; j++)
            {
                Models.Rettifica.FlussoMisureDatiPod pod = FlussoRettifica.DatiPod[j];

                // VARIABILE DINAMICA UNICA - come sopra, alcuni campi come EaF1
                // si chiamano così oppure EaF1int sono in sottoclassi differenti
                dynamic d = pod.Item;

                //Variabili Consumo
                var consumoRv2 = pod.Item as Models.Rettifica.DettaglioConsumoRv2Type;
                var consumoRv2Imm = pod.Item as Models.Rettifica.DettaglioConsumoRv2IntImmType;
                IdLettura++;
                //Stringhe
                string tipoRettifica = pod.TipoRettifica.ToString() ?? "";

                DataRow dr = dtLetture.NewRow();

                //Campi comuni a tutti i POD
                dr["Id"] = IdLettura;
                dr["CodFlusso"] = codiceFlusso;
                dr["PIvaUtente"] = piVaUtente;
                dr["PIvaDistributore"] = piVaDistributore;
                dr["CodContrDisp"] = codContrDisp;
                dr["Pod"] = pod.Pod;
                nPod = pod.Pod;
                dr["MeseAnno"] = ParseMonthYearOrDbNull(pod.MeseAnno);

                dr["DataMisura"] = ParseDateOrDbNull(pod?.DataMisura);

                DataMisure = dr["DataMisura"];

                dr["DataRilevazione"] = ParseDateOrDbNull(pod.DataRilevazione);
                dr["DataPrest"] = ParseDateOrDbNull(pod.DataPrest);
                dr["TipoRettifica"] = tipoRettifica ?? (object)DBNull.Value;
                //dr["Motivazione"] = pod?.Motivazione.ToString().Replace("Item", "0") ?? (object)DBNull.Value;
                dr["Motivazione"] = GetXmlEnumValue(pod.Motivazione);
                dr["CodPrat_SII"] = pod.CodPrat_SII ?? (object)DBNull.Value;
                dr["Trattamento"] = (object)pod.DatiPdp?.Trattamento ?? DBNull.Value;
                dr["Forfait"] = (object)pod.DatiPdp?.Forfait ?? DBNull.Value;
                dr["GruppoMis"] = (object)pod.DatiPdp?.GruppoMis ?? DBNull.Value;
                dr["Tensione"] = (object)pod.DatiPdp?.Tensione ?? DBNull.Value;
                dr["Ka"] = ParseDecimalOrDbNull(GetPropOrDbNull(pod.DatiPdp, "Ka")?.ToString());
                dr["Kr"] = ParseDecimalOrDbNull(GetPropOrDbNull(pod.DatiPdp, "Kr")?.ToString());
                dr["Kp"] = ParseDecimalOrDbNull(GetPropOrDbNull(pod.DatiPdp, "Kp")?.ToString());

                //misure
                dr["MisuraEaF1"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EaF1")?.ToString());
                dr["MisuraEaF2"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EaF2")?.ToString());
                dr["MisuraEaF3"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EaF3")?.ToString());
                dr["MisuraEaF4"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EaF4")?.ToString());
                dr["MisuraEaF5"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EaF5")?.ToString());
                dr["MisuraEaF6"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EaF6")?.ToString());
                dr["MisuraErF1"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErF1")?.ToString());
                dr["MisuraErF2"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErF2")?.ToString());
                dr["MisuraErF3"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErF3")?.ToString());
                dr["MisuraErF4"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErF4")?.ToString());
                dr["MisuraErF5"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErF5")?.ToString());
                dr["MisuraErF6"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErF6")?.ToString());
                dr["MisuraPotF1"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "PotF1")?.ToString());
                dr["MisuraPotF2"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "PotF2")?.ToString());
                dr["MisuraPotF3"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "PotF3")?.ToString());
                dr["MisuraPotF4"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "PotF4")?.ToString());
                dr["MisuraPotF5"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "PotF5")?.ToString());
                dr["MisuraPotF6"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "PotF6")?.ToString());
                dr["MisuraEaM"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EaM")?.ToString());
                dr["MisuraPotMax"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "PotMax")?.ToString());
                //erc
                dr["MisuraErcF1"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErcF1")?.ToString());
                dr["MisuraErcF2"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErcF2")?.ToString());
                dr["MisuraErcF3"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErcF3")?.ToString());
                dr["MisuraErcF4"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErcF4")?.ToString());
                dr["MisuraErcF5"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErcF5")?.ToString());
                dr["MisuraErcF6"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErcF6")?.ToString());
                dr["MisuraErcM"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErcM")?.ToString());
                //eri
                dr["MisuraEriF1"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EriF1")?.ToString());
                dr["MisuraEriF2"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EriF2")?.ToString());
                dr["MisuraEriF3"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EriF3")?.ToString());
                dr["MisuraEriF4"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EriF4")?.ToString());
                dr["MisuraEriF5"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EriF5")?.ToString());
                dr["MisuraEriF6"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EriF6")?.ToString());
                dr["MisuraEriM"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EriM")?.ToString());
                dr["MisuraErM"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "ErM")?.ToString());

                dr["MisuraEaM"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EaM")?.ToString());
                dr["MisuraPotM"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "PotM")?.ToString());

                dr["ConsumoDataInizioPeriodo"] = ParseDateOrDbNull(consumoRv2?.DataInizioPeriodo ?? consumoRv2Imm?.DataInizioPeriodo);
                dr["ConsumoEaF1"] = ParseDecimalOrDbNull(consumoRv2?.EaF1).ToString();
                dr["ConsumoEaF2"] = ParseDecimalOrDbNull(consumoRv2?.EaF2).ToString();
                dr["ConsumoEaF3"] = ParseDecimalOrDbNull(consumoRv2?.EaF3).ToString();
                dr["ConsumoErF1"] = ParseDecimalOrDbNull(consumoRv2?.ErF1).ToString();
                dr["ConsumoErF2"] = ParseDecimalOrDbNull(consumoRv2?.ErF2).ToString();
                dr["ConsumoErF3"] = ParseDecimalOrDbNull(consumoRv2?.ErF3).ToString();
                dr["ConsumoPotF1"] = ParseDecimalOrDbNull(consumoRv2?.PotF1).ToString();
                dr["ConsumoPotF2"] = ParseDecimalOrDbNull(consumoRv2?.PotF2).ToString();
                dr["ConsumoPotF3"] = ParseDecimalOrDbNull(consumoRv2?.PotF3).ToString();
                dr["ConsumoEaM"] = ParseDecimalOrDbNull(consumoRv2?.EaM).ToString();
                dr["ConsumoPotM"] = ParseDecimalOrDbNull(consumoRv2?.PotM).ToString();
                dr["ConsumoErcF1"] = ParseDecimalOrDbNull(consumoRv2?.ErcF1).ToString();
                dr["ConsumoErcF2"] = ParseDecimalOrDbNull(consumoRv2?.ErcF2).ToString();
                dr["ConsumoErcF3"] = ParseDecimalOrDbNull(consumoRv2?.ErcF3).ToString();
                dr["ConsumoErcM"] = ParseDecimalOrDbNull(consumoRv2?.ErcM).ToString();
                dr["ConsumoEriF1"] = ParseDecimalOrDbNull(consumoRv2?.EriF1).ToString();
                dr["ConsumoEriF2"] = ParseDecimalOrDbNull(consumoRv2?.EriF2).ToString();
                dr["ConsumoEriF3"] = ParseDecimalOrDbNull(consumoRv2?.EriF3).ToString();
                dr["ConsumoEaM"] = ParseDecimalOrDbNull(consumoRv2?.EaM).ToString();
                dr["ConsumoPotM"] = ParseDecimalOrDbNull(consumoRv2?.PotM).ToString();
                dr["ConsumoErcF1"] = ParseDecimalOrDbNull(consumoRv2?.ErcF1).ToString();
                dr["ConsumoErcF2"] = ParseDecimalOrDbNull(consumoRv2?.ErcF2).ToString();
                dr["ConsumoErcF3"] = ParseDecimalOrDbNull(consumoRv2?.ErcF3).ToString();
                dr["ConsumoErcM"] = ParseDecimalOrDbNull(consumoRv2?.EriF1).ToString();
                dr["ConsumoEriF1"] = ParseDecimalOrDbNull(consumoRv2?.EriF1).ToString();
                dr["ConsumoEriF2"] = ParseDecimalOrDbNull(consumoRv2?.EriF2).ToString();
                dr["ConsumoEriF3"] = ParseDecimalOrDbNull(consumoRv2?.EriF3).ToString();
                dr["ConsumoEaM"] = ParseDecimalOrDbNull(consumoRv2?.EaM).ToString();
                dr["ConsumoEriM"] = ParseDecimalOrDbNull(consumoRv2?.EriM ?? consumoRv2Imm?.EriMint).ToString();
                dr["ConsumoEriM"] = ParseDecimalOrDbNull(consumoRv2?.EriM ?? consumoRv2Imm?.EriMint).ToString();
                dr["ConsumoEriM"] = ParseDecimalOrDbNull(consumoRv2?.EriM ?? consumoRv2Imm?.EriMint).ToString();
                dr["TimeStamp"] = timeStamp;

                dr["Valido"] = true;
                dr["IdFile"] = IdFile;

                dtLetture.Rows.Add(dr);

                //Quartini Rettifiche
                if (pod.Item is Models.Rettifica.DettaglioMisuraRRType rr)
                {
                    MappaQuartini(QE, IdFile, IdLettura, "Ea", rr.Ea);
                    MappaQuartini(QE, IdFile, IdLettura, "Er", rr.Er);
                    MappaQuartini(QE, IdFile, IdLettura, "Erc", rr.Erc);
                    MappaQuartini(QE, IdFile, IdLettura, "Eri", rr.Eri);
                }
                else if (pod.Item is Models.Rettifica.DettaglioMisuraRRIntType rrint)
                {
                    MappaQuartini(QE, IdFile, IdLettura, "Ea", rrint.Ea);
                    MappaQuartini(QE, IdFile, IdLettura, "Er", rrint.Er);
                    MappaQuartini(QE, IdFile, IdLettura, "Erc", rrint.Erc);
                    MappaQuartini(QE, IdFile, IdLettura, "Eri", rrint.Eri);
                }
                else if (pod.Item is Models.Rettifica.DettaglioMisuraRNRType rnr)
                {
                    MappaQuartini(QE, IdFile, IdLettura, "Ea", rnr.Ea);
                    MappaQuartini(QE, IdFile, IdLettura, "Er", rnr.Er);
                    MappaQuartini(QE, IdFile, IdLettura, "Erc", rnr.Erc);
                    MappaQuartini(QE, IdFile, IdLettura, "Eri", rnr.Eri);
                }
                else if (pod.Item is Models.Rettifica.DettaglioMisuraRORType ror)
                {
                    MappaQuartini(QE, IdFile, IdLettura, "Ea", ror.Ea);
                    MappaQuartini(QE, IdFile, IdLettura, "Er", ror.Er);
                    MappaQuartini(QE, IdFile, IdLettura, "Erc", ror.Erc);
                    MappaQuartini(QE, IdFile, IdLettura, "Eri", ror.Eri);
                }
                else if (pod.Item is Models.Rettifica.DettaglioMisuraRFOv2Type rfov2)
                {
                    MappaQuartini(QE, IdFile, IdLettura, "Ea", rfov2.Ea);
                    MappaQuartini(QE, IdFile, IdLettura, "Er", rfov2.Er);
                    MappaQuartini(QE, IdFile, IdLettura, "Erc", rfov2.Erc);
                    MappaQuartini(QE, IdFile, IdLettura, "Eri", rfov2.Eri);
                }

                ControllaRettifica.IsRettificato(connessione, piVaUtente, piVaDistributore, nPod, DataMisure);
            }

            DataRow drFile = FileXml.NewRow();
            drFile["DataIns"] = DateTime.Now;
            drFile["NomeFile"] = fileName;
            drFile["FileXml"] = "";
            drFile["Lavorato"] = true;

            FileXml.Rows.Add(drFile);

            //Scrittura col Bulk
            Bulk2DB(dtLetture, "Letture", connessione);
            Bulk2DB(QE, "Curve", connessione);
            Bulk2DB(FileXml, "FileXml", connessione);

            FileLavorato(FolderLavoro, fileName, connessione);
        }

        public static void SaveFlusso2DB(Models.Smis.FlussoMisure FlussoSmis, SqlConnection connessione, string FolderLavoro, int IdFile, string fileName)
        {
            if (FlussoSmis == null || FlussoSmis.DatiPod == null || FlussoSmis.DatiPod.Length == 0)
                return;
            string[] arrName = fileName.Split('_');
            string timeStamp = arrName[4];

            //PREPARAZIONE DATATABLE
            //LettureSmisEE
            DataTable dtLettureSmisEE = new DataTable();

            dtLettureSmisEE.Columns.Add("Id", typeof(int)).AutoIncrement = true;
            dtLettureSmisEE.Columns.Add("CodFlusso", typeof(string));
            dtLettureSmisEE.Columns.Add("PIvaUtente", typeof(string));
            dtLettureSmisEE.Columns.Add("PIvaDistributore", typeof(string));
            dtLettureSmisEE.Columns.Add("CodContrDisp", typeof(string));
            dtLettureSmisEE.Columns.Add("Pod", typeof(string));
            dtLettureSmisEE.Columns.Add("Motivazione", typeof(string));
            dtLettureSmisEE.Columns.Add("TipologiaMisuratoreSmontaggio", typeof(string));
            dtLettureSmisEE.Columns.Add("DataMisuraSmontaggio", typeof(DateTime));
            dtLettureSmisEE.Columns.Add("SmontaggioTipoDato", typeof(string));
            dtLettureSmisEE.Columns.Add("SmontaggioEaF1", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioEaF2", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioEaF3", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioEaF4", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioEaF5", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioEaF6", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioErF1", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioErF2", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioErF3", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioErF4", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioErF5", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioErF6", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioPotF1", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioPotF2", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioPotF3", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioPotF4", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioPotF5", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioPotF6", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioEaM", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioErM", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioPotM", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioErcF1", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioErcF2", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioErcF3", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioErcF4", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioErcF5", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioErcF6", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioErcM", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioEriF1", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioEriF2", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioEriF3", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioEriF4", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioEriF5", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioEriF6", typeof(decimal));
            dtLettureSmisEE.Columns.Add("SmontaggioEriM", typeof(decimal));
            dtLettureSmisEE.Columns.Add("TipologiaMisuratoreMontaggio", typeof(string));
            dtLettureSmisEE.Columns.Add("DataMisuraMontaggio", typeof(DateTime));
            dtLettureSmisEE.Columns.Add("DataMessaReggime", typeof(DateTime));
            dtLettureSmisEE.Columns.Add("Tensione", typeof(int));
            dtLettureSmisEE.Columns.Add("Ka", typeof(decimal));
            dtLettureSmisEE.Columns.Add("Kr", typeof(decimal));
            dtLettureSmisEE.Columns.Add("Kp", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MatrAtt", typeof(string));
            dtLettureSmisEE.Columns.Add("MatrRea", typeof(string));
            dtLettureSmisEE.Columns.Add("MatrPot", typeof(string));
            dtLettureSmisEE.Columns.Add("CifreAtt", typeof(string));
            dtLettureSmisEE.Columns.Add("CifreRea", typeof(string));
            dtLettureSmisEE.Columns.Add("CifrePot", typeof(string));
            dtLettureSmisEE.Columns.Add("MontaggioEaF1", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioEaF2", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioEaF3", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioEaF4", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioEaF5", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioEaF6", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioErF1", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioErF2", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioErF3", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioErF4", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioErF5", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioErF6", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioPotF1", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioPotF2", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioPotF3", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioPotF4", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioPotF5", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioPotF6", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioEaM", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioErM", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioPotM", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioErcF1", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioErcF2", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioErcF3", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioErcF4", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioErcF5", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioErcF6", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioErcM", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioEriF1", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioEriF2", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioEriF3", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioEriF4", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioEriF5", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioEriF6", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MontaggioEriM", typeof(decimal));
            dtLettureSmisEE.Columns.Add("MeseAnno", typeof(DateTime));
            dtLettureSmisEE.Columns.Add("IdFile", typeof(int));
            dtLettureSmisEE.Columns.Add("TimeStamp", typeof(string));
            dtLettureSmisEE.Columns.Add("Valido", typeof(bool));
            

            dtLettureSmisEE.PrimaryKey = new DataColumn[] { dtLettureSmisEE.Columns["Id"] };

            //PRELIEVO ULTIMO ID LETTURA
            int IdLettura = 0;

            string queryId = @"SELECT IDENT_CURRENT('LettureSmisEE') AS IdLettura";

            using (SqlCommand cmd = new SqlCommand(queryId, connessione))
            {
                object result = cmd.ExecuteScalar();
                IdLettura = Convert.ToInt32(result);
            }

            // CICLO POD (Riempimento DataRow)
            for (int j = 0; j < FlussoSmis.DatiPod.Length; j++)
            {
                Models.Smis.FlussoMisureDatiPod pod = FlussoSmis.DatiPod[j];
                IdLettura++;

                DataRow dr = dtLettureSmisEE.NewRow();

                dr["Id"] = IdLettura;
                dr["CodFlusso"] = FlussoSmis.CodFlusso;
                dr["PIvaUtente"] = FlussoSmis.IdentificativiFlusso.PIvaUtente;
                dr["PIvaDistributore"] = FlussoSmis.IdentificativiFlusso.PIvaDistributore;
                dr["CodContrDisp"] = FlussoSmis.IdentificativiFlusso.CodContrDisp;
                dr["Pod"] = pod.Pod;
                dr["Motivazione"] = GetXmlEnumValue(pod.Motivazione);
                dr["TipologiaMisuratoreSmontaggio"] = pod.Smontaggio.TipoMisuratore.ToString();
                dr["DataMisuraSmontaggio"] = pod.Smontaggio.DataMisura;
                dr["SmontaggioTipoDato"] = pod.Smontaggio.TipoDato;

                //Metodo dinamico 
                for (int i = 0; i < pod.Smontaggio.Items.Length; i++)
                {
                    string nomeColonna = $"Smontaggio{pod.Smontaggio.ItemsElementName[i]}";

                    if (dtLettureSmisEE.Columns.Contains(nomeColonna))
                    {
                        dr[nomeColonna] = pod.Smontaggio.Items[i];
                    }
                }

                dr["TipologiaMisuratoreMontaggio"] = pod.Montaggio.TipoMisuratore.ToString();
                dr["DataMisuraMontaggio"] = pod.Montaggio.DataMisura;
                DateTime data;
                dr["DataMessaReggime"] = DateTime.TryParse(pod.Montaggio.DataMessaRegime2G, out data)? (object)data: DBNull.Value;
                dr["Tensione"] = pod.Montaggio.Tensione;
                dr["Ka"] = ParseDecimalOrDbNull(pod.Montaggio.Ka?.ToString());
                dr["Kr"] = ParseDecimalOrDbNull(pod.Montaggio.Kr?.ToString());
                dr["Kp"] = ParseDecimalOrDbNull(pod.Montaggio.Kp?.ToString()); 
                dr["MatrAtt"] = pod.Montaggio.MatrAtt;
                dr["MatrRea"] = pod.Montaggio.MatrRea;
                dr["MatrPot"] = pod.Montaggio.MatrPot;
                dr["CifreAtt"] = pod.Montaggio.CifreAtt;
                dr["CifreRea"] = pod.Montaggio.CifreRea;
                dr["CifrePot"] = pod.Montaggio.CifrePot;

                //metodo dinamico
                for (int i = 0; i < pod.Montaggio.Items.Length; i++)
                {
                    string nomeColonna = $"Montaggio{pod.Montaggio.ItemsElementName[i]}";

                    if (dtLettureSmisEE.Columns.Contains(nomeColonna))
                    {
                        dr[nomeColonna] = pod.Montaggio.Items[i];
                    }
                }

                dr["MeseAnno"] = DBNull.Value;
                dr["IdFile"] = IdFile;
                dr["TimeStamp"] = timeStamp;
                dr["Valido"] = true;

                dtLettureSmisEE.Rows.Add(dr);
            }

            //scrittura per Tabella Xml
            DataTable FileXml = new DataTable();
            FileXml.Columns.Add("IdFile", typeof(int)).AutoIncrement = true;
            FileXml.Columns.Add("DataIns", typeof(DateTime));
            FileXml.Columns.Add("NomeFile", typeof(string)).MaxLength = 250;
            FileXml.Columns.Add("FileXml", typeof(string));
            FileXml.Columns.Add("Lavorato", typeof(bool));

            DataRow drFile = FileXml.NewRow();
            drFile["DataIns"] = DateTime.Now;
            drFile["NomeFile"] = fileName;
            drFile["FileXml"] = "";
            drFile["Lavorato"] = true;

            FileXml.Rows.Add(drFile);

            //Scrittura col Bulk
            Bulk2DB(dtLettureSmisEE, "LettureSmisEE", connessione);
            Bulk2DB(FileXml, "FileXml", connessione);
            FileLavorato(FolderLavoro, fileName, connessione);
        }

        // Metodo per mappare i quartini in QE, con gestione dinamica delle proprietà e parsing dei valori
        private static void MappaQuartini(DataTable dt, int IdFile, int IdLettura, string tipo, IEnumerable<object> listaQuartini)
        {
            if (listaQuartini == null) return;

            foreach (object e in listaQuartini)
            {
                if (e == null)
                    continue;

                DataRow row = dt.NewRow();
                row["IdLetture"] = IdLettura;
                row["IdFile"] = IdFile;
                row["Giorno"] = e.GetType().GetProperty("Value")?.GetValue(e);
                row["Tipo"] = tipo;

                for (int i = 1; i <= 100; i++)
                {
                    string propName = "E" + i;
                    PropertyInfo prop = e.GetType().GetProperty(propName);
                    if (prop == null)
                    {
                        row[propName] = 0m;
                        continue;
                    }

                    string rawValue = prop.GetValue(e)?.ToString();
                    row[propName] = ParseDecimalOrDbNull(rawValue);
                }
                dt.Rows.Add(row);
            }
        }

        // Metodo generico per scrivere DataTable in DB con SqlBulkCopy
        public static void Bulk2DB(DataTable dtLetture, string nomeTabella, SqlConnection connessione)
        {
            try
            {
                if (dtLetture.Rows.Count > 0)
                {
                    using (SqlBulkCopy bulk = new SqlBulkCopy(connessione))
                    {
                        bulk.DestinationTableName = nomeTabella;
                        var sb = new StringBuilder();
                        foreach (DataColumn col in dtLetture.Columns)
                        {
                            bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            sb.AppendFormat("{0}:{1};", col.ColumnName, col.DataType.Name);
                        }

                        //foreach (DataColumn c in dtLetture.Columns)
                        //{
                        //    object v = dtLetture.Rows[0][c];
                        //}

                        bulk.WriteToServer(dtLetture);
                    }
                }
            }
            //catch (SqlException se)
            //{
            //    HubLog.SaveLog2DB("Error", $"Bulk tab:{nomeTabella}", se.Message, connessione);
            //}
            catch (Exception ex)
            {
                HubLog.SaveLog2DB("INFO", $"Bulk tab:{nomeTabella}", ex.Message, connessione);
            }
            
        }
        // Metodi di supporto per parsing e gestione valori nulli
        private static object ParseDateOrDbNull(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return DBNull.Value;

            if (DateTime.TryParseExact(
                value.Trim(),
                "dd/MM/yyyy",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out DateTime result))
            {
                return result;
            }

            return DBNull.Value;
        }
        private static object ParseMonthYearOrDbNull(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return DBNull.Value;

            if (DateTime.TryParseExact(
                value.Trim(),
                "MM/yyyy",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out DateTime result))
            {
                return result;
            }

            return DBNull.Value;
        }

        private static decimal ParseDecimalOrDbNull(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return 0m;

            if (decimal.TryParse(value, NumberStyles.Any, new CultureInfo("it-IT"), out decimal result))
                return result;

            return 0m;
        }

        

        /* Metodo generico per ottenere il valore di una proprietà da un oggetto,
        restituendo DBNull.Value se l'oggetto o la proprietà sono null*/
        private static object GetPropOrDbNull(object source, string propName)
        {
            if (source == null) return DBNull.Value;

            PropertyInfo prop = source.GetType().GetProperty(propName);
            if (prop == null) return DBNull.Value;

            return prop.GetValue(source) ?? DBNull.Value;
        }

        // Metodo per eliminare il file dopo la lavorazione e loggare l'evento
        private static void FileLavorato(string folderLavoro, string namefile, SqlConnection connessione)
        {
            File.Delete(Path.Combine(folderLavoro, namefile));
            HubLog.SaveLog2DB("INFO", $"File lavorato: {namefile}", "", connessione);
        }
        public static string GetXmlEnumValue(MotivazioneType value)
        {
            var field = value.GetType().GetField(value.ToString());

            var attr = field?
                .GetCustomAttribute<XmlEnumAttribute>();

            return attr?.Name; 
        }
        public static string GetXmlEnumValue(MotivazioneSostType value)
        {
            var field = value.GetType().GetField(value.ToString());

            var attr = field?
                .GetCustomAttribute<XmlEnumAttribute>();

            return attr?.Name; 
        }
    }
}
