using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Reflection;
using Vendita.HubMisureEE.Models.Periodico;

namespace Vendita.HubMisureEE.Services
{
    internal class SaveFlusso
    {
        public static void SaveFlusso2DB(Models.Periodico.FlussoMisure FlussoMisura, SqlConnection connessione, string FolderLavoro, int IdFile, string fileName)
        {

            if (FlussoMisura == null || FlussoMisura.DatiPod == null || FlussoMisura.DatiPod.Length == 0)
                return;

            string[] arrName = fileName.Split('_');
            string timeStamp = arrName[4];

            // Preparazione tabella Letture
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
            dtLetture.PrimaryKey = new DataColumn[] { dtLetture.Columns["ID"] };

            int IdLettura = 0;
            string queryId = @"SELECT IDENT_CURRENT('Letture') AS IdLettura";

            try
            {
                using (SqlCommand cmd = new SqlCommand(queryId, connessione))
                {
                    object result = cmd.ExecuteScalar();
                    IdLettura = Convert.ToInt32(result);
                }
            }
            catch { }

            //Preparazione tabella quartini
            DataTable QE;
            QE = new DataTable();
            QE.Columns.Add("Id", typeof(int)).AutoIncrement = true;
            QE.Columns.Add("IdLetture", typeof(int));
            QE.Columns.Add("IdFile", typeof(int));
            QE.Columns.Add("Giorno", typeof(int));
            QE.Columns.Add("Tipo", typeof(string)).MaxLength = 105;
            for (int i = 1; i <= 100; i++)
                try
                {
                    {
                        DataColumn col = new DataColumn($"E{i}", typeof(decimal))
                        {
                            DefaultValue = 0m,
                            AllowDBNull = false
                        };

                        QE.Columns.Add(col);
                    }
                }
                catch { }
            QE.PrimaryKey = new DataColumn[] { QE.Columns["Id"] };

            //Preparazione Tabella FileXml
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
                try
                {
                    {
                        var name = FlussoMisura.IdentificativiFlusso.ItemsElementName[i];
                        string value = FlussoMisura.IdentificativiFlusso.Items[i];
                        if (name == ItemsChoiceType.PIvaUtente) piVaUtente = value;
                        else if (name == ItemsChoiceType.PIvaDistributore) piVaDistributore = value;
                        else if (name == ItemsChoiceType.CodContrDisp) codContrDisp = value;
                    }
                }
                catch { }

            // Riempimento tabella Letture
            for (int j = 0; j < FlussoMisura.DatiPod.Length; j++)
            {
                try
                {
                    FlussoMisureDatiPod pod = FlussoMisura.DatiPod[j];
                    dynamic d = pod.Item;
                    DettaglioMisuraNOv2Type misuraNOv2 = d as DettaglioMisuraNOv2Type;
                    DettaglioConsumoV2Type consumo = d as DettaglioConsumoV2Type;
                    IdLettura++;

                    DataRow dr = dtLetture.NewRow();
                    dr["Id"] = IdLettura;
                    dr["CodFlusso"] = codiceFlusso;
                    dr["PIvaUtente"] = piVaUtente ?? "";
                    dr["PIvaDistributore"] = piVaDistributore ?? "";
                    dr["CodContrDisp"] = codContrDisp ?? "";
                    dr["Pod"] = pod.Pod ?? "";
                    dr["MeseAnno"] = ParseMonthYearOrDbNull(pod.MeseAnno);
                    dr["DataMisura"] = ParseDateOrDbNull(pod?.DataMisura);
                    dr["DataPrest"] = ParseDateOrDbNull(pod?.DataPrest);
                    dr["CodPrat_SII"] = pod.CodPrat_SII ?? "";
                    dr["TipoRettifica"] = (d.GetType().GetProperty("TipoRettifica") != null) ? d?.TipoRettifica ?? DBNull.Value : ((d.GetType().GetProperty("TipoRettifica") != null) ? d?.TipoRettifica ?? DBNull.Value : DBNull.Value);
                    dr["DataRilevazione"] = (d.GetType().GetProperty("DataRilevazione") != null) ? d?.DataRilevazione ?? DBNull.Value : ((d.GetType().GetProperty("DataRilevazione") != null) ? d?.DataRilevazione ?? DBNull.Value : DBNull.Value);
                    dr["Motivazione"] = (d.GetType().GetProperty("Motivazione") != null) ? d?.Motivazione ?? DBNull.Value : ((d.GetType().GetProperty("Motivazione") != null) ? d?.Motivazione ?? DBNull.Value : DBNull.Value);
                    dr["MisuraRaccolta"] = (object)d?.Raccolta ?? DBNull.Value;
                    dr["MisuraTipoDato"] = d?.TipoDato != null ? d.TipoDato.ToString() : DBNull.Value;
                    dr["MisuraTipoCp"] = d?.TipoCp != null ? d.TipoCp.ToString() : DBNull.Value;
                    dr["MisuraCausaOstativa"] = d?.CausaOstativa != null ? d.CausaOstativa.ToString() : DBNull.Value;
                    dr["MisuraValidato"] = d?.Validato != null ? d.Validato.ToString() : DBNull.Value;
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

                    // Riempimento tabella quartini
                    if (pod.Item is DettaglioMisuraPDOv2Type pdo)
                        try
                        {
                            {
                                MappaQuartini(QE, IdFile, IdLettura, "Ea", pdo.Ea);
                                MappaQuartini(QE, IdFile, IdLettura, "Er", pdo.Er);
                                MappaQuartini(QE, IdFile, IdLettura, "Erc", pdo.Erc);
                                MappaQuartini(QE, IdFile, IdLettura, "Eri", pdo.Eri);
                            }
                        }
                        catch { }
                    else if (pod.Item is DettaglioMisuraPeriodico2GORType gor)
                        try
                        {
                            {
                                MappaQuartini(QE, IdFile, IdLettura, "Ea", gor.Ea);
                                MappaQuartini(QE, IdFile, IdLettura, "Er", gor.Er);
                                MappaQuartini(QE, IdFile, IdLettura, "Erc", gor.Erc);
                                MappaQuartini(QE, IdFile, IdLettura, "Eri", gor.Eri);
                            }
                        }
                        catch { }
                    else if (pod.Item is DettaglioMisuraPeriodico2GRType gr)
                        try
                        {
                            {
                                MappaQuartini(QE, IdFile, IdLettura, "Ea", gr.Ea);
                                MappaQuartini(QE, IdFile, IdLettura, "Er", gr.Er);
                                MappaQuartini(QE, IdFile, IdLettura, "Erc", gr.Erc);
                                MappaQuartini(QE, IdFile, IdLettura, "Eri", gr.Eri);
                            }
                        }
                        catch { }
                    else if (pod.Item is DettaglioMisuraPeriodicoIntType pint)
                        try
                        {
                            {
                                MappaQuartini(QE, IdFile, IdLettura, "Ea", pint.Ea);
                                MappaQuartini(QE, IdFile, IdLettura, "Er", pint.Er);
                                MappaQuartini(QE, IdFile, IdLettura, "Erc", pint.Erc);
                                MappaQuartini(QE, IdFile, IdLettura, "Eri", pint.Eri);

                            }
                        }
                        catch { }
                }
                catch { }
            }
                // Riempimento tabella FileXml
                DataRow drFile = FileXml.NewRow();
                drFile["DataIns"] = DateTime.Now;
                drFile["NomeFile"] = fileName;
                drFile["FileXml"] = "";
                // drFile["FileXml"] = File.ReadAllText(Path.Combine(FolderLavoro, fileName));
                drFile["Lavorato"] = true;
                FileXml.Rows.Add(drFile);
            try
            {
                //Scrittura col bulk
                Bulk2DB(QE, "Curve", connessione);
                Bulk2DB(dtLetture, "Letture", connessione);
                Bulk2DB(FileXml, "FileXml", connessione);
                FileLavorato(FolderLavoro, fileName);
            }
            catch { }
        }
        public static void SaveFlusso2DB(Models.Rettifica.FlussoMisure FlussoRettifica, SqlConnection connessione, string FolderLavoro, int IdFile, string fileName)
        {
            if (FlussoRettifica == null || FlussoRettifica.DatiPod == null || FlussoRettifica.DatiPod.Length == 0)
                return;

            string[] arrName = fileName.Split('_');
            string timeStamp = arrName[4];

            // Preparazione tabella Letture
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
            dtLetture.PrimaryKey = new DataColumn[] { dtLetture.Columns["ID"] };

            int IdLettura = 0;
            string queryId = @"SELECT IDENT_CURRENT('Letture') AS IdLettura";

            try
            {
                using (SqlCommand cmd = new SqlCommand(queryId, connessione))
                {
                    object result = cmd.ExecuteScalar();
                    IdLettura = Convert.ToInt32(result);
                }
            }
            catch { }

            string piVaUtente = "";
            string piVaDistributore = "";
            string codContrDisp = "";
            string codiceFlusso = FlussoRettifica.CodFlusso.ToString();
            string nPod = "";
            DateTime DataMisure = new DateTime();

            for (int i = 0; i < FlussoRettifica.IdentificativiFlusso.Items.Length; i++)
            {
                var name = FlussoRettifica.IdentificativiFlusso.ItemsElementName[i];
                string value = FlussoRettifica.IdentificativiFlusso.Items[i] as string;
                if (name == Models.Rettifica.ItemsChoiceType.PIvaUtente) piVaUtente = value;
                else if (name == Models.Rettifica.ItemsChoiceType.PIvaDistributore) piVaDistributore = value;
                else if (name == Models.Rettifica.ItemsChoiceType.CodContrDisp) codContrDisp = value;
            }

            //Preparazione tabella quartini
            DataTable QE;
            QE = new DataTable();
            QE.Columns.Add("Id", typeof(int)).AutoIncrement = true;
            QE.Columns.Add("IdLetture", typeof(int));
            QE.Columns.Add("IdFile", typeof(int));
            QE.Columns.Add("Giorno", typeof(int));
            QE.Columns.Add("Tipo", typeof(string)).MaxLength = 105;
            for (int i = 1; i <= 100; i++)
                try
                {
                    {
                        DataColumn col = new DataColumn($"E{i}", typeof(decimal))
                        {
                            DefaultValue = 0m,
                            AllowDBNull = false
                        };
                        QE.Columns.Add(col);
                    }
                }
                catch { }
            QE.PrimaryKey = new DataColumn[] { QE.Columns["Id"] };

            // Riempimento tabella Letture
            for (int j = 0; j < FlussoRettifica.DatiPod.Length; j++)
            {

                Models.Rettifica.FlussoMisureDatiPod pod = FlussoRettifica.DatiPod[j];
                dynamic d = pod.Item;

                //Variabili misure e consumi
                var misuraRR = pod.Item as Models.Rettifica.DettaglioMisuraRRType;
                var misuraRRIntImm = pod.Item as Models.Rettifica.DettaglioMisuraRRIntImmType;
                var misuraRRInt = pod.Item as Models.Rettifica.DettaglioMisuraRRIntType;
                var misuraRNR = pod.Item as Models.Rettifica.DettaglioMisuraRNRType;
                var misuraRSNR = pod.Item as Models.Rettifica.DettaglioMisuraRSNRType;
                var misuraRNOv2 = pod.Item as Models.Rettifica.DettaglioMisuraRNOv2Type;
                var consumoRv2 = pod.Item as Models.Rettifica.DettaglioConsumoRv2Type;
                var consumoRv2Imm = pod.Item as Models.Rettifica.DettaglioConsumoRv2IntImmType;
                IdLettura++;
                string tipoRettifica = pod.TipoRettifica.ToString() ?? "";

                try
                {

                    DataRow dr = dtLetture.NewRow();

                    dr["Id"] = IdLettura;
                    dr["CodFlusso"] = codiceFlusso;
                    dr["PIvaUtente"] = piVaUtente;
                    dr["PIvaDistributore"] = piVaDistributore;
                    dr["CodContrDisp"] = codContrDisp;
                    dr["Pod"] = pod.Pod;
                    nPod = pod.Pod;
                    dr["MeseAnno"] = ParseMonthYearOrDbNull(pod.MeseAnno);
                    dr["DataMisura"] = ParseDateOrDbNull(pod?.DataMisura);
                    dr["DataRilevazione"] = ParseDateOrDbNull(pod.DataRilevazione);
                    dr["DataPrest"] = ParseDateOrDbNull(pod.DataPrest);
                    dr["TipoRettifica"] = tipoRettifica.Length > 1 ? tipoRettifica.Substring(0, 1) : tipoRettifica;
                    dr["Motivazione"] = (pod.Motivazione.ToString().Length > 2) ? pod.Motivazione.ToString().Substring(0, 2).Replace("Item", "0") : (object)pod.Motivazione.ToString().Replace("Item", "0") ?? DBNull.Value;
                    dr["CodPrat_SII"] = (pod.CodPrat_SII?.Length > 15) ? pod.CodPrat_SII.Substring(0, 15) : (pod.CodPrat_SII ?? "");
                    dr["Trattamento"] = (pod.DatiPdp?.Trattamento.ToString().Length > 1) ? pod.DatiPdp.Trattamento.ToString().Substring(0, 1) : (object)pod.DatiPdp?.Trattamento ?? DBNull.Value;
                    dr["Forfait"] = (pod.DatiPdp?.Forfait.ToString().Length > 2) ? pod.DatiPdp.Forfait.ToString().Substring(0, 2) : (object)pod.DatiPdp?.Forfait ?? DBNull.Value;
                    dr["GruppoMis"] = (pod.DatiPdp?.GruppoMis.ToString().Length > 2) ? pod.DatiPdp.GruppoMis.ToString().Substring(0, 2) : (object)pod.DatiPdp?.GruppoMis ?? DBNull.Value;
                    dr["Tensione"] = (object)pod.DatiPdp?.Tensione ?? DBNull.Value;
                    dr["Ka"] = ParseDecimalOrDbNull(GetPropOrDbNull(pod.DatiPdp, "Ka")?.ToString());
                    dr["Kr"] = ParseDecimalOrDbNull(GetPropOrDbNull(pod.DatiPdp, "Kr")?.ToString());
                    dr["Kp"] = ParseDecimalOrDbNull(GetPropOrDbNull(pod.DatiPdp, "Kp")?.ToString());
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
                    dr["MisuraEaM"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "EaM")?.ToString());
                    dr["MisuraPotM"] = ParseDecimalOrDbNull(GetPropOrDbNull(d, "PotM")?.ToString());
                    dr["ConsumoDataInizioPeriodo"] = ParseDateOrDbNull(consumoRv2?.DataInizioPeriodo ?? consumoRv2Imm?.DataInizioPeriodo);
                    dr["ConsumoEaF1"] = (object)consumoRv2?.EaF1?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoEaF2"] = (object)consumoRv2?.EaF2?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoEaF3"] = (object)consumoRv2?.EaF3?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoErF1"] = (object)consumoRv2?.ErF1?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoErF2"] = (object)consumoRv2?.ErF2?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoErF3"] = (object)consumoRv2?.ErF3?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoPotF1"] = (object)consumoRv2?.PotF1?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoPotF2"] = (object)consumoRv2?.PotF2?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoPotF3"] = (object)consumoRv2?.PotF3?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoEaM"] = (object)consumoRv2?.EaM?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoPotM"] = (object)consumoRv2?.PotM?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoErcF1"] = (object)consumoRv2?.ErcF1?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoErcF2"] = (object)consumoRv2?.ErcF2?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoErcF3"] = (object)consumoRv2?.ErcF3?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoErcM"] = (object)consumoRv2?.EriF1?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoEriF1"] = (object)consumoRv2?.EriF1?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoEriF2"] = (object)consumoRv2?.EriF2?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoEriF3"] = (object)consumoRv2?.EriF3?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoEaM"] = (object)consumoRv2?.EaM?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoPotM"] = (object)consumoRv2?.PotM?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoErcF1"] = (object)consumoRv2?.ErcF1?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoErcF2"] = (object)consumoRv2?.ErcF2?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoErcF3"] = (object)consumoRv2?.ErcF3?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoErcM"] = (object)consumoRv2?.EriF1?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoEriF1"] = (object)consumoRv2?.EriF1?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoEriF2"] = (object)consumoRv2?.EriF2?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoEriF3"] = (object)consumoRv2?.EriF3?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoEaM"] = (object)consumoRv2?.EaM?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoEriM"] = (object)consumoRv2?.EriM?.Replace(",", ".") ?? (object)consumoRv2Imm?.EriMint?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoEriM"] = (object)consumoRv2?.EriM?.Replace(",", ".") ?? (object)consumoRv2Imm?.EriMint?.Replace(",", ".") ?? DBNull.Value;
                    dr["ConsumoEriM"] = (object)consumoRv2?.EriM?.Replace(",", ".") ?? (object)consumoRv2Imm?.EriMint?.Replace(",", ".") ?? DBNull.Value;
                    dr["TimeStamp"] = timeStamp;
                    dr["Valido"] = true;
                    dr["IdFile"] = IdFile;

                    dtLetture.Rows.Add(dr);
                }
                catch { }

                // Riempimento tabella quartini
                if (pod.Item is Models.Rettifica.DettaglioMisuraRRType rr)
                    try
                    {
                        {
                            MappaQuartini(QE, IdFile, IdLettura, "Ea", rr.Ea);
                            MappaQuartini(QE, IdFile, IdLettura, "Er", rr.Er);
                            MappaQuartini(QE, IdFile, IdLettura, "Erc", rr.Erc);
                            MappaQuartini(QE, IdFile, IdLettura, "Eri", rr.Eri);
                        }
                    }
                    catch { }
                else if (pod.Item is Models.Rettifica.DettaglioMisuraRRIntType rrint)
                    try
                    {
                        {
                            MappaQuartini(QE, IdFile, IdLettura, "Ea", rrint.Ea);
                            MappaQuartini(QE, IdFile, IdLettura, "Er", rrint.Er);
                            MappaQuartini(QE, IdFile, IdLettura, "Erc", rrint.Erc);
                            MappaQuartini(QE, IdFile, IdLettura, "Eri", rrint.Eri);
                        }
                    }
                    catch { }
                else if (pod.Item is Models.Rettifica.DettaglioMisuraRNRType rnr)
                    try
                    {
                        {
                            MappaQuartini(QE, IdFile, IdLettura, "Ea", rnr.Ea);
                            MappaQuartini(QE, IdFile, IdLettura, "Er", rnr.Er);
                            MappaQuartini(QE, IdFile, IdLettura, "Erc", rnr.Erc);
                            MappaQuartini(QE, IdFile, IdLettura, "Eri", rnr.Eri);
                        }
                    }
                    catch { }
                else if (pod.Item is Models.Rettifica.DettaglioMisuraRORType ror)
                    try
                    {
                        {
                            MappaQuartini(QE, IdFile, IdLettura, "Ea", ror.Ea);
                            MappaQuartini(QE, IdFile, IdLettura, "Er", ror.Er);
                            MappaQuartini(QE, IdFile, IdLettura, "Erc", ror.Erc);
                            MappaQuartini(QE, IdFile, IdLettura, "Eri", ror.Eri);
                        }
                    }
                    catch { }
                else if (pod.Item is Models.Rettifica.DettaglioMisuraRFOv2Type rfov2)
                    try
                    {
                        {
                            MappaQuartini(QE, IdFile, IdLettura, "Ea", rfov2.Ea);
                            MappaQuartini(QE, IdFile, IdLettura, "Er", rfov2.Er);
                            MappaQuartini(QE, IdFile, IdLettura, "Erc", rfov2.Erc);
                            MappaQuartini(QE, IdFile, IdLettura, "Eri", rfov2.Eri);
                        }
                    }
                    catch { }
            }

            // Riempimento tabella FileXml
            DataTable FileXml = new DataTable();
            FileXml.Columns.Add("IdFile", typeof(int)).AutoIncrement = true;
            FileXml.Columns.Add("DataIns", typeof(DateTime));
            FileXml.Columns.Add("NomeFile", typeof(string)).MaxLength = 250;
            FileXml.Columns.Add("FileXml", typeof(string));
            FileXml.Columns.Add("Lavorato", typeof(bool));

            try
            {
                DataRow drFile = FileXml.NewRow();
                drFile["DataIns"] = DateTime.Now;
                drFile["NomeFile"] = fileName;
                drFile["FileXml"] = "";
                drFile["Lavorato"] = true;

                FileXml.Rows.Add(drFile);
            }
            catch { }

            try
            {   //scrittura con Bulk
                Bulk2DB(dtLetture, "Letture", connessione);
                Bulk2DB(QE, "Curve", connessione);
                Bulk2DB(FileXml, "FileXml", connessione);
                ControllaRettifica.IsRettificato(connessione, piVaUtente, piVaDistributore, nPod, DataMisure);
                FileLavorato(FolderLavoro, fileName);

            }
            catch { }
        }
        private static void MappaQuartini(DataTable dt, int IdFile, int IdLettura, string tipo, IEnumerable<object> listaQuartini)
        {
            if (listaQuartini == null) return;
            try
            {

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
            catch { }
        }
        public static void Bulk2DB(DataTable dtLetture, string nomeTabella, SqlConnection connessione)
        {
            try
            {
                if (dtLetture.Rows.Count > 0)
                {
                    using (SqlBulkCopy bulk = new SqlBulkCopy(connessione))
                    {
                        bulk.DestinationTableName = nomeTabella;
                        foreach (DataColumn col in dtLetture.Columns)
                            bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);

                        bulk.WriteToServer(dtLetture);
                    }
                }
            }
            catch (SqlException se)
            {
                HubLog.SaveLog2DB("Error", $"Bulk tab:{nomeTabella}", se.Message, connessione);
            }
            catch (Exception ex)
            {
                HubLog.SaveLog2DB("INFO", $"Bulk tab:{nomeTabella}", ex.Message, connessione);
            }
        }
        private static object ParseDateOrDbNull(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
                try
                {
                    return DBNull.Value;
                }
                catch { }

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
                try { return DBNull.Value; } catch { }

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
        private static object ParseDataMisure(string value, int DataMisura)
        {
            if (string.IsNullOrWhiteSpace(value))

                try
                {
                    return DBNull.Value;
                }
                catch { }
            {

                if (DateTime.TryParseExact(
                value.Trim(),
                "MM/yyyy",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out DateTime result))
                {
                    return new DateTime(result.Year, result.Month, DataMisura);
                }

                return DBNull.Value;
            }
        }

        private static object GetPropOrDbNull(object source, string propName)
        {
            if (source == null)
                try { return DBNull.Value; } catch { }

            PropertyInfo prop = source.GetType().GetProperty(propName);
            if (prop == null)
                try
                {
                    return DBNull.Value;
                }
                catch { }

            return prop.GetValue(source) ?? DBNull.Value;
        }
        private static decimal ParseDecimalOrDbNull(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return 0m;

            if (decimal.TryParse(value, NumberStyles.Any, new CultureInfo("it-IT"), out decimal result))
                return result;

            return 0m;
        }
        private static object ParseDataMisureSafe(string meseAnno, int giorno)
        {
            if (string.IsNullOrWhiteSpace(meseAnno))

                try { return DBNull.Value; } catch { }

            if (!DateTime.TryParseExact(
                meseAnno.Trim(),
                "MM/yyyy",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out DateTime result))
            {
                return DBNull.Value;
            }

            if (giorno < 1 || giorno > DateTime.DaysInMonth(result.Year, result.Month))
                return DBNull.Value;

            return new DateTime(result.Year, result.Month, giorno);
        }

        private static void FileLavorato(string folderLavoro, string namefile)
        {
            try
            {
                File.Delete(Path.Combine(folderLavoro, namefile));
            }
            catch
            {
                HubLog.SaveLog2DB("INFO", $"File lavorato: {namefile}", "", "");
            }
        }
    }
}