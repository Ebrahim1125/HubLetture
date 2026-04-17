using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Globalization;
using System.Reflection;

namespace Vendita.HubMisureEE.Services
{
    internal class SaveFlusso
    {
        public static void SaveFlusso2DB(Models.Periodico.FlussoMisure FlussoMisura, SqlConnection connessione, string FolderLavoro, int idLetture, string fileName)
        {
            if (FlussoMisura == null || FlussoMisura.DatiPod == null || FlussoMisura.DatiPod.Length == 0)
                return;

            // 1. PREPARAZIONE DATATABLE
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
            dtLetture.Columns.Add("IdLetture", typeof(int));
            dtLetture.PrimaryKey = new DataColumn[] { dtLetture.Columns["ID"] };

            int IdLettura = 0;
            string queryId = @"SELECT IDENT_CURRENT('Letture') AS IdLettura ";

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
            FileXml.Columns.Add("IdLetture", typeof(int)).AutoIncrement = true;
            FileXml.Columns.Add("DataIns", typeof(DateTime));
            FileXml.Columns.Add("NomeFile", typeof(string)).MaxLength = 250;
            FileXml.Columns.Add("FileXml", typeof(string));
            FileXml.Columns.Add("Lavorato", typeof(bool));

            FileXml.PrimaryKey = new DataColumn[] { FileXml.Columns["Id"] };

            DataRow drFile = FileXml.NewRow();
            drFile["DataIns"] = DateTime.Now;
            drFile["NomeFile"] = fileName;
            drFile["FileXml"] = File.ReadAllText(Path.Combine(FolderLavoro, fileName));
            drFile["Lavorato"] = true;

            FileXml.Rows.Add(drFile);

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

            // 2. CICLO POD (Riempimento DataRow)
            for (int j = 0; j < FlussoMisura.DatiPod.Length; j++)
            {
                Models.Periodico.FlussoMisureDatiPod pod = FlussoMisura.DatiPod[j];

                // --- VARIABILE DINAMICA - mi sono vista costretta ad usare una variabile dinamica perchè 
                // le classi da prendere erano tante e con nomi diversi, in questo modo prendo tutto da una sola variabile 
                dynamic d = pod.Item;
                var misuraNOv2 = pod.Item as Models.Periodico.DettaglioMisuraNOv2Type;
                var consumo = pod.Item as Models.Periodico.DettaglioConsumoV2Type;
                IdLettura++;

                DataRow dr = dtLetture.NewRow();
                dr["Id"] = IdLettura;
                dr["CodFlusso"] = codiceFlusso;
                dr["PIvaUtente"] = piVaUtente ?? "";
                dr["PIvaDistributore"] = piVaDistributore ?? "";
                dr["CodContrDisp"] = codContrDisp ?? "";
                dr["Pod"] = pod.Pod ?? "";
                dr["MeseAnno"] = ParseMonthYearOrDbNull(pod.MeseAnno);
                dr["DataMisura"] = ParseDateOrDbNull(pod.DataMisura);
                dr["DataPrest"] = ParseDateOrDbNull(pod.DataPrest);
                dr["CodPrat_SII"] = pod.CodPrat_SII ?? "";

                //Campi Rettifica (forse si possono togliere qui)
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
                dr["Ka"] = (object)pod.DatiPdp?.Ka?.Replace(",", ".") ?? DBNull.Value;
                dr["Kr"] = (object)pod.DatiPdp?.Kr?.Replace(",", ".") ?? DBNull.Value;
                dr["Kp"] = (object)pod.DatiPdp?.Kp?.Replace(",", ".") ?? DBNull.Value;
                dr["MisuraPotMax"] = ParseDecimalOrDbNull(GetPropOrDbNull(pod.Item, "PotMax")?.ToString());
                dr["MisuraEaF1"] = (d.GetType().GetProperty("EaF1") != null) ? d?.EaF1 ?? DBNull.Value : DBNull.Value; //(object)d?.EaF1 ?? DBNull.Value;
                dr["MisuraEaF2"] = (d.GetType().GetProperty("EaF2") != null) ? d?.EaF2 ?? DBNull.Value : DBNull.Value;//(object)d?.EaF2 ?? DBNull.Value;
                dr["MisuraEaF3"] = (d.GetType().GetProperty("EaF3") != null) ? d?.EaF3 ?? DBNull.Value : DBNull.Value;//(object)d?.EaF3 ?? DBNull.Value;
                dr["MisuraEaF4"] = (d.GetType().GetProperty("EaF4") != null) ? d?.EaF4 ?? DBNull.Value : DBNull.Value;
                dr["MisuraEaF5"] = (d.GetType().GetProperty("EaF5") != null) ? d?.EaF5 ?? DBNull.Value : DBNull.Value;
                dr["MisuraEaF6"] = (d.GetType().GetProperty("EaF6") != null) ? d?.EaF6 ?? DBNull.Value : DBNull.Value;
                dr["MisuraErF1"] = (d.GetType().GetProperty("ErF1") != null) ? d?.ErF1 ?? DBNull.Value : DBNull.Value;
                dr["MisuraErF2"] = (d.GetType().GetProperty("ErF2") != null) ? d?.ErF2 ?? DBNull.Value : DBNull.Value;
                dr["MisuraErF3"] = (d.GetType().GetProperty("ErF3") != null) ? d?.ErF3 ?? DBNull.Value : DBNull.Value;
                dr["MisuraErF4"] = (d.GetType().GetProperty("ErF4") != null) ? d?.ErF4 ?? DBNull.Value : DBNull.Value;
                dr["MisuraErF5"] = (d.GetType().GetProperty("ErF5") != null) ? d?.ErF5 ?? DBNull.Value : DBNull.Value;
                dr["MisuraErF6"] = (d.GetType().GetProperty("ErF6") != null) ? d?.ErF6 ?? DBNull.Value : DBNull.Value;
                dr["MisuraPotF1"] = (d.GetType().GetProperty("PotF1") != null) ? d?.PotF1 ?? DBNull.Value : DBNull.Value;
                dr["MisuraPotF2"] = (d.GetType().GetProperty("PotF2") != null) ? d?.PotF2 ?? DBNull.Value : DBNull.Value;
                dr["MisuraPotF3"] = (d.GetType().GetProperty("PotF3") != null) ? d?.PotF3 ?? DBNull.Value : DBNull.Value;
                dr["MisuraPotF4"] = (d.GetType().GetProperty("PotF4") != null) ? d?.PotF4 ?? DBNull.Value : DBNull.Value;
                dr["MisuraPotF5"] = (d.GetType().GetProperty("PotF5") != null) ? d?.PotF5 ?? DBNull.Value : DBNull.Value;
                dr["MisuraPotF6"] = (d.GetType().GetProperty("PotF6") != null) ? d?.PotF6 ?? DBNull.Value : DBNull.Value;
                dr["MisuraEaM"] = (d.GetType().GetProperty("EaM") != null) ? d?.EaM ?? DBNull.Value : DBNull.Value;
                dr["MisuraPotM"] = (d.GetType().GetProperty("PotM") != null) ? d?.PotM ?? DBNull.Value : DBNull.Value;
                dr["MisuraErcF1"] = (d.GetType().GetProperty("ErcF1") != null) ? d?.ErcF1 ?? DBNull.Value : DBNull.Value;
                dr["MisuraErcF2"] = (d.GetType().GetProperty("ErcF2") != null) ? d?.ErcF2 ?? DBNull.Value : DBNull.Value;
                dr["MisuraErcF3"] = (d.GetType().GetProperty("ErcF3") != null) ? d?.ErcF3 ?? DBNull.Value : DBNull.Value;
                dr["MisuraErcF4"] = (d.GetType().GetProperty("ErcF4") != null) ? d?.ErcF4 ?? DBNull.Value : DBNull.Value;
                dr["MisuraErcF5"] = (d.GetType().GetProperty("ErcF5") != null) ? d?.ErcF5 ?? DBNull.Value : DBNull.Value;
                dr["MisuraErcF6"] = (d.GetType().GetProperty("ErcF6") != null) ? d?.ErcF6 ?? DBNull.Value : DBNull.Value;
                dr["MisuraErcM"] = (d.GetType().GetProperty("ErcM") != null) ? d?.ErcM ?? DBNull.Value : DBNull.Value;
                dr["MisuraEriF1"] = (d.GetType().GetProperty("EriF1") != null) ? d?.EriF1 ?? DBNull.Value : DBNull.Value;
                dr["MisuraEriF2"] = (d.GetType().GetProperty("EriF2") != null) ? d?.EriF2 ?? DBNull.Value : DBNull.Value;
                dr["MisuraEriF3"] = (d.GetType().GetProperty("EriF3") != null) ? d?.EriF3 ?? DBNull.Value : DBNull.Value;
                dr["MisuraEriF4"] = (d.GetType().GetProperty("EriF4") != null) ? d?.EriF4 ?? DBNull.Value : DBNull.Value;
                dr["MisuraEriF5"] = (d.GetType().GetProperty("EriF5") != null) ? d?.EriF5 ?? DBNull.Value : DBNull.Value;
                dr["MisuraEriF6"] = (d.GetType().GetProperty("EriF6") != null) ? d?.EriF6 ?? DBNull.Value : DBNull.Value;
                dr["MisuraEriM"] = (d.GetType().GetProperty("EriM") != null) ? d?.EriM ?? DBNull.Value : DBNull.Value;
                dr["MisuraErM"] = (d.GetType().GetProperty("ErM") != null) ? d?.ErM ?? DBNull.Value : DBNull.Value;
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
                dr["ConsumoEaM"] = (d.GetType().GetProperty("EaM") != null) ? d?.EaM ?? DBNull.Value : DBNull.Value;
                dr["ConsumoPotM"] = (d.GetType().GetProperty("PotM") != null) ? d?.PotM ?? DBNull.Value : DBNull.Value;
                dr["ConsumoErcF1"] = (d.GetType().GetProperty("ErcF1") != null) ? d?.ErcF1 ?? DBNull.Value : DBNull.Value;
                dr["ConsumoErcF2"] = (d.GetType().GetProperty("ErcF2") != null) ? d?.ErcF2 ?? DBNull.Value : DBNull.Value;
                dr["ConsumoErcF3"] = (d.GetType().GetProperty("ErcF3") != null) ? d?.ErcF3 ?? DBNull.Value : DBNull.Value;
                dr["ConsumoErcF4"] = (d.GetType().GetProperty("ErcF4") != null) ? d?.ErcF4 ?? DBNull.Value : DBNull.Value;
                dr["ConsumoErcF5"] = (d.GetType().GetProperty("ErcF5") != null) ? d?.ErcF5 ?? DBNull.Value : DBNull.Value;
                dr["ConsumoErcF6"] = (d.GetType().GetProperty("ErcF6") != null) ? d?.ErcF6 ?? DBNull.Value : DBNull.Value;
                dr["ConsumoErcM"] = (d.GetType().GetProperty("ErcM") != null) ? d?.ErcM ?? DBNull.Value : DBNull.Value;
                dr["ConsumoEriF1"] = (d.GetType().GetProperty("EriF1") != null) ? d?.EriF1 ?? DBNull.Value : DBNull.Value;
                dr["ConsumoEriF2"] = (d.GetType().GetProperty("EriF2") != null) ? d?.EriF2 ?? DBNull.Value : DBNull.Value;
                dr["ConsumoEriF3"] = (d.GetType().GetProperty("EriF3") != null) ? d?.EriF3 ?? DBNull.Value : DBNull.Value;
                dr["ConsumoEriF4"] = (d.GetType().GetProperty("EriF4") != null) ? d?.EriF4 ?? DBNull.Value : DBNull.Value;
                dr["ConsumoEriF5"] = (d.GetType().GetProperty("EriF5") != null) ? d?.EriF5 ?? DBNull.Value : DBNull.Value;
                dr["ConsumoEriF6"] = (d.GetType().GetProperty("EriF6") != null) ? d?.EriF6 ?? DBNull.Value : DBNull.Value;
                dr["ConsumoEriM"] = (d.GetType().GetProperty("EriM") != null) ? d?.EriM ?? DBNull.Value : DBNull.Value;
                dr["ConsumoErM"] = (d.GetType().GetProperty("ErM") != null) ? d?.ErM ?? DBNull.Value : DBNull.Value;

                dr["Valido"] = true;
                dr["IdLetture"] = idLetture;

                dtLetture.Rows.Add(dr);

                //Quartini Rettifiche
                //Models.Periodico.EnergiaType[] Ea = d?.Ea;
                //Models.Periodico.EnergiaType[] Er = d?.Er;
                //Models.Periodico.EnergiaType[] Erc = d?.Erc;
                //Models.Periodico.EnergiaType[] Eri = d?.Eri;

                //MappaQuartini(QE, IdLettura, "Ea", Ea);
                //MappaQuartini(QE, IdLettura, "Er", Er);
                //MappaQuartini(QE, IdLettura, "Erc", Erc);
                //MappaQuartini(QE, IdLettura, "Eri", Eri);

                if (pod.Item is Models.Periodico.DettaglioMisuraPDOv2Type pdo)
                {
                    MappaQuartini(QE, IdLettura, "Ea", pdo.Ea);
                    MappaQuartini(QE, IdLettura, "Er", pdo.Er);
                    MappaQuartini(QE, IdLettura, "Erc", pdo.Erc);
                    MappaQuartini(QE, IdLettura, "Eri", pdo.Eri);
                }
                else if (pod.Item is Models.Periodico.DettaglioMisuraPeriodico2GORType gor)
                {
                    MappaQuartini(QE, IdLettura, "Ea", gor.Ea);
                    MappaQuartini(QE, IdLettura, "Er", gor.Er);
                    MappaQuartini(QE, IdLettura, "Erc", gor.Erc);
                    MappaQuartini(QE, IdLettura, "Eri", gor.Eri);
                }
                else if (pod.Item is Models.Periodico.DettaglioMisuraPeriodico2GRType gr)
                {
                    MappaQuartini(QE, IdLettura, "Ea", gr.Ea);
                    MappaQuartini(QE, IdLettura, "Er", gr.Er);
                    MappaQuartini(QE, IdLettura, "Erc", gr.Erc);
                    MappaQuartini(QE, IdLettura, "Eri", gr.Eri);
                }
                else if (pod.Item is Models.Periodico.DettaglioMisuraPeriodicoIntType pint)
                {
                    MappaQuartini(QE, IdLettura, "Ea", pint.Ea);
                    MappaQuartini(QE, IdLettura, "Er", pint.Er);
                    MappaQuartini(QE, IdLettura, "Erc", pint.Erc);
                    MappaQuartini(QE, IdLettura, "Eri", pint.Eri);

                }
            }
            //Scrittura col bulk
            Bulk2DB(QE, "Curve", connessione);
            Bulk2DB(dtLetture, "Letture", connessione);
            Bulk2DB(FileXml, "FileXml", connessione);
        }
        public static void SaveFlusso2DB(Models.Rettifica.FlussoMisure FlussoRettifica, SqlConnection connessione, string FolderLavoro, int idLetture, string fileName)
        {
            if (FlussoRettifica == null || FlussoRettifica.DatiPod == null || FlussoRettifica.DatiPod.Length == 0)
                return;

            // 1. PREPARAZIONE DATATABLE
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
            dtLetture.Columns.Add("IdLetture", typeof(string));
            dtLetture.PrimaryKey = new DataColumn[] { dtLetture.Columns["ID"] };


            //va cambiato IdLettura?
            int IdLettura = 0;

            string queryId = @"SELECT IDENT_CURRENT('Letture') AS IdLettura ";

            using (SqlCommand cmd = new SqlCommand(queryId, connessione))
            {
                object result = cmd.ExecuteScalar();
                IdLettura = Convert.ToInt32(result);
            }

            // 4. tabella CURVE
            DataTable QE;
            QE = new DataTable();
            QE.Columns.Add("Id", typeof(int)).AutoIncrement = true;
            QE.Columns.Add("IdLetture", typeof(int));
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
            FileXml.Columns.Add("IdLetture", typeof(int)).AutoIncrement = true;
            FileXml.Columns.Add("DataIns", typeof(DateTime));
            FileXml.Columns.Add("NomeFile", typeof(string)).MaxLength = 250;
            FileXml.Columns.Add("FileXml", typeof(string));
            FileXml.Columns.Add("Lavorato", typeof(bool));

            //FileXml.PrimaryKey = new DataColumn[] { FileXml.Columns["Id"] };

            DataRow drFile = FileXml.NewRow();
            drFile["DataIns"] = DateTime.Now;
            drFile["NomeFile"] = fileName;
            drFile["FileXml"] = File.ReadAllText(Path.Combine(FolderLavoro, fileName));
            drFile["Lavorato"] = true;

            FileXml.Rows.Add(drFile);

            string piVaUtente = "";
            string piVaDistributore = "";
            string codContrDisp = "";
            string codiceFlusso = FlussoRettifica.CodFlusso.ToString();

            for (int i = 0; i < FlussoRettifica.IdentificativiFlusso.Items.Length; i++)
            {
                var name = FlussoRettifica.IdentificativiFlusso.ItemsElementName[i];
                string value = FlussoRettifica.IdentificativiFlusso.Items[i] as string;
                if (name == Models.Rettifica.ItemsChoiceType.PIvaUtente) piVaUtente = value;
                else if (name == Models.Rettifica.ItemsChoiceType.PIvaDistributore) piVaDistributore = value;
                else if (name == Models.Rettifica.ItemsChoiceType.CodContrDisp) codContrDisp = value;
            }


            // 2. CICLO POD (Riempimento DataRow)
            for (int j = 0; j < FlussoRettifica.DatiPod.Length; j++)
            {

                Models.Rettifica.FlussoMisureDatiPod pod = FlussoRettifica.DatiPod[j];

                // VARIABILE DINAMICA UNICA - come sopra, alcuni campi come EaF1 si chiamano così oppure EaF1int sono in sottoclassi differenti
                dynamic d = pod.Item;

                //Variabili misure
                var misuraRR = pod.Item as Models.Rettifica.DettaglioMisuraRRType;
                var misuraRRIntImm = pod.Item as Models.Rettifica.DettaglioMisuraRRIntImmType;
                var misuraRRInt = pod.Item as Models.Rettifica.DettaglioMisuraRRIntType;
                var misuraRNR = pod.Item as Models.Rettifica.DettaglioMisuraRNRType;
                var misuraRSNR = pod.Item as Models.Rettifica.DettaglioMisuraRSNRType;
                var misuraRNOv2 = pod.Item as Models.Rettifica.DettaglioMisuraRNOv2Type;

                //Variabili Consumo
                var consumoRv2 = pod.Item as Models.Rettifica.DettaglioConsumoRv2Type;
                var consumoRv2Imm = pod.Item as Models.Rettifica.DettaglioConsumoRv2IntImmType;
                IdLettura++;
                //Stringhe
                string tipoRettifica = pod.TipoRettifica.ToString() ?? "";

                DataRow dr = dtLetture.NewRow();

                dr["Id"] = IdLettura;
                dr["CodFlusso"] = codiceFlusso;
                dr["PIvaUtente"] = piVaUtente ?? "";
                dr["PIvaDistributore"] = piVaDistributore ?? "";
                dr["CodContrDisp"] = codContrDisp ?? "";
                dr["Pod"] = pod.Pod ?? "";
                dr["MeseAnno"] = ParseMonthYearOrDbNull(pod.MeseAnno);
                dr["DataMisura"] = ParseDateOrDbNull(pod.DataMisura);
                dr["DataRilevazione"] = ParseDateOrDbNull(pod.DataRilevazione);
                dr["DataPrest"] = ParseDateOrDbNull(pod.DataPrest);
                dr["TipoRettifica"] = tipoRettifica.Length > 1 ? tipoRettifica.Substring(0, 1) : tipoRettifica;
                dr["Motivazione"] = (pod.Motivazione.ToString().Length > 2) ? pod.Motivazione.ToString().Substring(0, 2).Replace("Item", "0") : (object)pod.Motivazione.ToString().Replace("Item", "0") ?? DBNull.Value;
                dr["CodPrat_SII"] = (pod.CodPrat_SII?.Length > 15) ? pod.CodPrat_SII.Substring(0, 15) : (pod.CodPrat_SII ?? "");
                dr["Trattamento"] = (pod.DatiPdp?.Trattamento.ToString().Length > 1) ? pod.DatiPdp.Trattamento.ToString().Substring(0, 1) : (object)pod.DatiPdp?.Trattamento ?? DBNull.Value;
                dr["Forfait"] = (pod.DatiPdp?.Forfait.ToString().Length > 2) ? pod.DatiPdp.Forfait.ToString().Substring(0, 2) : (object)pod.DatiPdp?.Forfait ?? DBNull.Value;
                dr["GruppoMis"] = (pod.DatiPdp?.GruppoMis.ToString().Length > 2) ? pod.DatiPdp.GruppoMis.ToString().Substring(0, 2) : (object)pod.DatiPdp?.GruppoMis ?? DBNull.Value;
                dr["Tensione"] = (object)pod.DatiPdp?.Tensione ?? DBNull.Value;
                dr["Ka"] = (object)pod.DatiPdp?.Ka?.Replace(",", ".") ?? DBNull.Value;
                dr["Kr"] = (object)pod.DatiPdp?.Kr?.Replace(",", ".") ?? DBNull.Value;
                dr["Kp"] = (object)pod.DatiPdp?.Kp?.Replace(",", ".") ?? DBNull.Value;
                // --- MISURE (GLI ERRORI DI RuntimeBinderException IN GENERE AVVENGONO QUI) ---
                dr["MisuraEaF1"] = (object)misuraRR?.EaF1?.Replace(",", ".") ?? (object)misuraRRInt?.EaF1?.Replace(",", ".") ?? (object)misuraRNR?.EaF1?.Replace(",", ".") ?? (object)misuraRSNR?.EaF1?.Replace(",", ".") ?? (object)misuraRNOv2?.EaF1?.Replace(",", ".") ?? DBNull.Value;
                dr["MisuraEaF2"] = (object)misuraRR?.EaF2?.Replace(",", ".") ?? (object)misuraRRInt?.EaF2?.Replace(",", ".") ?? (object)misuraRNR?.EaF2?.Replace(",", ".") ?? (object)misuraRSNR?.EaF2?.Replace(",", ".") ?? (object)misuraRNOv2?.EaF2?.Replace(",", ".") ?? DBNull.Value;
                dr["MisuraEaF3"] = (object)misuraRR?.EaF3?.Replace(",", ".") ?? (object)misuraRRInt?.EaF3?.Replace(",", ".") ?? (object)misuraRNR?.EaF3?.Replace(",", ".") ?? (object)misuraRSNR?.EaF3?.Replace(",", ".") ?? (object)misuraRNOv2?.EaF3?.Replace(",", ".") ?? DBNull.Value;
                dr["MisuraErF1"] = (object)misuraRR?.ErF1?.Replace(",", ".") ?? (object)misuraRRInt?.ErF1?.Replace(",", ".") ?? (object)misuraRNR?.ErF1?.Replace(",", ".") ?? (object)misuraRSNR?.ErF1?.Replace(",", ".") ?? (object)misuraRNOv2?.ErF1?.Replace(",", ".") ?? DBNull.Value;
                dr["MisuraErF2"] = (object)misuraRR?.ErF2?.Replace(",", ".") ?? (object)misuraRRInt?.ErF2?.Replace(",", ".") ?? (object)misuraRNR?.ErF2?.Replace(",", ".") ?? (object)misuraRSNR?.ErF2?.Replace(",", ".") ?? (object)misuraRNOv2?.ErF2?.Replace(",", ".") ?? DBNull.Value;
                dr["MisuraErF3"] = (object)misuraRR?.ErF3?.Replace(",", ".") ?? (object)misuraRRInt?.ErF3?.Replace(",", ".") ?? (object)misuraRNR?.ErF3?.Replace(",", ".") ?? (object)misuraRSNR?.ErF3?.Replace(",", ".") ?? (object)misuraRNOv2?.ErF3?.Replace(",", ".") ?? DBNull.Value;
                dr["MisuraPotMax"] = ParseDecimalOrDbNull(GetPropOrDbNull(pod.Item, "PotMax")?.ToString());
                // --- Fino a qui
                dr["MisuraEaM"] = (object)misuraRR?.EaM?.Replace(",", ".") ?? DBNull.Value;
                dr["MisuraPotM"] = (object)misuraRR?.PotM?.Replace(",", ".") ?? DBNull.Value;
                dr["MisuraEriM"] = (object)misuraRR?.EriM?.Replace(",", ".") ?? DBNull.Value;
                dr["MisuraEriF1"] = (object)misuraRR?.EriF1?.Replace(",", ".") ?? (object)misuraRRInt?.EriF1?.Replace(",", ".") ?? (object)misuraRNR?.ErF1?.Replace(",", ".") ?? (object)misuraRSNR?.ErF1?.Replace(",", ".") ?? (object)misuraRNOv2?.ErF1?.Replace(",", ".") ?? DBNull.Value;
                dr["MisuraEriF2"] = (object)misuraRR?.EriF2?.Replace(",", ".") ?? (object)misuraRRInt?.EriF2?.Replace(",", ".") ?? (object)misuraRNR?.ErF2?.Replace(",", ".") ?? (object)misuraRSNR?.ErF2?.Replace(",", ".") ?? (object)misuraRNOv2?.ErF2?.Replace(",", ".") ?? DBNull.Value;
                dr["MisuraEriF3"] = (object)misuraRR?.EriF3?.Replace(",", ".") ?? (object)misuraRRInt?.EriF3?.Replace(",", ".") ?? (object)misuraRNR?.ErF3?.Replace(",", ".") ?? (object)misuraRSNR?.ErF3?.Replace(",", ".") ?? (object)misuraRNOv2?.ErF3?.Replace(",", ".") ?? DBNull.Value;
                string dataInizioPeriodo = consumoRv2?.DataInizioPeriodo ?? consumoRv2Imm?.DataInizioPeriodo;
                dr["ConsumoDataInizioPeriodo"] = ParseDateOrDbNull(dataInizioPeriodo);
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

                dr["Valido"] = true;
                dr["IdLetture"] = 0;

                dtLetture.Rows.Add(dr);

                //Quartini Rettifiche
                if (pod.Item is Models.Rettifica.DettaglioMisuraRRType rr)
                {
                    MappaQuartini(QE, IdLettura, "Ea", rr.Ea);
                    MappaQuartini(QE, IdLettura, "Er", rr.Er);
                    MappaQuartini(QE, IdLettura, "Erc", rr.Erc);
                    MappaQuartini(QE, IdLettura, "Eri", rr.Eri);
                }
                else if (pod.Item is Models.Rettifica.DettaglioMisuraRRIntType rrint)
                {
                    MappaQuartini(QE, IdLettura, "Ea", rrint.Ea);
                    MappaQuartini(QE, IdLettura, "Er", rrint.Er);
                    MappaQuartini(QE, IdLettura, "Erc", rrint.Erc);
                    MappaQuartini(QE, IdLettura, "Eri", rrint.Eri);
                }
                else if (pod.Item is Models.Rettifica.DettaglioMisuraRNRType rnr)
                {
                    MappaQuartini(QE, IdLettura, "Ea", rnr.Ea);
                    MappaQuartini(QE, IdLettura, "Er", rnr.Er);
                    MappaQuartini(QE, IdLettura, "Erc", rnr.Erc);
                    MappaQuartini(QE, IdLettura, "Eri", rnr.Eri);
                }
                else if (pod.Item is Models.Rettifica.DettaglioMisuraRORType ror)
                {
                    MappaQuartini(QE, IdLettura, "Ea", ror.Ea);
                    MappaQuartini(QE, IdLettura, "Er", ror.Er);
                    MappaQuartini(QE, IdLettura, "Erc", ror.Erc);
                    MappaQuartini(QE, IdLettura, "Eri", ror.Eri);
                }
                else if (pod.Item is Models.Rettifica.DettaglioMisuraRFOv2Type rfov2)
                {
                    MappaQuartini(QE, IdLettura, "Ea", rfov2.Ea);
                    MappaQuartini(QE, IdLettura, "Er", rfov2.Er);
                    MappaQuartini(QE, IdLettura, "Erc", rfov2.Erc);
                    MappaQuartini(QE, IdLettura, "Eri", rfov2.Eri);
                }
            }
            //Scrittura col Bulk
            Bulk2DB(dtLetture, "Letture", connessione);
            Bulk2DB(QE, "Curve", connessione);
            Bulk2DB(FileXml, "FileXml", connessione);
        }

        private static void MappaQuartini(DataTable dt, int IdLetture, string tipo, IEnumerable<object> listaQuartini)
        {
            if (listaQuartini == null) return;

            foreach (object e in listaQuartini)
            {
                if (e == null)
                    continue;

                DataRow row = dt.NewRow();
                row["IdLetture"] = IdLetture;
                row["Tipo"] = tipo;

                for (int i = 1; i <= 96; i++)
                {
                    string propName = "E" + i;
                    PropertyInfo prop = e.GetType().GetProperty(propName);
                    if (prop == null)
                    {
                        row[propName] = 0m;
                        continue;
                    }

                    string rawValue = prop.GetValue(e)?.ToString();
                    row[propName] = ParseDecimalOrDbNull(rawValue) == DBNull.Value ? 0m : ParseDecimalOrDbNull(rawValue);
                    if (prop.GetValue(e) != null)
                    {
                        row[propName] = rawValue.Replace(",", ".");
                    }
                    else
                    {
                        row[propName] = DBNull.Value;
                    }
                }
                dt.Rows.Add(row);
            }
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
                return new DateTime(result.Year, result.Month, 1);
            }

            return DBNull.Value;
        }

        private static object GetPropOrDbNull(object source, string propName)
        {
            if (source == null) return DBNull.Value;

            PropertyInfo prop = source.GetType().GetProperty(propName);
            if (prop == null) return DBNull.Value;

            return prop.GetValue(source) ?? DBNull.Value;
        }
        private static object ParseDecimalOrDbNull(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return DBNull.Value;

            if (decimal.TryParse(value, NumberStyles.Any, new CultureInfo("it-IT"), out decimal result))
                return result;

            return DBNull.Value;
        }

    }
}
