using log4net;
using System;
using System.Data;
using System.Data.SqlClient;

namespace Vendita.HubMisureEE.Services
{
    internal class SaveFlusso
    {
        private static readonly ILog log = LogManager.GetLogger(typeof(SaveFlusso));
        public static void SaveFlusso2DB(Models.Periodico.FlussoMisure FlussoMisura, SqlConnection connessione, string FolderLavoro, int idFileXml, string fileName)
        {
            if (FlussoMisura == null || FlussoMisura.DatiPod == null || FlussoMisura.DatiPod.Length == 0)
                return;

            //Tabella FileXml
            DataTable FileXml = new DataTable();
            FileXml.Columns.Add("Id", typeof(int)).AutoIncrement = true;
            FileXml.Columns.Add("DataIns", typeof(DateTime));
            FileXml.Columns.Add("NomeFile", typeof(string)).MaxLength = 250;
            FileXml.Columns.Add("FileXml", typeof(string));
            FileXml.Columns.Add("Lavorato", typeof(bool));
            FileXml.PrimaryKey = new DataColumn[] { FileXml.Columns["Id"] };

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
            dtLetture.Columns.Add("IdFileXml", typeof(string));
            dtLetture.PrimaryKey = new DataColumn[] { dtLetture.Columns["ID"] };

            string piVaUtente = "";
            string piVaDistributore = "";
            string codContrDisp = "";
            string codiceFlusso = FlussoMisura.CodFlusso.ToString();

            DataTable QE;
            QE = new DataTable();
            QE.Columns.Add("Id", typeof(int)).AutoIncrement = true;
            QE.Columns.Add("IdLet", typeof(int));
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
            int IdLettura = 0;


            for (int i = 0; i < FlussoMisura.IdentificativiFlusso.Items.Length; i++)
            {
                var name = FlussoMisura.IdentificativiFlusso.ItemsElementName[i];
                string value = FlussoMisura.IdentificativiFlusso.Items[i] as string;
                if (name == Vendita.HubMisureEE.Models.Periodico.ItemsChoiceType.PIvaUtente) piVaUtente = value;
                else if (name == Vendita.HubMisureEE.Models.Periodico.ItemsChoiceType.PIvaDistributore) piVaDistributore = value;
                else if (name == Vendita.HubMisureEE.Models.Periodico.ItemsChoiceType.CodContrDisp) codContrDisp = value;
            }

            // 2. CICLO POD (Riempimento DataRow)
            for (int j = 0; j < FlussoMisura.DatiPod.Length; j++)
            {
                Models.Periodico.FlussoMisureDatiPod pod = FlussoMisura.DatiPod[j];

                // --- VARIABILE DINAMICA - mi sono vista costretta ad usare una variabile dinamica perchè 
                // le classi da prendere erano tante e con nomi diversi, in questo modo prendo tutto da una sola variabile 
                dynamic d = pod.Item;
                var consumo = pod.Item as Models.Periodico.DettaglioConsumoV2Type;

                DataRow dr = dtLetture.NewRow();

                // Dati POD
                dr["CodFlusso"] = codiceFlusso;
                dr["PIvaUtente"] = piVaUtente ?? "";
                dr["PIvaDistributore"] = piVaDistributore ?? "";
                dr["CodContrDisp"] = codContrDisp ?? "";
                dr["Pod"] = pod.Pod ?? "";
                dr["MeseAnno"] = (object)pod.MeseAnno ?? DBNull.Value;
                dr["DataMisura"] = (object)pod.DataMisura ?? DBNull.Value;
                dr["DataPrest"] = (object)pod.DataPrest ?? DBNull.Value;
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

                // Dati PDP
                dr["Trattamento"] = (object)pod.DatiPdp?.Trattamento ?? DBNull.Value;
                dr["Tensione"] = (object)pod.DatiPdp?.Tensione ?? DBNull.Value;
                dr["Forfait"] = (object)pod.DatiPdp?.Forfait ?? DBNull.Value;
                dr["GruppoMis"] = (object)pod.DatiPdp?.GruppoMis ?? DBNull.Value;
                dr["Ka"] = (object)pod.DatiPdp?.Ka ?? DBNull.Value;
                dr["Kr"] = (object)pod.DatiPdp?.Kr ?? DBNull.Value;
                dr["Kp"] = (object)pod.DatiPdp?.Kp ?? DBNull.Value;
                // --- ENERGIA ATTIVA ---
                dr["MisuraPotMax"] = (object)d?.PotMax ?? DBNull.Value;
                dr["MisuraEaF1"] = (object)d?.EaF1 ?? DBNull.Value;
                dr["MisuraEaF2"] = (object)d?.EaF2 ?? DBNull.Value;
                dr["MisuraEaF3"] = (object)d?.EaF3 ?? DBNull.Value;
                dr["MisuraEaF4"] = (object)d?.EaF4 ?? DBNull.Value;
                dr["MisuraEaF5"] = (object)d?.EaF5 ?? DBNull.Value;
                dr["MisuraEaF6"] = (object)d?.EaF6 ?? DBNull.Value;

                // --- ENERGIA REATTIVA ---
                dr["MisuraErF1"] = (object)d?.ErF1 ?? DBNull.Value;
                dr["MisuraErF2"] = (object)d?.ErF2 ?? DBNull.Value;
                dr["MisuraErF3"] = (object)d?.ErF3 ?? DBNull.Value;
                dr["MisuraErF4"] = (object)d?.ErF4 ?? DBNull.Value;
                dr["MisuraErF5"] = (object)d?.ErF5 ?? DBNull.Value;
                dr["MisuraErF6"] = (object)d?.ErF6 ?? DBNull.Value;

                // --- POTENZA ---
                dr["MisuraPotF1"] = (object)d?.PotF1 ?? DBNull.Value;
                dr["MisuraPotF2"] = (object)d?.PotF2 ?? DBNull.Value;
                dr["MisuraPotF3"] = (object)d?.PotF3 ?? DBNull.Value;
                dr["MisuraPotF4"] = (object)d?.PotF4 ?? DBNull.Value;
                dr["MisuraPotF5"] = (object)d?.PotF5 ?? DBNull.Value;
                dr["MisuraPotF6"] = (object)d?.PotF6 ?? DBNull.Value;

                // --- CONTROLLAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
                dr["MisuraEaM"] = (d.GetType().GetProperty("EaMint") != null) ? d?.EaMint ?? DBNull.Value : ((d.GetType().GetProperty("Ea") != null && !(d.Ea is Array)) ? d?.Ea ?? DBNull.Value : DBNull.Value);
                dr["MisuraPotM"] = (d.GetType().GetProperty("PotMint") != null) ? d?.PotMint ?? DBNull.Value : ((d.GetType().GetProperty("Pot") != null) ? d?.Pot ?? DBNull.Value : DBNull.Value);

                dr["MisuraErcF1"] = (object)d?.ErcF1 ?? DBNull.Value;
                dr["MisuraErcF2"] = (object)d?.ErcF2 ?? DBNull.Value;
                dr["MisuraErcF3"] = (object)d?.ErcF3 ?? DBNull.Value;
                dr["MisuraErcF4"] = (object)d?.ErcF4 ?? DBNull.Value;
                dr["MisuraErcF5"] = (object)d?.ErcF5 ?? DBNull.Value;
                dr["MisuraErcF6"] = (object)d?.ErcF6 ?? DBNull.Value;
                dr["MisuraErcM"] = (d.GetType().GetProperty("ErcM") != null) ? d?.ErcM ?? DBNull.Value : DBNull.Value;

                dr["MisuraEriF1"] = (object)d?.EriF1 ?? DBNull.Value;
                dr["MisuraEriF2"] = (object)d?.EriF2 ?? DBNull.Value;
                dr["MisuraEriF3"] = (object)d?.EriF3 ?? DBNull.Value;
                dr["MisuraEriF4"] = (object)d?.EriF4 ?? DBNull.Value;
                dr["MisuraEriF5"] = (object)d?.EriF5 ?? DBNull.Value;
                dr["MisuraEriF6"] = (object)d?.EriF6 ?? DBNull.Value;
                dr["MisuraEriM"] = (d.GetType().GetProperty("EriM") != null) ? d?.EriM ?? DBNull.Value : DBNull.Value;
                dr["MisuraErM"] = (d.GetType().GetProperty("ErM") != null) ? d?.ErM ?? DBNull.Value : DBNull.Value;

                // --- SEZIONE CONSUMI ---
                dr["ConsumoDataInizioPeriodo"] = (object)consumo?.DataInizioPeriodo ?? DBNull.Value;
                dr["ConsumoEaF1"] = (d.GetType().GetProperty("EaF1") != null) ? (object)d.EaF1 : DBNull.Value;
                dr["ConsumoEaF2"] = (d.GetType().GetProperty("EaF2") != null) ? (object)d.EaF2 : DBNull.Value;
                dr["ConsumoEaF3"] = (d.GetType().GetProperty("EaF3") != null) ? (object)d.EaF3 : DBNull.Value;
                dr["ConsumoErF1"] = (d.GetType().GetProperty("ErF1") != null) ? (object)d.ErF1 : DBNull.Value;
                dr["ConsumoErF2"] = (d.GetType().GetProperty("ErF2") != null) ? (object)d.ErF2 : DBNull.Value;
                dr["ConsumoErF3"] = (d.GetType().GetProperty("ErF3") != null) ? (object)d.ErF3 : DBNull.Value;
                dr["ConsumoPotF1"] = (d.GetType().GetProperty("PotF1") != null) ? (object)d.PotF1 : DBNull.Value;
                dr["ConsumoPotF2"] = (d.GetType().GetProperty("PotF2") != null) ? (object)d.PotF2 : DBNull.Value;
                dr["ConsumoPotF3"] = (d.GetType().GetProperty("PotF3") != null) ? (object)d.PotF3 : DBNull.Value;

                dr["ConsumoEaM"] = (object)consumo?.EaM ?? DBNull.Value;
                dr["ConsumoPotM"] = (object)consumo?.PotM ?? DBNull.Value;

                dr["ConsumoErcF1"] = (d.GetType().GetProperty("ErcF1") != null) ? (d.ErcF1 ?? DBNull.Value) : DBNull.Value;
                dr["ConsumoErcF2"] = (d.GetType().GetProperty("ErcF2") != null) ? (d.ErcF2 ?? DBNull.Value) : DBNull.Value;
                dr["ConsumoErcF3"] = (d.GetType().GetProperty("ErcF3") != null) ? (d.ErcF3 ?? DBNull.Value) : DBNull.Value;
                dr["ConsumoErcF4"] = (d.GetType().GetProperty("ErcF4") != null) ? (d.ErcF4 ?? DBNull.Value) : DBNull.Value;
                dr["ConsumoErcF5"] = (object)d?.ErcF5 ?? DBNull.Value;
                dr["ConsumoErcF6"] = (object)d?.ErcF6 ?? DBNull.Value;
                dr["ConsumoErcM"] = (d.GetType().GetProperty("ErcM") != null) ? d?.ErcM ?? DBNull.Value : DBNull.Value;

                dr["ConsumoEriF1"] = (object)d?.EriF1 ?? DBNull.Value;
                dr["ConsumoEriF2"] = (object)d?.EriF2 ?? DBNull.Value;
                dr["ConsumoEriF3"] = (object)d?.EriF3 ?? DBNull.Value;
                dr["ConsumoEriF4"] = (object)d?.EriF4 ?? DBNull.Value;
                dr["ConsumoEriF5"] = (object)d?.EriF5 ?? DBNull.Value;
                dr["ConsumoEriF6"] = (object)d?.EriF6 ?? DBNull.Value;
                dr["ConsumoEriM"] = (d.GetType().GetProperty("EriM") != null) ? d?.EriM ?? DBNull.Value : DBNull.Value;
                dr["ConsumoErM"] = (d.GetType().GetProperty("ErM") != null) ? d?.ErM ?? DBNull.Value : DBNull.Value;

                dr["Valido"] = true;
                dr["IdFileXml"] = 0;

                dtLetture.Rows.Add(dr);
                DataRow rowFX = FileXml.NewRow();
                rowFX["Id"] = idFileXml++;
                rowFX["DataIns"] = DateTime.Now;
                rowFX["NomeFile"] = fileName;
                rowFX["FileXml"] = "";
                rowFX["Lavorato"] = true;
                FileXml.Rows.Add(rowFX);

                Bulk2DB(dtLetture, "Letture", connessione);
                Bulk2DB(FileXml, "FileXml", connessione);

                try
                {
                    // try per log
                }
                catch (Exception ex)
                {
                    // log 
                }

                //Quartini Rettifiche

                Models.Periodico.EnergiaType[] Ea = d?.Ea;
                Models.Periodico.EnergiaType[] Er = d?.Er;


                if (Ea != null)
                {

                    foreach (Models.Periodico.EnergiaType e in Ea)
                    {
                        DataRow Quarto = QE.NewRow();

                        Quarto["IdLet"] = IdLettura;
                        Quarto["Tipo"] = "Ea";

                        Quarto["E1"] = e.E1;
                        Quarto["E2"] = e.E2;
                        Quarto["E3"] = e.E3;
                        Quarto["E4"] = e.E4;
                        Quarto["E5"] = e.E5;
                        Quarto["E6"] = e.E6;
                        Quarto["E7"] = e.E7;
                        Quarto["E8"] = e.E8;
                        Quarto["E9"] = e.E9;
                        Quarto["E10"] = e.E10;
                        Quarto["E11"] = e.E11;
                        Quarto["E12"] = e.E12;
                        Quarto["E13"] = e.E13;
                        Quarto["E14"] = e.E14;
                        Quarto["E15"] = e.E15;
                        Quarto["E16"] = e.E16;
                        Quarto["E17"] = e.E17;
                        Quarto["E18"] = e.E18;
                        Quarto["E19"] = e.E19;
                        Quarto["E20"] = e.E20;
                        Quarto["E21"] = e.E21;
                        Quarto["E22"] = e.E22;
                        Quarto["E23"] = e.E23;
                        Quarto["E24"] = e.E24;
                        Quarto["E25"] = e.E25;
                        Quarto["E26"] = e.E26;
                        Quarto["E27"] = e.E27;
                        Quarto["E28"] = e.E28;
                        Quarto["E29"] = e.E29;
                        Quarto["E30"] = e.E30;
                        Quarto["E31"] = e.E31;
                        Quarto["E32"] = e.E32;
                        Quarto["E33"] = e.E33;
                        Quarto["E34"] = e.E34;
                        Quarto["E35"] = e.E35;
                        Quarto["E36"] = e.E36;
                        Quarto["E37"] = e.E37;
                        Quarto["E38"] = e.E38;
                        Quarto["E39"] = e.E39;
                        Quarto["E40"] = e.E40;
                        Quarto["E41"] = e.E41;
                        Quarto["E42"] = e.E42;
                        Quarto["E43"] = e.E43;
                        Quarto["E44"] = e.E44;
                        Quarto["E45"] = e.E45;
                        Quarto["E46"] = e.E46;
                        Quarto["E47"] = e.E47;
                        Quarto["E48"] = e.E48;
                        Quarto["E49"] = e.E49;
                        Quarto["E50"] = e.E50;
                        Quarto["E51"] = e.E51;
                        Quarto["E52"] = e.E52;
                        Quarto["E53"] = e.E53;
                        Quarto["E54"] = e.E54;
                        Quarto["E55"] = e.E55;
                        Quarto["E56"] = e.E56;
                        Quarto["E57"] = e.E57;
                        Quarto["E58"] = e.E58;
                        Quarto["E59"] = e.E59;
                        Quarto["E60"] = e.E60;
                        Quarto["E61"] = e.E61;
                        Quarto["E62"] = e.E62;
                        Quarto["E63"] = e.E63;
                        Quarto["E64"] = e.E64;
                        Quarto["E65"] = e.E65;
                        Quarto["E66"] = e.E66;
                        Quarto["E67"] = e.E67;
                        Quarto["E68"] = e.E68;
                        Quarto["E69"] = e.E69;
                        Quarto["E70"] = e.E70;
                        Quarto["E71"] = e.E71;
                        Quarto["E72"] = e.E72;
                        Quarto["E73"] = e.E73;
                        Quarto["E74"] = e.E74;
                        Quarto["E75"] = e.E75;
                        Quarto["E76"] = e.E76;
                        Quarto["E77"] = e.E77;
                        Quarto["E78"] = e.E78;
                        Quarto["E79"] = e.E79;
                        Quarto["E80"] = e.E80;
                        Quarto["E81"] = e.E81;
                        Quarto["E82"] = e.E82;
                        Quarto["E83"] = e.E83;
                        Quarto["E84"] = e.E84;
                        Quarto["E85"] = e.E85;
                        Quarto["E86"] = e.E86;
                        Quarto["E87"] = e.E87;
                        Quarto["E88"] = e.E88;
                        Quarto["E89"] = e.E89;

                        Quarto["E90"] = e.E90 ?? (object)DBNull.Value;
                        Quarto["E91"] = e.E91 ?? (object)DBNull.Value;
                        Quarto["E92"] = e.E92 ?? (object)DBNull.Value;
                        Quarto["E93"] = e.E93 ?? (object)DBNull.Value;
                        Quarto["E94"] = e.E94 ?? (object)DBNull.Value;
                        Quarto["E95"] = e.E95 ?? (object)DBNull.Value;
                        Quarto["E96"] = e.E96 ?? (object)DBNull.Value;

                        QE.Rows.Add(Quarto);
                    }


                }


                if (Er != null)
                {

                    foreach (Models.Periodico.EnergiaType e in Er)
                    {
                        DataRow Quarto = QE.NewRow();

                        Quarto["IdLet"] = IdLettura;
                        Quarto["Tipo"] = "Er";

                        Quarto["E1"] = e.E1;
                        Quarto["E2"] = e.E2;
                        Quarto["E3"] = e.E3;
                        Quarto["E4"] = e.E4;
                        Quarto["E5"] = e.E5;
                        Quarto["E6"] = e.E6;
                        Quarto["E7"] = e.E7;
                        Quarto["E8"] = e.E8;
                        Quarto["E9"] = e.E9;
                        Quarto["E10"] = e.E10;
                        Quarto["E11"] = e.E11;
                        Quarto["E12"] = e.E12;
                        Quarto["E13"] = e.E13;
                        Quarto["E14"] = e.E14;
                        Quarto["E15"] = e.E15;
                        Quarto["E16"] = e.E16;
                        Quarto["E17"] = e.E17;
                        Quarto["E18"] = e.E18;
                        Quarto["E19"] = e.E19;
                        Quarto["E20"] = e.E20;
                        Quarto["E21"] = e.E21;
                        Quarto["E22"] = e.E22;
                        Quarto["E23"] = e.E23;
                        Quarto["E24"] = e.E24;
                        Quarto["E25"] = e.E25;
                        Quarto["E26"] = e.E26;
                        Quarto["E27"] = e.E27;
                        Quarto["E28"] = e.E28;
                        Quarto["E29"] = e.E29;
                        Quarto["E30"] = e.E30;
                        Quarto["E31"] = e.E31;
                        Quarto["E32"] = e.E32;
                        Quarto["E33"] = e.E33;
                        Quarto["E34"] = e.E34;
                        Quarto["E35"] = e.E35;
                        Quarto["E36"] = e.E36;
                        Quarto["E37"] = e.E37;
                        Quarto["E38"] = e.E38;
                        Quarto["E39"] = e.E39;
                        Quarto["E40"] = e.E40;
                        Quarto["E41"] = e.E41;
                        Quarto["E42"] = e.E42;
                        Quarto["E43"] = e.E43;
                        Quarto["E44"] = e.E44;
                        Quarto["E45"] = e.E45;
                        Quarto["E46"] = e.E46;
                        Quarto["E47"] = e.E47;
                        Quarto["E48"] = e.E48;
                        Quarto["E49"] = e.E49;
                        Quarto["E50"] = e.E50;
                        Quarto["E51"] = e.E51;
                        Quarto["E52"] = e.E52;
                        Quarto["E53"] = e.E53;
                        Quarto["E54"] = e.E54;
                        Quarto["E55"] = e.E55;
                        Quarto["E56"] = e.E56;
                        Quarto["E57"] = e.E57;
                        Quarto["E58"] = e.E58;
                        Quarto["E59"] = e.E59;
                        Quarto["E60"] = e.E60;
                        Quarto["E61"] = e.E61;
                        Quarto["E62"] = e.E62;
                        Quarto["E63"] = e.E63;
                        Quarto["E64"] = e.E64;
                        Quarto["E65"] = e.E65;
                        Quarto["E66"] = e.E66;
                        Quarto["E67"] = e.E67;
                        Quarto["E68"] = e.E68;
                        Quarto["E69"] = e.E69;
                        Quarto["E70"] = e.E70;
                        Quarto["E71"] = e.E71;
                        Quarto["E72"] = e.E72;
                        Quarto["E73"] = e.E73;
                        Quarto["E74"] = e.E74;
                        Quarto["E75"] = e.E75;
                        Quarto["E76"] = e.E76;
                        Quarto["E77"] = e.E77;
                        Quarto["E78"] = e.E78;
                        Quarto["E79"] = e.E79;
                        Quarto["E80"] = e.E80;
                        Quarto["E81"] = e.E81;
                        Quarto["E82"] = e.E82;
                        Quarto["E83"] = e.E83;
                        Quarto["E84"] = e.E84;
                        Quarto["E85"] = e.E85;
                        Quarto["E86"] = e.E86;
                        Quarto["E87"] = e.E87;
                        Quarto["E88"] = e.E88;
                        Quarto["E89"] = e.E89;

                        Quarto["E90"] = e.E90 ?? (object)DBNull.Value;
                        Quarto["E91"] = e.E91 ?? (object)DBNull.Value;
                        Quarto["E92"] = e.E92 ?? (object)DBNull.Value;
                        Quarto["E93"] = e.E93 ?? (object)DBNull.Value;
                        Quarto["E94"] = e.E94 ?? (object)DBNull.Value;
                        Quarto["E95"] = e.E95 ?? (object)DBNull.Value;
                        Quarto["E96"] = e.E96 ?? (object)DBNull.Value;

                        QE.Rows.Add(Quarto);
                    }
                }

                Bulk2DB(QE, "Curve", connessione);

                try
                {
                    // try per log
                }
                catch (Exception ex)
                {
                    // log 
                }
            }
        }

        public static void SaveFlusso2DB(Models.Rettifica.FlussoMisure FlussoRettifica, SqlConnection connessione, string FolderLavoro, int idFileXml, string fileName)
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
            dtLetture.Columns.Add("IdFileXml", typeof(string));
            dtLetture.PrimaryKey = new DataColumn[] { dtLetture.Columns["ID"] };

            string piVaUtente = "";
            string piVaDistributore = "";
            string codContrDisp = "";
            string codiceFlusso = FlussoRettifica.CodFlusso.ToString();



            // 4. tabella CURVE
            DataTable QE;
            QE = new DataTable();
            QE.Columns.Add("Id", typeof(int)).AutoIncrement = true;
            QE.Columns.Add("IdLet", typeof(int));
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
            int IdLettura = 0;

            //Tabella FileXml
            DataTable FileXml = new DataTable();
            FileXml.Columns.Add("Id", typeof(int)).AutoIncrement = true;
            FileXml.Columns.Add("DataIns", typeof(DateTime));
            FileXml.Columns.Add("NomeFile", typeof(string)).MaxLength = 250;
            FileXml.Columns.Add("FileXml", typeof(string));
            FileXml.Columns.Add("Lavorato", typeof(bool));
            FileXml.PrimaryKey = new DataColumn[] { FileXml.Columns["Id"] };

            string queryId = @"SELECT IDENT_CURRENT('Letture') AS IdLettura ";

            using (SqlCommand cmd = new SqlCommand(queryId, connessione))
            {
                object result = cmd.ExecuteScalar();
                IdLettura = Convert.ToInt32(result);
            }

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
                dynamic d = (dynamic)pod.Item;

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

                //Stringhe
                //string GruppoMis = pod.DatiPdp?.GruppoMis.ToString() ?? "";
                //string forfait = pod.DatiPdp?.Forfait.ToString() ?? "";
                //string trattamento = pod.DatiPdp?.Trattamento.ToString() ?? "";
                //string motivazione = pod.Motivazione.ToString() ?? "";
                string tipoRettifica = pod.TipoRettifica.ToString() ?? "";

                DataRow dr = dtLetture.NewRow();

                dr["CodFlusso"] = codiceFlusso;
                dr["PIvaUtente"] = piVaUtente ?? "";
                dr["PIvaDistributore"] = piVaDistributore ?? "";
                dr["CodContrDisp"] = codContrDisp ?? "";
                dr["Pod"] = pod.Pod ?? "";
                dr["MeseAnno"] = (object)pod.MeseAnno ?? DBNull.Value;
                dr["DataMisura"] = (object)pod.DataMisura ?? DBNull.Value;
                dr["DataRilevazione"] = (object)pod.DataRilevazione ?? DBNull.Value;
                dr["DataPrest"] = (object)pod.DataPrest ?? DBNull.Value;

                dr["TipoRettifica"] = tipoRettifica.Length > 1 ? tipoRettifica.Substring(0, 1) : (object)tipoRettifica;
                dr["Motivazione"] = (pod.Motivazione.ToString().Length > 2) ? pod.Motivazione.ToString().Substring(0, 2).Replace("Item", "0") : (object)pod.Motivazione.ToString().Replace("Item", "0") ?? DBNull.Value;
                dr["CodPrat_SII"] = (pod.CodPrat_SII?.Length > 15) ? pod.CodPrat_SII.Substring(0, 15) : (pod.CodPrat_SII ?? "");
                dr["Trattamento"] = (pod.DatiPdp?.Trattamento.ToString().Length > 1) ? pod.DatiPdp.Trattamento.ToString().Substring(0, 1) : (object)pod.DatiPdp?.Trattamento ?? DBNull.Value;
                dr["Forfait"] = (pod.DatiPdp?.Forfait.ToString().Length > 2) ? pod.DatiPdp.Forfait.ToString().Substring(0, 2) : (object)pod.DatiPdp?.Forfait ?? DBNull.Value;
                dr["GruppoMis"] = (pod.DatiPdp?.GruppoMis.ToString().Length > 2) ? pod.DatiPdp.GruppoMis.ToString().Substring(0, 2) : (object)pod.DatiPdp?.GruppoMis ?? DBNull.Value;

                dr["Tensione"] = (object)pod.DatiPdp?.Tensione ?? DBNull.Value;
                dr["Ka"] = (object)pod.DatiPdp?.Ka ?? DBNull.Value;
                dr["Kr"] = (object)pod.DatiPdp?.Kr ?? DBNull.Value;
                dr["Kp"] = (object)pod.DatiPdp?.Kp ?? DBNull.Value;

                // --- MISURE (GLI ERRORI DI RuntimeBinderException IN GENERE AVVENGONO QUI) ---
                dr["MisuraEaF1"] = (object)misuraRR?.EaF1 ?? (object)misuraRRInt?.EaF1 ?? (object)misuraRNR?.EaF1 ?? (object)misuraRSNR?.EaF1 ?? (object)misuraRNOv2?.EaF1 ?? DBNull.Value;
                dr["MisuraEaF2"] = (object)misuraRR?.EaF2 ?? (object)misuraRRInt?.EaF2 ?? (object)misuraRNR?.EaF2 ?? (object)misuraRSNR?.EaF2 ?? (object)misuraRNOv2?.EaF2 ?? DBNull.Value;
                dr["MisuraEaF3"] = (object)misuraRR?.EaF3 ?? (object)misuraRRInt?.EaF3 ?? (object)misuraRNR?.EaF3 ?? (object)misuraRSNR?.EaF3 ?? (object)misuraRNOv2?.EaF3 ?? DBNull.Value;
                dr["MisuraErF1"] = (object)misuraRR?.ErF1 ?? (object)misuraRRInt?.ErF1 ?? (object)misuraRNR?.ErF1 ?? (object)misuraRSNR?.ErF1 ?? (object)misuraRNOv2?.ErF1 ?? DBNull.Value;
                dr["MisuraErF2"] = (object)misuraRR?.ErF2 ?? (object)misuraRRInt?.ErF2 ?? (object)misuraRNR?.ErF2 ?? (object)misuraRSNR?.ErF2 ?? (object)misuraRNOv2?.ErF2 ?? DBNull.Value;
                dr["MisuraErF3"] = (object)misuraRR?.ErF3 ?? (object)misuraRRInt?.ErF3 ?? (object)misuraRNR?.ErF3 ?? (object)misuraRSNR?.ErF3 ?? (object)misuraRNOv2?.ErF3 ?? DBNull.Value;
                dr["MisuraPotMax"] = (d.GetType().GetProperty("PotMax") != null) ? (d.PotMax ?? DBNull.Value) : DBNull.Value;

                // --- MONORARIE / ERI ---
                dr["MisuraEaM"] = (object)misuraRR?.EaM ?? DBNull.Value;
                dr["MisuraPotM"] = (object)misuraRR?.PotM ?? DBNull.Value;
                dr["MisuraEriM"] = (object)misuraRR?.EriM ?? DBNull.Value;
                dr["MisuraEriF1"] = (object)misuraRR?.EriF1 ?? (object)misuraRRInt?.EriF1 ?? (object)misuraRNR?.ErF1 ?? (object)misuraRSNR?.ErF1 ?? (object)misuraRNOv2?.ErF1 ?? DBNull.Value;
                dr["MisuraEriF2"] = (object)misuraRR?.EriF2 ?? (object)misuraRRInt?.EriF2 ?? (object)misuraRNR?.ErF2 ?? (object)misuraRSNR?.ErF2 ?? (object)misuraRNOv2?.ErF2 ?? DBNull.Value;
                dr["MisuraEriF3"] = (object)misuraRR?.EriF3 ?? (object)misuraRRInt?.EriF3 ?? (object)misuraRNR?.ErF3 ?? (object)misuraRSNR?.ErF3 ?? (object)misuraRNOv2?.ErF3 ?? DBNull.Value;

                // --- SEZIONE CONSUMI ---
                dr["ConsumoDataInizioPeriodo"] = (object)consumoRv2Imm?.DataInizioPeriodo ?? DBNull.Value;
                dr["ConsumoEaF1"] = (object)consumoRv2?.EaF1 ?? DBNull.Value;
                dr["ConsumoEaF2"] = (object)consumoRv2?.EaF2 ?? DBNull.Value;
                dr["ConsumoEaF3"] = (object)consumoRv2?.EaF3 ?? DBNull.Value;
                dr["ConsumoErF1"] = (object)consumoRv2?.ErF1 ?? DBNull.Value;
                dr["ConsumoErF2"] = (object)consumoRv2?.ErF2 ?? DBNull.Value;
                dr["ConsumoErF3"] = (object)consumoRv2?.ErF3 ?? DBNull.Value;
                dr["ConsumoEaM"] = (object)consumoRv2?.EaM ?? DBNull.Value;
                dr["ConsumoEriM"] = (object)consumoRv2?.EriM ?? (object)consumoRv2Imm?.EriMint ?? DBNull.Value;

                dr["Valido"] = true;
                dr["IdFileXml"] = 0;

                dtLetture.Rows.Add(dr);

                try
                {
                    // try per log
                }
                catch (Exception ex)
                {
                    // log 
                }


                //Quartini Rettifiche
                Models.Rettifica.DettaglioMisuraRRIntImmType dettaglioRettifica = pod.Item as Models.Rettifica.DettaglioMisuraRRIntImmType;
                Models.Rettifica.EnergiaType[] Ea = dettaglioRettifica?.Eaint;
                Models.Rettifica.EnergiaType[] Er = dettaglioRettifica?.Erint;

                if (Ea != null)
                {

                    foreach (var e in Ea)
                    {
                        DataRow Quarto = QE.NewRow();

                        Quarto["IdLet"] = IdLettura;
                        Quarto["Tipo"] = "Ea";

                        Quarto["E1"] = e.E1;
                        Quarto["E2"] = e.E2;
                        Quarto["E3"] = e.E3;
                        Quarto["E4"] = e.E4;
                        Quarto["E5"] = e.E5;
                        Quarto["E6"] = e.E6;
                        Quarto["E7"] = e.E7;
                        Quarto["E8"] = e.E8;
                        Quarto["E9"] = e.E9;
                        Quarto["E10"] = e.E10;
                        Quarto["E11"] = e.E11;
                        Quarto["E12"] = e.E12;
                        Quarto["E13"] = e.E13;
                        Quarto["E14"] = e.E14;
                        Quarto["E15"] = e.E15;
                        Quarto["E16"] = e.E16;
                        Quarto["E17"] = e.E17;
                        Quarto["E18"] = e.E18;
                        Quarto["E19"] = e.E19;
                        Quarto["E20"] = e.E20;
                        Quarto["E21"] = e.E21;
                        Quarto["E22"] = e.E22;
                        Quarto["E23"] = e.E23;
                        Quarto["E24"] = e.E24;
                        Quarto["E25"] = e.E25;
                        Quarto["E26"] = e.E26;
                        Quarto["E27"] = e.E27;
                        Quarto["E28"] = e.E28;
                        Quarto["E29"] = e.E29;
                        Quarto["E30"] = e.E30;
                        Quarto["E31"] = e.E31;
                        Quarto["E32"] = e.E32;
                        Quarto["E33"] = e.E33;
                        Quarto["E34"] = e.E34;
                        Quarto["E35"] = e.E35;
                        Quarto["E36"] = e.E36;
                        Quarto["E37"] = e.E37;
                        Quarto["E38"] = e.E38;
                        Quarto["E39"] = e.E39;
                        Quarto["E40"] = e.E40;
                        Quarto["E41"] = e.E41;
                        Quarto["E42"] = e.E42;
                        Quarto["E43"] = e.E43;
                        Quarto["E44"] = e.E44;
                        Quarto["E45"] = e.E45;
                        Quarto["E46"] = e.E46;
                        Quarto["E47"] = e.E47;
                        Quarto["E48"] = e.E48;
                        Quarto["E49"] = e.E49;
                        Quarto["E50"] = e.E50;
                        Quarto["E51"] = e.E51;
                        Quarto["E52"] = e.E52;
                        Quarto["E53"] = e.E53;
                        Quarto["E54"] = e.E54;
                        Quarto["E55"] = e.E55;
                        Quarto["E56"] = e.E56;
                        Quarto["E57"] = e.E57;
                        Quarto["E58"] = e.E58;
                        Quarto["E59"] = e.E59;
                        Quarto["E60"] = e.E60;
                        Quarto["E61"] = e.E61;
                        Quarto["E62"] = e.E62;
                        Quarto["E63"] = e.E63;
                        Quarto["E64"] = e.E64;
                        Quarto["E65"] = e.E65;
                        Quarto["E66"] = e.E66;
                        Quarto["E67"] = e.E67;
                        Quarto["E68"] = e.E68;
                        Quarto["E69"] = e.E69;
                        Quarto["E70"] = e.E70;
                        Quarto["E71"] = e.E71;
                        Quarto["E72"] = e.E72;
                        Quarto["E73"] = e.E73;
                        Quarto["E74"] = e.E74;
                        Quarto["E75"] = e.E75;
                        Quarto["E76"] = e.E76;
                        Quarto["E77"] = e.E77;
                        Quarto["E78"] = e.E78;
                        Quarto["E79"] = e.E79;
                        Quarto["E80"] = e.E80;
                        Quarto["E81"] = e.E81;
                        Quarto["E82"] = e.E82;
                        Quarto["E83"] = e.E83;
                        Quarto["E84"] = e.E84;
                        Quarto["E85"] = e.E85;
                        Quarto["E86"] = e.E86;
                        Quarto["E87"] = e.E87;
                        Quarto["E88"] = e.E88;
                        Quarto["E89"] = e.E89;

                        Quarto["E90"] = e.E90 ?? (object)DBNull.Value;
                        Quarto["E91"] = e.E91 ?? (object)DBNull.Value;
                        Quarto["E92"] = e.E92 ?? (object)DBNull.Value;
                        Quarto["E93"] = e.E93 ?? (object)DBNull.Value;
                        Quarto["E94"] = e.E94 ?? (object)DBNull.Value;
                        Quarto["E95"] = e.E95 ?? (object)DBNull.Value;
                        Quarto["E96"] = e.E96 ?? (object)DBNull.Value;

                        QE.Rows.Add(Quarto);
                    }

                }

                if (Er != null)
                {

                    foreach (var e in Er)
                    {
                        DataRow Quarto = QE.NewRow();

                        Quarto["IdLet"] = IdLettura;
                        Quarto["Tipo"] = "Er";

                        Quarto["E1"] = e.E1;
                        Quarto["E2"] = e.E2;
                        Quarto["E3"] = e.E3;
                        Quarto["E4"] = e.E4;
                        Quarto["E5"] = e.E5;
                        Quarto["E6"] = e.E6;
                        Quarto["E7"] = e.E7;
                        Quarto["E8"] = e.E8;
                        Quarto["E9"] = e.E9;
                        Quarto["E10"] = e.E10;
                        Quarto["E11"] = e.E11;
                        Quarto["E12"] = e.E12;
                        Quarto["E13"] = e.E13;
                        Quarto["E14"] = e.E14;
                        Quarto["E15"] = e.E15;
                        Quarto["E16"] = e.E16;
                        Quarto["E17"] = e.E17;
                        Quarto["E18"] = e.E18;
                        Quarto["E19"] = e.E19;
                        Quarto["E20"] = e.E20;
                        Quarto["E21"] = e.E21;
                        Quarto["E22"] = e.E22;
                        Quarto["E23"] = e.E23;
                        Quarto["E24"] = e.E24;
                        Quarto["E25"] = e.E25;
                        Quarto["E26"] = e.E26;
                        Quarto["E27"] = e.E27;
                        Quarto["E28"] = e.E28;
                        Quarto["E29"] = e.E29;
                        Quarto["E30"] = e.E30;
                        Quarto["E31"] = e.E31;
                        Quarto["E32"] = e.E32;
                        Quarto["E33"] = e.E33;
                        Quarto["E34"] = e.E34;
                        Quarto["E35"] = e.E35;
                        Quarto["E36"] = e.E36;
                        Quarto["E37"] = e.E37;
                        Quarto["E38"] = e.E38;
                        Quarto["E39"] = e.E39;
                        Quarto["E40"] = e.E40;
                        Quarto["E41"] = e.E41;
                        Quarto["E42"] = e.E42;
                        Quarto["E43"] = e.E43;
                        Quarto["E44"] = e.E44;
                        Quarto["E45"] = e.E45;
                        Quarto["E46"] = e.E46;
                        Quarto["E47"] = e.E47;
                        Quarto["E48"] = e.E48;
                        Quarto["E49"] = e.E49;
                        Quarto["E50"] = e.E50;
                        Quarto["E51"] = e.E51;
                        Quarto["E52"] = e.E52;
                        Quarto["E53"] = e.E53;
                        Quarto["E54"] = e.E54;
                        Quarto["E55"] = e.E55;
                        Quarto["E56"] = e.E56;
                        Quarto["E57"] = e.E57;
                        Quarto["E58"] = e.E58;
                        Quarto["E59"] = e.E59;
                        Quarto["E60"] = e.E60;
                        Quarto["E61"] = e.E61;
                        Quarto["E62"] = e.E62;
                        Quarto["E63"] = e.E63;
                        Quarto["E64"] = e.E64;
                        Quarto["E65"] = e.E65;
                        Quarto["E66"] = e.E66;
                        Quarto["E67"] = e.E67;
                        Quarto["E68"] = e.E68;
                        Quarto["E69"] = e.E69;
                        Quarto["E70"] = e.E70;
                        Quarto["E71"] = e.E71;
                        Quarto["E72"] = e.E72;
                        Quarto["E73"] = e.E73;
                        Quarto["E74"] = e.E74;
                        Quarto["E75"] = e.E75;
                        Quarto["E76"] = e.E76;
                        Quarto["E77"] = e.E77;
                        Quarto["E78"] = e.E78;
                        Quarto["E79"] = e.E79;
                        Quarto["E80"] = e.E80;
                        Quarto["E81"] = e.E81;
                        Quarto["E82"] = e.E82;
                        Quarto["E83"] = e.E83;
                        Quarto["E84"] = e.E84;
                        Quarto["E85"] = e.E85;
                        Quarto["E86"] = e.E86;
                        Quarto["E87"] = e.E87;
                        Quarto["E88"] = e.E88;
                        Quarto["E89"] = e.E89;

                        Quarto["E90"] = e.E90 ?? (object)DBNull.Value;
                        Quarto["E91"] = e.E91 ?? (object)DBNull.Value;
                        Quarto["E92"] = e.E92 ?? (object)DBNull.Value;
                        Quarto["E93"] = e.E93 ?? (object)DBNull.Value;
                        Quarto["E94"] = e.E94 ?? (object)DBNull.Value;
                        Quarto["E95"] = e.E95 ?? (object)DBNull.Value;
                        Quarto["E96"] = e.E96 ?? (object)DBNull.Value;

                        QE.Rows.Add(Quarto);
                    }
                }
                //Bulk2DB(QE, "Curve", connessione);

                try
                {
                    // try per log
                }
                catch (Exception ex)
                {
                    // log 
                }

                dtLetture.Rows.Add(dr);
                DataRow rowFX = FileXml.NewRow();
                rowFX["Id"] = idFileXml++;
                rowFX["DataIns"] = DateTime.Now;
                rowFX["NomeFile"] = fileName;
                rowFX["FileXml"] = "";
                rowFX["Lavorato"] = true;
                FileXml.Rows.Add(rowFX);

                Bulk2DB(dtLetture, "Letture", connessione);
                Bulk2DB(QE, "Curve", connessione);
            }
        }

        public static void Bulk2DB(DataTable dtLetture, string nomeTabella, SqlConnection connessione)
        {
            if (dtLetture.Rows.Count > 0)
            {
                using (SqlBulkCopy bulk = new SqlBulkCopy(connessione))
                {
                    bulk.DestinationTableName = nomeTabella;
                    foreach (DataColumn col in dtLetture.Columns)
                        bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);

                    bulk.WriteToServer(dtLetture);

                    try
                    {
                        // try per log
                    }
                    catch (Exception ex)
                    {
                        // log 
                    }
                }
            }
        }
    }
}
