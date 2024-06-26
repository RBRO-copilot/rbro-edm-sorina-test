CREATE OR REPLACE PACKAGE RAPORTDWH.Pkg_Rap_1129_Mlra_Reporting IS

  /****************************INFO GENERALE*************************************

     NAME:       PKG_1129_MLRA_REPORTING
     PURPOSE:    PACHETUL INCARCA DATE PENTRU RAPORTARILE MLRA SI CHESTIONAR BNR LA DATA RAPORTARII
     NOTES:      PACHETUL ESTE RULAT LA CERERE, PRIN APELAREA PROCEDURII CALL_ALL

     REVISIONS:
     VER        DATE        AUTHOR           DESCRIPTION
     ---------  ----------  ---------------  ------------------------------------
     1.0                                        1. CREATED THIS PACKAGE
     2.0        26/11/2020   ALEXANDRA GHEBAUR  2. ADD NEW PRODUCTS (RO,SM)
                                                   INCLUDING CHESTIONAR BNR
     3.0        15/11/2020   Sorina Nutu        1. PBLMGT-5544/PBLMGT-5484
                                                2. optimizare rulare
                                                3. schimbare surse pt cod gpc si weigth risk
                                                4. istorizare tabele finale (cele pe baza carora se fac pivotii finali)
     4.0        28.02.2022    Denisa Fasie      1. PBLMGT-5258 Decomisionare CTNI
     5.0        29.04.2022    Sorina Nutu       1. PBLMGT-6042 - eliminare comisioane si anumite transaction_type
     6.0        17.05.2023    Ioana P           1. RWH1191 -Adaugare Athena PJ + Imbunatatiri performanta
     7.0        17.05.2023    Mihai Tanasescu   1. CD_89 PBLMGT_7668 corectie conditie incarcare carduri debit history_indicator = 1 din dm.dim_customer
     8.0        06.03.2024    Sorina Nutu       1. raportare 2023 - aliniere nomenclator tari
                                                2. eliminare western union - nu mai tranzactionam de cativa ani
                                                3. midas_customer pentru tranzactii - sa ia din dwh_col pt ca merge zi pe zi,
                                                   nu din stg unde avem te miri ce si mai nimic ca date - 4 eom si ultimele 4 zile lucratoare
                                                4. eliminare Checkuri_Incarcare_Tabele din call_all - nu vreau sa iasa pe eroare
                                                   nu mi le incarca nimeni manual unu cate unu, mi le incarc din procedura cand am nevoie de date la ce data am nevoie
                                                   pastrez temp-urile pentru ca unele sunt folosite de mai multe ori si sa nu tot verific stg-ul daca are date

                                                   ECAB 16.05.2024
     9.0        24.05.2024    Magda Dogaru      1. CDS-268, CDS-56: Adaugare produs nou Athena Corporate in procedura 'PROC_PRODUSE' - 'RBRO_RO_CORP'                     
     10.0       03.06.2024    Sorina Nutu       1. PBLMGT-9117 mlra remapare cif raif 0000000026 la 0000000298
  ******************************************************************************/

  p_Nr_Inregistrari NUMBER := 0;
  p_Tip             VARCHAR2(10);
  p_Nume_Tabela     VARCHAR2(100);
  p_Desc_Eveniment  VARCHAR2(200);
  p_Param_1         VARCHAR2(100);
  p_Param_2         VARCHAR2(100);
  p_Param_3         VARCHAR2(100);
  p_Autonumber      NUMBER;
  p_Nume_Pachet     VARCHAR2(100) := $$PLSQL_UNIT;
  p_Nume_Procedura  VARCHAR2(100);
  p_Eroare          VARCHAR2(250) := 'FARA EROARE';
  p_Nume_Schema     VARCHAR2(100) := Sys_Context('userenv',
                                                 'current_schema');
  p_Task_Run_Id     NUMBER;

  v_Datai DATE;

  PROCEDURE Proc_Add_Partition
  ( p_Task_Run_Id    NUMBER,
    p_Reporting_Date DATE,
    p_Table_Name     VARCHAR2
  );
  PROCEDURE Proc_Write_To_From_Hist
  ( p_Task_Run_Id    NUMBER,
    p_Reporting_Date DATE,
    p_Table_Name_i   VARCHAR2,
    p_Table_Name_f   VARCHAR2
  );

  PROCEDURE Proc_Temp_Table_Txn( p_Task_Run_Id    NUMBER,p_Reporting_Date DATE);
  PROCEDURE Proc_Temp_Table_Prod( p_Task_Run_Id    NUMBER,p_Reporting_Date DATE);

  PROCEDURE Proc_Clienti( p_Task_Run_Id    NUMBER,p_Reporting_Date DATE);

  PROCEDURE Proc_Tranzactii( p_Task_Run_Id    NUMBER,p_Reporting_Date DATE);

  /******************* PROC_PRODUSE ****************

     NAME:       PROC_PRODUSE
     PURPOSE:    PROCEDURA INCARCA PRODUSELE

     REVISIONS:
     VER        DATE        AUTHOR           DESCRIPTION
     ---------  ----------  ---------------  ------------------------------------
     1.0                                        1. CREATED THIS PROCEDURE
     2.0        26/11/2020   ALEXANDRA GHEBAUR  2. CHANGE TO STG TABLES
                                                   ADD NEW PRODUCTS
     3.0        05/05/2023   MIHAI TANASESCU    1. CD_89 PBLMGT_7668 corectie conditie incarcare carduri debit history_indicator = 1 din dm.dim_customer
     PARAMETERS: P_REPORTING_DATE - DATA DATELOR - ZI LUCRATOARE - PARAMETRU DE TIP IN

  ******************************************************************************/
  PROCEDURE Proc_Produse( p_Task_Run_Id    NUMBER,p_Reporting_Date DATE);

  PROCEDURE Proc_Tranz_Tabele_Finale( p_Task_Run_Id    NUMBER);

  PROCEDURE Proc_Produse_Tabele_Finale ( p_Task_Run_Id    NUMBER);

  PROCEDURE Proc_Chestionar_Bnr( p_Task_Run_Id    NUMBER,p_Reporting_Date DATE);

  PROCEDURE Call_All
  (
    p_Task_Run_Id    NUMBER,
    p_Reporting_Date DATE
  );
  PROCEDURE Call_Tabele_Finale( p_Task_Run_Id    NUMBER,p_Reporting_Date DATE);
  FUNCTION Check_Table_Date
  ( p_Task_Run_Id    NUMBER,
    p_Reporting_Date DATE,
    p_Table_Name     VARCHAR2
  ) RETURN NUMBER;

  PROCEDURE Proc_Clean_Temp_Tables ( p_Task_Run_Id    NUMBER);

  --RWH1191 adaugare pas verificare tabele
  PROCEDURE Checkuri_Incarcare_Tabele
  (
    p_Task_Run_Id    NUMBER,
    p_Reporting_Date DATE
  );

  procedure check_and_load (p_task_run_id number, p_reporting_date date, p_table_name varchar2);

END;
/

CREATE OR REPLACE PACKAGE BODY RAPORTDWH.Pkg_Rap_1129_Mlra_Reporting AS

  PROCEDURE Proc_Add_Partition
  ( p_Task_Run_Id    NUMBER,
    p_Reporting_Date DATE,
    p_Table_Name     VARCHAR2
  ) AS
    v_Nr             NUMBER;
    v_Part_Name      VARCHAR2(10);
    v_Sql            VARCHAR2(400);
    v_Reporting_Date VARCHAR2(20);

  BEGIN
    v_Part_Name := 'P_' || To_Char(p_Reporting_Date, 'yyyymmdd');
    SELECT COUNT(*)
      INTO v_Nr
      FROM Dba_Tab_Partitions
     WHERE Table_Name = p_Table_Name
       AND Partition_Name = v_Part_Name;
    IF v_Nr = 0 THEN
      v_Reporting_Date := To_Char(p_Reporting_Date, 'yyyy-mm-dd') ||
                          ' 00:00:00';
      v_Sql            := 'ALTER TABLE ' || p_Table_Name ||
                          ' add partition ' || v_Part_Name ||
                          ' values (TO_DATE(''' || v_Reporting_Date ||
                          ''', ''SYYYY-MM-DD HH24:MI:SS'', ''NLS_CALENDAR=GREGORIAN''))';
      EXECUTE IMMEDIATE v_Sql;

    ELSE
      v_Sql := 'ALTER TABLE ' || p_Table_Name || ' TRUNCATE partition ' ||
               v_Part_Name;
      EXECUTE IMMEDIATE v_Sql;

    END IF;
  END Proc_Add_Partition;

  PROCEDURE Proc_Write_To_From_Hist
  ( p_Task_Run_Id    NUMBER,
    p_Reporting_Date DATE,
    p_Table_Name_i   VARCHAR2,
    p_Table_Name_f   VARCHAR2
  ) AS
    v_Col        VARCHAR2(1000);
    v_Sql        VARCHAR2(4000);
    v_Date_Field VARCHAR2(40);
    v_Nr         NUMBER;
  BEGIN

    p_Nume_Procedura := 'PROC_WRITE_TO_FROM_HIST';
    p_Nume_Tabela    := p_Table_Name_i;
    p_Tip            := 'I';
    p_Desc_Eveniment := p_Table_Name_f;
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    v_Col := NULL;
    FOR Rec IN (SELECT Column_Name
                  FROM All_Tab_Columns
                 WHERE Owner = 'RAPORTDWH'
                   AND Table_Name = p_Table_Name_i) LOOP
      v_Col := v_Col || Rec.Column_Name || ',';
    END LOOP;
    v_Col := Substr(v_Col, 1, Length(v_Col) - 1);
    SELECT COUNT(*)
      INTO v_Nr
      FROM All_Tab_Columns
     WHERE Owner = 'RAPORTDWH'
       AND Table_Name = p_Table_Name_i
       AND Column_Name = 'REPORT_CUTOFF_DATE';
    IF v_Nr = 1 THEN
      v_Date_Field := 'REPORT_CUTOFF_DATE';
    ELSE
      v_Date_Field := 'REPORTING_DATE';
    END IF;
    v_Sql := 'INSERT INTO ' || p_Table_Name_f || ' (' || v_Col ||
             ') select ' || v_Col || ' from ' || p_Table_Name_i ||
             ' WHERE ' || v_Date_Field || ' = ''' || p_Reporting_Date || '''';
    EXECUTE IMMEDIATE v_Sql;
    COMMIT;

    p_Nr_Inregistrari := 0;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);
  END Proc_Write_To_From_Hist;

  PROCEDURE Proc_Temp_Table_Txn( p_Task_Run_Id    NUMBER,p_Reporting_Date DATE) AS

  BEGIN

    --SEP_INCOMING
    p_Nume_Procedura := 'PROC_TEMP_TABLE_TXN';
    p_Nume_Tabela    := 'tmp_1129_mlra_sep_incoming';
    p_Tip            := 'F';
    p_Desc_Eveniment := '1. SEP_INCOMING';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    EXECUTE IMMEDIATE ('TRUNCATE TABLE tmp_1129_mlra_sep_incoming');

    INSERT INTO Tmp_1129_Mlra_Sep_Incoming
    ( Date_From,
             Unid,
             Bic_Bnc_Beneficiara,
             Iban_Beneficiar,
             Nume_Beneficiar,
             Bic_Bnc_Ordonatoare,
             Iban_Platitor,
             Nume_Platitor,
             Id_Tranzactie,
             tipul_mesajului)
      SELECT Date_From,
             Unid,
             Bic_Bnc_Beneficiara,
             Iban_Beneficiar,
             Nume_Beneficiar,
             Bic_Bnc_Ordonatoare,
             Iban_Platitor,
             Nume_Platitor,
             Id_Tranzactie,
             tipul_mesajului
        FROM Dwh_Col.Co_Sep_Incoming_Lcl@Gdwh24_Dwh_Col
       WHERE Date_From = p_Reporting_Date;

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --SEP_OUTGOING
    p_Nume_Tabela    := 'tmp_1129_mlra_sep_outgoing';
    p_Tip            := 'I';
    p_Desc_Eveniment := '2. SEP_OUTGOING';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    EXECUTE IMMEDIATE ('TRUNCATE TABLE tmp_1129_mlra_sep_outgoing');

    INSERT INTO Tmp_1129_Mlra_Sep_Outgoing
      SELECT Date_From,
             Unid,
             Bic_Bnc_Beneficiara,
             Iban_Beneficiar,
             Bic_Bnc_Ordonatoare,
             Iban_Platitor
        FROM Dwh_Col.Co_Sep_Outgoing_Lcl@Gdwh24_Dwh_Col
       WHERE Date_From = p_Reporting_Date;

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --TRAN_TXN_STMT
    p_Nume_Tabela    := 'tmp_1129_mlra_tran_txn_stmt';
    p_Tip            := 'I';
    p_Desc_Eveniment := '3. TRAN_TXN_STMT';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    EXECUTE IMMEDIATE ('TRUNCATE TABLE tmp_1129_mlra_tran_txn_stmt');

    INSERT INTO Tmp_1129_Mlra_Tran_Txn_Stmt
      SELECT Date_From,
             Unid,
             Cod_Tranzactie_Cont_a,
             Cod_Tranzactie_Pst_b,
             Cod_Tranzactie_Pst_a
        FROM Dwh_Col.Co_Tran_Txn_Stmt_Lcl@Gdwh24_Dwh_Col
       WHERE Date_From = p_Reporting_Date;

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --WBI_TXN
    p_Nume_Tabela    := 'tmp_1129_mlra_wbi_txn';
    p_Tip            := 'I';
    p_Desc_Eveniment := '4. WBI_TXN';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    EXECUTE IMMEDIATE ('TRUNCATE TABLE tmp_1129_mlra_wbi_txn');

    INSERT INTO Tmp_1129_Mlra_Wbi_Txn
    (DATE_FROM, UNID, ID_TRANZACTIE, BIC_BANCA_BENF)
      SELECT Date_From,
             NULL AS Unid,
             Id_Tranzactie,
             Bic_Banca_Benf
        FROM Dwh_Col.Co_Wbi_Txn_Lcl@Gdwh24_Dwh_Col
       WHERE Date_From = p_Reporting_Date;

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --M_INPAY
    p_Nume_Tabela    := 'tmp_1129_mlra_m_inpay';
    p_Tip            := 'I';
    p_Desc_Eveniment := '5. M_INPAY';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    EXECUTE IMMEDIATE ('TRUNCATE TABLE tmp_1129_mlra_m_inpay');

    INSERT INTO Tmp_1129_Mlra_m_Inpay
      (Date_From,
       Unid,
       Benefic_Cust_Type,
       Beneficiary1,
       Sender1,
       Sender_Type,
       Bank_Inf1,
       Ordering_Bank_Typ,
       Ordering_Bank1,
       Ordering_Cust_Typ,
       Ordering_Cust1,
       Payment_Reference,
       Payment_Subtype,
       Payment_Type,
       Beneficiary5,
       Front_Office_Transaction_Id)
      SELECT Date_From,
             Unid,
             Benefic_Cust_Type,
             Beneficiary1,
             Sender1,
             Sender_Type,
             Bank_Inf1,
             Ordering_Bank_Typ,
             Ordering_Bank1,
             Ordering_Cust_Typ,
             Ordering_Cust1,
             Payment_Reference,
             Payment_Subtype,
             Payment_Type,
             Beneficiary5,
             Front_Office_Transaction_Id
        FROM Dwh_Col.Co_m_Inpay_Lcl@Gdwh24_Dwh_Col
       WHERE Date_From = p_Reporting_Date;

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --MIDAS_ACCT_TXN
    p_Nume_Tabela    := 'tmp_1129_mlra_midas_acct_txn';
    p_Tip            := 'I';
    p_Desc_Eveniment := '6. MIDAS_ACCT_TXN';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    EXECUTE IMMEDIATE ('TRUNCATE TABLE tmp_1129_mlra_midas_acct_txn');

    INSERT INTO Tmp_1129_Mlra_Midas_Acct_Txn
    (DATE_FROM, UNID, PAYMENT_REF, PRODUCT_SUBTYPE_ID)
      SELECT Date_From,
             Unid,
             Payment_Ref,
             Product_Subtype_Id
        FROM Dwh_Col.Co_Midas_Acct_Txn_Lcl@Gdwh24_Dwh_Col
       WHERE Date_From = p_Reporting_Date;

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --M_OTPAY
    p_Nume_Tabela    := 'tmp_1129_mlra_m_otpay';
    p_Tip            := 'I';
    p_Desc_Eveniment := '7. M_OTPAY';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    EXECUTE IMMEDIATE ('TRUNCATE TABLE tmp_1129_mlra_m_otpay');

    INSERT INTO Tmp_1129_Mlra_m_Otpay
      (Date_From,
       Unid,
       Payment_Type,
       Payment_Subtype,
       Ordering_Cust_Type,
       Ordering_Cust1,
       Acc_With_Bank,
       Acc_With_Bank1,
       Benef_Type,
       Benef_Cust1,
       Payment_Reference)
      SELECT Date_From,
             Unid,
             Payment_Type,
             Payment_Subtype,
             Ordering_Cust_Type,
             Ordering_Cust1,
             Acc_With_Bank,
             Acc_With_Bank1,
             Benef_Type,
             Benef_Cust1,
             Payment_Reference
        FROM Dwh_Col.Co_m_Otpay_Lcl@Gdwh24_Dwh_Col
       WHERE Date_From = p_Reporting_Date;

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);


  --MIDAS_CUSTOMER
    p_Nume_Procedura := 'PROC_TEMP_TABLE_TXN';
    p_Nume_Tabela    := 'TMP_1129_MLRA_MIDAS_CUSTOMER';
    p_Tip            := 'F';
    p_Desc_Eveniment := '8. MIDAS_CUSTOMER';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    EXECUTE IMMEDIATE ('TRUNCATE TABLE TMP_1129_MLRA_MIDAS_CUSTOMER');

 INSERT INTO Tmp_1129_Mlra_Midas_Customer
   (Date_From, Customer_Id, Country_Of_Citizenship)
   SELECT Date_From,
          Customer_Id,
          Country_Of_Citizenship
     FROM Dwh_Col.Co_Midas_Customer_Lcl@Gdwh24_Dwh_Col
    WHERE Date_From = p_Reporting_Date;

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);


    Dbms_Stats.Gather_Table_Stats(Ownname          => 'RAPORTDWH',
                                  Tabname          => 'tmp_1129_mlra_sep_incoming',
                                  Estimate_Percent => Dbms_Stats.Auto_Sample_Size,
                                  Cascade          => TRUE,
                                  Method_Opt       => 'FOR ALL COLUMNS SIZE AUTO');

    Dbms_Stats.Gather_Table_Stats(Ownname          => 'RAPORTDWH',
                                  Tabname          => 'tmp_1129_mlra_sep_outgoing',
                                  Estimate_Percent => Dbms_Stats.Auto_Sample_Size,
                                  Cascade          => TRUE,
                                  Method_Opt       => 'FOR ALL COLUMNS SIZE AUTO');

    Dbms_Stats.Gather_Table_Stats(Ownname          => 'RAPORTDWH',
                                  Tabname          => 'tmp_1129_mlra_tran_txn_stmt',
                                  Estimate_Percent => Dbms_Stats.Auto_Sample_Size,
                                  Cascade          => TRUE,
                                  Method_Opt       => 'FOR ALL COLUMNS SIZE AUTO');

    Dbms_Stats.Gather_Table_Stats(Ownname          => 'RAPORTDWH',
                                  Tabname          => 'tmp_1129_mlra_wbi_txn',
                                  Estimate_Percent => Dbms_Stats.Auto_Sample_Size,
                                  Cascade          => TRUE,
                                  Method_Opt       => 'FOR ALL COLUMNS SIZE AUTO');

    Dbms_Stats.Gather_Table_Stats(Ownname          => 'RAPORTDWH',
                                  Tabname          => 'tmp_1129_mlra_m_inpay',
                                  Estimate_Percent => Dbms_Stats.Auto_Sample_Size,
                                  Cascade          => TRUE,
                                  Method_Opt       => 'FOR ALL COLUMNS SIZE AUTO');

    Dbms_Stats.Gather_Table_Stats(Ownname          => 'RAPORTDWH',
                                  Tabname          => 'tmp_1129_mlra_midas_acct_txn',
                                  Estimate_Percent => Dbms_Stats.Auto_Sample_Size,
                                  Cascade          => TRUE,
                                  Method_Opt       => 'FOR ALL COLUMNS SIZE AUTO');

    Dbms_Stats.Gather_Table_Stats(Ownname          => 'RAPORTDWH',
                                  Tabname          => 'tmp_1129_mlra_m_otpay',
                                  Estimate_Percent => Dbms_Stats.Auto_Sample_Size,
                                  Cascade          => TRUE,
                                  Method_Opt       => 'FOR ALL COLUMNS SIZE AUTO');

    Dbms_Stats.Gather_Table_Stats(Ownname          => 'RAPORTDWH',
                                  Tabname          => 'TMP_1129_MLRA_MIDAS_CUSTOMER',
                                  Estimate_Percent => Dbms_Stats.Auto_Sample_Size,
                                  Cascade          => TRUE,
                                  Method_Opt       => 'FOR ALL COLUMNS SIZE AUTO');

  END Proc_Temp_Table_Txn;

  PROCEDURE Proc_Temp_Table_Prod( p_Task_Run_Id    NUMBER,p_Reporting_Date DATE) AS

  BEGIN
    p_Nume_Procedura := 'PROC_TEMP_TABLE_PROD';

    --DEBIT_CARD
    raportdwh.pkg_rap_1129_mlra_reporting.check_and_load(p_task_run_id,p_Reporting_Date,'STG_DEBIT_CARD');


    p_Nume_Tabela    := 'tmp_1129_mlra_debit_card';
    p_Tip            := 'I';
    p_Desc_Eveniment := '8. DEBIT_CARD';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    EXECUTE IMMEDIATE ('TRUNCATE TABLE tmp_1129_mlra_debit_card');

    INSERT INTO Tmp_1129_Mlra_Debit_Card
      SELECT Date_From,
             NULL AS Unid,
             Citizen_Id,
             Active_Inactive,
             Card_Type,
             Card_Number,
             Personal_Corporate_Card
        FROM WRK.STG_DEBIT_CARD --RWH1191 inlocuit db_link
       WHERE Date_From = p_Reporting_Date;

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --WSS2DWHFX
     raportdwh.pkg_rap_1129_mlra_reporting.check_and_load(p_task_run_id,p_Reporting_Date,'STG_WSS2DWHFX');

    p_Nume_Tabela    := 'tmp_1129_mlra_wss2dwhfx';
    p_Tip            := 'I';
    p_Desc_Eveniment := '9. WSS2DWHFX';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    EXECUTE IMMEDIATE ('TRUNCATE TABLE tmp_1129_mlra_wss2dwhfx');

    INSERT INTO Tmp_1129_Mlra_Wss2dwhfx
      SELECT Date_From,
             NULL AS Unid,
             Product,
             Deal_Number,
             Coconut,
             Status_Flag,
             Reversal_Flag
        FROM WRK.STG_WSS2DWHFX --RWH1191
       WHERE Date_From = p_Reporting_Date;

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --WSS2DWHMM
    raportdwh.pkg_rap_1129_mlra_reporting.check_and_load(p_task_run_id,p_Reporting_Date,'STG_WSS2DWHMM');

    p_Nume_Tabela    := 'tmp_1129_mlra_wss2dwhmm';
    p_Tip            := 'I';
    p_Desc_Eveniment := '10. WSS2DWHMM';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    EXECUTE IMMEDIATE ('TRUNCATE TABLE tmp_1129_mlra_wss2dwhmm');

    INSERT INTO Tmp_1129_Mlra_Wss2dwhmm
      SELECT Date_From,
             NULL AS Unid,
             Product,
             Deal_Number,
             Cocunut,
             Deal_Status
        FROM WRK.STG_WSS2DWHMM --RWH1191 inlocuit db_link
       WHERE Date_From = p_Reporting_Date;

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    Dbms_Stats.Gather_Table_Stats(Ownname          => 'RAPORTDWH',
                                  Tabname          => 'tmp_1129_mlra_wss2dwhmm',
                                  Estimate_Percent => Dbms_Stats.Auto_Sample_Size,
                                  Cascade          => TRUE,
                                  Method_Opt       => 'FOR ALL COLUMNS SIZE AUTO');

    Dbms_Stats.Gather_Table_Stats(Ownname          => 'RAPORTDWH',
                                  Tabname          => 'tmp_1129_mlra_debit_card',
                                  Estimate_Percent => Dbms_Stats.Auto_Sample_Size,
                                  Cascade          => TRUE,
                                  Method_Opt       => 'FOR ALL COLUMNS SIZE AUTO');

    Dbms_Stats.Gather_Table_Stats(Ownname          => 'RAPORTDWH',
                                  Tabname          => 'tmp_1129_mlra_wss2dwhfx',
                                  Estimate_Percent => Dbms_Stats.Auto_Sample_Size,
                                  Cascade          => TRUE,
                                  Method_Opt       => 'FOR ALL COLUMNS SIZE AUTO');

  END Proc_Temp_Table_Prod;

  PROCEDURE Proc_Clienti( p_Task_Run_Id    NUMBER,p_Reporting_Date DATE) AS
    --v_Nr  NUMBER;
   --v_Sql VARCHAR2(400);
   v_cust_lob varchar2(50); 
   v_cust_rr_level  varchar2(50);

  BEGIN

  --  EXECUTE IMMEDIATE 'TRUNCATE TABLE tmp_1129_mlra_clienti ';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE tmp_1129_mlra_clienti_f ';
/*
p_Nume_Procedura := 'PROC_CLIENTI';
    p_Nume_Tabela    := 'tmp_1129_mlra_clienti';
    p_Tip            := 'F';
    p_Desc_Eveniment := 'CLIENTI all';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);
--rap 2023
INSERT INTO \*+ APPEND*\
    Tmp_1129_Mlra_Clienti
      SELECT case when dc.cui ='*n.a.*' then 'PF' ELSE 'PJ' END AS Categ_Client,
             Cust.Reporting_Date AS Reporting_Date,
             Cust.Bk_Cust_Crm AS Bk_Cust_Crm,
             Cust.Bk_Cust_Icbs AS Cif,
             decode (cust.BK_SUBSEGMENTARE,'-1','NA','-2','NA',NULL,'NA', CUST.BK_SUBSEGMENTARE) AS Bus_Segm,
             decode (cust.segment_kyc,null,'NA',CUST.SEGMENT_KYC)  AS Kyc_Segment,
             cast(NULL as varchar2(200)) AS x_Caen_Version,
             decode (cust.BK_SUBSEGMENTARE,'-1','NA','-2','NA',NULL,'NA', CUST.BK_SUBSEGMENTARE) AS Sub_Type_Ind,
             --  Sub_Type_Ind folosit in aml_customer_type pt pj - nefolosit mai departe. se poate ramane doar cu coloana bus_segm
             cast(NULL as varchar2(200)) AS Lang_Id,
             Cust.Bk_Status_Crm AS Bk_Status_Crm,
             Cust.Bk_Customer_Type AS Bk_Customer_Type
        FROM Dm.Fct_Customer Cust
      inner join dm.dim_customer dc
      on cust.sk_customer=dc.sk_customer
      AND CUST.REPORTING_DATE BETWEEN DC.DATE_FROM AND DC.DATE_UNTIL
       WHERE Cust.Reporting_Date = p_Reporting_Date
         AND Cust.Source_System = 'CRM'
         AND Cust.Bk_Cust_Icbs IS NOT NULL;

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);
    */
 /*
    --CLIENTI PF
    p_Nume_Procedura := 'PROC_CLIENTI';
    p_Nume_Tabela    := 'tmp_1129_mlra_clienti';
    p_Tip            := 'F';
    p_Desc_Eveniment := '1. CLIENTI PF';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO \*+ APPEND*\
    --RWH1191 inlocuire db-link cu STG
    Tmp_1129_Mlra_Clienti
         SELECT 'PF' AS Categ_Client,
             Cust.Reporting_Date AS Reporting_Date,
             Cust.Bk_Cust_Crm AS Bk_Cust_Crm,
             Cust.Bk_Cust_Icbs AS Cif,
             Fnx2.Attrib_42 AS Bus_Segm,
             Fnx2.Attrib_05 AS Kyc_Segment,
             NULL AS x_Caen_Version,
             Fnx2.Attrib_42 AS Sub_Type_Ind,
             NULL AS Lang_Id,
             Cust.Bk_Status_Crm AS Bk_Status_Crm,
             Cust.Bk_Customer_Type AS Bk_Customer_Type
        FROM Dm.Fct_Customer Cust
        JOIN Wrk.Stg_Crm_s_Contact Con
          ON Con.Date_From = p_Reporting_Date
         AND Cust.Bk_Cust_Crm = Con.Row_Id
        LEFT JOIN Wrk.Stg_Crm_s_Contact2_Fnx Fnx2
          ON Fnx2.Date_From = p_Reporting_Date
         AND Con.Row_Id = Fnx2.Par_Row_Id
       WHERE Cust.Reporting_Date = p_Reporting_Date
         AND Cust.Source_System = 'CRM';

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --CLIENTI PJ
    p_Nume_Tabela    := 'tmp_1129_mlra_clienti';
    p_Tip            := 'I';
    p_Desc_Eveniment := '2. CLIENTI PJ';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO \*+ APPEND*\
    --RWH1191 inlocuire db-link cu STG
    Tmp_1129_Mlra_Clienti
      SELECT 'PJ' AS Categ_Client,
             Cust.Reporting_Date AS Reporting_Date,
             Cust.Bk_Cust_Crm AS Bk_Cust_Crm,
             Cust.Bk_Cust_Icbs AS Cif,
             Fnx.Bus_Type_Cd AS Bus_Segm,
             Org_x.x_Kyc_Segment2 AS Kyc_Segment,
             Ind.x_Caen_Version AS x_Caen_Version,
             Ind.Sub_Type AS Sub_Type_Ind,
             Ind.Lang_Id AS Lang_Id,
             Cust.Bk_Status_Crm AS Bk_Status_Crm,
             Cust.Bk_Customer_Type AS Bk_Customer_Type
        FROM Dm.Fct_Customer Cust
        JOIN Wrk.Stg_Crm_s_Org_Ext Org
          ON Org.Date_From = p_Reporting_Date
         AND Cust.Bk_Cust_Crm = Org.Row_Id
        LEFT JOIN Wrk.Stg_Crm_s_Org_Ext_x Org_x
          ON Org_x.Date_From = p_Reporting_Date
         AND Org.Row_Id = Org_x.Row_Id
        LEFT JOIN Wrk.Stg_Crm_s_Org_Ext_Fnx Fnx
          ON Fnx.Date_From = p_Reporting_Date
         AND Org.Row_Id = Fnx.Par_Row_Id
        LEFT JOIN Wrk.Stg_Crm_s_Indust Ind
          ON Ind.Date_From = p_Reporting_Date
         AND Org.Pr_Indust_Id = Ind.Row_Id
         AND Ind.Lang_Id = 'ENU'
         AND Ind.x_Caen_Version = 'Rev2'
       WHERE Cust.Reporting_Date = p_Reporting_Date
         AND Source_System = 'CRM';

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);*/

 /*   Dbms_Stats.Gather_Table_Stats(Ownname          => 'RAPORTDWH',
                                  Tabname          => 'tmp_1129_mlra_clienti',
                                  Estimate_Percent => Dbms_Stats.Auto_Sample_Size,
                                  Cascade          => TRUE,
                                  Method_Opt       => 'FOR ALL COLUMNS SIZE AUTO');*/

    --CLIENTII CU FLAGURI AML
    p_Nume_Procedura := 'PROC_CLIENTI';
    p_Nume_Tabela    := 'tmp_1129_mlra_clienti_F';
    p_Tip            := 'L';
    p_Desc_Eveniment := '3. CLIENTII CU FLAGURI AML';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

--   INSERT INTO /*+ APPEND*/
--    Tmp_1129_Mlra_Clienti_f
--      SELECT Cl.Categ_Client,
--             Cl.Reporting_Date,
--             Cl.Bk_Cust_Crm,
--             Cl.Cif,
--             Cl.Bus_Segm,
--             Cl.Kyc_Segment,
--             Cl.x_Caen_Version,
--             Cl.Sub_Type_Ind,
--             Cl.Lang_Id,
--             CASE
--               WHEN Cl.Categ_Client = 'PF' THEN
--                'PI'
--               ELSE
--                Nvl(Dict_1.Cod_Dest, Cl.Bus_Segm)
--             END AS Customer_Lob,
--             Upper(Kyc_Segment) AS Customer_Rr_Level,
--    /*         CASE
--               WHEN Cl.Categ_Client = 'PF' THEN
--                Cl.Bus_Segm
--               ELSE
--                Cl.Sub_Type_Ind
--             END AS Aml_Customer_Type,*/
--             NULL Aml_Customer_Type,
--             Cl.Bk_Status_Crm AS Bk_Status_Crm,
--             Cl.Bk_Customer_Type AS Bk_Customer_Type
--        FROM Tmp_1129_Mlra_Clienti Cl
--        LEFT JOIN Man_1129_Mlra_Dict Dict_1
--          ON Dict_1.Id_Categ = 1 --Customer_LOB
--         AND Cl.Bus_Segm = Dict_1.Cod_Src;


INSERT INTO /*+ APPEND*/
Raportdwh.Tmp_1129_Mlra_Clienti_f
  (Categ_Client,
   Reporting_Date,
   Bk_Cust_Crm,
   Cif,
   Bus_Segm,
   Kyc_Segment,
   x_Caen_Version,
   Sub_Type_Ind,
   Lang_Id,
   Customer_Lob,
   Customer_Rr_Level,
   Aml_Customer_Type,
   Bk_Status_Crm,
   Bk_Customer_Type)
WITH CLIENTI AS (
  SELECT /*+ append*/ CASE
           WHEN j.Flg_Client_Type = '0' THEN
            'PF'
           ELSE
            'PJ'
         END Categ_Client,
         Cust.Reporting_Date AS Reporting_Date,
         Cust.Bk_Cust_Crm AS Bk_Cust_Crm,
         Cust.Bk_Cust_Icbs AS Cif,
         CASE
           WHEN Irh.Source_System = 'DEFAULT' THEN
            'NA'
           ELSE
            Decode(Irh.Segment_En, NULL, 'NA', Irh.Segment_En)
         END AS Bus_Segm,
         Decode(Cust.Segment_Kyc, NULL, 'NA', Cust.Segment_Kyc) AS Kyc_Segment,
         CAST(NULL AS VARCHAR2(200)) AS x_Caen_Version,
         CASE
           WHEN Irh.Source_System = 'DEFAULT' THEN
            'NA'
           ELSE
            Decode(Irh.Subsegment_En, NULL, 'NA', Irh.Subsegment_En)
         END AS Sub_Type_Ind,
         --  Sub_Type_Ind folosit in aml_customer_type pt pj - nefolosit mai departe. se poate ramane doar cu coloana bus_segm
         CAST(NULL AS VARCHAR2(200)) AS Lang_Id,
         CASE
           WHEN j.Flg_Client_Type = '0' THEN
            'PI'
           ELSE
            CASE
              WHEN Irh.Source_System = 'DEFAULT' THEN
               'NA'

              ELSE
               Decode(Irh.Bus_Seg_Lev_2,
                      NULL,
                      'NA',
                      'IMM',
                      'SME',
                      Irh.Bus_Seg_Lev_2)
            END
         END Customer_Lob,
         Upper(Decode(Cust.Segment_Kyc, NULL, 'NA', Cust.Segment_Kyc)) AS Customer_Rr_Level,
         /*         CASE
           WHEN Cl.Categ_Client = 'PF' THEN
            Cl.Bus_Segm
           ELSE
            Cl.Sub_Type_Ind
         END AS Aml_Customer_Type,*/
         CAST(NULL AS VARCHAR2(200)) Aml_Customer_Type,
         Cust.Bk_Status_Crm    AS Bk_Status_Crm,
         Cust.Bk_Customer_Type AS Bk_Customer_Type
    FROM Dm.Fct_Customer Cust
   INNER JOIN Dm.Dim_Customer Dc
      ON Cust.Sk_Customer = Dc.Sk_Customer
   INNER JOIN Dm.Dim_Junk_Flags j
      ON j.Sk_Junk_Flags = Cust.Sk_Junk_Flags
     AND Cust.Reporting_Date BETWEEN Dc.Date_From AND Dc.Date_Until
    LEFT JOIN Dm.Irh_Business_Segmentation Irh
      ON p_Reporting_Date BETWEEN Irh.Date_From AND Irh.Date_Until
     AND Irh.Bk_Subsegmentation = Cust.Bk_Subsegmentare
   WHERE Cust.Reporting_Date = p_Reporting_Date
  --   AND Cust.Source_System = 'CRM' pierd clienti cu asta
     AND Cust.Bk_Cust_Icbs IS NOT NULL)
     SELECT
     Categ_Client,
   Reporting_Date,
   Bk_Cust_Crm,
   Cif,
   Bus_Segm,
   Kyc_Segment,
   x_Caen_Version,
   Sub_Type_Ind,
   Lang_Id,
   DECODE(Customer_Lob,'NA','SME','Unspecified','Corporate',Customer_Lob) Customer_Lob,
   Customer_Rr_Level,
   Aml_Customer_Type,
   Bk_Status_Crm,
   Bk_Customer_Type
   FROM CLIENTI;


    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

  p_Nume_Procedura := 'PROC_CLIENTI';
    p_Nume_Tabela    := 'tmp_1129_mlra_clienti_F';
    p_Tip            := 'L';
    p_Desc_Eveniment := 'remapare cif 0000000026 la 0000000298';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);


select customer_lob, CUSTOMER_RR_LEVEL into v_cust_lob, v_cust_rr_level FROM raportdwh.tmp_1129_mlra_clienti_F WHERE cif = '0000000298';

UPDATE RAPORTDWH.TMP_1129_MLRA_CLIENTI_F 
SET CUSTOMER_LOB = V_CUST_LOB, CUSTOMER_RR_LEVEL =V_CUST_RR_LEVEL
WHERE CIF = '0000000026';
 p_Nr_Inregistrari := SQL%ROWCOUNT;
   COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);




    Dbms_Stats.Gather_Table_Stats(Ownname          => 'RAPORTDWH',
                                  Tabname          => 'tmp_1129_mlra_clienti_F',
                                  Estimate_Percent => Dbms_Stats.Auto_Sample_Size,
                                  Cascade          => TRUE,
                                  Method_Opt       => 'FOR ALL COLUMNS SIZE AUTO');

    Proc_Add_Partition( p_Task_Run_Id ,p_Reporting_Date, 'TMP_1129_MLRA_CLIENTI_F_H');
    Proc_Write_To_From_Hist(p_Task_Run_Id ,p_Reporting_Date,
                            'TMP_1129_MLRA_CLIENTI_F',
                            'TMP_1129_MLRA_CLIENTI_F_H');

  END Proc_Clienti;

  PROCEDURE Proc_Tranzactii( p_Task_Run_Id    NUMBER,p_Reporting_Date DATE) AS

  BEGIN



    -- tranzactii pe an
   v_Datai := Trunc(p_Reporting_Date, 'YEAR');

   Proc_Add_Partition(p_Task_Run_Id ,p_Reporting_Date, 'TMP_1129_MLRA_TXN_F_DET_H');

   EXECUTE IMMEDIATE ('TRUNCATE TABLE tmp_1129_mlra_txn_f_det');

    WHILE v_Datai <= p_Reporting_Date

     LOOP

      EXECUTE IMMEDIATE 'TRUNCATE TABLE tmp_1129_mlra_txn_tmp ';
      EXECUTE IMMEDIATE 'TRUNCATE TABLE tmp_1129_mlra_txn_det ';

      Proc_Temp_Table_Txn(p_Task_Run_Id ,v_Datai);

      p_Nume_Procedura := 'PROC_TRANZACTII';
      p_Nume_Tabela    := 'tmp_1129_mlra_txn_tmp';
      p_Param_1        := To_Char(v_Datai, 'dd-mon-yyyy');
      p_Desc_Eveniment := 'txn temp';
      Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                                 p_Nume_Procedura,
                                 p_Nume_Schema,
                                 p_Nume_Tabela,
                                 p_Tip,
                                 p_Param_1,
                                 p_Param_2,
                                 p_Param_3,
                                 p_Desc_Eveniment,
                                 p_Autonumber,
                                  p_Task_Run_Id);
      --RWH1191 inlocuire dblink cu tabele
      INSERT INTO Raportdwh.Tmp_1129_Mlra_Txn_Tmp
        (Reporting_Date,
         Table_Source,
         Table_Source_Id,
         Channel_Transaction,
         Type_Tool,
         Debit_Credit_a,
         Subsystem_Description_b,
         Amount_Eur,
         Cif_a_Src,
         Iban_Code_a_Src,
         Account_a,
         Product_Account_a_Src,
         Cif_b_Src,
         Iban_Code_b_Src,
         Account_b,
         Product_Account_b_Src,
         Subsystem_b_Src,
         Description_2,
         Bk_Bank,
         Transaction_Category_Id,
         Transaction_Subtype_Id,
         Business_Channel,
         Description_1,
         Description_5,
         flag_cash_irh,
         BENEFICIARY_BK_COUNTRY,
         ORIGINATOR_BK_COUNTRY,
         flag_loro)
SELECT /*+ PARALLEL(TXN 4)*/
 Txn.Reporting_Date,
 Txn.Table_Source,
 Txn.Table_Source_Id,
 Txn.Channel_Transaction,
 Txn.Type_Tool,
 Txn.Debit_Credit_a,
 Txn.Subsystem_Description_b,
 Txn.Amount_Eur,
 --CASE
  -- WHEN Txn.Table_Source = 'CO_PST00101_LCL' AND
  --      Txn.Subsystem_Description_a = 'GL' AND Txn.Cif_a_Src IS NULL AND
  --      Txn.Cif_b_Src IS NULL THEN
  --  '0000000298' -- VIRARI SALARII SI DIURNE DIN CONT GL DAR PT CARE NU SE POPULEAZA NICIU  CIF SAU IBAN
  -- ELSE
    /*   nvl( nvl( Nvl(Nvl(Nvl(Txn.Cif_a_Src, Tran_Acc.Bk_Customer_Icbs),Tran_iban.Bk_Customer_Icbs),
    Time_Acc.Bk_Customer_Icbs),Time_iban.Bk_Customer_Icbs), Loan_Acc.Bk_Customer_Icbs)
    */
   Nvl(Nvl(Nvl(Txn.Cif_a_Src, Tran_Acc.Bk_Customer_Icbs), Time_Acc.Bk_Customer_Icbs), Loan_Acc.Bk_Customer_Icbs)
 --END
 Cif_a_Src,
 Txn.Iban_Code_a_Src,
 Txn.Account_a,
 Txn.Product_Account_a_Src,
 Nvl(Txn.Cif_b_Src, Iban.Bk_Customer_Icbs) Cif_b_Src,
/* CASE WHEN TXN.IBAN_CODE_B_SRC like 'IBAN%' THEN regexp_replace(iban_CODE_B_SRC,'IBAN','')
   WHEN TXN.IBAN_CODE_B_SRC like 'TEXT .%' THEN regexp_replace(iban_CODE_B_SRC,'TEXT .\) .','')
   ELSE
   Txn.Iban_Code_b_Src END Iban_Code_b_Src,*/
    Txn.Iban_Code_b_Src,
 Txn.Account_b,
 Txn.Product_Account_b_Src,
 Txn.Subsystem_b_Src,
 Txn.Description_2,
 CASE
   WHEN Txn.Bk_Bank IN ('*noval*', '*n.a.*') THEN
    NULL
   ELSE
    Txn.Bk_Bank
 END Bk_Bank,
 Txn.Transaction_Category_Id,
 Txn.Transaction_Subtype_Id,
 Txn.Business_Channel,
 Txn.Description_1,
 Txn.Description_5,
 CASE
   WHEN Ty.Transaction_Group = 'Cash' THEN
    'Y'
   WHEN Txn.Channel_Transaction IN ('ABT', 'BATCH') AND
        Txn.Description_1 LIKE 'DN/%' THEN
    'Y' --   ////sunt tranz de Transport numerar de la banca la client, nemarcate in MIROO ca si Cash
   ELSE
    'N'
 END AS Flag_Cash_Irh,
 Decode(Txn.Beneficiary_Bk_Country,
        '*n.a.*',
        NULL,
        Txn.Beneficiary_Bk_Country) Beneficiary_Bk_Country,
 Decode(Txn.Originator_Bk_Country,
        '*n.a.*',
        NULL,
        Txn.Originator_Bk_Country) Originator_Bk_Country,
        case when
          Ty.Transaction_Category LIKE '%LORO%' OR Ty.Transaction_Type LIKE '%LORO%' then 'Y'
          ELSE 'N'
        end flag_loro
  FROM Dm.Fct_Transaction Txn
  LEFT JOIN Dm.Dim_Tran_Account Iban --Dwh_Col.Co_Iban_Cust_Codes_Lcl@Gdwh24_Dwh_Col Iban
    ON Txn.Iban_Code_b_Src = Iban.Iban_Code
   AND v_Datai BETWEEN Iban.Date_From AND Iban.Date_Until
/*  ON Iban.Date_From = '31-DEC-2021'
AND Txn.Iban_Code_b_Src = Iban.Iban_Code*/
/* LEFT JOIN Dwh_Col.Co_Icbs_Cust_Account_Lcl@Gdwh24_Dwh_Col Cust_Acc
 ON Cust_Acc.Date_From = '31-DEC-2021'
AND Cust_Acc.Application_Acc = 20
AND Cust_Acc.Owner_Flag = 1
AND Iban.Account_Code = Cust_Acc.Icbs_Account*/
----PBLMGT-6042
  LEFT JOIN Dm.Irh_Transaction_Type Ty
    ON Txn.Sk_Transaction_Type = Ty.Sk_Transaction_Type
--tran acc
  LEFT JOIN  Dm.Dim_Tran_Account  Tran_Acc
    ON   Tran_Acc.Sk_Account = To_Char(Txn.Sk_Account_a )
    and v_Datai BETWEEN Tran_Acc.Date_From AND Tran_Acc.Date_Until
/*--tran iban
  LEFT JOIN  Dm.Dim_Tran_Account  Tran_iban
    ON  Tran_iban.Iban_Code = Txn.Iban_Code_a_Src
    and v_Datai BETWEEN Tran_iban.Date_From AND Tran_iban.Date_Until*/
--time acc
  LEFT JOIN Dm.Dim_Deposit_Account Time_Acc
    ON  Time_Acc.Sk_Account = To_Char(Txn.Sk_Account_a)
    and  v_Datai BETWEEN Time_Acc.Date_From AND Time_Acc.Date_Until
/*--time iban
  LEFT JOIN Dm.Dim_Deposit_Account Time_iban
    ON  Time_iban.Iban_Code = Txn.Iban_Code_a_Src
    and  v_Datai BETWEEN Time_Acc.Date_From AND Time_Acc.Date_Until*/
--loan
  LEFT JOIN Dm.Dim_Loan_Account Loan_Acc
    ON Loan_Acc.Bk_Account = To_Char(Txn.Sk_Account_a)
        and  v_Datai BETWEEN Loan_Acc.Date_From AND Loan_Acc.Date_Until

 WHERE Txn.Reporting_Date = v_Datai
   AND NOT (Txn.Subsystem_Description_b = 'GL' AND Txn.Branch = 0) --Eliminare comisioane, dobanzi, impozite
   AND Nvl(Txn.Cif_a_Src, 9999) <> Nvl(Txn.Cif_b_Src, 8888)--Eliminare tranzactii conturi proprii
    and  Txn.Subsystem_Description_b <> 'LOAN' --Eliminarea rambursari/acordari credite
    and not(Txn.Amount_a = 0 AND Txn.Amount_b = 0) --Eliminare tranzactii cu suma 0
    and   Txn.Transaction_Category_Id not IN (500100,--Stornari instrumente de debit - OUT
                                         500300,-- Corectii plati interbancare/Incasare
                                         500400,--Corectii incasari interbancare / Plata
                                         500500,--Corectii GL pozitie/contrapozitie valutara /Incasari si plati
                                         500600,--Corectii - Reglari cont Casa/ Lichidare DD  si  Retragere Ec
                                         509900,--Stornari prin taste functionale
                                         -3, -- nemapate
                                         700313, --Comisioane tranz Card/POS
                                         700100, --Returnare comisioane pentru clienti IMM
                                         700200 --Plata comisioane IMM
                                         )
   AND NOT (Txn.Subsystem_Description_b = 'GL' AND Txn.Account_b LIKE '7%') --Eliminare tranzactii de comisionare
   AND Txn.Table_Source NOT IN ('CO_GL_TRANSACTIONS_LCL') --Eliminare comisioane, dobanzi, impozite
   AND Ty.Transaction_Group NOT IN ('Reconciliere', 'Corectii') --Eliminare tranzactii de reconciliere / Eliminare tranzactii de corectii
   AND not (Nvl(Upper(Txn.Description_1), 'xxx') LIKE  '%COMISION ADMINISTRARE%' and Txn.SUBSYSTEM_DESCRIPTION_B='GL') --Eliminare comisioane de administrare
   AND NOT  (txn.Channel_Transaction = 'ABT' AND   Ty.Transaction_Category = 'Plata rata credit') --Eliminare plati rate
   AND Upper(Ty.Transaction_Type) NOT IN ('REJECTII IN SEP OUT') --Eliminare tranzactii rejectate in SEP_OUT
   AND NOT (txn.Channel_Transaction = 'AUT' AND  txn.Account_b > '900000000000') --Eliminare tranzactii extrabilantiere
   AND NOT (txn.Cif_a_Src = '0000000298' AND  txn.Cif_b_Src IS NULL) --Eliminare tranzactii pe CIF-ul bancii fara contrapartida
   AND NOT (txn.Cif_b_Src = '0000000298' AND  txn.Cif_a_Src IS NULL) --Eliminare tranzactii pe CIF-ul bancii fara contrapartida
   AND NOT (Ty.Transaction_Type = 'In nume propriu' AND txn.Account_a > 1000000000) --Eliminare tranzactii in nume propriu dintr-un cont GL
   and txn.TRANSACTION_SUBTYPE_ID <> '70440' --Excludem tranzactiile cu subtip "cont GL eronat"
   and ty.TRANSACTION_TYPE_ID <> 1000 --'RETUR INCASARE'
   AND NOT (CIF_A_SRC = '0000033190' and  SUBSYSTEM_DESCRIPTION_B ='GL') --IMPLEM SENT ANPC / + BEJ NON CONT CURENT
   and ty.transaction_subtype not like '%Bank to Bank%' --loro necomerciale/bank to bank

/*      --pst00101
   AND Upper(Txn.Description_1) NOT LIKE ('TRANZACTIE RESPINSA%')*/
   ;


      p_Nr_Inregistrari := SQL%ROWCOUNT;
      COMMIT;

      Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);
      Dbms_Stats.Gather_Table_Stats(Ownname          => 'RAPORTDWH',
                                    Tabname          => 'tmp_1129_mlra_txn_tmp',
                                    Estimate_Percent => Dbms_Stats.Auto_Sample_Size,
                                    Cascade          => TRUE,
                                    Method_Opt       => 'FOR ALL COLUMNS SIZE AUTO');

      --NR_TRANZ_ACDN
      p_Nume_Tabela    := 'tmp_1129_mlra_txn_det';
      p_Param_1        := To_Char(v_Datai, 'dd-mon-yyyy');
      p_Desc_Eveniment := 'NR_TRANZ_ACDN';
      Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                                 p_Nume_Procedura,
                                 p_Nume_Schema,
                                 p_Nume_Tabela,
                                 p_Tip,
                                 p_Param_1,
                                 p_Param_2,
                                 p_Param_3,
                                 p_Desc_Eveniment,
                                 p_Autonumber,
                                         p_Task_Run_Id);
      INSERT INTO Tmp_1129_Mlra_Txn_Det
        (Reporting_Date,
         Table_Source,
         Table_Source_Id,
         Debit_Credit_a,
         Amount_Eur,
         Cif_a_Src,
         Account_a,
         Cif_b_Src,
         Account_b,
         Loro_Account_Flag,
         Nostro_Account_Flag,
         Flag_Cash,
         Cod_Tara_Ordonator,
         Cod_Tara_Destinatar,
         flag_cash_irh)
        WITH Cust AS
         (SELECT Bk_Cust_Icbs,
                 Bk_Country_Legal
            FROM Dm.Fct_Customer
           WHERE Reporting_Date = v_Datai
             AND Source_Application = 'CRM_APP')
        SELECT /*+ PARALLEL(TXN 4)*/
         Txn.Reporting_Date,
         Txn.Table_Source,
         Txn.Table_Source_Id,
         Txn.Debit_Credit_a,
         Txn.Amount_Eur,
         Txn.Cif_a_Src,
         Txn.Account_a,
         Txn.Cif_b_Src,
         Txn.Account_b,
         TXN.flag_loro Loro_Account_Flag,
         'N' Nostro_Account_Flag,
         'N' Flag_Cash,
         Cust.Bk_Country_Legal Cod_Tara_Ordonator,
         'RO' Cod_Tara_Destinatar,
         TXN.flag_cash_irh
          FROM Tmp_1129_Mlra_Txn_Tmp Txn
         INNER JOIN Cust
            ON Txn.Cif_a_Src = Cust.Bk_Cust_Icbs
         WHERE Txn.Table_Source = 'NR_TRANZ_ACDN';
      p_Nr_Inregistrari := SQL%ROWCOUNT;
      COMMIT;
      Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

      --CO_PST00101_LCL
      p_Nume_Tabela    := 'tmp_1129_mlra_txn_det';
      p_Param_1        := To_Char(v_Datai, 'dd-mon-yyyy');
      p_Desc_Eveniment := 'CO_PST00101_LCL';
      Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                                 p_Nume_Procedura,
                                 p_Nume_Schema,
                                 p_Nume_Tabela,
                                 p_Tip,
                                 p_Param_1,
                                 p_Param_2,
                                 p_Param_3,
                                 p_Desc_Eveniment,
                                 p_Autonumber,
                                         p_Task_Run_Id);
      INSERT INTO Tmp_1129_Mlra_Txn_Det
        (Reporting_Date,
         Table_Source,
         Table_Source_Id,
         Debit_Credit_a,
         Amount_Eur,
         Cif_a_Src,
         Account_a,
         Cif_b_Src,
         Account_b,
         Loro_Account_Flag,
         Nostro_Account_Flag,
         Flag_Cash,
         Cod_Tara_Ordonator,
         Cod_Tara_Destinatar,
         flag_cash_irh)
        SELECT /*+ PARALLEL(TXN 4)*/
         Txn.Reporting_Date,
         Txn.Table_Source,
         Txn.Table_Source_Id,
         Txn.Debit_Credit_a,
         Txn.Amount_Eur,
         Txn.Cif_a_Src,
         Txn.Account_a,
         Txn.Cif_b_Src,
         Txn.Account_b,
         TXN.flag_loro Loro_Account_Flag,
         'N' Nostro_Account_Flag,
         'N' Flag_Cash,
         CASE
           WHEN Txn.Cif_a_Src IS NOT NULL THEN
            'RO'
           WHEN Txn.Business_Channel = 'AG_GHISEU' AND
                Txn.Channel_Transaction = 'ABT' AND
                Txn.Type_Tool = 'NUMERAR' THEN
            'RO'
           WHEN Txn.Channel_Transaction LIKE '%MTA%' THEN
            'RO'
           ELSE
            'RO' --!!!NULL
         END Cod_Tara_Ordonator,
         CASE
           WHEN Txn.Transaction_Category_Id = 200400 THEN
            'RO'
           WHEN Txn.Business_Channel = 'AG_GHISEU' AND
                Txn.Channel_Transaction = 'ABT' AND
                Txn.Type_Tool = 'NUMERAR' THEN
            'RO'
           ELSE
             'RO' --!!!NULL
         END Cod_Tara_Destinatar,
         TXN.flag_cash_irh
          FROM Tmp_1129_Mlra_Txn_Tmp Txn
         WHERE Txn.Table_Source = 'CO_PST00101_LCL';

      p_Nr_Inregistrari := SQL%ROWCOUNT;
      COMMIT;
      Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

      --CO_SEP_INCOMING_LCL
      p_Nume_Tabela    := 'tmp_1129_mlra_txn_det';
      p_Param_1        := To_Char(v_Datai, 'dd-mon-yyyy');
      p_Desc_Eveniment := 'CO_SEP_INCOMING_LCL';
      Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                                 p_Nume_Procedura,
                                 p_Nume_Schema,
                                 p_Nume_Tabela,
                                 p_Tip,
                                 p_Param_1,
                                 p_Param_2,
                                 p_Param_3,
                                 p_Desc_Eveniment,
                                 p_Autonumber,
                                         p_Task_Run_Id);
      INSERT INTO raportdwh.Tmp_1129_Mlra_Txn_Det
        (Reporting_Date,
         Table_Source,
         Table_Source_Id,
         Debit_Credit_a,
         Amount_Eur,
         Cif_a_Src,
         Account_a,
         Cif_b_Src,
         Account_b,
         Loro_Account_Flag,
         Nostro_Account_Flag,
         Flag_Cash,
         Cod_Tara_Ordonator,
         Cod_Tara_Destinatar,
         flag_cash_irh)
        SELECT /*+ PARALLEL(TXN 4)*/
         Txn.Reporting_Date,
         Txn.Table_Source,
         Txn.Table_Source_Id,
         Txn.Debit_Credit_a,
         Txn.Amount_Eur,
         Txn.Cif_a_Src,
         Txn.Account_a,
         Txn.Cif_b_Src,
         Txn.Account_b,
         TXN.flag_loro Loro_Account_Flag,
         'N' Nostro_Account_Flag,
         'N' Flag_Cash,
         CASE
           WHEN Sep_i.Bic_Bnc_Ordonatoare IS NOT NULL AND
                Substr(Sep_i.Bic_Bnc_Ordonatoare, 5, 2) IN
                (SELECT Country_Code
                   FROM Man_1129_Mlra_Country_Lookup) THEN
            Substr(Sep_i.Bic_Bnc_Ordonatoare, 5, 2)
           WHEN Length(Nvl(Regexp_Replace(Sep_i.Iban_Platitor, '-', ''), 'x')) > 15 AND
                Substr(Sep_i.Iban_Platitor, 1, 2) IN
                (SELECT Country_Code
                   FROM Man_1129_Mlra_Country_Lookup) THEN
            Substr(Sep_i.Iban_Platitor, 1, 2)
           WHEN Substr(Sep_i.Nume_Platitor, 5, 2) IN
                (SELECT Country_Code
                   FROM Man_1129_Mlra_Country_Lookup) THEN
            Substr(Sep_i.Nume_Platitor, 5, 2)
           WHEN Txn.Bk_Bank IS NOT NULL AND
                Substr(Txn.Bk_Bank, 5, 2) IN
                (SELECT Country_Code
                   FROM Man_1129_Mlra_Country_Lookup) THEN
            Substr(Txn.Bk_Bank, 5, 2)
              WHEN txn.ORIGINATOR_BK_COUNTRY IN (SELECT Country_Code
                   FROM Man_1129_Mlra_Country_Lookup)
              THEN txn.ORIGINATOR_BK_COUNTRY
           ELSE
           NULL--NVL(txn.ORIGINATOR_BK_COUNTRY,'RO') --!!!! original
         END Cod_Tara_Ordonator,
         CASE
           WHEN Sep_i.Bic_Bnc_Beneficiara IS NOT NULL AND
             SEP_I.Bic_Bnc_Beneficiara IN (
                                       SELECT Bank_Bic_Code FROM
                                       Dm.Nom_Bic_Swift_Code WHERE SOURCE_SYSTEM not in 'DEFAULT'
                                       union
                                       SELECT Bk_Bank_Branch_Bic_Code FROM
                                       Dm.Nom_Bic_Swift_Code  WHERE SOURCE_SYSTEM not in 'DEFAULT') --VALIDARE PE BIC
              AND   Substr(Sep_i.Bic_Bnc_Beneficiara, 5, 2) IN
                (SELECT Country_Code
                   FROM Man_1129_Mlra_Country_Lookup) THEN
            Substr(Sep_i.Bic_Bnc_Beneficiara, 5, 2)

             WHEN sEP_i.Tipul_Mesajului = 202 AND
                  SEP_I.Nume_Beneficiar IN (
                                       SELECT Bank_Bic_Code FROM
                                       Dm.Nom_Bic_Swift_Code WHERE SOURCE_SYSTEM not in 'DEFAULT'
                                       union
                                       SELECT Bk_Bank_Branch_Bic_Code FROM
                                       Dm.Nom_Bic_Swift_Code  WHERE SOURCE_SYSTEM not in 'DEFAULT') --VALIDARE PE BIC /NUME BENEFICIAR
           AND   Substr(Sep_i.Nume_Beneficiar, 5, 2) IN
                (SELECT Country_Code
                   FROM Man_1129_Mlra_Country_Lookup) THEN
            Substr(Sep_i.Nume_Beneficiar, 5, 2)

           WHEN Length(Nvl(Regexp_Replace(Sep_i.Iban_Beneficiar, '-', ''),
                           'x')) > 15 AND
                Substr(Sep_i.Iban_Beneficiar, 1, 2) IN
                (SELECT Country_Code
                   FROM Man_1129_Mlra_Country_Lookup) THEN
            UPPER(Substr(Sep_i.Iban_Beneficiar, 1, 2))
            WHEN txn.BENEFICIARY_BK_COUNTRY IN (SELECT Country_Code
                   FROM Man_1129_Mlra_Country_Lookup)
              THEN txn.BENEFICIARY_BK_COUNTRY
           ELSE
           NULL --  NVL(txn.BENEFICIARY_BK_COUNTRY,'RO') --!!!! original
         END Cod_Tara_Destinatar,
         TXN.flag_cash_irh
          FROM Tmp_1129_Mlra_Txn_Tmp Txn
         INNER JOIN Tmp_1129_Mlra_Sep_Incoming Sep_i
            ON Sep_i.Date_From = Txn.Reporting_Date
           AND Sep_i.Unid = Txn.Table_Source_Id
         WHERE Txn.Table_Source = 'CO_SEP_INCOMING_LCL';

      p_Nr_Inregistrari := SQL%ROWCOUNT;
      COMMIT;
      Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

      --CO_SEP_OUTGOING_LCL
      p_Nume_Tabela    := 'tmp_1129_mlra_txn_det';
      p_Param_1        := To_Char(v_Datai, 'dd-mon-yyyy');
      p_Desc_Eveniment := 'CO_SEP_OUTGOING_LCL';
      Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                                 p_Nume_Procedura,
                                 p_Nume_Schema,
                                 p_Nume_Tabela,
                                 p_Tip,
                                 p_Param_1,
                                 p_Param_2,
                                 p_Param_3,
                                 p_Desc_Eveniment,
                                 p_Autonumber,
                                         p_Task_Run_Id);
      INSERT INTO Tmp_1129_Mlra_Txn_Det
        (Reporting_Date,
         Table_Source,
         Table_Source_Id,
         Debit_Credit_a,
         Amount_Eur,
         Cif_a_Src,
         Account_a,
         Cif_b_Src,
         Account_b,
         Loro_Account_Flag,
         Nostro_Account_Flag,
         Flag_Cash,
         Cod_Tara_Ordonator,
         Cod_Tara_Destinatar,
         flag_cash_irh)
        SELECT /*+ PARALLEL(TXN 4)*/
         Txn.Reporting_Date,
         Txn.Table_Source,
         Txn.Table_Source_Id,
         Txn.Debit_Credit_a,
         Txn.Amount_Eur,
         Txn.Cif_a_Src,
         Txn.Account_a,
         Txn.Cif_b_Src,
         Txn.Account_b,
         TXN.flag_loro Loro_Account_Flag,
         'N' Nostro_Account_Flag,
         'N' Flag_Cash,
         CASE
           WHEN Regexp_Like(Iban_Platitor, '^[[:digit:]]+$') AND
                Substr(Sep_o.Bic_Bnc_Ordonatoare, 5, 2) IN
                (SELECT Country_Code
                   FROM Man_1129_Mlra_Country_Lookup) THEN
            Substr(Sep_o.Bic_Bnc_Ordonatoare, 5, 2)
           WHEN Substr(Sep_o.Iban_Platitor, 1, 2) IN
                (SELECT Country_Code
                   FROM Man_1129_Mlra_Country_Lookup) THEN
            Substr(Sep_o.Iban_Platitor, 1, 2)
           ELSE
            NVL(txn.ORIGINATOR_BK_COUNTRY,'RO') --!!!!  NULL
         END Cod_Tara_Ordonator,
         CASE
           WHEN Regexp_Like(Iban_Beneficiar, '^[[:digit:]]+$') AND
                Substr(Sep_o.Bic_Bnc_Beneficiara, 5, 2) IN
                (SELECT Country_Code
                   FROM Man_1129_Mlra_Country_Lookup) THEN
            Substr(Sep_o.Bic_Bnc_Beneficiara, 5, 2)
           WHEN Substr(Sep_o.Iban_Beneficiar, 1, 2) IN
                (SELECT Country_Code
                   FROM Man_1129_Mlra_Country_Lookup) THEN
            Substr(Sep_o.Iban_Beneficiar, 1, 2)
           ELSE
           NVL(txn.BENEFICIARY_BK_COUNTRY,'RO') --!!!! NULL
         END Cod_Tara_Destinatar,
         TXN.flag_cash_irh
          FROM Tmp_1129_Mlra_Txn_Tmp Txn
         INNER JOIN Tmp_1129_Mlra_Sep_Outgoing Sep_o
            ON Sep_o.Date_From = Txn.Reporting_Date
           AND Sep_o.Unid = Txn.Table_Source_Id
         WHERE Txn.Table_Source = 'CO_SEP_OUTGOING_LCL';
      p_Nr_Inregistrari := SQL%ROWCOUNT;
      COMMIT;
      Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

      --CO_TRAN_TXN_STMT_LCL
      p_Nume_Tabela    := 'tmp_1129_mlra_txn_det';
      p_Param_1        := To_Char(v_Datai, 'dd-mon-yyyy');
      p_Desc_Eveniment := 'CO_TRAN_TXN_STMT_LCL';
      Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                                 p_Nume_Procedura,
                                 p_Nume_Schema,
                                 p_Nume_Tabela,
                                 p_Tip,
                                 p_Param_1,
                                 p_Param_2,
                                 p_Param_3,
                                 p_Desc_Eveniment,
                                 p_Autonumber,p_Task_Run_Id);
      INSERT INTO raportdwh.Tmp_1129_Mlra_Txn_Det
        (Reporting_Date,
         Table_Source,
         Table_Source_Id,
         Debit_Credit_a,
         Amount_Eur,
         Cif_a_Src,
         Account_a,
         Cif_b_Src,
         Account_b,
         Loro_Account_Flag,
         Nostro_Account_Flag,
         Flag_Cash,
         Cod_Tara_Ordonator,
         Cod_Tara_Destinatar,
         flag_cash_irh)
        WITH Tran AS
         (SELECT Date_From,
                 Unid,
                 Cod_Tranzactie_Cont_a,
                 Cod_Tranzactie_Pst_b,
                 Cod_Tranzactie_Pst_a
            FROM Tmp_1129_Mlra_Tran_Txn_Stmt
           WHERE Date_From = v_Datai)
/*
-- nu mai tranzactionam western union
       , Wu AS
         (SELECT Table_Source_Id,
                 Upper(Substr(Description_1,
                              Length('Transfer WU-') + 1,
                              Length(Description_1))) Tara
            FROM Tmp_1129_Mlra_Txn_Tmp
           WHERE Reporting_Date = v_Datai
             AND Description_1 LIKE 'Transfer WU-%')*/
        SELECT /*+ PARALLEL(TXN 4)*/
         Txn.Reporting_Date,
         Txn.Table_Source,
         Txn.Table_Source_Id,
         Txn.Debit_Credit_a,
         Txn.Amount_Eur,
         Txn.Cif_a_Src,
         Txn.Account_a,
         Txn.Cif_b_Src,
         Txn.Account_b,
         TXN.flag_loro Loro_Account_Flag,
         'N' Nostro_Account_Flag,
         CASE
           WHEN Txn.Account_b LIKE '101%' AND Txn.Subsystem_b_Src = 40 THEN
            'Y'
           WHEN Txn.Channel_Transaction = 'EFT' AND
                Txn.Description_2 LIKE '04/%' THEN
            'Y'
           WHEN Tran.Cod_Tranzactie_Cont_a = 72 AND
                (Txn.Product_Account_b_Src IN (120, 121, 122, 123) OR
                Tran.Cod_Tranzactie_Pst_b = '0111') THEN
            'Y'
           WHEN Tran.Cod_Tranzactie_Pst_a IN
                ('0111', '012N', '0054', '0F95') THEN
            'Y'
           ELSE
            'N'
         END Flag_Cash,
         CASE
           WHEN Txn.Cif_a_Src IS NOT NULL THEN
            'RO'
           ELSE
          NVL( txn.originator_bk_country,'RO')--!!! NULL
         END Cod_Tara_Ordonator,
         CASE
           WHEN Txn.Cif_b_Src IS NOT NULL THEN
            'RO'
           WHEN Txn.Iban_Code_b_Src IS NOT NULL AND
                Substr(Txn.Iban_Code_b_Src, 1, 2) IN
                (SELECT Country_Code
                   FROM Man_1129_Mlra_Country_Lookup) THEN
            Substr(Txn.Iban_Code_b_Src, 1, 2)
           WHEN Txn.Account_b LIKE '7%' AND Length(Txn.Account_b) = 12 THEN
            'RO'
           WHEN Wbi.Bic_Banca_Benf IS NOT NULL AND
                Substr(Wbi.Bic_Banca_Benf, 5, 2) IN
                (SELECT Country_Code
                   FROM Man_1129_Mlra_Country_Lookup) THEN
            Substr(Wbi.Bic_Banca_Benf, 5, 2)
           WHEN Txn.Channel_Transaction = 'SEP_IN' AND
                Sep.Bic_Bnc_Ordonatoare IS NULL THEN
            'RO'
           WHEN Txn.Channel_Transaction = 'SEP_IN' AND
                Substr(Sep.Bic_Bnc_Ordonatoare, 5, 2) IN
                (SELECT Country_Code
                   FROM Man_1129_Mlra_Country_Lookup) THEN
            Substr(Sep.Bic_Bnc_Ordonatoare, 5, 2)
           WHEN Txn.Channel_Transaction = 'ABT' AND
                Txn.Account_b = 162100000001 AND
                Txn.Description_1 LIKE 'CPI/%' THEN
            'RO'
           WHEN Txn.Channel_Transaction = 'ABT' AND
                Txn.Account_b = 162100000001 AND
                Substr(Txn.Description_5, 1, 2) IN
                (SELECT Country_Code
                   FROM Man_1129_Mlra_Country_Lookup) THEN
            Substr(Txn.Description_5, 1, 2)
          /* WHEN Wu.Table_Source_Id IS NOT NULL AND
                Cntr.Country_Iso_Code IN
                (SELECT Country_Code
                   FROM Man_1129_Mlra_Country_Lookup) THEN
            Cntr.Country_Iso_Code
           WHEN Wu.Table_Source_Id IS NOT NULL AND Wu.Tara = 'STATELE UNITE' THEN
            'US'*/
           WHEN Txn.Channel_Transaction IN
                ('EFT',
                 'BATCH',
                 'GRN',
                 'PAID_IN',
                 'PAID_OUT',
                 'POS',
                 'SDD',
                 'PLATI_MLTP',
                 'ATM',
                 'CONTA',
                 'FCT') THEN
            'RO'
           WHEN Txn.Channel_Transaction LIKE '%MTA%' THEN
            'RO'
           WHEN Txn.Transaction_Category_Id = '600500' THEN
            'RO'
           ELSE
           NVL(txn.BENEFICIARY_BK_COUNTRY,'RO') --!!! NULL
         END Cod_Tara_Destinatar,
         TXN.flag_cash_irh
          FROM raportdwh.Tmp_1129_Mlra_Txn_Tmp Txn
          LEFT JOIN Tmp_1129_Mlra_Wbi_Txn Wbi
            ON Wbi.Date_From = Txn.Reporting_Date
           AND Wbi.Id_Tranzactie = Txn.Description_2
          LEFT JOIN Tmp_1129_Mlra_Sep_Incoming Sep
            ON Sep.Date_From = Txn.Reporting_Date
           AND Substr(Txn.Description_1, 1, 16) = Sep.Id_Tranzactie
         INNER JOIN Tran
            ON Tran.Date_From = Txn.Reporting_Date
           AND Tran.Unid = Txn.Table_Source_Id
/*          LEFT JOIN Wu
            ON Txn.Table_Source_Id = Wu.Table_Source_Id
          LEFT JOIN Raportdwh.St_Country Cntr
            ON Cntr.Country_Name = Wu.Tara*/
         WHERE Txn.Table_Source = 'CO_TRAN_TXN_STMT_LCL';
      p_Nr_Inregistrari := SQL%ROWCOUNT;
      COMMIT;
      Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

      --CO_M_INPAY_LCL
      p_Nume_Tabela    := 'tmp_1129_mlra_txn_det';
      p_Param_1        := To_Char(v_Datai, 'dd-mon-yyyy');
      p_Desc_Eveniment := 'CO_M_INPAY_LCL';
      Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                                 p_Nume_Procedura,
                                 p_Nume_Schema,
                                 p_Nume_Tabela,
                                 p_Tip,
                                 p_Param_1,
                                 p_Param_2,
                                 p_Param_3,
                                 p_Desc_Eveniment,
                                 p_Autonumber,p_Task_Run_Id);
      INSERT INTO Tmp_1129_Mlra_Txn_Det
        (Reporting_Date,
         Table_Source,
         Table_Source_Id,
         Debit_Credit_a,
         Amount_Eur,
         Cif_a_Src,
         Account_a,
         Cif_b_Src,
         Account_b,
         Loro_Account_Flag,
         Nostro_Account_Flag,
         Flag_Cash,
         Cod_Tara_Ordonator,
         Cod_Tara_Destinatar,
         flag_cash_irh)
        SELECT /*+ PARALLEL(TXN 4)*/
        DISTINCT Txn.Reporting_Date,
                 Txn.Table_Source,
                 Txn.Table_Source_Id,
                 Txn.Debit_Credit_a,
                 Txn.Amount_Eur,
                 Txn.Cif_a_Src,
                 Txn.Account_a,
                 Txn.Cif_b_Src,
                 Txn.Account_b,
                /* CASE
                   WHEN Acc.Product_Subtype_Id IN (1240, 1241, 1248, 1249) AND
                        Dict.Cod_Dest = 'Y' THEN
                    'Y'
                   ELSE
                    'N'
                 END Loro_Account_Flag,*/
                 TXN.flag_loro Loro_Account_Flag,
                 CASE
                   WHEN Acc.Product_Subtype_Id = 1212 THEN
                    'Y'
                   ELSE
                    'N'
                 END Nostro_Account_Flag,
                 'N' Flag_Cash,

                 CASE
                   WHEN Inpay.Ordering_Bank_Typ = 'S' AND
                        Substr(Inpay.Ordering_Bank1, 5, 2) IN
                        (SELECT Country_Code
                           FROM Man_1129_Mlra_Country_Lookup) THEN
                    Substr(Inpay.Ordering_Bank1, 5, 2)
                   WHEN Inpay.Sender_Type = 'S' AND
                        Substr(Inpay.Sender1, 5, 2) IN
                        (SELECT Country_Code
                           FROM Man_1129_Mlra_Country_Lookup) THEN
                    Substr(Inpay.Sender1, 5, 2)
                   WHEN Inpay.Bank_Inf1 IS NOT NULL AND
                        Substr(Inpay.Bank_Inf1, 10, 2) IN
                        (SELECT Country_Code
                           FROM Man_1129_Mlra_Country_Lookup) THEN
                    Substr(Inpay.Bank_Inf1, 10, 2)
                   WHEN Inpay.Ordering_Cust_Typ = 'A' AND
                        Substr(Inpay.Ordering_Cust1, 2, 2) IN
                        (SELECT Country_Code
                           FROM Man_1129_Mlra_Country_Lookup) THEN
                    Substr(Inpay.Ordering_Cust1, 2, 2)
                   WHEN Inpay.Ordering_Cust_Typ = 'S' AND
                        Substr(Inpay.Ordering_Cust1, 5, 2) IN
                        (SELECT Country_Code
                           FROM Man_1129_Mlra_Country_Lookup) THEN
                    Substr(Inpay.Ordering_Cust1, 5, 2)
                   WHEN Cust.Country_Of_Citizenship IS NOT NULL AND
                        Cust.Country_Of_Citizenship IN
                        (SELECT Country_Code
                           FROM Man_1129_Mlra_Country_Lookup) THEN
                    Cust.Country_Of_Citizenship
                    ELSE
           NULL --NVL( Txn.ORIGINATOR_BK_COUNTRY,'RO') --!!!
                 END Cod_Tara_Ordonator,

                 CASE
                   WHEN Inpay.Benefic_Cust_Type = 'S'
                      and Inpay.Beneficiary1 in (
                     SELECT BK_BANK_BRANCH_BIC_CODE FROM dm.nom_bic_swift_code
                              WHERE SOURCE_SYSTEM  NOT IN  'DEFAULT'
                              UNION
                              SELECT BANK_BIC_CODE FROM dm.nom_bic_swift_code
                              WHERE SOURCE_SYSTEM  NOT IN  'DEFAULT'
                     )
                   AND
                        Substr(Inpay.Beneficiary1, 3, 2) IN
                        (SELECT Country_Code
                           FROM Man_1129_Mlra_Country_Lookup) THEN
                    Substr(Inpay.Beneficiary1, 5, 2)
                    when Inpay.Benefic_Cust_Type = 'A'
                     and m_Wbi.Bic_Banca_Benf in (
                     SELECT BK_BANK_BRANCH_BIC_CODE FROM dm.nom_bic_swift_code
                              WHERE SOURCE_SYSTEM  NOT IN  'DEFAULT'
                              UNION
                              SELECT BANK_BIC_CODE FROM dm.nom_bic_swift_code
                              WHERE SOURCE_SYSTEM  NOT IN  'DEFAULT'
                     )
                   AND Substr(m_Wbi.Bic_Banca_Benf,  5, 2) in (SELECT Country_Code
                           FROM Man_1129_Mlra_Country_Lookup) THEN
                Substr(m_Wbi.Bic_Banca_Benf, 5, 2)

                   WHEN Inpay.Benefic_Cust_Type = 'A' AND
                        Substr(Inpay.Beneficiary1, 3, 2) IN
                        (SELECT Country_Code
                           FROM Man_1129_Mlra_Country_Lookup) THEN
                    Substr(Inpay.Beneficiary1, 3, 2)
                   WHEN Inpay.Benefic_Cust_Type = 'P' THEN
                    'RO'
                    --!!!!!
                    when Substr(Inpay.Beneficiary5, 5, 2) IN
                        (SELECT Country_Code
                           FROM Man_1129_Mlra_Country_Lookup)
                    then Substr(Inpay.Beneficiary5, 5, 2)

                    when Substr(Txn.Beneficiary_Bk_Country, 5, 2) IN
                        (SELECT Country_Code
                           FROM Man_1129_Mlra_Country_Lookup)
                    then Substr(Txn.Beneficiary_Bk_Country, 5, 2)

                    ELSE
         'RO'--NULL--    NVL(Txn.Beneficiary_Bk_Country,'RO') --!!!
                 END Cod_Tara_Destinatar,

                 TXN.flag_cash_irh
          FROM Tmp_1129_Mlra_Txn_Tmp Txn
         INNER JOIN Tmp_1129_Mlra_m_Inpay Inpay
            ON Txn.Reporting_Date = Inpay.Date_From
           AND Txn.Table_Source_Id = Inpay.Unid
          LEFT JOIN Tmp_1129_Mlra_Midas_Acct_Txn Acc
            ON Inpay.Date_From = Acc.Date_From
           AND Inpay.Payment_Reference = Acc.Payment_Ref
           AND Acc.Product_Subtype_Id IN (1240, 1241, 1248, 1249, 1212)
           LEFT JOIN raportdwh.Tmp_1129_Mlra_Wbi_Txn m_Wbi
          ON m_Wbi.Date_From = Inpay.Date_From
         AND m_Wbi.Id_Tranzactie =
             Substr(Inpay.Front_Office_Transaction_Id,
                    1,
                    Length(Inpay.Front_Office_Transaction_Id) - 2) || '00'
        /*  LEFT JOIN Man_1129_Mlra_Dict Dict
            ON Dict.Cod_Src = Inpay.Payment_Type
           AND Dict.Sub_Cod_Src = Inpay.Payment_Subtype
           AND Dict.Id_Categ = 3*/
          LEFT JOIN raportdwh.Tmp_1129_Mlra_Midas_customer cust--WRK.STG_MIDAS_CUSTOMER Cust--RWH1191 inlocuit db_link //!!!xxx - verifica ca sunt date acolo la v_datai
            ON Cust.Date_From = v_Datai
           AND Cust.Customer_Id = Substr(Inpay.Sender1, 1, 6)
         WHERE Txn.Table_Source = 'CO_M_INPAY_LCL';

      p_Nr_Inregistrari := SQL%ROWCOUNT;
      COMMIT;
      Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

      --CO_M_OTPAY_LCL
      p_Nume_Tabela    := 'tmp_1129_mlra_txn_det';
      p_Param_1        := To_Char(v_Datai, 'dd-mon-yyyy');
      p_Desc_Eveniment := 'CO_M_OTPAY_LCL';
      Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                                 p_Nume_Procedura,
                                 p_Nume_Schema,
                                 p_Nume_Tabela,
                                 p_Tip,
                                 p_Param_1,
                                 p_Param_2,
                                 p_Param_3,
                                 p_Desc_Eveniment,
                                 p_Autonumber,p_Task_Run_Id);
      INSERT INTO Tmp_1129_Mlra_Txn_Det
        (Reporting_Date,
         Table_Source,
         Table_Source_Id,
         Debit_Credit_a,
         Amount_Eur,
         Cif_a_Src,
         Account_a,
         Cif_b_Src,
         Account_b,
         Loro_Account_Flag,
         Nostro_Account_Flag,
         Flag_Cash,
         Cod_Tara_Ordonator,
         Cod_Tara_Destinatar,
         flag_cash_irh)
        WITH Produse AS
         (SELECT /*+ PARALLEL(TXN 4)*/
          DISTINCT Txn.Reporting_Date,
                   Txn.Table_Source,
                   Txn.Table_Source_Id,
                   Txn.Debit_Credit_a,
                   Txn.Amount_Eur,
                   Txn.Cif_a_Src,
                   Txn.Account_a,
                   Txn.Cif_b_Src,
                   Txn.Account_b,
                 /*  CASE
                     WHEN Acc.Product_Subtype_Id IN (1240, 1241, 1248, 1249) AND
                          Dict.Cod_Dest = 'Y' THEN
                      1
                     ELSE
                      0
                   END Loro_Account_Flag,*/
                   TXN.flag_loro Loro_Account_Flag,
                   CASE
                     WHEN Acc.Product_Subtype_Id = 1212 THEN
                      1
                     ELSE
                      0
                   END Nostro_Account_Flag,
                   'N' Flag_Cash,
                   CASE
                     WHEN Otpay.Ordering_Cust_Type = 'P' THEN
                      'RO'
                     WHEN Otpay.Ordering_Cust_Type = 'A' AND
                          Substr(Otpay.Ordering_Cust1, 1, 2) IN
                          (SELECT Country_Code
                             FROM Raportdwh.Man_1129_Mlra_Country_Lookup) THEN
                      Substr(Otpay.Ordering_Cust1, 1, 2)
                     WHEN Otpay.Ordering_Cust_Type = 'S' AND
                          Substr(Otpay.Ordering_Cust1, 5, 2) IN
                          (SELECT Country_Code
                             FROM Raportdwh.Man_1129_Mlra_Country_Lookup) THEN
                      Substr(Otpay.Ordering_Cust1, 5, 2)
                     ELSE
                        NVL(Txn.ORIGINATOR_BK_COUNTRY,'RO') --!!!NULL
                   END Cod_Tara_Ordonator,
                   CASE
                     WHEN Otpay.Acc_With_Bank = 'S' AND
                          Substr(Otpay.Acc_With_Bank1, 5, 2) IN
                          (SELECT Country_Code
                             FROM Raportdwh.Man_1129_Mlra_Country_Lookup) THEN
                      Substr(Otpay.Acc_With_Bank1, 5, 2)
                     WHEN Otpay.Benef_Type = 'S' AND
                          Substr(Otpay.Benef_Cust1, 5, 2) IN
                          (SELECT Country_Code
                             FROM Raportdwh.Man_1129_Mlra_Country_Lookup) THEN
                      Substr(Otpay.Benef_Cust1, 5, 2)
                     WHEN Otpay.Benef_Type <> 'S' AND
                          Substr(Otpay.Benef_Cust1, 2, 2) IN
                          (SELECT Country_Code
                             FROM Raportdwh.Man_1129_Mlra_Country_Lookup) THEN
                      Substr(Otpay.Benef_Cust1, 2, 2)
                     WHEN Txn.Bk_Bank IS NOT NULL AND
                          Substr(Txn.Bk_Bank, 5, 2) IN
                          (SELECT Country_Code
                             FROM Raportdwh.Man_1129_Mlra_Country_Lookup) THEN
                      Substr(Txn.Bk_Bank, 5, 2)
                     --!!!
                      WHEN Txn.Iban_Code_b_Src IS NOT NULL AND
                          Substr(Txn.Iban_Code_b_Src, 1, 2) IN
                          (SELECT Country_Code
                             FROM Raportdwh.Man_1129_Mlra_Country_Lookup) THEN
                      Substr(Txn.Iban_Code_b_Src, 1, 2)
                     ELSE
                      NVL( Txn.Beneficiary_Bk_Country,'RO') --!!!NULL
                   END Cod_Tara_Destinatar,
                   Otpay.Payment_Reference,
                   Acc.Unid,
                   TXN.flag_cash_irh
            FROM Raportdwh.Tmp_1129_Mlra_Txn_Tmp Txn
           INNER JOIN Raportdwh.Tmp_1129_Mlra_m_Otpay Otpay
              ON Txn.Reporting_Date = Otpay.Date_From
             AND Txn.Table_Source_Id = Otpay.Unid
            LEFT JOIN Raportdwh.Tmp_1129_Mlra_Midas_Acct_Txn Acc
              ON Otpay.Date_From = Acc.Date_From
             AND Otpay.Payment_Reference = Acc.Payment_Ref
             AND Acc.Product_Subtype_Id IN (1240, 1241, 1248, 1249, 1212)
           /* LEFT JOIN Raportdwh.Man_1129_Mlra_Dict Dict
              ON Dict.Cod_Src = Otpay.Payment_Type
             AND Dict.Sub_Cod_Src = Otpay.Payment_Subtype
             AND Dict.Id_Categ = 3*/
           WHERE Txn.Table_Source = 'CO_M_OTPAY_LCL')
        SELECT Reporting_Date,
               Table_Source,
               Table_Source_Id,
               Debit_Credit_a,
               Amount_Eur,
               Cif_a_Src,
               Account_a,
               Cif_b_Src,
               Account_b,
          /*     CASE
                 WHEN SUM(Loro_Account_Flag) >= 1 THEN
                  'Y'
                 ELSE
                  'N'
               END Loro_Account_Flag,*/
                Loro_Account_Flag,
               CASE
                 WHEN SUM(Nostro_Account_Flag) >= 1 THEN
                  'Y'
                 ELSE
                  'N'
               END Nostro_Account_Flag,
               Flag_Cash,
               Cod_Tara_Ordonator,
               Cod_Tara_Destinatar,
               flag_cash_irh
          FROM Produse
         GROUP BY Reporting_Date,
                  Table_Source,
                  Table_Source_Id,
                  Debit_Credit_a,
                  Amount_Eur,
                  Cif_a_Src,
                  Account_a,
                  Cif_b_Src,
                  Account_b,
                     Loro_Account_Flag,
                  Flag_Cash,
                  Cod_Tara_Ordonator,
                  Cod_Tara_Destinatar,
                  flag_cash_irh;
      p_Nr_Inregistrari := SQL%ROWCOUNT;
      COMMIT;
      Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

      Dbms_Stats.Gather_Table_Stats(Ownname          => 'RAPORTDWH',
                                    Tabname          => 'tmp_1129_mlra_txn_det',
                                    Estimate_Percent => Dbms_Stats.Auto_Sample_Size,
                                    Cascade          => TRUE,
                                    Method_Opt       => 'FOR ALL COLUMNS SIZE AUTO');

      p_Nume_Tabela    := 'tmp_1129_mlra_txn_f_det';
      p_Param_1        := To_Char(v_Datai, 'dd-mon-yyyy');
      p_Desc_Eveniment := 'mapare finala - tabela detaliata de tranzactii';
      Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                                 p_Nume_Procedura,
                                 p_Nume_Schema,
                                 p_Nume_Tabela,
                                 p_Tip,
                                 p_Param_1,
                                 p_Param_2,
                                 p_Param_3,
                                 p_Desc_Eveniment,
                                 p_Autonumber,p_Task_Run_Id);

      INSERT INTO Tmp_1129_Mlra_Txn_f_Det
        (Reporting_Date,
         Report_Cutoff_Date,
         Table_Source,
         Table_Source_Id,
         Cif_a_Src,
         Customer_Lob,
         Customer_Rr_Level,
         Aml_Customer_Type,
         Loro_Account_Flag,
         Nostro_Account_Flag,
         Transaction_Group,
         Amount_Eur,
         Debit_Credit,
         Flag_Cash,
         Cod_Tara_Ordonator,
         Domestic_Flag,
         COD_TARA_DESTINATAR,
         flag_cash_irh)
    /*    WITH Country_Lookup AS
         (SELECT Country_Code,
                 CASE
                   WHEN Region_For_Reporting = 'EU15 (excl. Domestic)' THEN
                    'EU15_TXN'
                   WHEN Fatf_Flag = 'Y' THEN
                    'FATF_TXN'
                   WHEN Region_For_Reporting =
                        'High Risk (excl. EU-HR and Offshore)' THEN
                    'HIGHRISK_TXN'
                   WHEN Offshore_Flag = 'Y' THEN
                    'OFFSHORE_TXN'
                   WHEN Region_For_Reporting =
                        'Third Country (excl. EU-HR, HR, EU15, Offshore)' THEN
                    'THIRD_COUNTRY'
                 END Grup_Tara
            FROM Man_1129_Mlra_Country_Lookup)*/
        SELECT Txn.Reporting_Date,
               p_Reporting_Date Report_Cutoff_Date,
               Txn.Table_Source,
               Txn.Table_Source_Id,
               Txn.Cif_a_Src,
               Nvl(Cl.Customer_Lob, 'NO_VAL') Customer_Lob,
               Nvl(Cl.Customer_Rr_Level, 'NO_VAL') Customer_Rr_Level,
               Cl.Aml_Customer_Type,
               Txn.Loro_Account_Flag,
               Txn.Nostro_Account_Flag,
               CASE
                 WHEN Txn.flag_cash_irh = 'Y' THEN
                  'CSH_TXN'
                  when txn.cod_tara_destinatar ='RO' AND TXN.COD_TARA_ORDONATOR='RO' THEN 'DOMESTIC'
                  WHEN TXN.COD_TARA_DESTINATAR ='RO' AND TXN.COD_TARA_ORDONATOR NOT IN ('RO') THEN ORD.REGION_FOR_REPORTING
                  WHEN TXN.COD_TARA_DESTINATAR NOT IN ('RO') AND TXN.COD_TARA_ORDONATOR ='RO' THEN DEST.REGION_FOR_REPORTING
               when Txn.Debit_Credit_a ='D' THEN DEST.REGION_FOR_REPORTING
               when Txn.Debit_Credit_a ='C' THEN ORD.REGION_FOR_REPORTING
                 ELSE NULL
               END Transaction_Group,
               Txn.Amount_Eur,
               Txn.Debit_Credit_a,
               Txn.Flag_Cash,
               Txn.Cod_Tara_Ordonator,
               CASE
                 WHEN Txn.Cod_Tara_Ordonator = 'RO' AND
                      Txn.Cod_Tara_Destinatar = 'RO' THEN
                  'Y'
                 ELSE
                  'N'
               END Domestic_Flag,
               txn.COD_TARA_DESTINATAR,
               TXN.flag_cash_irh
          FROM Tmp_1129_Mlra_Txn_Det Txn
          LEFT JOIN Tmp_1129_Mlra_Clienti_f Cl
            ON Txn.Cif_a_Src = Cl.Cif
           AND Cl.Reporting_Date = p_Reporting_Date
          LEFT JOIN raportdwh.Man_1129_Mlra_Country_Lookup Ord
            ON Txn.Cod_Tara_Ordonator = Ord.Country_Code
          LEFT JOIN raportdwh.Man_1129_Mlra_Country_Lookup Dest
            ON Txn.Cod_Tara_Destinatar = Dest.Country_Code
/*          LEFT JOIN Man_1129_Mlra_Country_Matrix Mx
            ON Ord.Grup_Tara = Mx.Region_Rep_Ordonator
           AND Dest.Grup_Tara = Mx.Region_Rep_Destinatar*/;

      p_Nr_Inregistrari := SQL%ROWCOUNT;
      COMMIT;
      Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);


      v_Datai := Raportdwh.Next_Working_Day(v_Datai);
    END LOOP;

      Dbms_Stats.Gather_Table_Stats(Ownname          => 'RAPORTDWH',
                                    Tabname          => 'tmp_1129_mlra_txn_f_det',
                                    Estimate_Percent => Dbms_Stats.Auto_Sample_Size,
                                    Cascade          => TRUE,
                                    Method_Opt       => 'FOR ALL COLUMNS SIZE AUTO');

    Proc_Write_To_From_Hist(p_Task_Run_Id ,p_Reporting_Date,
                            'TMP_1129_MLRA_TXN_F_DET',
                            'TMP_1129_MLRA_TXN_F_DET_H');
  END;

  PROCEDURE Proc_Produse( p_Task_Run_Id    NUMBER,p_Reporting_Date DATE) AS

  BEGIN

    Proc_Temp_Table_Prod(p_Task_Run_Id ,p_Reporting_Date);

    EXECUTE IMMEDIATE 'TRUNCATE TABLE tmp_1129_mlra_produse_temp ';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE tmp_1129_mlra_produse_gpc_TMP ';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE tmp_1129_mlra_produse_gpc ';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE Tmp_1129_Mlra_Assets_Temp';

    --ASSETS
    --RWH1191 noua tabela temporara
    p_Nume_Procedura := 'PROC_PRODUSE';
    p_Nume_Tabela    := 'Tmp_1129_Mlra_Assets_Temp';
    p_Tip            := 'F';
    p_Desc_Eveniment := '0. ASSETS';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO Raportdwh.Tmp_1129_Mlra_Assets_Temp
      (Row_Id,
       Date_From,
       Owner_Con_Id,
       Owner_Accnt_Id,
       Prod_Id,
       Prod_Name,
       Status_Cd,
       Pf_Integration_Id,
       Pj_Integration_Id)
      SELECT Ath.Row_Id,
             Ath.Date_From,
             Ath.Owner_Con_Id,
             Ath.Owner_Accnt_Id,
             Ath.Prod_Id,
             Prod.Name,
             Ath.Status_Cd,
             Lpad(Pf.Integration_Id, 10, 0),
             Lpad(Pj.Integration_Id, 10, 0)
        FROM Wrk.Stg_Crm_s_Asset Ath
        JOIN Wrk.Stg_Crm_s_Prod_Int Prod
          ON Ath.Prod_Id = Prod.Row_Id
         AND Ath.Date_From = Prod.Date_From
         AND Prod.Name IN ('RBRO_RO_PF', --Daca mai e nevoie de alte produse se aduc aici
                           'RBRO_SM_PF',
                           'RBRO_SM_PJ',
                           'RBRO_RO_PJ',
                           'RBRO_RO_CORP') -- 24.05.2024 Magda Dogaru CDS-268 Athena Corporate
        LEFT JOIN Wrk.Stg_Crm_s_Contact Pf
          ON Pf.Row_Id = Ath.Owner_Con_Id
         AND Pf.Date_From = Ath.Date_From
         AND Pf.Con_Cd = 'Customer'
        LEFT JOIN Wrk.Stg_Crm_s_Org_Ext Pj
          ON Pj.Row_Id = Ath.Owner_Accnt_Id
         AND Pj.Date_From = Ath.Date_From
       WHERE Ath.Status_Cd = 'Active'
         AND Ath.Date_From = p_Reporting_Date;

    --CREDIT_CARD_APP
    p_Nume_Procedura := 'PROC_PRODUSE';
    p_Nume_Tabela    := 'tmp_1129_mlra_produse_temp';
    p_Tip            := 'F';
    p_Desc_Eveniment := '1. CREDIT_CARD_APP';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO /*+ APPEND*/
    Raportdwh.Tmp_1129_Mlra_Produse_Temp
      (Reporting_Date,
       Source_App,
       Product_Type_Id,
       Product_Subtype_Id,
       Cif,
       Contract_Id,
       Cod_Gpc)
      SELECT Pr.Reporting_Date AS Reporting_Date,
             'CREDIT_CARD_APP' AS Source_App,
             Pr.Application_Type AS Product_Type_Id,
             NULL AS Product_Subtype_Id, --null ca sa se mapeze la gpc - toate carduri de credit au acelasi cod gpc
             Pr.Customer_Icbs_Id AS Cif,
             Pr.Facility_Id_Contract_Number AS Contract_Id,
             Pr.Gpc_Code Cod_Gpc
        FROM Dm.Fct_All_Products_Monthly Pr
       WHERE Pr.Reporting_Date = p_Reporting_Date
         AND Pr.Source_Application = 'CREDIT_CARD_APP'
         AND Pr.Facility_Status <> '-2';

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --DEBIT_CARD_APP PF
    p_Nume_Tabela    := 'tmp_1129_mlra_produse_temp';
    p_Tip            := 'I';
    p_Desc_Eveniment := '2. DEBIT_CARD_APP PF';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO /*+ APPEND*/
    Raportdwh.Tmp_1129_Mlra_Produse_Temp
      (Reporting_Date,
       Source_App,
       Product_Type_Id,
       Product_Subtype_Id,
       Cif,
       Contract_Id,
       Cod_Gpc)
      SELECT DISTINCT Deb.Date_From AS Reporting_Date,
                      'DEBIT_CARD_APP' AS Source_App,
                      'DC' AS Product_Type_Id,
                      Deb.Card_Type AS Product_Subtype_Id,
                      Pf.Cif_Crm AS Cif,
                      Deb.Card_Number AS Contract_Id,
                      NULL Cod_Gpc
        FROM Raportdwh.Tmp_1129_Mlra_Debit_Card Deb
        JOIN Dm.Dim_Customer Pf
          ON Deb.Date_From BETWEEN Pf.Date_From AND Pf.Date_Until
         AND Pf.Source_Application = 'CRM_APP'
         AND Deb.Citizen_Id = Pf.Cnp
         AND Deb.Personal_Corporate_Card = 'P'
            -->> Start CD_89 PBLMGT_7668
            --AND Pf.History_Indicator = 1
            --<< End CD_89 PBLMGT_7668
         AND Pf.Operation_Type <> 'D'
       WHERE Deb.Date_From = p_Reporting_Date
         AND Deb.Active_Inactive = 'A';

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --DEBIT_CARD_APP PJ
    p_Nume_Tabela    := 'tmp_1129_mlra_produse_temp';
    p_Tip            := 'I';
    p_Desc_Eveniment := '3. DEBIT_CARD_APP PJ';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO /*+ APPEND*/
    Raportdwh.Tmp_1129_Mlra_Produse_Temp
      (Reporting_Date,
       Source_App,
       Product_Type_Id,
       Product_Subtype_Id,
       Cif,
       Contract_Id,
       Cod_Gpc)
      SELECT DISTINCT Deb.Date_From AS Reporting_Date,
                      'DEBIT_CARD_APP' AS Source_App,
                      'DC' AS Product_Type_Id,
                      Deb.Card_Type AS Product_Subtype_Id,
                      Pj.Cif_Crm AS Cif,
                      Deb.Card_Number AS Contract_Id,
                      NULL Cod_Gpc
        FROM Raportdwh.Tmp_1129_Mlra_Debit_Card Deb
        JOIN Dm.Dim_Customer Pj
          ON Deb.Date_From BETWEEN Pj.Date_From AND Pj.Date_Until
         AND Pj.Source_Application = 'CRM_APP'
         AND Deb.Citizen_Id = Pj.Cui
         AND Deb.Personal_Corporate_Card = 'C'
            -->> Start CD_89 PBLMGT_7668
            --AND Pj.History_Indicator = 1
            --<< End CD_89 PBLMGT_7668
         AND Pj.Operation_Type <> 'D'
       WHERE Deb.Date_From = p_Reporting_Date
         AND Deb.Active_Inactive = 'A';

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --DEP_ICBS
    p_Nume_Tabela    := 'tmp_1129_mlra_produse_temp';
    p_Tip            := 'I';
    p_Desc_Eveniment := '4. DEP_ICBS';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO /*+ APPEND*/
    Raportdwh.Tmp_1129_Mlra_Produse_Temp
      (Reporting_Date,
       Source_App,
       Product_Type_Id,
       Product_Subtype_Id,
       Cif,
       Contract_Id,
       Cod_Gpc)
      SELECT Pr.Reporting_Date AS Reporting_Date,
             'DEP_ICBS' AS Source_App,
             'TM' AS Product_Type_Id,
             Pr.Product_Code AS Product_Subtype_Id,
             Pr.Customer_Icbs_Id AS Cif,
             Pr.Facility_Id_Contract_Number AS Contract_Id,
             Pr.Gpc_Code AS Cod_Gpc
        FROM Dm.Fct_All_Products_Monthly Pr
       WHERE Pr.Reporting_Date = p_Reporting_Date
         AND Pr.Source_Application = 'DEP_ICBS'
         AND Pr.Facility_Status = '1';

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --DEP_MIDAS_CONTRACT
 raportdwh.pkg_rap_1129_mlra_reporting.check_and_load(p_task_run_id,p_Reporting_Date,'STG_MIDAS_DEPO_CONTRACT');
 raportdwh.pkg_rap_1129_mlra_reporting.check_and_load(p_task_run_id,p_Reporting_Date,'STG_MIDAS_ICBS_CUST');

    p_Nume_Tabela    := 'tmp_1129_mlra_produse_temp';
    p_Tip            := 'I';
    p_Desc_Eveniment := '5. DEP_MIDAS_CONTRACT';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO /*+ APPEND*/
    Raportdwh.Tmp_1129_Mlra_Produse_Temp
      (Reporting_Date,
       Source_App,
       Product_Type_Id,
       Product_Subtype_Id,
       Cif,
       Contract_Id,
       Cod_Gpc)
      SELECT /*+ DRIVING_SITE(CTR)*/
       Ctr.Date_From AS Reporting_Date,
       'DEP_MIDAS' AS Source_App,
       Ctr.Product_Type_Id AS Product_Type_Id,
       Ctr.Product_Subtype_Id AS Product_Subtype_Id,
       Icbs.Icbs_Customer_Id AS Cif, --C00023633 25nov2020
       Ctr.Contract_Id AS Contract_Id,
       NULL AS Cod_Gpc
        FROM WRK.STG_MIDAS_DEPO_CONTRACT Ctr --RWH1191 inlocuit db_link
        JOIN WRK.STG_MIDAS_ICBS_CUST Icbs --RWH1191 inlocuit db_link
          ON Ctr.Customer_Id = Icbs.Midas_Customer_Id
         AND Ctr.Date_From = Icbs.Date_From
       WHERE Ctr.Date_From = p_Reporting_Date
         AND Nvl(Ctr.Maturity_Date, Ctr.Date_From) >= Ctr.Date_From;

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --DEP_MIDAS_FX

 raportdwh.pkg_rap_1129_mlra_reporting.check_and_load(p_task_run_id,p_Reporting_Date,'STG_MIDAS_FX_DEALS');
 raportdwh.pkg_rap_1129_mlra_reporting.check_and_load(p_task_run_id,p_Reporting_Date,'STG_MIDAS_ICBS_CUST');


    p_Nume_Tabela    := 'tmp_1129_mlra_produse_temp';
    p_Tip            := 'I';
    p_Desc_Eveniment := '6. DEP_MIDAS_FX';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO /*+ APPEND*/
    Raportdwh.Tmp_1129_Mlra_Produse_Temp
      (Reporting_Date,
       Source_App,
       Product_Type_Id,
       Product_Subtype_Id,
       Cif,
       Contract_Id,
       Cod_Gpc)
      SELECT /*+ DRIVING_SITE(CTR)*/
       Ctr.Date_From AS Reporting_Date,
       'DEP_MIDAS' AS Source_App,
       Ctr.Deal_Type_Id AS Product_Type_Id,
       Ctr.Deal_Subtype_Id AS Product_Subtype_Id,
       Icbs.Icbs_Customer_Id AS Cif, --C00023633 25nov2020
       Ctr.Deal_No AS Contract_Id,
       NULL AS Cod_Gpc
        FROM WRK.STG_MIDAS_FX_DEALS Ctr --RWH1191 inlocuit db_link
        JOIN WRK.STG_MIDAS_ICBS_CUST Icbs --RWH1191 inlocuit db_link
          ON Ctr.Customer_Id = Icbs.Midas_Customer_Id
         AND Ctr.Date_From = Icbs.Date_From
       WHERE Ctr.Date_From = p_Reporting_Date;

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --EF3_LIMIT_APP
    p_Nume_Tabela    := 'tmp_1129_mlra_produse_temp';
    p_Tip            := 'I';
    p_Desc_Eveniment := '7. EF3_LIMIT_APP';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO /*+ APPEND*/
    Raportdwh.Tmp_1129_Mlra_Produse_Temp
      (Reporting_Date,
       Source_App,
       Product_Type_Id,
       Product_Subtype_Id,
       Cif,
       Contract_Id,
       Cod_Gpc)
      SELECT Pr.Reporting_Date AS Reporting_Date,
             'EF3_APP' AS Source_App,
             'FC' AS Product_Type_Id,
             Pr.Product_Code AS Product_Subtype_Id,
             Pr.Customer_Icbs_Id AS Cif,
             Pr.Facility_Id_Contract_Number AS Contract_Id,
             Pr.Gpc_Code AS Cod_Gpc
        FROM Dm.Fct_All_Products_Monthly Pr
       WHERE Pr.Reporting_Date = p_Reporting_Date
         AND Pr.Source_Application = 'EF3_LIMIT_APP'
         AND Pr.Facility_Status = 'A';

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

  --IMX_APP
    p_Nume_Tabela    := 'tmp_1129_mlra_produse_temp';
    p_Tip            := 'I';
    p_Desc_Eveniment := '7. IMX_APP';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO /*+ APPEND*/
    Raportdwh.Tmp_1129_Mlra_Produse_Temp
      (Reporting_Date,
       Source_App,
       Product_Type_Id,
       Product_Subtype_Id,
       Cif,
       Contract_Id,
       Cod_Gpc)
      SELECT Pr.Reporting_Date AS Reporting_Date,
             pr.Source_Application AS Source_App,
             'FC' AS Product_Type_Id,
             Pr.Product_Code AS Product_Subtype_Id,
             Pr.Customer_Icbs_Id AS Cif,
             Pr.Facility_Id_Contract_Number AS Contract_Id,
             Pr.Gpc_Code AS Cod_Gpc
        FROM Dm.Fct_All_Products_Monthly Pr
       WHERE Pr.Reporting_Date = p_Reporting_Date
         AND Pr.Source_Application = 'IMX_APP'
         AND Pr.Facility_Status = 'A';

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);


    --EQUITY_EXPOSURE
        raportdwh.pkg_rap_1129_mlra_reporting.check_and_load(p_task_run_id,p_Reporting_Date,'STG_EQUITY');


    p_Nume_Tabela    := 'tmp_1129_mlra_produse_temp';
    p_Tip            := 'I';
    p_Desc_Eveniment := '8. EQUITY_EXPOSURE';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO /*+ APPEND*/
    Raportdwh.Tmp_1129_Mlra_Produse_Temp
      (Reporting_Date,
       Source_App,
       Product_Type_Id,
       Product_Subtype_Id,
       Cif,
       Contract_Id,
       Cod_Gpc)
      SELECT /*+ DRIVING_SITE(EQ)*/
       Eq.Date_From AS Reporting_Date,
       'EQUITY_EXPOSURE' AS Source_App,
       Eq.Product_Type_Id AS Product_Type_Id,
       Eq.Product_Subtype_Id AS Product_Subtype_Id,
       Eq.Customer_Id AS Cif,
       Eq.Equity_Exposure_Id AS Contract_Id,
       NULL AS Cod_Gpc
        FROM WRK.STG_EQUITY Eq --RWH1191 inlocuit db_link
       WHERE Eq.Date_From = p_Reporting_Date
         AND Eq.Status_Indicator = 'A';

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --FIPO_APP
        raportdwh.pkg_rap_1129_mlra_reporting.check_and_load(p_task_run_id,p_Reporting_Date,'STG_FIPO_ST_SEC_PORTFOLIO');
        raportdwh.pkg_rap_1129_mlra_reporting.check_and_load(p_task_run_id,p_Reporting_Date,'STG_FIPO_ST_CUSTOMER');


    p_Nume_Tabela    := 'tmp_1129_mlra_produse_temp';
    p_Tip            := 'I';
    p_Desc_Eveniment := '9. FIPO_APP';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO /*+ APPEND*/
    Raportdwh.Tmp_1129_Mlra_Produse_Temp
      (Reporting_Date,
       Source_App,
       Product_Type_Id,
       Product_Subtype_Id,
       Cif,
       Contract_Id,
       Cod_Gpc)
      SELECT /*+ DRIVING_SITE(FP)*/
       Fp.Date_From AS Reporting_Date,
       'FIPO_APP' AS Source_App,
       Fp.Product_Type_Id AS Product_Type_Id,
       Fp.Product_Subtype_Id AS Product_Subtype_Id,
       Icbs.Registration_Number AS Cif,
       Fp.Sec_Portfolio_Id || '_' || ROWNUM AS Contract_Id, -- ROWNUM AS Contract_Id, -- inlocuit Fp.Unid cu rownum
       NULL AS Cod_Gpc
        FROM WRK.STG_FIPO_ST_SEC_PORTFOLIO Fp
        JOIN WRK.STG_FIPO_ST_CUSTOMER Icbs --RWH1191 inlocuit db_link
          ON Fp.Fipo_Customer_Id = Icbs.Customer_Id
         AND Fp.Date_From = Icbs.Date_From
       WHERE Fp.Date_From = p_Reporting_Date
         AND Fp.Status_Indicator <> 'C';

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --FX
    p_Nume_Tabela    := 'tmp_1129_mlra_produse_temp';
    p_Tip            := 'I';
    p_Desc_Eveniment := '10. FX';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO /*+ APPEND*/
    Raportdwh.Tmp_1129_Mlra_Produse_Temp
      (Reporting_Date,
       Source_App,
       Product_Type_Id,
       Product_Subtype_Id,
       Cif,
       Contract_Id,
       Cod_Gpc)
      SELECT DISTINCT Fx.Date_From AS Reporting_Date,
                      'FX' AS Source_App,
                      Fx.Product AS Product_Type_Id,
                      CAST(NULL AS VARCHAR2(20)) AS Product_Subtype_Id,
                      Cust.Cif_Crm AS Cif,
                      Fx.Deal_Number AS Contract_Id,
                      NULL AS Cod_Gpc
        FROM Raportdwh.Tmp_1129_Mlra_Wss2dwhfx Fx
        JOIN Dm.Dim_Customer Cust
          ON Fx.Date_From BETWEEN Cust.Date_From AND Cust.Date_Until
         AND Fx.Coconut = To_Char(Cust.Cocunut)
         AND Cust.Cif_Crm IS NOT NULL
       WHERE Fx.Date_From = p_Reporting_Date
         AND Fx.Coconut NOT LIKE 'BK%'
         AND Fx.Status_Flag IS NULL
         AND Fx.Reversal_Flag IS NULL;

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --LOAN_ICBS
    p_Nume_Tabela    := 'tmp_1129_mlra_produse_temp';
    p_Tip            := 'I';
    p_Desc_Eveniment := '11. LOAN_ICBS';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO /*+ APPEND*/
    Raportdwh.Tmp_1129_Mlra_Produse_Temp
      (Reporting_Date,
       Source_App,
       Product_Type_Id,
       Product_Subtype_Id,
       Cif,
       Contract_Id,
       Cod_Gpc)
      SELECT Pr.Reporting_Date AS Reporting_Date,
             'LOAN_ICBS' AS Source_App,
             'LN' AS Product_Type_Id,
             Pr.Product_Code AS Product_Subtype_Id,
             Pr.Customer_Icbs_Id AS Cif,
             Pr.Facility_Id_Contract_Number AS Contract_Id,
             Pr.Gpc_Code AS Cod_Gpc
        FROM Dm.Fct_All_Products_Monthly Pr
       WHERE Pr.Reporting_Date = p_Reporting_Date
         AND Pr.Source_Application = 'LOAN_ICBS'
         AND Pr.Facility_Status = 'A';

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --MM
    p_Nume_Tabela    := 'tmp_1129_mlra_produse_temp';
    p_Tip            := 'I';
    p_Desc_Eveniment := '12. MM';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO /*+ APPEND*/
    Raportdwh.Tmp_1129_Mlra_Produse_Temp
      (Reporting_Date,
       Source_App,
       Product_Type_Id,
       Product_Subtype_Id,
       Cif,
       Contract_Id,
       Cod_Gpc)
      SELECT DISTINCT Mm.Date_From AS Reporting_Date,
                      'MM' AS Source_App,
                      Mm.Product AS Product_Type_Id,
                      CAST(NULL AS VARCHAR2(20)) AS Product_Subtype_Id,
                      Cust.Cif_Crm AS Cif,
                      Mm.Deal_Number AS Contract_Id,
                      NULL AS Cod_Gpc
        FROM Raportdwh.Tmp_1129_Mlra_Wss2dwhmm Mm
        LEFT JOIN Dm.Dim_Customer Cust
          ON Mm.Date_From BETWEEN Cust.Date_From AND Cust.Date_Until
         AND Mm.Cocunut = To_Char(Cust.Cocunut)
         AND Cust.Cif_Crm IS NOT NULL
       WHERE Mm.Date_From = p_Reporting_Date
         AND Mm.Deal_Status IS NULL;

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --TRAN_ICBS
    p_Nume_Tabela    := 'tmp_1129_mlra_produse_temp';
    p_Tip            := 'I';
    p_Desc_Eveniment := '14. TRAN_ICBS';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO /*+ APPEND*/
    Raportdwh.Tmp_1129_Mlra_Produse_Temp
      (Reporting_Date,
       Source_App,
       Product_Type_Id,
       Product_Subtype_Id,
       Cif,
       Contract_Id,
       Cod_Gpc)
      SELECT Pr.Reporting_Date AS Reporting_Date,
             'TRAN_ICBS' AS Source_App,
             CAST(NULL AS VARCHAR2(20)) AS Product_Type_Id,
             Pr.Product_Code AS Product_Subtype_Id,
             Pr.Customer_Icbs_Id AS Cif,
             Pr.Facility_Id_Contract_Number AS Contract_Id,
             Pr.Gpc_Code AS Cod_Gpc
        FROM Dm.Fct_All_Products_Monthly Pr
       WHERE Pr.Reporting_Date = p_Reporting_Date
         AND Pr.Source_Application = 'TRAN_ICBS'
         AND Pr.Facility_Status IN ('1', '6')
       GROUP BY Pr.Reporting_Date,
                Pr.Product_Code,
                Pr.Customer_Icbs_Id,
                Pr.Facility_Id_Contract_Number,
                Pr.Gpc_Code;

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --TRAN_MIDAS
     raportdwh.pkg_rap_1129_mlra_reporting.check_and_load(p_task_run_id,p_Reporting_Date,'STG_MIDAS_ACCOUNT');
     raportdwh.pkg_rap_1129_mlra_reporting.check_and_load(p_task_run_id,p_Reporting_Date,'STG_MIDAS_ICBS_CUST');


    p_Nume_Tabela    := 'tmp_1129_mlra_produse_temp';
    p_Tip            := 'I';
    p_Desc_Eveniment := '15. TRAN_MIDAS';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO /*+ APPEND*/
    Raportdwh.Tmp_1129_Mlra_Produse_Temp
      (Reporting_Date,
       Source_App,
       Product_Type_Id,
       Product_Subtype_Id,
       Cif,
       Contract_Id,
       Cod_Gpc)
      SELECT /*+ DRIVING_SITE(LC)*/
       Md.Date_From AS Reporting_Date,
       'TRAN_MIDAS' AS Source_App,
       Md.Product_Type_Id AS Product_Type_Id,
       Md.Product_Subtype_Id AS Product_Subtype_Id,
       Icbs.Icbs_Customer_Id AS Cif,
       Md.Account_Id AS Contract_Id,
       NULL AS Cod_Gpc
        FROM WRK.STG_MIDAS_ACCOUNT Md --RWH1191 inlocuit db_link
        JOIN WRK.STG_MIDAS_ICBS_CUST Icbs --RWH1191 inlocuit db_link
          ON Icbs.Midas_Customer_Id = Md.Midas_Customer_Id
         AND Icbs.Date_From = Md.Date_From
       WHERE Md.Date_From = p_Reporting_Date
         AND Md.Status_Indicator = 'D';

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --LEO_LC

        raportdwh.pkg_rap_1129_mlra_reporting.check_and_load(p_task_run_id,p_Reporting_Date,'STG_L_DOCUMENTARY_LC');
        raportdwh.pkg_rap_1129_mlra_reporting.check_and_load(p_task_run_id,p_Reporting_Date,'STG_L_CUSTOMER_PROFILE');
        raportdwh.pkg_rap_1129_mlra_reporting.check_and_load(p_task_run_id,p_Reporting_Date,'STG_MIDAS_ICBS_CUST');
    p_Nume_Tabela    := 'tmp_1129_mlra_produse_temp';
    p_Tip            := 'I';
    p_Desc_Eveniment := '16. LEO_LC';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO /*+ APPEND*/
    Raportdwh.Tmp_1129_Mlra_Produse_Temp
      (Reporting_Date,
       Source_App,
       Product_Type_Id,
       Product_Subtype_Id,
       Cif,
       Contract_Id,
       Cod_Gpc)
      SELECT /*+ DRIVING_SITE(LC)*/
       Lc.Date_From AS Reporting_Date,
       'TRADE_LEO' AS Source_App,
       'LEO_LC' AS Product_Type_Id,
       Lc.Process_Code AS Product_Subtype_Id,
       Icbs.Icbs_Customer_Id AS Cif,
       Lc.Letter_Of_Credit_Id AS Contract_Id,
       NULL AS Cod_Gpc
        FROM WRK.STG_L_DOCUMENTARY_LC Lc --RWH1191 inlocuit db_link
        LEFT JOIN WRK.STG_L_CUSTOMER_PROFILE Pr --RWH1191 inlocuit db_link
          ON Lc.Applicant_Customer_Id = Pr.Leo_Customer_Id
         AND Pr.Date_From = p_Reporting_Date
        JOIN WRK.STG_MIDAS_ICBS_CUST Icbs --RWH1191 inlocuit db_link
          ON Pr.Leo_Customer_Number = Icbs.Midas_Customer_Id
         AND Pr.Date_From = Icbs.Date_From
       WHERE Lc.Date_From = p_Reporting_Date
         AND Lc.Status = 'P';

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --LEO_COLL
        raportdwh.pkg_rap_1129_mlra_reporting.check_and_load(p_task_run_id,p_Reporting_Date,'STG_L_COLLECTION_MASTER');
        raportdwh.pkg_rap_1129_mlra_reporting.check_and_load(p_task_run_id,p_Reporting_Date,'STG_L_CUSTOMER_PROFILE');
        raportdwh.pkg_rap_1129_mlra_reporting.check_and_load(p_task_run_id,p_Reporting_Date,'STG_MIDAS_ICBS_CUST');

    p_Nume_Tabela    := 'tmp_1129_mlra_produse_temp';
    p_Tip            := 'I';
    p_Desc_Eveniment := '17. LEO_COLL';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO /*+ APPEND*/
    Raportdwh.Tmp_1129_Mlra_Produse_Temp
      (Reporting_Date,
       Source_App,
       Product_Type_Id,
       Product_Subtype_Id,
       Cif,
       Contract_Id,
       Cod_Gpc)
      SELECT /*+ DRIVING_SITE(LC)*/
       Lc.Date_From AS Reporting_Date,
       'TRADE_LEO' AS Source_App,
       'LEO_COLL' AS Product_Type_Id,
       Lc.Process_Code AS Product_Subtype_Id,
       Icbs.Icbs_Customer_Id AS Cif,
       Lc.Transaction_Reference_Id || '_' || Lc.Sub_Number AS Contract_Id,
       NULL AS Cod_Gpc
        FROM WRK.STG_L_COLLECTION_MASTER Lc  --RWH1191 inlocuit db_link
        LEFT JOIN  WRK.STG_L_CUSTOMER_PROFILE  Pr --RWH1191 inlocuit db_link
          ON Lc.Presenter_Id = Pr.Leo_Customer_Id
         AND Pr.Date_From = p_Reporting_Date
        JOIN WRK.STG_MIDAS_ICBS_CUST Icbs --RWH1191 inlocuit db_link
          ON Pr.Leo_Customer_Number = Icbs.Midas_Customer_Id
         AND Pr.Date_From = Icbs.Date_From
       WHERE Lc.Date_From = p_Reporting_Date
         AND Lc.Status_Indicator NOT IN ('CLS', 'CAN');

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --RO

    IF p_Reporting_Date >= To_Date('30jun2020', 'ddmonyyyy') -- S-a schimbat aplicatia pt PF
     THEN

      --RO_PF
      p_Nume_Tabela    := 'tmp_1129_mlra_produse_temp';
      p_Tip            := 'I';
      p_Desc_Eveniment := '18. RO_PF';
      p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

      Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                                 p_Nume_Procedura,
                                 p_Nume_Schema,
                                 p_Nume_Tabela,
                                 p_Tip,
                                 p_Param_1,
                                 p_Param_2,
                                 p_Param_3,
                                 p_Desc_Eveniment,
                                 p_Autonumber,
                                 p_Task_Run_Id);

      INSERT INTO /*+ APPEND*/
      Raportdwh.Tmp_1129_Mlra_Produse_Temp
        (Reporting_Date,
         Source_App,
         Product_Type_Id,
         Product_Subtype_Id,
         Cif,
         Contract_Id,
         Cod_Gpc)
        SELECT DISTINCT Asst.Date_From AS Reporting_Date,
                        'RO' AS Source_App,
                        'RO' AS Product_Type_Id,
                        CAST(NULL AS VARCHAR2(20)) AS Product_Subtype_Id,
                        Lpad(Asst.Pf_Integration_Id, 10, 0) AS Cif,
                        Asst.Owner_Con_Id || '_' || Asst.Prod_Id AS Contract_Id,
                        NULL AS Cod_Gpc
          FROM Raportdwh.Tmp_1129_Mlra_Assets_Temp Asst
         WHERE Prod_Name = 'RBRO_RO_PF';

      p_Nr_Inregistrari := SQL%ROWCOUNT;

      COMMIT;

      Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    ELSE
        raportdwh.pkg_rap_1129_mlra_reporting.check_and_load(p_task_run_id,p_Reporting_Date,'STG_IBK_CUSTOMER_DETAILS');

      --RO_PF
      p_Nume_Tabela    := 'tmp_1129_mlra_produse_temp';
      p_Tip            := 'I';
      p_Desc_Eveniment := '18. RO_PF';
      p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

      Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                                 p_Nume_Procedura,
                                 p_Nume_Schema,
                                 p_Nume_Tabela,
                                 p_Tip,
                                 p_Param_1,
                                 p_Param_2,
                                 p_Param_3,
                                 p_Desc_Eveniment,
                                 p_Autonumber,
                                 p_Task_Run_Id);

      INSERT INTO /*+ APPEND*/
      Raportdwh.Tmp_1129_Mlra_Produse_Temp
        (Reporting_Date,
         Source_App,
         Product_Type_Id,
         Product_Subtype_Id,
         Cif,
         Contract_Id,
         Cod_Gpc)
        SELECT DISTINCT Pf.Date_From AS Reporting_Date,
                        'RO' AS Source_App,
                        'RO' AS Product_Type_Id,
                        CAST(NULL AS VARCHAR2(20)) AS Product_Subtype_Id,
                        Lpad(Pf.Integration_Id, 10, 0) AS Cif,
                        Customer_Id || '_' || Template_Customer_Id AS Contract_Id,
                        NULL AS Cod_Gpc
          FROM WRK.STG_IBK_CUSTOMER_DETAILS Ibk --RWH1191 inlocuit db_link
          JOIN WRK.STG_CRM_S_CONTACT Pf --RWH1191 inlocuit db_link
            ON Lpad(Pf.Integration_Id, 10, 0) = Ibk.Customer_Id
           AND Pf.Date_From = Ibk.Date_From
           AND Pf.Con_Cd = 'Customer'
         WHERE Ibk.Template_Customer_Id IN ('0007', '0002')
           AND Ibk.Active_Inactive = 'Activ'
           AND Pf.Date_From = p_Reporting_Date;

      p_Nr_Inregistrari := SQL%ROWCOUNT;

      COMMIT;

      Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    END IF;

    --RO_PJ

          raportdwh.pkg_rap_1129_mlra_reporting.check_and_load(p_task_run_id,p_Reporting_Date,'STG_IBK_CUSTOMER_DETAILS');


    p_Nume_Tabela    := 'tmp_1129_mlra_produse_temp';
    p_Tip            := 'I';
    p_Desc_Eveniment := '19. RO_PJ';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO /*+ APPEND*/
    Raportdwh.Tmp_1129_Mlra_Produse_Temp
      (Reporting_Date,
       Source_App,
       Product_Type_Id,
       Product_Subtype_Id,
       Cif,
       Contract_Id,
       Cod_Gpc)
      SELECT DISTINCT Pj.Date_From AS Reporting_Date,
                      'RO' AS Source_App,
                      'RO' AS Product_Type_Id,
                      CAST(NULL AS VARCHAR2(20)) AS Product_Subtype_Id,
                      Lpad(Pj.Integration_Id, 10, 0) AS Cif,
                      Customer_Id || '_' || Template_Customer_Id AS Contract_Id,
                      NULL AS Cod_Gpc
        FROM WRK.STG_IBK_CUSTOMER_DETAILS Ibk --RWH1191 inlocuit db_link
        JOIN WRK.STG_CRM_S_ORG_EXT Pj --RWH1191 inlocuit db_link
          ON Lpad(Pj.Integration_Id, 10, 0) = Ibk.Customer_Id
         AND Pj.Date_From = Ibk.Date_From
       WHERE Ibk.Template_Customer_Id IN ('0003', '0004', '0006', '0008')
         AND Ibk.Active_Inactive = 'Activ'
         AND Pj.Date_From = p_Reporting_Date;

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --SM
    IF p_Reporting_Date >= To_Date('30jun2020', 'ddmonyyyy') -- S-a schimbat aplicatia pt PF
     THEN

      --SM_PF
      p_Nume_Tabela    := 'tmp_1129_mlra_produse_temp';
      p_Tip            := 'I';
      p_Desc_Eveniment := '20. SM_PF';
      p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

      Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                                 p_Nume_Procedura,
                                 p_Nume_Schema,
                                 p_Nume_Tabela,
                                 p_Tip,
                                 p_Param_1,
                                 p_Param_2,
                                 p_Param_3,
                                 p_Desc_Eveniment,
                                 p_Autonumber,
                                 p_Task_Run_Id);

      INSERT INTO /*+ APPEND*/
      Raportdwh.Tmp_1129_Mlra_Produse_Temp
        (Reporting_Date,
         Source_App,
         Product_Type_Id,
         Product_Subtype_Id,
         Cif,
         Contract_Id,
         Cod_Gpc)
        SELECT DISTINCT Asst.Date_From AS Reporting_Date,
                        'SM' AS Source_App,
                        'SM' AS Product_Type_Id,
                        CAST(NULL AS VARCHAR2(20)) AS Product_Subtype_Id,
                        Lpad(Asst.Pf_Integration_Id, 10, 0) AS Cif,
                        Asst.Owner_Con_Id || '_' || Asst.Prod_Id AS Contract_Id,
                        NULL AS Cod_Gpc
          FROM Raportdwh.Tmp_1129_Mlra_Assets_Temp Asst
         WHERE Prod_Name = 'RBRO_SM_PF';

      p_Nr_Inregistrari := SQL%ROWCOUNT;

      COMMIT;

      Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    ELSE

         raportdwh.pkg_rap_1129_mlra_reporting.check_and_load(p_task_run_id,p_Reporting_Date,'STG_IBK_CUSTOMER_DETAILS');


      --SM_PF
      p_Nume_Tabela    := 'tmp_1129_mlra_produse_temp';
      p_Tip            := 'I';
      p_Desc_Eveniment := '20. SM_PF';
      p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

      Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                                 p_Nume_Procedura,
                                 p_Nume_Schema,
                                 p_Nume_Tabela,
                                 p_Tip,
                                 p_Param_1,
                                 p_Param_2,
                                 p_Param_3,
                                 p_Desc_Eveniment,
                                 p_Autonumber,
                                 p_Task_Run_Id);

      INSERT INTO /*+ APPEND*/
      Raportdwh.Tmp_1129_Mlra_Produse_Temp
        (Reporting_Date,
         Source_App,
         Product_Type_Id,
         Product_Subtype_Id,
         Cif,
         Contract_Id,
         Cod_Gpc)
        SELECT DISTINCT Pf.Date_From AS Reporting_Date,
                        'SM' AS Source_App,
                        'SM' AS Product_Type_Id,
                        CAST(NULL AS VARCHAR2(20)) AS Product_Subtype_Id,
                        Lpad(Pf.Integration_Id, 10, 0) AS Cif,
                        Customer_Id || '_' || Template_Customer_Id AS Contract_Id,
                        NULL AS Cod_Gpc
          FROM WRK.STG_IBK_CUSTOMER_DETAILS Ibk --RWH1191 inlocuit db_link
          JOIN WRK.STG_CRM_S_CONTACT Pf --RWH1191 inlocuit db_link
            ON Lpad(Pf.Integration_Id, 10, 0) = Ibk.Customer_Id
           AND Pf.Date_From = Ibk.Date_From
           AND Pf.Con_Cd = 'Customer'
         WHERE Ibk.Template_Customer_Id IN ('0015')
           AND Ibk.Active_Inactive = 'Activ'
           AND Pf.Date_From = p_Reporting_Date;

      p_Nr_Inregistrari := SQL%ROWCOUNT;

      COMMIT;

      Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    END IF;

    --SM_PJ
raportdwh.pkg_rap_1129_mlra_reporting.check_and_load(p_task_run_id,p_Reporting_Date,'STG_IBK_CUSTOMER_DETAILS');
raportdwh.pkg_rap_1129_mlra_reporting.check_and_load(p_task_run_id,p_Reporting_Date,'STG_SMART_MOBILE_PF_IMM');

    p_Nume_Tabela    := 'tmp_1129_mlra_produse_temp';
    p_Tip            := 'I';
    p_Desc_Eveniment := '21. SM_PJ';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO /*+ APPEND*/
    Raportdwh.Tmp_1129_Mlra_Produse_Temp
      (Reporting_Date,
       Source_App,
       Product_Type_Id,
       Product_Subtype_Id,
       Cif,
       Contract_Id,
       Cod_Gpc)
      SELECT DISTINCT Pj.Date_From AS Reporting_Date,
                      'SM' AS Source_App,
                      'SM' AS Product_Type_Id,
                      CAST(NULL AS VARCHAR2(20)) AS Product_Subtype_Id,
                      Lpad(Pj.Integration_Id, 10, 0) AS Cif,
                      Customer_Id || '_' || Template_Customer_Id AS Contract_Id,
                      NULL AS Cod_Gpc
        FROM WRK.STG_IBK_CUSTOMER_DETAILS Ibk --RWH1191 inlocuit db_link
        JOIN WRK.STG_SMART_MOBILE_PF_IMM Sm --RWH1191 inlocuit db_link
          ON Sm.Cif_Number = Ibk.Customer_Id
         AND Sm.Date_From = Ibk.Date_From
        JOIN WRK.STG_CRM_S_ORG_EXT Pj --RWH1191 inlocuit db_link
          ON Lpad(Pj.Integration_Id, 10, 0) = Ibk.Customer_Id
         AND Pj.Date_From = Ibk.Date_From
       WHERE Sm.Template_Id IN ('0013', '0014')
         AND Ibk.Active_Inactive = 'Activ'
         AND Pj.Date_From = p_Reporting_Date;

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --RWH1191
    --Athena_PJ_SM
    p_Nume_Tabela    := 'tmp_1129_mlra_produse_temp';
    p_Tip            := 'I';
    p_Desc_Eveniment := '26.Athena_PJ_SM';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO /*+ APPEND*/
    Raportdwh.Tmp_1129_Mlra_Produse_Temp
      (Reporting_Date,
       Source_App,
       Product_Type_Id,
       Product_Subtype_Id,
       Cif,
       Contract_Id,
       Cod_Gpc)
      SELECT DISTINCT Asst.Date_From AS Reporting_Date,
                      'SM' AS Source_App,
                      'SM' AS Product_Type_Id,
                      CAST(NULL AS VARCHAR2(20)) AS Product_Subtype_Id,
                      Lpad(Asst.Pj_Integration_Id, 10, 0) AS Cif,
                      Asst.Owner_Accnt_Id || '_' || Asst.Prod_Id AS Contract_Id,
                      NULL AS Cod_Gpc
        FROM Raportdwh.Tmp_1129_Mlra_Assets_Temp Asst
       WHERE Prod_Name = 'RBRO_SM_PJ';

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    --RWH1191
    --Athena_PJ_RO
    p_Nume_Tabela    := 'tmp_1129_mlra_produse_temp';
    p_Tip            := 'I';
    p_Desc_Eveniment := '27.Athena_RO_SM';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO /*+ APPEND*/
    Raportdwh.Tmp_1129_Mlra_Produse_Temp
      (Reporting_Date,
       Source_App,
       Product_Type_Id,
       Product_Subtype_Id,
       Cif,
       Contract_Id,
       Cod_Gpc)
      SELECT DISTINCT Asst.Date_From AS Reporting_Date,
                      'RO' AS Source_App,
                      'RO' AS Product_Type_Id,
                      CAST(NULL AS VARCHAR2(20)) AS Product_Subtype_Id,
                      Lpad(Asst.Pj_Integration_Id, 10, 0) AS Cif,
                      Asst.Owner_Accnt_Id || '_' || Asst.Prod_Id AS Contract_Id,
                      NULL AS Cod_Gpc
        FROM Raportdwh.Tmp_1129_Mlra_Assets_Temp Asst
       WHERE Prod_Name IN ('RBRO_RO_PJ', 'RBRO_RO_CORP'); -- 24.05.2024 Magda Dogaru CDS-268 Athena Corporate

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    Dbms_Stats.Gather_Table_Stats(Ownname          => 'RAPORTDWH',
                                  Tabname          => 'tmp_1129_mlra_produse_temp',
                                  Estimate_Percent => Dbms_Stats.Auto_Sample_Size,
                                  Cascade          => TRUE,
                                  Method_Opt       => 'FOR ALL COLUMNS SIZE AUTO');

    DELETE FROM Tmp_1129_Mlra_Produse_Temp
     WHERE Cif IS NULL;
    COMMIT;

 raportdwh.pkg_rap_1129_mlra_reporting.check_and_load(p_task_run_id,p_Reporting_Date,'STG_TB0_PRODUCT_TYPE_SUBTYPE');
 raportdwh.pkg_rap_1129_mlra_reporting.check_and_load(p_task_run_id,p_Reporting_Date,'STG_OTH_MANU_LKP_PRODUCT_AML');


    --type si subtype not null - 15.11.2021 Sorina Nutu acopera si cazurile cu GPC code adus din temporara
    p_Nume_Tabela    := 'tmp_1129_mlra_produse_gpc_TMP';
    p_Tip            := 'I';
    p_Desc_Eveniment := '22. type si subtype not null';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO /*+ APPEND*/
    Raportdwh.Tmp_1129_Mlra_Produse_Gpc_Tmp
      SELECT Pr.Reporting_Date AS Reporting_Date,
             Pr.Source_App AS Source_App,
             Pr.Product_Type_Id AS Product_Type_Id,
             Pr.Product_Subtype_Id AS Product_Subtype_Id,
             Pr.Cif AS Cif,
             Pr.Contract_Id AS Contract_Id,
             Nvl(Pr.Cod_Gpc, Prgpc.Rzb_Grp_Prod_Type_Id) AS Cod_Gpc,
             Gpc_Desc.Product_Desc AS Product_Description,
             Gpc_Desc.Weight AS Product_Risk,
             Nvl(Cl.Customer_Lob, 'NO_VAL') AS Customer_Lob,
             Nvl(Cl.Customer_Rr_Level, 'NO_VAL') AS Customer_Rr_Level,
             Gpc_Desc.Prod_Category_Level1 AS Product_Level_1,
             Gpc_Desc.Prod_Category_Level2 AS Product_Level_2,
             Gpc_Desc.Prod_Category_Level3 AS Product_Level_3,
             Cl.Categ_Client AS Categ_Client,
             Cl.Bk_Status_Crm AS Bk_Status_Crm,
             Cl.Bk_Customer_Type AS Bk_Customer_Type
        FROM Raportdwh.Tmp_1129_Mlra_Produse_Temp Pr
        LEFT JOIN Wrk.Stg_Tb0_Product_Type_Subtype Prgpc
          ON Prgpc.Date_From = p_Reporting_Date
         AND Prgpc.Original_Entity_Id = 'RBRO'
         AND Prgpc.Status_Indicator = 'A'
         AND TRIM(Pr.Product_Type_Id) = Prgpc.Product_Type_Id
         AND TRIM(Pr.Product_Subtype_Id) = TRIM(Prgpc.Product_Subtype_Id)
         AND Pr.Source_App = Prgpc.Application_Id
        LEFT JOIN wrk.STG_OTH_MANU_LKP_PRODUCT_AML Gpc_Desc--Others_Arh.Oth_Manu_Lkp_Product_Aml@Gdwh24_Dwh_Col Gpc_Desc
          ON Gpc_Desc.Date_From = p_Reporting_Date
         AND Nvl(Pr.Cod_Gpc, Prgpc.Rzb_Grp_Prod_Type_Id) =
             Gpc_Desc.Product_Code
        LEFT JOIN Raportdwh.Tmp_1129_Mlra_Clienti_f Cl
          ON Cl.Reporting_Date = p_Reporting_Date
         AND Pr.Cif = Cl.Cif
       WHERE (Pr.Product_Type_Id IS NOT NULL AND
             Pr.Product_Subtype_Id IS NOT NULL)
          OR Pr.Cod_Gpc IS NOT NULL;

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    --type is null and subtype not null - 15.11.2021 Sorina Nutu - nu mai sunt cazuri

    --type not null si subtype null
    p_Nume_Tabela    := 'tmp_1129_mlra_produse_gpc_TMP';
    p_Tip            := 'I';
    p_Desc_Eveniment := '24. type not null si subtype null';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO /*+ APPEND*/
    Raportdwh.Tmp_1129_Mlra_Produse_Gpc_Tmp
      SELECT DISTINCT Pr.Reporting_Date AS Reporting_Date,
                      Pr.Source_App AS Source_App,
                      Pr.Product_Type_Id AS Product_Type_Id,
                      Pr.Product_Subtype_Id AS Product_Subtype_Id,
                      Pr.Cif AS Cif,
                      Pr.Contract_Id AS Contract_Id,
                      Nvl(Nvl(Pr.Cod_Gpc, Prgpc.Rzb_Grp_Prod_Type_Id),
                          Dict_Gpc.Cod_Dest) AS Cod_Gpc,
                      Gpc_Desc.Product_Desc AS Product_Description,
                      Gpc_Desc.Weight AS Product_Risk,
                      Nvl(Cl.Customer_Lob, 'NO_VAL') AS Customer_Lob,
                      Nvl(Cl.Customer_Rr_Level, 'NO_VAL') AS Customer_Rr_Level,
                      Gpc_Desc.Prod_Category_Level1 AS Product_Level_1,
                      Gpc_Desc.Prod_Category_Level2 AS Product_Level_2,
                      Gpc_Desc.Prod_Category_Level3 AS Product_Level_3,
                      Cl.Categ_Client AS Categ_Client,
                      Cl.Bk_Status_Crm AS Bk_Status_Crm,
                      Cl.Bk_Customer_Type AS Bk_Customer_Type
        FROM Raportdwh.Tmp_1129_Mlra_Produse_Temp Pr
        LEFT JOIN Wrk.Stg_Tb0_Product_Type_Subtype Prgpc
          ON Prgpc.Date_From = p_Reporting_Date
         AND Prgpc.Original_Entity_Id = 'RBRO'
         AND Prgpc.Status_Indicator = 'A'
         AND TRIM(Pr.Product_Type_Id) = Prgpc.Product_Type_Id
         AND Pr.Source_App = Prgpc.Application_Id
        LEFT JOIN Raportdwh.Man_1129_Mlra_Dict Dict_Gpc -- pt RO/SM hardcoded
          ON TRIM(Pr.Product_Type_Id) = TRIM(Dict_Gpc.Cod_Src)
         AND Dict_Gpc.Id_Categ = 4
         AND Dict_Gpc.Src = Pr.Source_App
        LEFT JOIN wrk.STG_OTH_MANU_LKP_PRODUCT_AML Gpc_Desc--Others_Arh.Oth_Manu_Lkp_Product_Aml@Gdwh24_Dwh_Col Gpc_Desc
          ON Gpc_Desc.Date_From = p_Reporting_Date
         AND Nvl(Nvl(Pr.Cod_Gpc, Prgpc.Rzb_Grp_Prod_Type_Id),
                 Dict_Gpc.Cod_Dest) = Gpc_Desc.Product_Code
        LEFT JOIN Raportdwh.Tmp_1129_Mlra_Clienti_f Cl
          ON Cl.Reporting_Date = p_Reporting_Date
         AND Pr.Cif = Cl.Cif
       WHERE Pr.Product_Type_Id IS NOT NULL
         AND Pr.Product_Subtype_Id IS NULL
         AND Pr.Cod_Gpc IS NULL;

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    Dbms_Stats.Gather_Table_Stats(Ownname          => 'RAPORTDWH',
                                  Tabname          => 'tmp_1129_mlra_produse_gpc_TMP',
                                  Estimate_Percent => Dbms_Stats.Auto_Sample_Size,
                                  Cascade          => TRUE,
                                  Method_Opt       => 'FOR ALL COLUMNS SIZE AUTO');

    p_Nume_Tabela    := 'tmp_1129_mlra_produse_gpc';
    p_Tip            := 'L';
    p_Desc_Eveniment := '25. incarcare produse';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    INSERT INTO /*+ APPEND*/
    Tmp_1129_Mlra_Produse_Gpc
      WITH No_Prod_Per_Prod_Risk AS
       (SELECT Product_Risk,
               COUNT(DISTINCT Cod_Gpc) No_Prod_Per_Prod_Risk
          FROM Tmp_1129_Mlra_Produse_Gpc_Tmp
         GROUP BY Product_Risk)
      SELECT a.Reporting_Date,
             a.Source_App,
             a.Product_Type_Id,
             a.Product_Subtype_Id,
             a.Cif,
             a.Contract_Id,
             Decode(a.Cod_Gpc, '*n.a.*', NULL, a.Cod_Gpc) AS Cod_Gpc,
             a.Product_Description,
             a.Product_Risk,
             a.Customer_Lob,
             a.Customer_Rr_Level,
             a.Product_Level_1,
             a.Product_Level_2,
             a.Product_Level_3,
             1 Nr_Of_Prod_Usage,
             Bb.No_Prod_Per_Prod_Risk,
             a.Categ_Client,
             a.Bk_Status_Crm,
             a.Bk_Customer_Type
        FROM Tmp_1129_Mlra_Produse_Gpc_Tmp a
        JOIN No_Prod_Per_Prod_Risk Bb
          ON Bb.Product_Risk = a.Product_Risk;

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    Dbms_Stats.Gather_Table_Stats(Ownname          => 'RAPORTDWH',
                                  Tabname          => 'tmp_1129_mlra_produse_gpc',
                                  Estimate_Percent => Dbms_Stats.Auto_Sample_Size,
                                  Cascade          => TRUE,
                                  Method_Opt       => 'FOR ALL COLUMNS SIZE AUTO');

    Proc_Add_Partition(p_Task_Run_Id ,p_Reporting_Date, 'TMP_1129_MLRA_PRODUSE_GPC_H');
    Proc_Write_To_From_Hist(p_Task_Run_Id ,p_Reporting_Date,
                            'TMP_1129_MLRA_PRODUSE_GPC',
                            'TMP_1129_MLRA_PRODUSE_GPC_H');
  END Proc_Produse;

  PROCEDURE Proc_Tranz_Tabele_Finale ( p_Task_Run_Id    NUMBER) AS
    v_Sql VARCHAR2(6000);
    v_Nr  NUMBER;
    v_Col VARCHAR2(2000);

  BEGIN
    --dev history
    /*
    parametru de data la rulare
    verificare ca in tabela curenta se afla anul raportat
    daca  nu il ia din history

    nu mai verific daca ultimul an raportat este istorizat pentru ca bag istorizare zi cu zi de la rularea anului
    */
    p_Nume_Procedura := 'PROC_TRANZ_TABELE_FINALE';
    p_Nume_Tabela    := 'tmp_1129_mlra_tab28_tmp';
    p_Param_1        := NULL;
    p_Desc_Eveniment := '';
    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,p_Task_Run_Id);
    SELECT COUNT(*)
      INTO v_Nr
      FROM User_Tables
     WHERE Table_Name = 'TMP_1129_MLRA_TAB28_TMP';
    IF v_Nr = 1 THEN
      v_Sql := 'drop table tmp_1129_mlra_tab28_tmp purge';
      EXECUTE IMMEDIATE v_Sql;
    END IF;
    v_Col := NULL;
    FOR Rec IN (SELECT DISTINCT Customer_Lob
                  FROM Tmp_1129_Mlra_Txn_f_Det) LOOP
      v_Col := v_Col || '''' || Rec.Customer_Lob || ''' ' ||
               REPLACE(Rec.Customer_Lob, '+', '_') || ',';
    END LOOP;
    v_Col := Substr(v_Col, 1, Length(v_Col) - 1);

    v_Sql := 'create table tmp_1129_mlra_tab28_tmp as
  with totaluri as
 (SELECT Report_Cutoff_Date,
         DEBIT_CREDIT,
         TRANSACTION_GROUP,
         count(table_source_id) NR_OF_TX,
         suM(nvl(amount_eur, 0)) SUM_OF_TZ_EUR
    FROM tmp_1129_mlra_txn_f_det GROUP BY Report_Cutoff_Date,
         DEBIT_CREDIT,
         TRANSACTION_GROUP)
         SELECT a.*, b.NR_OF_TX,b.SUM_OF_TZ_EUR
         FROM (
  SELECT * FROM
(
  SELECT Report_Cutoff_Date, DEBIT_CREDIT, TRANSACTION_GROUP, customer_lob,TABLE_SOURCE_ID,AMOUNT_EUR
  FROM tmp_1129_mlra_txn_f_det
)
PIVOT
(
  count (TABLE_SOURCE_ID) nr_of_tx,
  sum(nvl(AMOUNT_EUR,0)) SUM_OF_TZ_EUR
  FOR customer_lob
  in (' || v_Col || '))
ORDER BY DEBIT_CREDIT, TRANSACTION_GROUP )  a left join totaluri b
 on a.Report_Cutoff_Date=b.Report_Cutoff_Date and a.DEBIT_CREDIT=b.debit_credit and  a.TRANSACTION_GROUP=b.transaction_group ';
    EXECUTE IMMEDIATE v_Sql;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, 0);

    p_Nume_Tabela    := 'rap_1129_mlra_tab28';
    p_Param_1        := NULL;
    p_Desc_Eveniment := '';
    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,p_Task_Run_Id);
    SELECT COUNT(*)
      INTO v_Nr
      FROM User_Tables
     WHERE Table_Name = 'RAP_1129_MLRA_TAB28';
    IF v_Nr = 1 THEN
      v_Sql := 'drop table rap_1129_mlra_tab28 purge';
      EXECUTE IMMEDIATE v_Sql;
    END IF;
    v_Col := 'Report_Cutoff_Date, DECODE(GROUPING(DEBIT_CREDIT)
             , 0, DEBIT_CREDIT
             , 1, ''TOTAL''
             ) DEBIT_CREDIT,
             DECODE(GROUPING(TRANSACTION_GROUP)
             , 0, TRANSACTION_GROUP
             , 1, ''TOTAL''
             ) TRANSACTION_GROUP,';
    FOR Rec IN (SELECT Column_Name
                  FROM User_Tab_Columns
                 WHERE Table_Name = 'TMP_1129_MLRA_TAB28_TMP'
                   AND Column_Id NOT IN (1, 2, 3)
                 ORDER BY Column_Id) LOOP
      v_Col := v_Col || 'suM(nvl(' || Rec.Column_Name || ',0)) ' ||
               Rec.Column_Name || ',';
    END LOOP;
    v_Col := Substr(v_Col, 1, Length(v_Col) - 1);
    v_Sql := 'create table rap_1129_mlra_tab28 as
 select ' || v_Col ||
             ' from tmp_1129_mlra_tab28_tmp
  GROUP BY Report_Cutoff_Date, ROLLUP (DEBIT_CREDIT,TRANSACTION_GROUP)';

    EXECUTE IMMEDIATE v_Sql;
     execute immediate 'grant select on raportdwh.rap_1129_mlra_tab28 to monitor';

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, 0);
    --------------------------------------------------------------------------
    p_Nume_Tabela    := 'tmp_1129_mlra_tab29_tmp';
    p_Param_1        := NULL;
    p_Desc_Eveniment := '';
    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,p_Task_Run_Id);
    SELECT COUNT(*)
      INTO v_Nr
      FROM User_Tables
     WHERE Table_Name = 'TMP_1129_MLRA_TAB29_TMP';
    IF v_Nr = 1 THEN
      v_Sql := 'drop table tmp_1129_mlra_tab29_tmp purge';
      EXECUTE IMMEDIATE v_Sql;
    END IF;
    v_Col := NULL;
    FOR Rec IN (SELECT DISTINCT Customer_Lob
                  FROM Tmp_1129_Mlra_Txn_f_Det) LOOP
      v_Col := v_Col || '''' || Rec.Customer_Lob || ''' ' ||
               REPLACE(Rec.Customer_Lob, '+', '_') || ',';
    END LOOP;
    v_Col := Substr(v_Col, 1, Length(v_Col) - 1);

    v_Sql := 'create table tmp_1129_mlra_tab29_tmp as
   with totaluri as
 (SELECT Report_Cutoff_Date,
         DEBIT_CREDIT,
         TRANSACTION_GROUP,
         count(table_source_id) NR_OF_TX,
         suM(nvl(amount_eur, 0)) SUM_OF_TZ_EUR
    FROM tmp_1129_mlra_txn_f_det
    WHERE LORO_ACCOUNT_FLAg= ''Y'' OR NOSTRO_ACCOUNT_FLAG =''Y''
    GROUP BY Report_Cutoff_Date,
         DEBIT_CREDIT,
         TRANSACTION_GROUP)
         SELECT a.*, b.NR_OF_TX,b.SUM_OF_TZ_EUR
         FROM (
  SELECT * FROM
(
  SELECT Report_Cutoff_Date, DEBIT_CREDIT, TRANSACTION_GROUP, customer_lob,TABLE_SOURCE_ID,AMOUNT_EUR
  FROM tmp_1129_mlra_txn_f_det WHERE LORO_ACCOUNT_FLAg= ''Y'' OR NOSTRO_ACCOUNT_FLAG =''Y''
)
PIVOT
(
  count (TABLE_SOURCE_ID) nr_of_tx,
  sum(nvl(AMOUNT_EUR,0)) SUM_OF_TZ_EUR
  FOR customer_lob
  in (' || v_Col || '))
ORDER BY DEBIT_CREDIT, TRANSACTION_GROUP  )  a left join totaluri b
 on a.Report_Cutoff_Date=b.Report_Cutoff_Date and a.DEBIT_CREDIT=b.debit_credit and  a.TRANSACTION_GROUP=b.transaction_group';
    EXECUTE IMMEDIATE v_Sql;
    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, 0);

    p_Nume_Tabela    := 'rap_1129_mlra_tab29';
    p_Param_1        := NULL;
    p_Desc_Eveniment := '';
    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,p_Task_Run_Id);
    SELECT COUNT(*)
      INTO v_Nr
      FROM User_Tables
     WHERE Table_Name = 'RAP_1129_MLRA_TAB29';
    IF v_Nr = 1 THEN
      v_Sql := 'drop table rap_1129_mlra_tab29 purge';
      EXECUTE IMMEDIATE v_Sql;
    END IF;
    v_Col := 'Report_Cutoff_Date, DECODE(GROUPING(DEBIT_CREDIT)
             , 0, DEBIT_CREDIT
             , 1, ''TOTAL''
             ) DEBIT_CREDIT,
             DECODE(GROUPING(TRANSACTION_GROUP)
             , 0, TRANSACTION_GROUP
             , 1, ''TOTAL''
             ) TRANSACTION_GROUP,';
    FOR Rec IN (SELECT Column_Name
                  FROM User_Tab_Columns
                 WHERE Table_Name = 'TMP_1129_MLRA_TAB29_TMP'
                   AND Column_Id NOT IN (1, 2, 3)
                 ORDER BY Column_Id) LOOP
      v_Col := v_Col || 'suM(nvl(' || Rec.Column_Name || ',0)) ' ||
               Rec.Column_Name || ',';
    END LOOP;
    v_Col := Substr(v_Col, 1, Length(v_Col) - 1);
    v_Sql := 'create table rap_1129_mlra_tab29 as
 select ' || v_Col ||
             ' from tmp_1129_mlra_tab29_tmp
  GROUP BY Report_Cutoff_Date, ROLLUP (DEBIT_CREDIT,TRANSACTION_GROUP)';
    EXECUTE IMMEDIATE v_Sql;
     execute immediate 'grant select on raportdwh.rap_1129_mlra_tab29 to monitor';

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, 0);
    ----------------------------------------------------------------------------------------------------------
    p_Nume_Tabela    := 'tmp_1129_mlra_tab30_tmp';
    p_Param_1        := NULL;
    p_Desc_Eveniment := '';
    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,p_Task_Run_Id);
    SELECT COUNT(*)
      INTO v_Nr
      FROM User_Tables
     WHERE Table_Name = 'TMP_1129_MLRA_TAB30_TMP';
    IF v_Nr = 1 THEN
      v_Sql := 'drop table tmp_1129_mlra_tab30_tmp purge';
      EXECUTE IMMEDIATE v_Sql;
    END IF;
    v_Col := NULL;
    FOR Rec IN (SELECT DISTINCT Customer_Lob
                  FROM Tmp_1129_Mlra_Txn_f_Det) LOOP
      v_Col := v_Col || '''' || Rec.Customer_Lob || ''' ' ||
               REPLACE(Rec.Customer_Lob, '+', '_') || ',';
    END LOOP;
    v_Col := Substr(v_Col, 1, Length(v_Col) - 1);

    v_Sql := 'create table tmp_1129_mlra_tab30_tmp as
       with totaluri as
 (SELECT Report_Cutoff_Date,
         DEBIT_CREDIT,
         TRANSACTION_GROUP,
         CUSTOMER_RR_LEVEL,
         count(table_source_id) NR_OF_TX,
         suM(nvl(amount_eur, 0)) SUM_OF_TZ_EUR
    FROM tmp_1129_mlra_txn_f_det
    GROUP BY Report_Cutoff_Date,
         DEBIT_CREDIT,
         TRANSACTION_GROUP,
         CUSTOMER_RR_LEVEL)
         SELECT a.*, b.NR_OF_TX,b.SUM_OF_TZ_EUR
         from (
  SELECT * FROM
(
  SELECT Report_Cutoff_Date, DEBIT_CREDIT, TRANSACTION_GROUP, CUSTOMER_RR_LEVEL,customer_lob,TABLE_SOURCE_ID,AMOUNT_EUR
  FROM tmp_1129_mlra_txn_f_det
)
PIVOT
(
  count (TABLE_SOURCE_ID) nr_of_tx,
  sum(AMOUNT_EUR) SUM_OF_TZ_EUR
  FOR customer_lob
  in (' || v_Col || '))
ORDER BY DEBIT_CREDIT, TRANSACTION_GROUP,CUSTOMER_RR_LEVEL)  a left join totaluri b
 on a.Report_Cutoff_Date=b.Report_Cutoff_Date and a.DEBIT_CREDIT=b.debit_credit and  a.TRANSACTION_GROUP=b.transaction_group and a.CUSTOMER_RR_LEVEL=b.CUSTOMER_RR_LEVEL ';
    EXECUTE IMMEDIATE v_Sql;
    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, 0);

    p_Nume_Tabela    := 'rap_1129_mlra_tab30';
    p_Param_1        := NULL;
    p_Desc_Eveniment := '';
    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,p_Task_Run_Id);
    SELECT COUNT(*)
      INTO v_Nr
      FROM User_Tables
     WHERE Table_Name = 'RAP_1129_MLRA_TAB30';
    IF v_Nr = 1 THEN
      v_Sql := 'drop table rap_1129_mlra_tab30 purge';
      EXECUTE IMMEDIATE v_Sql;
    END IF;
    v_Col := 'Report_Cutoff_Date, DECODE(GROUPING(DEBIT_CREDIT)
             , 0, DEBIT_CREDIT
             , 1, ''TOTAL''
             ) DEBIT_CREDIT,
             DECODE(GROUPING(TRANSACTION_GROUP)
             , 0, TRANSACTION_GROUP
             , 1, ''TOTAL''
             ) TRANSACTION_GROUP, CUSTOMER_RR_LEVEL,';
    FOR Rec IN (SELECT Column_Name
                  FROM User_Tab_Columns
                 WHERE Table_Name = 'TMP_1129_MLRA_TAB30_TMP'
                   AND Column_Id NOT IN (1, 2, 3, 4)
                 ORDER BY Column_Id) LOOP
      v_Col := v_Col || 'suM(nvl(' || Rec.Column_Name || ',0)) ' ||
               Rec.Column_Name || ',';
    END LOOP;
    v_Col := Substr(v_Col, 1, Length(v_Col) - 1);
    v_Sql := 'create table rap_1129_mlra_tab30 as
 select ' || v_Col ||
             ' from tmp_1129_mlra_tab30_tmp
  GROUP BY Report_Cutoff_Date, ROLLUP (DEBIT_CREDIT,TRANSACTION_GROUP,CUSTOMER_RR_LEVEL)';
    EXECUTE IMMEDIATE v_Sql;

    v_Sql := 'delete from rap_1129_mlra_tab30 where CUSTOMER_RR_LEVEL is null and transaction_group not like ''TOTAL%''';
    EXECUTE IMMEDIATE v_Sql;
    COMMIT;
         execute immediate 'grant select on raportdwh.rap_1129_mlra_tab30 to monitor';

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, 0);
    ---------------------------------------------------------------------------------------------
    p_Nume_Tabela    := 'tmp_1129_mlra_tab31_tmp';
    p_Param_1        := NULL;
    p_Desc_Eveniment := '';
    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,p_Task_Run_Id);
    SELECT COUNT(*)
      INTO v_Nr
      FROM User_Tables
     WHERE Table_Name = 'TMP_1129_MLRA_TAB31_TMP';
    IF v_Nr = 1 THEN
      v_Sql := 'drop table tmp_1129_mlra_tab31_tmp purge';
      EXECUTE IMMEDIATE v_Sql;
    END IF;
    v_Col := NULL;
    FOR Rec IN (SELECT DISTINCT Customer_Lob
                  FROM Tmp_1129_Mlra_Txn_f_Det) LOOP
      v_Col := v_Col || '''' || Rec.Customer_Lob || ''' ' ||
               REPLACE(Rec.Customer_Lob, '+', '_') || ',';
    END LOOP;
    v_Col := Substr(v_Col, 1, Length(v_Col) - 1);

    v_Sql := 'create table tmp_1129_mlra_tab31_tmp as
       with totaluri as
 (SELECT Report_Cutoff_Date,
         DEBIT_CREDIT,
         TRANSACTION_GROUP,
         CUSTOMER_RR_LEVEL,
         count(table_source_id) NR_OF_TX,
         suM(nvl(amount_eur, 0)) SUM_OF_TZ_EUR
    FROM tmp_1129_mlra_txn_f_det
     WHERE LORO_ACCOUNT_FLAg= ''Y'' OR NOSTRO_ACCOUNT_FLAG =''Y''
    GROUP BY Report_Cutoff_Date,
         DEBIT_CREDIT,
         TRANSACTION_GROUP,
         CUSTOMER_RR_LEVEL)
         SELECT a.*, b.NR_OF_TX,b.SUM_OF_TZ_EUR
         from (
  SELECT * FROM
(
  SELECT Report_Cutoff_Date, DEBIT_CREDIT, TRANSACTION_GROUP, CUSTOMER_RR_LEVEL,customer_lob,TABLE_SOURCE_ID,AMOUNT_EUR
  FROM tmp_1129_mlra_txn_f_det  WHERE LORO_ACCOUNT_FLAg= ''Y'' OR NOSTRO_ACCOUNT_FLAG =''Y''
)
PIVOT
(
  count (TABLE_SOURCE_ID) nr_of_tx,
  sum(AMOUNT_EUR) SUM_OF_TZ_EUR
  FOR customer_lob
  in (' || v_Col || '))
ORDER BY DEBIT_CREDIT, TRANSACTION_GROUP,CUSTOMER_RR_LEVEL )  a left join totaluri b
 on a.Report_Cutoff_Date=b.Report_Cutoff_Date and a.DEBIT_CREDIT=b.debit_credit and  a.TRANSACTION_GROUP=b.transaction_group and a.CUSTOMER_RR_LEVEL=b.CUSTOMER_RR_LEVEL ';
    EXECUTE IMMEDIATE v_Sql;
    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, 0);

    p_Nume_Tabela    := 'rap_1129_mlra_tab31';
    p_Param_1        := NULL;
    p_Desc_Eveniment := '';
    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,p_Task_Run_Id);
    SELECT COUNT(*)
      INTO v_Nr
      FROM User_Tables
     WHERE Table_Name = 'RAP_1129_MLRA_TAB31';
    IF v_Nr = 1 THEN
      v_Sql := 'drop table rap_1129_mlra_tab31 purge';
      EXECUTE IMMEDIATE v_Sql;
    END IF;
    v_Col := 'Report_Cutoff_Date, DECODE(GROUPING(DEBIT_CREDIT)
             , 0, DEBIT_CREDIT
             , 1, ''TOTAL''
             ) DEBIT_CREDIT,
             DECODE(GROUPING(TRANSACTION_GROUP)
             , 0, TRANSACTION_GROUP
             , 1, ''TOTAL''
             ) TRANSACTION_GROUP, CUSTOMER_RR_LEVEL,';
    FOR Rec IN (SELECT Column_Name
                  FROM User_Tab_Columns
                 WHERE Table_Name = 'TMP_1129_MLRA_TAB31_TMP'
                   AND Column_Id NOT IN (1, 2, 3, 4)
                 ORDER BY Column_Id) LOOP
      v_Col := v_Col || 'suM(nvl(' || Rec.Column_Name || ',0)) ' ||
               Rec.Column_Name || ',';
    END LOOP;
    v_Col := Substr(v_Col, 1, Length(v_Col) - 1);
    v_Sql := 'create table rap_1129_mlra_tab31 as
 select ' || v_Col ||
             ' from tmp_1129_mlra_tab31_tmp
  GROUP BY Report_Cutoff_Date, ROLLUP (DEBIT_CREDIT,TRANSACTION_GROUP,CUSTOMER_RR_LEVEL)';
    EXECUTE IMMEDIATE v_Sql;

    v_Sql := 'delete from rap_1129_mlra_tab31 where CUSTOMER_RR_LEVEL is null and transaction_group not like ''TOTAL%''';
    EXECUTE IMMEDIATE v_Sql;
    COMMIT;
         execute immediate 'grant select on raportdwh.rap_1129_mlra_tab31 to monitor';

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, 0);
    ----------------------------------------------------------------------------------------------------
    p_Nume_Tabela    := 'tmp_1129_mlra_tab32_tmp';
    p_Param_1        := NULL;
    p_Desc_Eveniment := '';
    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,p_Task_Run_Id);
    SELECT COUNT(*)
      INTO v_Nr
      FROM User_Tables
     WHERE Table_Name = 'TMP_1129_MLRA_TAB32_TMP';
    IF v_Nr = 1 THEN
      v_Sql := 'drop table tmp_1129_mlra_tab32_tmp purge';
      EXECUTE IMMEDIATE v_Sql;
    END IF;
    v_Col := NULL;
    FOR Rec IN (SELECT DISTINCT Customer_Rr_Level
                  FROM Tmp_1129_Mlra_Txn_f_Det) LOOP
      v_Col := v_Col || '''' || Rec.Customer_Rr_Level || ''' ' ||
               REPLACE(Rec.Customer_Rr_Level, '+', '_') || ',';
    END LOOP;
    v_Col := Substr(v_Col, 1, Length(v_Col) - 1);
    v_Sql := 'create table tmp_1129_mlra_tab32_tmp as
    with totaluri as (
SELECT Report_Cutoff_Date, case when LORO_ACCOUNT_FLAg= ''Y'' OR NOSTRO_ACCOUNT_FLAG =''Y'' then ''Y'' else ''N'' end LORO_NOSTRO_FLAG,
  decode(flag_cash,''Y'',''CASH'',''N'',''NON_CASH'') CASH_NON_CASH, DECODE(DOMESTIC_FLAG,''Y'',''DOMESTIC'',''INTERNATIONAL'') TRX_REGION,
   count(table_source_id) NR_OF_TX,
         suM(nvl(amount_eur, 0)) SUM_OF_TZ_EUR
  FROM tmp_1129_mlra_txn_f_det
  GROUP BY Report_Cutoff_Date, case when LORO_ACCOUNT_FLAg= ''Y'' OR NOSTRO_ACCOUNT_FLAG =''Y'' then ''Y'' else ''N'' end ,
  decode(flag_cash,''Y'',''CASH'',''N'',''NON_CASH'') , DECODE(DOMESTIC_FLAG,''Y'',''DOMESTIC'',''INTERNATIONAL'')
  ) SELECT a.*, b.NR_OF_TX, b.SUM_OF_TZ_EUR
FROM (
  SELECT * FROM
(
  SELECT Report_Cutoff_Date, case when LORO_ACCOUNT_FLAg= ''Y'' OR NOSTRO_ACCOUNT_FLAG =''Y'' then ''Y'' else ''N'' end LORO_NOSTRO_FLAG,
  decode(flag_cash,''Y'',''CASH'',''N'',''NON_CASH'') CASH_NON_CASH, DECODE(DOMESTIC_FLAG,''Y'',''DOMESTIC'',''INTERNATIONAL'') TRX_REGION,
    CUSTOMER_RR_LEVEL , TABLE_SOURCE_ID,AMOUNT_EUR
  FROM tmp_1129_mlra_txn_f_det
)
PIVOT
(
  count (TABLE_SOURCE_ID) nr_of_tx,
  sum(nvl(AMOUNT_EUR,0)) SUM_OF_TZ_EUR
  FOR CUSTOMER_RR_LEVEL
  in (' || v_Col || ')) ) a inner join totaluri b on
a.Report_Cutoff_Date=b.Report_Cutoff_Date and a.LORO_NOSTRO_FLAG=b.LORO_NOSTRO_FLAG and  a.CASH_NON_CASH=b.CASH_NON_CASH and a.TRX_REGION=b.TRX_REGION
 ';
    EXECUTE IMMEDIATE v_Sql;
    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, 0);

    p_Nume_Tabela    := 'rap_1129_mlra_tab32';
    p_Param_1        := NULL;
    p_Desc_Eveniment := '';
    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,p_Task_Run_Id);
    SELECT COUNT(*)
      INTO v_Nr
      FROM User_Tables
     WHERE Table_Name = 'RAP_1129_MLRA_TAB32';
    IF v_Nr = 1 THEN
      v_Sql := 'drop table rap_1129_mlra_tab32 purge';
      EXECUTE IMMEDIATE v_Sql;
    END IF;
    v_Col := 'Report_Cutoff_Date, DECODE(GROUPING(LORO_NOSTRO_FLAG)
             , 0, LORO_NOSTRO_FLAG
             , 1, ''TOTAL''
             ) LORO_NOSTRO_FLAG,
             DECODE(GROUPING(CASH_NON_CASH)
             , 0, CASH_NON_CASH
             , 1, ''TOTAL''
             ) CASH_NON_CASH,
              DECODE(GROUPING(TRX_REGION)
             , 0, TRX_REGION
             , 1, ''TOTAL''
             ) TRX_REGION, ';
    FOR Rec IN (SELECT Column_Name
                  FROM User_Tab_Columns
                 WHERE Table_Name = 'TMP_1129_MLRA_TAB32_TMP'
                   AND Column_Id NOT IN (1, 2, 3, 4)
                 ORDER BY Column_Id) LOOP
      v_Col := v_Col || 'suM(nvl(' || Rec.Column_Name || ',0)) ' ||
               Rec.Column_Name || ',';
    END LOOP;
    v_Col := Substr(v_Col, 1, Length(v_Col) - 1);
    v_Sql := 'create table rap_1129_mlra_tab32 as
 select ' || v_Col ||
             ' from tmp_1129_mlra_tab32_tmp
  GROUP BY Report_Cutoff_Date, ROLLUP (LORO_NOSTRO_FLAG, CASH_NON_CASH, TRX_REGION)';
    EXECUTE IMMEDIATE v_Sql;

    v_Sql := 'delete from rap_1129_mlra_tab32 where CASH_NON_CASH NOT like ''TOTAL%'' and TRX_REGION like ''TOTAL%''';
    EXECUTE IMMEDIATE v_Sql;

    COMMIT;
         execute immediate 'grant select on raportdwh.rap_1129_mlra_tab32 to monitor';

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, 0);
    -------------------------------------------------------------------------------------------------------------------
    p_Nume_Tabela    := 'rap_1129_mlra_tab33';
    p_Param_1        := NULL;
    p_Desc_Eveniment := '';
    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,p_Task_Run_Id);
    SELECT COUNT(*)
      INTO v_Nr
      FROM User_Tables
     WHERE Table_Name = 'RAP_1129_MLRA_TAB33';
    IF v_Nr = 1 THEN
      v_Sql := 'drop table rap_1129_mlra_tab33 purge';
      EXECUTE IMMEDIATE v_Sql;
    END IF;

    v_Sql := 'create table rap_1129_mlra_tab33 as SELECT REPORT_CUTOFF_DATE,
       DECODE(GROUPING(DEBIT_CREDIT), 0, DEBIT_CREDIT, 1, ''TOTAL'') DEBIT_CREDIT,
       sum(NO_OF_TZ) NO_OF_TZ,
       sum(RATIO_NO_OF_TZ) RATIO_NO_OF_TZ,
       sum(VAL_OF_TZ_EUR) VAL_OF_TZ_EUR,
       sum(RATIO_VAL_OF_TZ_EUR) RATIO_VAL_OF_TZ_EUR
  FROM (SELECT txn.REPORT_CUTOFF_DATE,
               TXN.DEBIT_CREDIT,
               COUNT(TABLE_SOURCE_ID) NO_OF_TZ,
               ROUND(RATIO_TO_REPORT(COUNT(TABLE_SOURCE_ID)) OVER(), 4) RATIO_NO_OF_TZ,
               SUM(TXN.AMOUNT_EUR) VAL_OF_TZ_EUR,
               ROUND(RATIO_TO_REPORT(SUM(TXN.AMOUNT_EUR)) OVER(), 4) RATIO_VAL_OF_TZ_EUR
          FROM tmp_1129_mlra_txn_f_det txn
         inner join man_1129_mlra_country_lookup lk
            on txn.cod_tara_ordonator = lk.country_code
           and lk.offshore_flag = ''Y''
         GROUP BY txn.REPORT_CUTOFF_DATE, TXN.DEBIT_CREDIT)
 GROUP BY REPORT_CUTOFF_DATE, ROLLUP(DEBIT_CREDIT)';
    EXECUTE IMMEDIATE v_Sql;
         execute immediate 'grant select on raportdwh.rap_1129_mlra_tab33 to monitor';

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, 0);

  END;

  PROCEDURE Proc_Produse_Tabele_Finale ( p_Task_Run_Id    NUMBER) AS

    v_Sql VARCHAR2(6000);
    v_Nr  NUMBER;
    v_Col VARCHAR2(2000);

  BEGIN
    p_Nume_Procedura := 'PROC_PRODUSE_TABELE_FINALE';

    p_Nume_Tabela    := 'rap_1129_mlra_tab16';
    p_Param_1        := NULL;
    p_Desc_Eveniment := '';
    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,p_Task_Run_Id);
    SELECT COUNT(*)
      INTO v_Nr
      FROM User_Tables
     WHERE Table_Name = 'RAP_1129_MLRA_TAB16';
    IF v_Nr = 1 THEN
      v_Sql := 'drop table rap_1129_mlra_tab16 purge';
      EXECUTE IMMEDIATE v_Sql;
    END IF;
    v_Col := NULL;
    FOR Rec IN (SELECT DISTINCT Customer_Rr_Level
                  FROM Tmp_1129_Mlra_Produse_Gpc) LOOP
      v_Col := v_Col || '''' || Rec.Customer_Rr_Level || ''' ' ||
               Rec.Customer_Rr_Level || ',';
    END LOOP;
    v_Col := Substr(v_Col, 1, Length(v_Col) - 1);

    v_Sql := 'create table rap_1129_mlra_tab16 as
   with totalURI AS
   (SELECT reporting_date,
           PRODUCT_RISK,
           sum (NR_OF_PROD_USAGE) TOTAL,
           ROUND(RATIO_TO_REPORT(sum (NR_OF_PROD_USAGE)) OVER(), 4) RATIO
      FROM tmp_1129_mlra_produse_gpc
     GROUP BY reporting_date, PRODUCT_RISK),
  avg_prod_risk as (SELECT reporting_date, PRODUCT_RISK,NO_PROD_PER_PROD_RISK ,
   ROUND(RATIO_TO_REPORT(avg(NO_PROD_PER_PROD_RISK)) OVER(), 4) RATIO_PROD_PER_PROD_RISK
  FROM tmp_1129_mlra_produse_gpc group by reporting_date, PRODUCT_RISK,NO_PROD_PER_PROD_RISK  )
  SELECT
  A.*, B.TOTAL, B.RATIO, bb.RATIO_PROD_PER_PROD_RISK
  FROM
  (
  SELECT * FROM
(
  SELECT reporting_date, PRODUCT_RISK,NO_PROD_PER_PROD_RISK ,
   CUSTOMER_RR_LEVEL,NR_OF_PROD_USAGE
  FROM tmp_1129_mlra_produse_gpc
)
PIVOT
(
  sum (NR_OF_PROD_USAGE)
  FOR CUSTOMER_RR_LEVEL
  in (' || v_Col || '))
ORDER BY PRODUCT_RISK ) A INNER JOIN TOTALURI B  ON A.REPORTING_DATE=B.REPORTING_DATE AND
A.PRODUCT_RISK=B.PRODUCT_RISK
inner join avg_prod_risk bb on a.reporting_date=bb.reporting_date and a.product_risk=bb.product_risk
 ';
    EXECUTE IMMEDIATE v_Sql;
    
 execute immediate 'grant select on raportdwh.rap_1129_mlra_tab16 to monitor';

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, 0);

    p_Nume_Tabela    := 'rap_1129_mlra_tab17';
    p_Param_1        := NULL;
    p_Desc_Eveniment := '';
    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,p_Task_Run_Id);

    SELECT COUNT(*)
      INTO v_Nr
      FROM User_Tables
     WHERE Table_Name = 'RAP_1129_MLRA_TAB17';
    IF v_Nr = 1 THEN
      v_Sql := 'drop table rap_1129_mlra_tab17 purge';
      EXECUTE IMMEDIATE v_Sql;
    END IF;
    v_Col := NULL;
    FOR Rec IN (SELECT DISTINCT Customer_Lob
                  FROM Tmp_1129_Mlra_Produse_Gpc) LOOP
      v_Col := v_Col || '''' || Rec.Customer_Lob || ''' ' ||
               REPLACE(Rec.Customer_Lob, '+', '_') || ',';
    END LOOP;
    v_Col := Substr(v_Col, 1, Length(v_Col) - 1);

    v_Sql := 'create table rap_1129_mlra_tab17 as
    with totalURI AS
   (SELECT reporting_date,
           PRODUCT_RISK,
           sum(NR_OF_PROD_USAGE) TOTAL,
            ROUND(RATIO_TO_REPORT(sum (NR_OF_PROD_USAGE)) OVER(), 4) RATIO
        FROM tmp_1129_mlra_produse_gpc
       GROUP BY reporting_date, PRODUCT_RISK),
       avg_prod_risk as (SELECT reporting_date, PRODUCT_RISK,NO_PROD_PER_PROD_RISK ,
   ROUND(RATIO_TO_REPORT(avg(NO_PROD_PER_PROD_RISK)) OVER(), 4) RATIO_PROD_PER_PROD_RISK
  FROM tmp_1129_mlra_produse_gpc group by reporting_date, PRODUCT_RISK,NO_PROD_PER_PROD_RISK  )
    SELECT
    A.*, B.TOTAL, B.RATIO, bb.RATIO_PROD_PER_PROD_RISK
    FROM
    (
    SELECT * FROM
  (
    SELECT reporting_date, PRODUCT_RISK,NO_PROD_PER_PROD_RISK as no_of_product_codes, customer_lob,NR_OF_PROD_USAGE
    FROM tmp_1129_mlra_produse_gpc
  )
  PIVOT
  (
     sum (NR_OF_PROD_USAGE)
    FOR customer_lob
    in (' || v_Col || '))
  ORDER BY PRODUCT_RISK  ) A INNER JOIN TOTALURI B  ON A.REPORTING_DATE=B.REPORTING_DATE AND
  A.PRODUCT_RISK=B.PRODUCT_RISK
  inner join avg_prod_risk bb on a.reporting_date=bb.reporting_date and a.product_risk=bb.product_risk';
    EXECUTE IMMEDIATE v_Sql;
     execute immediate 'grant select on raportdwh.rap_1129_mlra_tab17 to monitor';

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, 0);

    p_Nume_Tabela    := 'rap_1129_mlra_tab18';
    p_Param_1        := NULL;
    p_Desc_Eveniment := '';
    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,p_Task_Run_Id);
    SELECT COUNT(*)
      INTO v_Nr
      FROM User_Tables
     WHERE Table_Name = 'RAP_1129_MLRA_TAB18';
    IF v_Nr = 1 THEN
      v_Sql := 'drop table rap_1129_mlra_tab18 purge';
      EXECUTE IMMEDIATE v_Sql;
    END IF;
    v_Col := NULL;
    FOR Rec IN (SELECT DISTINCT Customer_Lob
                  FROM Tmp_1129_Mlra_Produse_Gpc) LOOP
      v_Col := v_Col || '''' || Rec.Customer_Lob || ''' ' ||
               REPLACE(Rec.Customer_Lob, '+', '_') || ',';
    END LOOP;
    v_Col := Substr(v_Col, 1, Length(v_Col) - 1);

    v_Sql := ' create table rap_1129_mlra_tab18 as
 with totalURI AS
   (SELECT reporting_date, PRODUCT_RISK, PRODUCT_DESCRIPTION,
           sum(NR_OF_PROD_USAGE) TOTAL,
          ROUND(RATIO_TO_REPORT(sum (NR_OF_PROD_USAGE)) OVER(), 4) RATIO
      FROM tmp_1129_mlra_produse_gpc
     GROUP BY reporting_date, PRODUCT_RISK, PRODUCT_DESCRIPTION)
  SELECT
  A.*, B.TOTAL, B.RATIO
  FROM
  (
  SELECT * FROM
(
  SELECT reporting_date, PRODUCT_RISK, PRODUCT_DESCRIPTION,customer_lob,NR_OF_PROD_USAGE
  FROM tmp_1129_mlra_produse_gpc
)
PIVOT
(
   sum (NR_OF_PROD_USAGE)
  FOR customer_lob
  in (' || v_Col || '))
ORDER BY PRODUCT_RISK) A INNER JOIN TOTALURI B  ON A.REPORTING_DATE=B.REPORTING_DATE AND
A.PRODUCT_RISK=B.PRODUCT_RISK and a.PRODUCT_DESCRIPTION=b.PRODUCT_DESCRIPTION ';
    EXECUTE IMMEDIATE v_Sql;
    execute immediate 'grant select on raportdwh.rap_1129_mlra_tab18 to monitor';
    
    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, 0);

    p_Nume_Tabela    := 'rap_1129_mlra_tab19';
    p_Param_1        := NULL;
    p_Desc_Eveniment := '';
    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,p_Task_Run_Id);

    SELECT COUNT(*)
      INTO v_Nr
      FROM User_Tables
     WHERE Table_Name = 'RAP_1129_MLRA_TAB19';
    IF v_Nr = 1 THEN
      v_Sql := 'drop table rap_1129_mlra_tab19 purge';
      EXECUTE IMMEDIATE v_Sql;
    END IF;
    v_Col := NULL;
    FOR Rec IN (SELECT DISTINCT Customer_Lob
                  FROM Tmp_1129_Mlra_Produse_Gpc) LOOP
      v_Col := v_Col || '''' || Rec.Customer_Lob || ''' ' ||
               REPLACE(Rec.Customer_Lob, '+', '_') || ',';
    END LOOP;
    v_Col := Substr(v_Col, 1, Length(v_Col) - 1);

    v_Sql := 'create table rap_1129_mlra_tab19 as
  SELECT * FROM
(
  SELECT reporting_date, PRODUCT_RISK, PRODUCT_LEVEL_1, PRODUCT_LEVEL_2, PRODUCT_LEVEL_3,customer_lob,NR_OF_PROD_USAGE
  FROM tmp_1129_mlra_produse_gpc
)
PIVOT
(
  sum (NR_OF_PROD_USAGE)
  FOR customer_lob
  in (' || v_Col || '))
ORDER BY PRODUCT_RISK ';
    EXECUTE IMMEDIATE v_Sql;
    execute immediate 'grant select on raportdwh.rap_1129_mlra_tab19 to monitor';
    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, 0);
  END;

  PROCEDURE Proc_Chestionar_Bnr( p_Task_Run_Id    NUMBER,p_Reporting_Date DATE) AS

  BEGIN

    p_Nume_Procedura := 'PROC_CHESTIONAR_BNR';
    p_Nume_Tabela    := 'rap_1190_bnr_aml_produse';
    p_Tip            := 'F';
    p_Desc_Eveniment := '1. Macheta Produse';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    EXECUTE IMMEDIATE 'TRUNCATE TABLE raportdwh.RAP_1190_BNR_AML_PRODUSE ';

    INSERT INTO Raportdwh.Rap_1190_Bnr_Aml_Produse
      SELECT Reporting_Date,
             Product_Description,
             Product_Level_1,
             Product_Level_2,
             Product_Level_3,
             Product_Risk,
             Categ_Client,
             COUNT(DISTINCT Cif) AS No_Cif
        FROM Raportdwh.Tmp_1129_Mlra_Produse_Gpc_h
       WHERE Reporting_Date = p_Reporting_Date
         AND Categ_Client IS NOT NULL
         AND Bk_Status_Crm IN
             ('Active', 'ACTIVE', 'Dormant', 'DORMANT', 'Active-Delinquent')
         AND Bk_Customer_Type = 'Customer'
       GROUP BY Reporting_Date,
                Product_Description,
                Product_Level_1,
                Product_Level_2,
                Product_Level_3,
                Product_Risk,
                Categ_Client
       ORDER BY Product_Description,
                Categ_Client;

    p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

  END Proc_Chestionar_Bnr;

  PROCEDURE Call_All
  (
    p_Task_Run_Id    NUMBER,
    p_Reporting_Date DATE
  ) AS

  BEGIN
/*
--pe prod Others_Arh.Oth_Manu_Lkp_Country_Aml este fara region_for_reporting
--asta este scriptul pentru obtinerea datelor la dec 2023
--ramane tabela manuala momentan

p_Nume_Procedura := 'call_all';
    p_Nume_Tabela    := 'Man_1129_Mlra_Country_Lookup';
    p_Tip            := 'F';
    p_Desc_Eveniment := 'preluare din nom_country - sursa pt chestionar bnr';
    p_Param_1        := To_Char(p_Reporting_Date, 'dd-mon-yyyy');

    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

EXECUTE IMMEDIATE 'TRUNCATE TABLE RAPORTDWH.Man_1129_Mlra_Country_Lookup ';
INSERT INTO Raportdwh.Man_1129_Mlra_Country_Lookup
  (Country_Code,
   Country_Name,
   Offshore_Flag,
   Fatf_Flag,
   Region_For_Reporting)
  SELECT a.Cod_Country          Country_Code,
         a.Name_Country         Country_Name,
         a.Offshore_Flag,
         a.Fatf_Gray_Flag       Fatf_Flag,
         b.Region_For_Reporting
    FROM Dm.Nom_Country a
    inner JOIN Others_Arh.Oth_Manu_Lkp_Country_Aml@Gdwh24_Dwh_Col b
      ON a.Cod_Country = b.Country_Code
     AND b.Date_From = p_Reporting_Date --29-dec-2023
   WHERE p_Reporting_Date BETWEEN a.Date_From AND a.Date_Until
   and a.Source_System<>'DEFAULT';
--inner join - mai vin balarii din crm, am nevoie doar de codurile valiadte crisp, cele cu region_for_reporting completat
  p_Nr_Inregistrari := SQL%ROWCOUNT;

    COMMIT;

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);*/


    --Checkuri_Incarcare_Tabele(p_Task_Run_Id, p_Reporting_Date); --nu scot pe eroare daca nu am date, mi le aduc
    Proc_Clienti(p_Task_Run_Id ,p_Reporting_Date);
    Proc_Tranzactii(p_Task_Run_Id ,p_Reporting_Date);
    Proc_Produse(p_Task_Run_Id ,p_Reporting_Date);

    Call_Tabele_Finale(p_Task_Run_Id ,p_Reporting_Date);

    Proc_Chestionar_Bnr(p_Task_Run_Id,p_Reporting_Date);

    p_Tip            := 'L'; --
    p_Desc_Eveniment := 'Trimitere mail';
    Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                               p_Nume_Procedura,
                               p_Nume_Schema,
                               p_Nume_Tabela,
                               p_Tip,
                               p_Param_1,
                               p_Param_2,
                               p_Param_3,
                               p_Desc_Eveniment,
                               p_Autonumber,
                               p_Task_Run_Id);

    Monitor.Pkg_Notification.Call_Info_Warning(p_Notif_Call_Proc      => 'CALL_RAP_1129_MLRA_REPORTING',
                                               p_App_Name             => 'RAPORTDWH',
                                               p_Crt_Processing_Date  => p_Reporting_Date,
                                               p_Prev_Processing_Date => NULL);

    Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, p_Nr_Inregistrari);

    Proc_Clean_Temp_Tables(p_Task_Run_Id );

  END Call_All;

  PROCEDURE Call_Tabele_Finale( p_Task_Run_Id    NUMBER,p_Reporting_Date DATE) AS

  BEGIN
    IF Check_Table_Date(p_Task_Run_Id ,p_Reporting_Date, 'TMP_1129_MLRA_CLIENTI_F') = 0 THEN
      EXECUTE IMMEDIATE ('TRUNCATE TABLE TMP_1129_MLRA_CLIENTI_F');
      Proc_Write_To_From_Hist(p_Task_Run_Id ,p_Reporting_Date,
                              'TMP_1129_MLRA_CLIENTI_F_H',
                              'TMP_1129_MLRA_CLIENTI_F');
    END IF;

    IF Check_Table_Date(p_Task_Run_Id ,p_Reporting_Date, 'TMP_1129_MLRA_PRODUSE_GPC') = 0 THEN
      EXECUTE IMMEDIATE ('TRUNCATE TABLE TMP_1129_MLRA_PRODUSE_GPC');
      Proc_Write_To_From_Hist(p_Task_Run_Id ,p_Reporting_Date,
                              'TMP_1129_MLRA_PRODUSE_GPC_H',
                              'TMP_1129_MLRA_PRODUSE_GPC');
    END IF;

    IF Check_Table_Date(p_Task_Run_Id ,p_Reporting_Date, 'TMP_1129_MLRA_TXN_F_DET') = 0 THEN
      EXECUTE IMMEDIATE ('TRUNCATE TABLE TMP_1129_MLRA_TXN_F_DET');
      Proc_Write_To_From_Hist(p_Task_Run_Id ,p_Reporting_Date,
                              'TMP_1129_MLRA_TXN_F_DET_H',
                              'TMP_1129_MLRA_TXN_F_DET');
    END IF;

    Proc_Tranz_Tabele_Finale(p_Task_Run_Id );
    Proc_Produse_Tabele_Finale(p_Task_Run_Id );

  END Call_Tabele_Finale;

  FUNCTION Check_Table_Date
  ( p_Task_Run_Id    NUMBER,
    p_Reporting_Date DATE,
    p_Table_Name     VARCHAR2
  ) RETURN NUMBER IS
    v_Nr         NUMBER;
    v_Date_Field VARCHAR2(40);
    v_Sql        VARCHAR2(4000);
  BEGIN
    SELECT COUNT(*)
      INTO v_Nr
      FROM All_Tab_Columns
     WHERE Owner = 'RAPORTDWH'
       AND Table_Name = p_Table_Name
       AND Column_Name = 'REPORT_CUTOFF_DATE';
    IF v_Nr = 1 THEN
      v_Date_Field := 'REPORT_CUTOFF_DATE';
    ELSE
      v_Date_Field := 'REPORTING_DATE';
    END IF;
    v_Sql := 'SELECT COUNT(*)  FROM ' || p_Table_Name || ' WHERE ' ||
             v_Date_Field || ' = ''' || p_Reporting_Date ||
             ''' AND ROWNUM=1';
    EXECUTE IMMEDIATE v_Sql
      INTO v_Nr;
    RETURN v_Nr;
  END;
  PROCEDURE Proc_Clean_Temp_Tables ( p_Task_Run_Id    NUMBER) AS
  BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_1129_MLRA_SEP_INCOMING ';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_1129_MLRA_SEP_OUTGOING ';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_1129_MLRA_TRAN_TXN_STMT ';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_1129_MLRA_WBI_TXN ';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_1129_MLRA_M_INPAY ';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_1129_MLRA_MIDAS_ACCT_TXN ';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_1129_MLRA_M_OTPAY ';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_1129_MLRA_DEBIT_CARD ';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_1129_MLRA_WSS2DWHFX';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_1129_MLRA_WSS2DWHMM ';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_1129_MLRA_CTNI_SFCNTR ';

  END Proc_Clean_Temp_Tables;

  --RWH1191 Verificare disponibilitate date pentru WRK
  PROCEDURE Checkuri_Incarcare_Tabele
  (
    p_Task_Run_Id    NUMBER,
    p_Reporting_Date DATE
  ) AS
    v_Nr_Inreg       NUMBER;
    p_Nume_Pachet    VARCHAR2(100) := 'PKG_RAP_1129_MLRA_REPORTING';
    p_Nume_Procedura VARCHAR2(100) := '';
    p_Desc_Eveniment VARCHAR2(200);
    p_Eroare         VARCHAR2(250) := 'FARA EROARE';
    p_Param_1        VARCHAR2(100) := To_Char(p_Reporting_Date,
                                              'DD-MON-YYYY');
    p_Param_2        VARCHAR2(100);
    p_Param_3        VARCHAR2(100);
    p_Autonumber     NUMBER;
    p_Nume_Schema    VARCHAR2(100) := 'RAPORTDWH';
    p_Nume_Tabela    VARCHAR2(100);
    p_Tip            VARCHAR2(10);

    v_Nr     NUMBER;
    p_Dblink VARCHAR2(100);
    p_Ssql   VARCHAR2(10000);
    Exception_Lipsa_Date_Sursa EXCEPTION;
    p_Mesaj_Eroare VARCHAR2(2000);
  BEGIN

    p_Param_1 := To_Char(p_Reporting_Date, 'DD-MON-YYYY');
    p_Param_2 := NULL;

    p_Nume_Procedura := 'CHECKURI_INCARCARE_TABELE';
    p_Tip            := 'F';
    p_Nume_Tabela    := '';
    p_Desc_Eveniment := 'Verificare tabele sursa';
    Raportdwh.Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                                         p_Nume_Procedura,
                                         p_Nume_Schema,
                                         p_Nume_Tabela,
                                         p_Tip,
                                         p_Param_1,
                                         p_Param_2,
                                         p_Param_3,
                                         p_Desc_Eveniment,
                                         p_Autonumber,
                                         p_Task_Run_Id);
    FOR Rec IN (SELECT Referenced_Owner,
                       Referenced_Name,
                       Referenced_Link_Name
                  FROM User_Dependencies
                 WHERE NAME = p_Nume_Pachet
                   AND ((Referenced_Owner = 'WRK' AND
                       Referenced_Name LIKE 'STG%'  AND
                       Referenced_Type = 'TABLE') OR
                       (Referenced_Owner IN ('DWH_COL', 'OTHERS_ARH') AND
                       Referenced_Type = 'TABLE'))) LOOP

    p_Dblink := '';
      IF Rec.Referenced_Owner IN ('DWH_COL', 'OTHERS_ARH') THEN
        p_Dblink := '@' || Rec.Referenced_Link_Name;
      END IF;
      p_Ssql := 'SELECT COUNT(*) FROM ' || Rec.Referenced_Owner || '.' ||
                Rec.Referenced_Name || p_Dblink || ' WHERE DATE_FROM = ''' ||
                p_Reporting_Date || ''' AND rownum=1';


      EXECUTE IMMEDIATE p_Ssql
        INTO v_Nr;

      IF v_Nr = 0 THEN
        p_Mesaj_Eroare := 'Tabela ' || Rec.Referenced_Owner || '.' ||
                          Rec.Referenced_Name || ' nu contine date la ' ||
                          p_Reporting_Date;
        RAISE Exception_Lipsa_Date_Sursa;
      END IF;

    END LOOP;


   EXCEPTION

    WHEN Exception_Lipsa_Date_Sursa THEN
      p_Eroare := p_Mesaj_Eroare;
      Raportdwh.Pack_Rapdwh_Log.End_Proc(p_Autonumber,
                                         p_Eroare,
                                         v_Nr_Inreg);
      Raise_Application_Error(-20101, p_Mesaj_Eroare);
    WHEN OTHERS THEN
      Monitor.Pkg_Task_Orchestration.Task_Block('TSK_RAP_1129_MLRA',
                                                p_Reporting_Date,
                                                p_Task_Run_Id);
      Raportdwh.Pack_Rapdwh_Log.End_Proc(p_Autonumber, SQLERRM, 0);
      RAISE;


    Raportdwh.Pack_Rapdwh_Log.End_Proc(p_Autonumber, p_Eroare, v_Nr_Inreg);

  END Checkuri_Incarcare_Tabele;

  procedure check_and_load (p_task_run_id number, p_reporting_date date, p_table_name varchar2)
    is
    v_sql varchar2(4000);
    v_nr number;
    begin

   p_Nume_Tabela    := p_table_name;
    p_Nume_Procedura := 'check_and_load';
    p_Tip            := 'I';
    p_Desc_Eveniment := 'check '||p_table_name;
    p_Param_1        := p_reporting_date;
    p_Param_2:=null;
    p_Param_3:=null;
    Raportdwh.Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                                         p_Nume_Procedura,
                                         p_Nume_Schema,
                                         p_Nume_Tabela,
                                         p_Tip,
                                         p_Param_1,
                                         p_Param_2,
                                         p_Param_3,
                                         p_Desc_Eveniment,
                                         p_Autonumber,
                                         p_Task_Run_Id);

v_sql := ' SELECT COUNT(1)
      FROM Wrk.'||p_table_name ||'
     WHERE Date_From = to_date('''|| to_char(p_reporting_date,'dd-mon-yyyy')||q'[','dd-mon-yyyy')]';
     DBMS_OUTPUT.PUT_LINE( v_sql);
    execute immediate v_sql   into v_nr;

 Raportdwh.Pack_Rapdwh_Log.End_Proc(p_Autonumber,
                                       p_Eroare,
                                       v_nr);

   p_Nume_Tabela    := p_table_name;
    p_Nume_Procedura := 'check_and_load';
    p_Tip            := 'I';
    p_Desc_Eveniment := 'load '||p_table_name;
    p_Param_1        := p_reporting_date;
    p_Param_2:='inreg la check '||v_nr;
    p_Param_3:=null;
    Raportdwh.Pack_Rapdwh_Log.Begin_Proc(p_Nume_Pachet,
                                         p_Nume_Procedura,
                                         p_Nume_Schema,
                                         p_Nume_Tabela,
                                         p_Tip,
                                         p_Param_1,
                                         p_Param_2,
                                         p_Param_3,
                                         p_Desc_Eveniment,
                                         p_Autonumber,
                                         p_Task_Run_Id);
    IF v_Nr = 0 THEN
      Wrk.Pkg_Stg_Load_Table.Load_Table_Prttn_By_Lst_Hist(p_table_name,
                                                          p_reporting_date,
                                                          1);
    END IF;
     p_Nr_Inregistrari:=0;
   Raportdwh.Pack_Rapdwh_Log.End_Proc(p_Autonumber,
                                       p_Eroare,
                                       p_Nr_Inregistrari);

    end check_and_load;
END;
/

