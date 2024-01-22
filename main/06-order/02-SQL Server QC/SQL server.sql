-- IM_PCD_source_loads_FACT_CustomerOrder_Open_lastyear_YTD_D
SELECT
        A.VBELN AS cCO_number
,       B.POSNR AS cCO_Item
,       A.AUDAT AS cCO_Date
,       A.KUNNR AS cSold_to_Party
,       F.ZZCBF AS cCBF
,       B.MATNR AS cMaterial_NO
,       C.GBSTA cStatus
,       A.AUART cOrder_Type
,       B.KWMENG iOrder_qty
,       NVL(D.Delivery_Qty,0)                         AS iDelivery_Qty
,       NVL(D.Billing_Qty,0)                          AS iBilling_Qty
,       NVL(D.PGI_Qty,0)                              AS iPGI_Qty
,       NVL(D.Delivery_Qty,0) - NVL(D.PGI_Qty,0)      AS iOpen_Delivery_Qty
,       B.KWMENG              - NVL(D.Delivery_Qty,0) AS iBalance_Qty
,       B.NETPR                                       AS iUnit_price
,       B.NETWR                                       AS iAmount
,       J.BZIRK cSales_Unit
,       H.KUNNR cShip_to_Party
,       E.EDATU cCust_Req_Date
,       B.WAERK           AS cLocal_currency B.ABGRU AS cRejection_Reason
,       A.VKORG           AS cSales_Org
,       A.VTWEG           AS cDChl
,       A.SPART           AS cDivision
,       A.ERDAT           AS cDelivery_Create_Date
,       C.GBSTA           AS cOverall_Status
,       C.LFSTA           AS cDelivery_Status
,       C.FKSAA           as cBilling_Status
,       B.PSTYV           AS cITEM_CATEGORY
,       B.WERKS           AS cPlant
,       J.BZIRK           AS cSALES_DISTRICT
,       A.BSTNK           AS cCustomer_PO_Number
,       B.KDMAT           AS cCustomer_Material
,       B.VSTEL           AS cShipping_Point
,       B.ROUTE           AS cRoute
,       A.VKBUR           AS cSales_Office
,       A.AUGRU           AS cOrder_Reason
,       A.VSBED           AS cShipping_Condition
,       B.PS_PSP_PNR      AS cWBS_Element
,       K.PLTYP           AS cPrice_List
,       A.VDATU           AS initial_Request_Delivery_Date
,       B.RBCR_YV_DG_DATE AS COrder_Downgraded_DATE
,       A.LIFSK           as cOrder_delivery_block_in_header
,       E.LIFSP           as cOrder_delivery_block_in_schedule_line
FROM
        MARD_DALI_AA.VBAK_PCD A
JOIN
        MARD_DALI_AA.VBAP_PCD B
ON
        A.VBELN=B.VBELN --Sales order
JOIN
        (
                SELECT
                        VBELN
                ,       POSNR
                ,       GBSTA
                ,       LFSTA
                ,       FKSAA
                FROM
                        MARD_DALI_AA.VBUP_PCD
                --WHERE NOT GBSTA=''C''
        ) C --status
        ON B.VBELN =C.VBELN
        AND B.POSNR=C.POSNR
LEFT JOIN
        (
                SELECT
                        VBELV
                ,       POSNV
                ,       sum(
                                CASE
                                WHEN
                                        VBTYP_N=''J''
                                THEN
                                        RFMNG
                                ELSE
                                        0
                                END) AS Delivery_Qty
                ,       sum(
                                CASE
                                WHEN
                                        VBTYP_N=''M''
                                THEN
                                        RFMNG
                                ELSE
                                        0
                                END) AS Billing_Qty
                ,       sum(
                                CASE
                                WHEN
                                        VBTYP_N=''R''
                                THEN
                                        RFMNG
                                ELSE
                                        0
                                END) AS PGI_Qty
                FROM
                        MARD_DALI_AA.VBFA_PCD
                WHERE
                        VBTYP_N IN (''J''
                                   , ''M''
                                   , ''R'')
                GROUP BY
                        VBELV
                ,       POSNV ) D --flow
ON
        B.VBELN=D.VBELV
AND     B.POSNR=D.POSNV
LEFT JOIN
        (
                SELECT
                        VBELN
                ,       POSNR
                ,       EDATU
                ,       LIFSP
                FROM
                        MARD_DALI_AA.VBEP_PCD
                WHERE
                        WMENG>0 )E --Schedule line data
ON
        B.VBELN=E.VBELN
AND     B.POSNR=E.POSNR
JOIN
        MARD_DALI_AA.MARA_PCD F
ON
        B.MATNR=F.MATNR --CBF data
LEFT JOIN
        (
                SELECT
                        KUNNR
                ,       BZIRK
                ,       VKORG
                ,       VTWEG
                ,       SPART
                FROM
                        MARD_DALI_AA.KNVV_PCD
                WHERE
                        AUFSD='' ''
                AND     LOEVM='' '') G
ON
        A.KUNNR=G.KUNNR
AND     A.VKORG=G.VKORG
AND     A.VTWEG=G.VTWEG
AND     A.SPART=G.SPART
LEFT JOIN
        (
                SELECT
                        VBELN
                ,       POSNR
                ,       PARVW
                ,       KUNNR
                FROM
                        MARD_DALI_AA.VBPA_PCD
                WHERE
                        PARVW=''WE'' ) H
ON
        B.VBELN=H.VBELN
LEFT JOIN
        (
                SELECT
                        VBELN
                ,       POSNR
                ,       BZIRK
                FROM
                        MARD_DALI_AA.VBKD_PCD ) J --SALES DISTRICT
ON
        B.VBELN=J.VBELN
AND     B.POSNR=J.POSNR
LEFT JOIN
        MARD_DALI_AA.VBKD_PCD K
ON
        B.VBELN=K.VBELN
AND     B.POSNR=K.POSNR
WHERE
        A.VBTYP IN (''C''
                   , ''I''
                   , ''E'') --order type
        --A.AUART IN (''YKSA'', ''YSCA'',''YKEA'',''YKNA'',''YKNE'',''ZSO'',''ZTP'',''ZCA'',''ZEA'',''ZCU'',''ZAR'', ''ZEX'', ''ZFC'', ''ZG2'', ''ZG32'', ''ZL32'',''ZOR'')
AND     B.WERKS IN (''66A1''
                   , ''K25M''
                   , ''W300''
                   , ''W301''
                   , ''62BW''
                   , ''W092''
                   , ''9631''
                   , ''8540''
                   , ''5640''
                   , ''W205''
                   , ''W200''
                   , ''W201''
                   , ''K554''
                   , ''K556''
                   , ''W901''
                   , ''W908''
                   , ''E13T''
                   , ''66A5'')
AND     NVL(B.ABGRU,'' '')='' ''
AND     A.VKORG IN (''CN20''
                   , ''CN2D''
                   , ''CN2E''
                   , ''CN2I''
                   , ''TW28''
                   , ''TW20''
                   , ''CN21''
                   , ''DE20''
                   , ''SG20''
                   , ''SG21''
                   , ''MY20''
                   , ''MY21''
                   , ''TH20''
                   , ''TH21''
                   , ''PH20''
                   , ''PH21''
                   , ''ID20''
                   , ''ID21''
                   , ''JP20''
                   , ''JP21''
                   , ''KR20''
                   , ''KR21''
                   , ''KR2L''
                   , ''KR2I''
                   , ''KR2E''
                   , ''AU20''
                   , ''AU21''
                   , ''AU2D''
                   , ''AU2I''
                   , ''AU2E'')
AND     A.VTWEG IN (''G1''
                   , ''G4'')
AND     (
                substr(A.AUDAT,1,4) in (''2021''
                                       , ''2022''))
AND
        CASE
        WHEN
                A.AUART=''ZCU''
        THEN
                NVL(D.PGI_Qty,0)
        ELSE
                NVL(D.Billing_Qty,0)
        END < B.KWMENG --open items
        -- Note: A.AUART=YBRE,B.KWMENG*(-1),B.NETWR*(-1)