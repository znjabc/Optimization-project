--odps sql 
--********************************************************************--
--脚本名称:管控营销融合宽表数据插入
--编写人:朱宁静
--脚本创建时间:2023-05-18 14:25:57
--源表名: DATA_ZJ_PROCESS_PROD.DWD_FIN_T_ZJ_BALIST_@@{yyyy}  --管控-资金监控交易年份表
--        DATA_ZJ_PROCESS_PROD.DWD_FIN_T_T_ZJJK_COLLECT_HD_@@{yyyy}  --管控-回单采集信息年份表
--        ODPS_ZJ_DM_FIN_PROD.ADS_FIN_DIM_BANKACCT  --银行账号维表
--        ODPS_ZJ_DM_FIN_PROD.ADS_FIN_FININFO_DIM_BANK_AND_ACCOUNT  --银行及账户基本信息表
--        ODPS_ZJ_DM_FIN_PROD.ADS_FIN_FININFO_DIM_ACCOUNT_TYPE  --账户分类表
--        ODPS_ZJ_DM_FIN_PROD.ADS_FIN_FININFO_DIM_MASTER_SUB_MAPPING   --主子账号映射表
--        DATA_ZJ_PROCESS_PROD.DWD_FIN_T_ZJJK_LINKAGE_BALIST2023  --银行对象联动交易记录表
--        DATA_ZJ_PROCESS_PROD.DWD_CST_BANK_CAP_RUN_SNPST   -- 营销-银行资金流水快照表
--        ODPS_ZJ_DM_FIN_PROD.ADS_FIN_DIM_UNIT_MAPPING  --'管控营销单位映射表'
--目标表名:ODPS_ZJ_DM_FIN_PROD.ADS_FIN_FININFO_DWD_FUSION
--业务逻辑简述:①资金监控交易年份表中的账号字段与银行账号表中的账号字段相关联限制管控的账号范围（只取融合的电费户账号下的流水用于分析）
--            ②资金监控交易年份表中的交易流水号字段与回单采集信息年份表中的交易流水号字段相关联获取流水的回单信息
--            ③资金监控交易年份表中的账号字段与银行及账户基本信息表相关联获取银行的开户金融机构、开户单位、账号等信息
--            ④银行及账户基本信息表中的账户分类字段与管理对象的分类体系表中的内部ID字段相关联获取账户分类及账户分类名称
--            ⑤资金监控交易年份表中的虚拟子账号与主子账号映射表中的管控子账号相关联获取使用单位代码
--            ⑥资金监控交易年份表中的交易流水号与银行对象联动交易记录表中的交易流水号相关联过滤自动联动的虚拟流水
--            ⑦营销-银行资金流水快照表中的利润中心字段与管控营销单位映射表的营销单位代码相关联获取映射到的管控单位信息
--            ⑧营销-银行资金流水快照表中的收款方账号字段与银行及账户基本信息表中的账号字段相关联获取开户单位及账户分类代码信息
--            ⑨银行及账户基本信息表中的账户分类代码与账户分类表中的内部ID相关联获取账户分类及账户账户分类名称
--            ⑩管控与营销的关联条件：中国银行：收款账号+日期+银行交易流水号("_"之前的位数)+金额+方向
--                                 农业银行：收款账号+日期+银行交易流水号(管控：后9位=营销：自第5位向后9位)+金额+方向
--                                 工商银行：收款账号+日期+银行交易流水号++金额+方向
--                                 建设银行：收款账号+日期+银行交易流水号(前19位)+金额+方向
--                                 交通银行：收款账号+日期+银行交易流水号(若营销流水号为24位：管控：自第1位向后16位=营销：自第9位向后16位；若营销流水号为25位：管控：全位数=营销：自第9位向后17位)
--                                 邮政银行：收款账号+日期+银行交易流水号+金额+方向
--    若不满足上方条件的判断收款账号+日期+金额+方向是否重复，若不重复则按照收款账号+日期+金额+方向匹配，若重复则判断收款账号+日期+金额+方向+子账号是否重复，若不重复则按照收款账号+日期+金额+方向+子账号匹配，若重复则判断收款账号+日期+金额+方向+子账号+对方账号是否重复，若不重复则按照收款账号+日期+金额+方向+子账号+对方账号匹，若重复则归集到匹配不上的数据中。
--********************************************************************--
WITH  FIN AS(
SELECT  DXDM    --'账号',
        ,BATITLE    --'账户名称',
        ,ACCBANK    --'开户银行名称',
        ,ACC_TYPE_CODE    --'账号分类'
        ,ACC_TYPE_NAME    --'账号分类名称'
        ,FIN_INST    --'开户金融机构代码'
        ,CAST(YMD AS STRING) AS YMD    --'日期',
        ,CAST(ISPAYOUT AS BIGINT) AS ISPAYOUT    --'收支方向',
        ,CAST(AMOUNT AS DECIMAL(20,6)) AS AMOUNT    --'发生金额',
        ,TRANSFLAG    --'交易标志',
        ,CAST(ENDBALANCE AS DECIMAL(20,6)) AS ENDBALANCE    --'交易后余额',
        ,OTHERBACODE    --'对方账号',
        ,OTHERBTITLE    --'对方银行',
        ,OTHERCTITLE    --'对方单位',
        ,OCCTIME    --'发生时间',
        ,LDESC    --'摘要',
        ,LSH    --'银行流水号',
        ,FINANCEUSE    --'资金用途',
        ,DZNO    --'对账标志',
        ,TRANSID    --'交易流水号',
        ,VIRTBACODE    --'虚拟子账号',
        ,VIRTBANAME    --'虚拟子账号名称',
        ,ERECEIPTBZ    --'电子回单标识',
        ,BZ    --'币种简称',
        ,FY    --'附言'
        ,S_CREATE_TIME    --'创建时间',
        ,HDNO    --'回单编号',
        ,HDPATH    --'回单存放路径',
        ,HDNAME    --'回单文件名',
        ,COLMODE    --'采集方式',
        ,PZNO    --'凭证编号',
        ,SERIALNUM    --'序列号',
        ,SLIPTYPE    --'回单类型',
        ,SIGNATUREVALID    --'验签结果'
        ,COMPID    --'开户单位代码'
        ,USE_ORG_CODE    --'使用单位代码'
        ,OPERFLAG    --'交易标识'
        ,B.IS_FUSION AS FIN_FUSION_FLAG    --'管控融合标识'
FROM    (
            SELECT  B.BANK_ACC AS DXDM    --'账号',
                    ,BATITLE    --'账户名称',
                    ,ACCBANK    --'开户银行名称',
                    ,B.FIN_INST AS FIN_INST    --'开户金融机构代码'
                    ,A.YMD    --'日期',
                    ,ISPAYOUT    --'收支方向',
                    ,AMOUNT    --'发生金额',
                    ,A.TRANSFLAG    --'交易标志',
                    ,ENDBALANCE    --'交易后余额',
                    ,OTHERBACODE    --'对方账号',
                    ,OTHERBTITLE    --'对方银行',
                    ,OTHERCTITLE    --'对方单位',
                    ,OCCTIME    --'发生时间',
                    ,LDESC    --'摘要',
                    ,LSH    --'银行流水号',
                    ,FINANCEUSE    --'资金用途',
                    ,DZNO    --'对账标志',
                    ,A.TRANSID    --'交易流水号',
                    ,VIRTBACODE    --'虚拟子账号',
                    ,VIRTBANAME    --'虚拟子账号名称',
                    ,ERECEIPTBZ    --'电子回单标识',
                    ,BZ    --'币种简称',
                    ,FY    --'附言'
                    ,S_CREATE_TIME    --'创建时间',
                    ,HDNO    --'回单编号',
                    ,HDPATH    --'回单存放路径',
                    ,HDNAME    --'回单文件名',
                    ,COLMODE    --'采集方式',
                    ,PZNO    --'凭证编号',
                    ,SERIALNUM    --'序列号',
                    ,SLIPTYPE    --'回单类型',
                    ,SIGNATUREVALID    --'验签结果'
                    ,COMPID    --'开户单位代码'
                    ,E.USE_ORG_CODE AS USE_ORG_CODE    --'使用单位代码'
                    ,SUBSTR(D.BLOCKCODE,1,8) AS ACC_TYPE_CODE    --'账号分类'
                    ,D.CAP2 AS ACC_TYPE_NAME    --'账号分类名称'
                    ,OPERFLAG    --'交易标识'
            FROM    (
                        SELECT  BAID    --'账号'
                                ,YMD    --'日期',
                                ,ISPAYOUT    --'收支方向',
                                ,AMOUNT    --'发生金额',
                                ,CAST(TRANSFLAG AS STRING) AS TRANSFLAG    --'交易标志',
                                ,ENDBALANCE    --'交易后余额',
                                ,OTHERBACODE    --'对方账号',
                                ,OTHERBTITLE    --'对方银行',
                                ,OTHERCTITLE    --'对方单位',
                                ,TO_DATE(OCCTIME,'yyyy-MM-dd hh:mi:ss') AS OCCTIME    --'发生时间',
                                ,LDESC    --'摘要',
                                ,LSH    --'银行流水号',
                                ,FINANCEUSE    --'资金用途',
                                ,DZNO    --'对账标志',
                                ,TRANSID    --'交易流水号',
                                ,VIRTBACODE    --'虚拟子账号',
                                ,VIRTBANAME    --'虚拟子账号名称',
                                ,ERECEIPTBZ    --'电子回单标识',
                                ,BZ    --'币种简称',
                                ,FY    --'附言'
                        FROM    DATA_ZJ_PROCESS_PROD.DWD_FIN_T_ZJ_BALIST_@@{yyyy}
                        WHERE   DT = MAX_PT('DATA_ZJ_PROCESS_PROD.DWD_FIN_T_ZJ_BALIST_@@{yyyy}')
                    ) AS A    --管控-资金监控交易年份表
            LEFT OUTER JOIN (
                                SELECT  DXID    --'对象内部代码'
                                        ,BANK_ACC    --'账号',
                                        ,BATITLE    --'账户名称',
                                        ,ACCBANK    --'开户银行名称',
                                        ,BID    --'开户金融机构'
                                        ,COMPID    --单位代码
                                        ,ZHFL    --'账户分类代码'
                                        ,FIN_INST    --'金融机构代码'
                                FROM    ODPS_ZJ_DM_FIN_PROD.ADS_FIN_FININFO_DIM_BANK_AND_ACCOUNT
                            ) AS B    --银行及账户基本信息表
            ON      A.BAID = B.DXID
            LEFT OUTER JOIN (
                                SELECT  TO_DATE(S_CREATE_TIME ,'yyyy-MM-dd hh:mi:ss') AS S_CREATE_TIME    --'创建时间',
                                        ,HDNO    --'回单编号',
                                        ,HDPATH    --'回单存放路径',
                                        ,HDNAME    --'回单文件名',
                                        ,CAST(COLMODE AS BIGINT) AS COLMODE    --'回单采集方式',
                                        ,PZNO    --'凭证编号',
                                        ,SERIALNUM    --'序列号',
                                        ,SLIPTYPE    --'回单类型',
                                        ,SIGNATUREVALID    --'验签结果'
                                        ,TRANSID    --'交易流水号'
                                        ,ROW_NUMBER() OVER(PARTITION BY TRANSID ORDER BY HDPATH) AS RN
                                FROM    DATA_ZJ_PROCESS_PROD.DWD_FIN_T_T_ZJJK_COLLECT_HD_@@{yyyy}
                                WHERE   DT = MAX_PT('DATA_ZJ_PROCESS_PROD.DWD_FIN_T_T_ZJJK_COLLECT_HD_@@{yyyy}')
                                AND     TRANSID IS NOT NULL
                            ) AS C    --管控-回单采集信息年份表
            ON      A.TRANSID = C.TRANSID
            AND     RN = 1
            LEFT OUTER JOIN (
                                SELECT  ITEMCODE    --内部ID
                                        ,IID    --名称
                                        ,BLOCKCODE    --块编码
                                        ,CAP2    --二级名称
                                FROM    ODPS_ZJ_DM_FIN_PROD.ADS_FIN_FININFO_DIM_ACCOUNT_TYPE
                            ) D    -- 账户分类表
            ON      B.ZHFL = D.IID
            LEFT OUTER JOIN (
                                SELECT  MASTER_ACCT    --'主账号',
                                        ,MKT_SUB_ACCT    --'营销子账号',
                                        ,CTRL_SUB_ACCT    --'管控子账号',
                                        ,PRFT_CENTER    --'利润中心',
                                        ,USE_ORG_CODE    --'使用单位代码',
                                        ,OPEN_ORG_CODE    --'开户单位代码',
                                FROM    ODPS_ZJ_DM_FIN_PROD.ADS_FIN_FININFO_DIM_MASTER_SUB_MAPPING
                                WHERE   DT = MAX_PT ('ODPS_ZJ_DM_FIN_PROD.ADS_FIN_FININFO_DIM_MASTER_SUB_MAPPING')
                            ) E    --主子账号映射维表
            ON      A.VIRTBACODE = E.CTRL_SUB_ACCT
            LEFT OUTER JOIN (
                                SELECT  BAID    --'账号',
                                        ,CREATEDATE    --'联动交易操作时间',
                                        ,OPERFLAG    --'交易标识 1-联动 2-归集',
                                        ,TRANSID    --'交易流水号',
                                        ,YHDM    --'联动交易操作员',
                                        ,YMD    --'交易日期',
                                        ,LASTTIME    --'最后变更时间',
                                        ,TRANSFLAG    --'交易类型',
                                FROM    DATA_ZJ_PROCESS_PROD.DWD_FIN_T_ZJJK_LINKAGE_BALIST@@{yyyy}
                                WHERE   DT = MAX_PT ('DATA_ZJ_PROCESS_PROD.DWD_FIN_T_ZJJK_LINKAGE_BALIST@@{yyyy}')
                            ) F    --银行对象联动交易记录表
            ON      TRIM(A.TRANSID) = TRIM(F.TRANSID)
        ) A    -- 管控融合表
LEFT OUTER JOIN (
                    SELECT  GID    --'主键'
                            ,ACC_NUM    --'账号'
                            ,ACC_NAME    --'账号名称'
                            ,IS_FUSION    --'是否融合 1-是 0-否'
                    FROM    ODPS_ZJ_DM_FIN_PROD.ADS_FIN_DIM_BANKACCT
                    WHERE   DT = MAX_PT('ODPS_ZJ_DM_FIN_PROD.ADS_FIN_DIM_BANKACCT')
                ) B    --银行账号表 
ON      B.ACC_NUM = A.DXDM
WHERE   OPERFLAG IS NULL 
),MKT as (
SELECT  BANK_CAP_RUN_SNPST_ID    --'银行资金流水快照标识是指银行资金流水快照的唯一标识',
        ,BANK_CAP_RUN_ACCT_CODE    --'银行资金流水编码是指银行资金流水的唯一编码',
        ,OCCUR_FLAG    --'产生标志是指银行资金流水的发生标志，引用国家电网公司营销管理代码类集：发生标志，枚举值，如历史、当日',
        ,OCCUR_FLAG_DESC    --'产生标志描述',
        ,OCCUR_AMT    --'发生额是指银行资金流水发生的金额',
        ,AVAIL_AMT    --'可用金额是指银行资金流水的可用剩余金额',
        ,TRANS_DATE    --'交易日期是指银行资金流水的交易日期，格式为yyyyMMDD',
        ,OPPO_BANK_ACCT    --'对方账号是指银行资金流水流出的银行账号',
        ,OPPO_BANK_NAME    --'对方银行名称是指银行资金流水流出的银行名称',
        ,OPPO_ORG_NAME    --'对方单位名称是指银行资金流水流出的单位名称',
        ,COLL_SISTER_BANK_NO    --'收款方联行号是指收款方所在地区银行的唯一识别标志，由12位组成：3位银行代码+4位城市代码+4位银行编号+1位校验位',
        ,COLL_BANK_ACCT_NO    --'收款方银行账号是指银行资金流水流入的银行账号',
        ,COLL_BANK_ACCT_NAME    --'收款方银行名称是指银行资金流水流入的银行名称',
        ,SUB_ACCT    --'子账号是指通过能源卡交费产生的银行资金流水，用于记录能源卡卡号',
        ,SUB_ACCT_NAME    --'子账号名称是指通过能源卡交费产生的银行资金流水，用于记录能源卡归属的客户名称',
        ,TP_SISTER_BANK_NO    --'勾对联行号'
        ,PRFT_CENTER    --'利润中心'
        ,GEN_TIME    --'生成时间是指催费计划生成的时间',
        ,ATTACH_ID    --'附件标识是指附件的唯一标识',
        ,DATA_SRC    --'采集方式 01-手工录入，02-银行电子回单，03-财务管控，04-网银文本导入，DIGIT-数字货币'
        ,INPUT_STF    --'录入人员'
        ,FILE_NAME    --'存储文件名称数据',
        ,CAP_CATEGE    --'资金类别'
        ,CAP_CATEGE_DESC    --'资金类别描述'
        ,BANK_DESC    --'备注',
        ,UDS_ATTACH_ID    --'存储UDS附件标识数据',
        ,WRITE_TIME    --'数据插入MAXCOMPUTE的系统时间'
        ,USE_ORG_CODE    --'使用单位编码'
        ,OPEN_ORG_CODE    --'开户单位编码'
        ,FIN_INST    --'银行金融机构代码'
        ,ACC_TYPE_CODE    --'账号分类'
        ,ACC_TYPE_NAME    --'账号分类名称'
        ,BATITLE    --'账户名称'
        ,B.IS_FUSION AS MKT_FUSION_FLAG    --'营销融合标记'
FROM    (
            SELECT  CAST(BANK_CAP_RUN_SNPST_ID AS STRING) AS BANK_CAP_RUN_SNPST_ID    --'银行资金流水快照标识是指银行资金流水快照的唯一标识',
                    ,BANK_CAP_RUN_ACCT_CODE    --'银行资金流水编码是指银行资金流水的唯一编码',
                    ,OCCUR_FLAG    --'产生标志是指银行资金流水的发生标志，引用国家电网公司营销管理代码类集：发生标志，枚举值，如历史、当日',
                    ,OCCUR_FLAG_DESC    --'产生标志描述',
                    ,OCCUR_AMT    --'发生额是指银行资金流水发生的金额',
                    ,AVAIL_AMT    --'可用金额是指银行资金流水的可用剩余金额',
                    ,TRANS_DATE    --'交易日期是指银行资金流水的交易日期，格式为yyyyMMDD',
                    ,OPPO_BANK_ACCT    --'对方账号是指银行资金流水流出的银行账号',
                    ,OPPO_BANK_NAME    --'对方银行名称是指银行资金流水流出的银行名称',
                    ,OPPO_ORG_NAME    --'对方单位名称是指银行资金流水流出的单位名称',
                    ,COLL_SISTER_BANK_NO    --'收款方联行号是指收款方所在地区银行的唯一识别标志，由12位组成：3位银行代码+4位城市代码+4位银行编号+1位校验位',
                    ,COLL_BANK_ACCT_NO    --'收款方银行账号是指银行资金流水流入的银行账号',
                    ,COLL_BANK_ACCT_NAME    --'收款方银行名称是指银行资金流水流入的银行名称',
                    ,SUB_ACCT    --'子账号是指通过能源卡交费产生的银行资金流水，用于记录能源卡卡号',
                    ,SUB_ACCT_NAME    --'子账号名称是指通过能源卡交费产生的银行资金流水，用于记录能源卡归属的客户名称',
                    ,TP_SISTER_BANK_NO    --'勾对联行号'
                    ,A.PRFT_CENTER    --'利润中心'
                    ,GEN_TIME    --'接收时间',
                    ,ATTACH_ID    --'附件标识是指附件的唯一标识',
                    ,DATA_SRC    --'采集方式 01-手工录入，02-银行电子回单，03-财务管控，04-网银文本导入，DIGIT-数字货币'
                    ,INPUT_STF    --'录入人员'
                    ,FILE_NAME    --'存储文件名称数据',
                    ,CAP_CATEGE    --'资金类别'
                    ,CAP_CATEGE_DESC    --'资金类别描述'
                    ,BANK_DESC    --'备注',
                    ,UDS_ATTACH_ID    --'存储UDS附件标识数据',
                    ,WRITE_TIME    --'数据插入MAXCOMPUTE的系统时间'
                    ,GK_ORG_CODE AS USE_ORG_CODE    --'使用单位编码'
                    ,C.COMPID AS OPEN_ORG_CODE    --'开户单位编码'
                    ,SUBSTR(TP_SISTER_BANK_NO,1,3) AS FIN_INST    --'银行金融机构代码'
                    ,SUBSTR(D.blockcode,1,8) AS ACC_TYPE_CODE    --'账号分类'
                    ,D.cap2 AS ACC_TYPE_NAME    --'账号分类名称'
                    ,C.BATITLE AS BATITLE    --'账户名称'
            FROM    (
                        SELECT  CAST(BANK_CAP_RUN_SNPST_ID AS STRING) AS BANK_CAP_RUN_SNPST_ID    --'银行资金流水快照标识是指银行资金流水快照的唯一标识',
                                ,BANK_CAP_RUN_ACCT_CODE    --'银行资金流水编码是指银行资金流水的唯一编码',
                                ,OCCUR_FLAG    --'产生标志是指银行资金流水的发生标志，引用国家电网公司营销管理代码类集：发生标志，枚举值，如历史、当日',
                                ,OCCUR_FLAG_DESC    --'产生标志描述',
                                ,OCCUR_AMT    --'发生额是指银行资金流水发生的金额',
                                ,AVAIL_AMT    --'可用金额是指银行资金流水的可用剩余金额',
                                ,TRANS_DATE    --'交易日期是指银行资金流水的交易日期，格式为yyyyMMDD',
                                ,OPPO_BANK_ACCT    --'对方账号是指银行资金流水流出的银行账号',
                                ,OPPO_BANK_NAME    --'对方银行名称是指银行资金流水流出的银行名称',
                                ,OPPO_ORG_NAME    --'对方单位名称是指银行资金流水流出的单位名称',
                                ,COLL_SISTER_BANK_NO    --'收款方联行号是指收款方所在地区银行的唯一识别标志，由12位组成：3位银行代码+4位城市代码+4位银行编号+1位校验位',
                                ,COLL_BANK_ACCT_NO    --'收款方银行账号是指银行资金流水流入的银行账号',
                                ,COLL_BANK_ACCT_NAME    --'收款方银行名称是指银行资金流水流入的银行名称',
                                ,SUB_ACCT    --'子账号是指通过能源卡交费产生的银行资金流水，用于记录能源卡卡号',
                                ,SUB_ACCT_NAME    --'子账号名称是指通过能源卡交费产生的银行资金流水，用于记录能源卡归属的客户名称',
                                ,TP_SISTER_BANK_NO    --'勾对联行号'
                                ,PRFT_CENTER    --'利润中心'
                                ,GEN_TIME    --'接收时间',
                                ,ATTACH_ID    --'附件标识是指附件的唯一标识',
                                ,DATA_SRC    --'存储数据来源数据'
                                ,INPUT_STF    --'录入人员'
                                ,FILE_NAME    --'存储文件名称数据',
                                ,CAP_CATEGE    --'资金类别'
                                ,CAP_CATEGE_DESC    --'资金类别描述'
                                ,BANK_DESC    --'备注',
                                ,UDS_ATTACH_ID    --'存储UDS附件标识数据',
                                ,WRITE_TIME    --'数据插入MAXCOMPUTE的系统时间'
                        FROM    DATA_ZJ_PROCESS_PROD.DWD_CST_BANK_CAP_RUN_SNPST    -- 营销-银行资金流水快照表
                        WHERE   DS = MAX_PT('DATA_ZJ_PROCESS_PROD.DWD_CST_BANK_CAP_RUN_SNPST ')
                        AND     SUBSTR(TRANS_DATE,1,4) = '@@{yyyy}'
                        AND     OCCUR_FLAG = '01'
                        AND     cap_run_acct_stat = '04'
                        AND     SUBSTR(TP_SISTER_BANK_NO,1,3) NOT IN ('402','999')
                    ) AS A    -- 营销-银行资金流水快照表
            LEFT OUTER JOIN (
                                SELECT  GK_ORG_CODE    --'管控单位代码'
                                        ,MKT_ORG_CODE    --'营销单位代码'
                                FROM    ODPS_ZJ_DM_FIN_PROD.ADS_FIN_DIM_UNIT_MAPPING
                                WHERE   DT = MAX_PT('ODPS_ZJ_DM_FIN_PROD.ADS_FIN_DIM_UNIT_MAPPING')
                            ) AS B    --'管控营销单位映射表'
            ON      A.PRFT_CENTER = B.MKT_ORG_CODE
            LEFT OUTER JOIN (
                                SELECT  DXID    --'对象内部代码'
                                        ,BANK_ACC    --'账号',
                                        ,BATITLE    --'账户名称',
                                        ,COMPID    --单位代码
                                        ,ZHFL    --'账户分类代码'
                                FROM    ODPS_ZJ_DM_FIN_PROD.ADS_FIN_FININFO_DIM_BANK_AND_ACCOUNT
                            ) AS C    --管控-银行及账户基本信息表
            ON      C.BANK_ACC = A.COLL_BANK_ACCT_NO
            LEFT OUTER JOIN (
                                SELECT  ITEMCODE    --内部ID
                                        ,IID    --名称
                                        ,BLOCKCODE    --块编码
                                        ,CAP2    --二级名称
                                FROM    ODPS_ZJ_DM_FIN_PROD.ADS_FIN_FININFO_DIM_ACCOUNT_TYPE
                            ) D    -- 管理对象的分类体系表
            ON      C.zhfl = D.IID
        ) AS A    -- 营销-银行资金流水快照表
LEFT OUTER JOIN (
                    SELECT  GID    --'主键'
                            ,ACC_NUM    --'账号'
                            ,ACC_NAME    --'账号名称'
                            ,IS_FUSION    --'是否融合 1-是 0-否'
                    FROM    ODPS_ZJ_DM_FIN_PROD.ADS_FIN_DIM_BANKACCT
                    WHERE   DT = MAX_PT('ODPS_ZJ_DM_FIN_PROD.ADS_FIN_DIM_BANKACCT')
                ) B    --银行账号表
ON      B.ACC_NUM = A.COLL_BANK_ACCT_NO
)
INSERT OVERWRITE TABLE ADS_FIN_FININFO_DWD_FUSION PARTITION(DT='@@{yyyyMMdd}')
SELECT  UUID() AS GID    -- '主键',
    ,CASE    WHEN B.TRANS_DATE IS NULL THEN 2
                 WHEN A.YMD IS NULL THEN 3 
                 ELSE 1 
         END AS DATA_SRC    -- '数据来源'3-营销 2-管控 1-融合
        ,COALESCE(A.DXDM,B.COLL_BANK_ACCT_NO) AS BANK_ACC    -- '账号',
        ,COALESCE(A.BATITLE,B.BATITLE) AS BANK_ACC_TITLE    -- '账户名称',
        ,COALESCE(A.ACC_TYPE_CODE,B.ACC_TYPE_CODE) AS ACC_TYPE_CODE    --'账号分类',
        ,COALESCE(A.ACC_TYPE_NAME,B.ACC_TYPE_NAME) AS ACC_TYPE_NAME    --'账号分类名称',
        ,COALESCE(A.COMPID,B.OPEN_ORG_CODE) AS OPEN_ORG_CODE    --'开户单位编码'
        ,COALESCE(A.USE_ORG_CODE,B.USE_ORG_CODE,A.COMPID) AS USE_ORG_CODE    --'使用单位编码'
        ,COALESCE(A.FIN_INST,SUBSTR(B.tp_sister_bank_no,1,3)) AS FIN_INST    -- '开户金融机构代码',
        ,COALESCE(A.YMD,B.TRANS_DATE) AS TRANS_DATE    -- '交易日期',
        ,COALESCE(A.ISPAYOUT,0) AS ISPAYOUT    -- '收支方向',
        ,COALESCE(A.AMOUNT,B.OCCUR_AMT) AS OCCUR_AMT    -- '发生金额',
        ,COALESCE(A.TRANSFLAG,B.OCCUR_FLAG) AS OCCUR_FLAG    -- '交易标志',
        ,COALESCE(A.ENDBALANCE,B.AVAIL_AMT) AS SURPLUS    -- '交易后余额',
        ,COALESCE(A.OTHERBACODE,OPPO_BANK_ACCT) AS OPPO_BANK_ACCT    -- '对方账号',
        ,COALESCE(A.OTHERBTITLE,OPPO_BANK_NAME) AS OPPO_BANK_NAME    -- '对方银行',
        ,COALESCE(A.OTHERCTITLE,OPPO_ORG_NAME) AS OPPO_UNIT    -- '对方单位',
        ,A.OCCTIME AS OCCUR_TIME    -- '交易发生时间',
        ,A.LDESC    -- '摘要',
        ,A.LSH AS BANK_SERIAL_NUM    -- '银行流水号',
        ,A.FINANCEUSE AS FIN_USE    -- '资金用途',
        ,A.DZNO AS RECO_FLAG    -- '对账标志',
        ,A.TRANSID    -- '交易流水号',
        ,COALESCE(A.VIRTBACODE,B.SUB_ACCT) AS SUB_ACCT    -- '虚拟子账号',
        ,COALESCE(A.VIRTBANAME,B.SUB_ACCT_NAME) AS SUB_ACCT_NAME    -- '虚拟子账号名称',
        ,A.ERECEIPTBZ AS RCPT_IDENT    -- '电子回单标识',
        ,A.BZ AS CURRENCY    -- '币种简称',
        ,A.FY AS POSTSCRIPT    -- '附言',
        ,A.S_CREATE_TIME AS CREATE_TIME    -- '创建时间',
        ,A.HDNO AS RCPT_NO    -- '回单编号',
        ,A.HDPATH AS RCPT_PATH    -- '回单存放路径',
        ,A.HDNAME AS RCPT_NAME    -- '回单文件名',
        ,A.COLMODE    -- '回单采集方式',
        ,A.PZNO AS DOC_NO    -- '凭证编号',
        ,A.SERIALNUM    -- '序列号',
        ,A.SLIPTYPE AS RCPT_TYPE    -- '回单类型',
        ,A.SIGNATUREVALID AS VERIF_RESULT    -- '验签结果',
        ,B.BANK_CAP_RUN_SNPST_ID AS BANK_CAP_FLOW_IDENT    -- '营销银行资金流水快照标识',
        ,B.BANK_CAP_RUN_ACCT_CODE AS BANK_CAP_FLOW_CODE    -- '营销银行资金流水编码',
        ,B.OCCUR_FLAG_DESC    -- '产生标志描述',
        ,B.COLL_SISTER_BANK_NO    -- '收款方联行号',
        ,B.COLL_BANK_ACCT_NAME    -- '收款方银行名称',
        ,B.GEN_TIME    -- '接收时间',
        ,B.ATTACH_ID    -- '附件标识',
        ,B.DATA_SRC AS ACP_METH  ----'采集方式 01-手工录入，02-银行电子回单，03-财务管控，04-网银文本导入，DIGIT-数字货币'
        ,B.INPUT_STF AS INPUT_STF --'录入人员'
        ,B.FILE_NAME    -- '文件名',
        ,B.BANK_DESC    -- '备注',
        ,B.UDS_ATTACH_ID    -- 'UDS附件标识',
        ,B.CAP_CATEGE    -- '资金类别',
        ,B.CAP_CATEGE_DESC    -- '资金类别描述'
        ,COALESCE(A.FIN_FUSION_FLAG,B.MKT_FUSION_FLAG) AS FUSION_FLAG --'融合标志'
        ,CASE    WHEN A.HDPATH IS NOT NULL THEN 1
                 WHEN B.UDS_ATTACH_ID IS NOT NULL THEN 1 
                 ELSE 0 
         END AS RCPT_STATUS    -- '回单挂接状态 1-已挂接 0-未挂接'
        ,GETDATE() AS UPDATE_TIME    -- '更新时间',
FROM    (
            SELECT  A.DXDM    --'账号',
                    ,BATITLE    --'账户名称',
                    ,ACCBANK    --'开户银行名称',
                    ,ACC_TYPE_CODE    --'账号分类'
                    ,ACC_TYPE_NAME    --'账号分类名称'
                    ,FIN_INST    --'开户金融机构代码'
                    ,A.YMD    --'日期',
                    ,A.ISPAYOUT    --'收支方向',
                    ,A.AMOUNT    --'发生金额',
                    ,A.TRANSFLAG    --'交易标志',
                    ,ENDBALANCE    --'交易后余额',
                    ,A.OTHERBACODE    --'对方账号',
                    ,OTHERBTITLE    --'对方银行',
                    ,OTHERCTITLE    --'对方单位',
                    ,OCCTIME    --'发生时间',
                    ,LDESC    --'摘要',
                    ,LSH    --'银行流水号',
                    ,FINANCEUSE    --'资金用途',
                    ,DZNO    --'对账标志',
                    ,TRANSID    --'交易流水号',
                    ,A.VIRTBACODE    --'虚拟子账号',
                    ,VIRTBANAME    --'虚拟子账号名称',
                    ,ERECEIPTBZ    --'电子回单标识',
                    ,BZ    --'币种简称',
                    ,FY    --'附言'
                    ,S_CREATE_TIME    --'创建时间',
                    ,HDNO    --'回单编号',
                    ,HDPATH    --'回单存放路径',
                    ,HDNAME    --'回单文件名',
                    ,COLMODE    --'回单采集方式',
                    ,PZNO    --'凭证编号',
                    ,SERIALNUM    --'序列号',
                    ,SLIPTYPE    --'回单类型',
                    ,SIGNATUREVALID    --'验签结果'
                    ,COMPID    --'单位代码'
                    ,USE_ORG_CODE    --'使用单位代码'
                    ,FIN_FUSION_FLAG  --'融合标志'
                    ,CASE    WHEN D.CNT = 1 THEN 1    --日期+账号+金额+方向 不重复
                             WHEN C.CNT = 1 THEN 2    --日期+账号+金额+方向+子账号 不重复
                             WHEN B.CNT = 1 THEN 3     --日期+账号+金额+方向+子账号+对方账号 不重复
                             ELSE 4     --日期+账号+金额+方向+子账号+对方账号 重复
                     END AS FIN_FLAG
            FROM    (SELECT * FROM FIN WHERE ISPAYOUT= 0) AS A
            LEFT OUTER JOIN (
                                SELECT  COUNT(1) AS CNT
                                        ,YMD
                                        ,DXDM
                                        ,AMOUNT
                                        ,ISPAYOUT
                                        ,VIRTBACODE
                                        ,OTHERBACODE
                                FROM    (SELECT * FROM FIN WHERE ISPAYOUT= 0)
                                GROUP BY YMD
                                         ,DXDM
                                         ,AMOUNT
                                         ,ISPAYOUT
                                         ,VIRTBACODE
                                         ,OTHERBACODE
                            ) AS B    --日期+账号+金额+方向+子账号+对方账号 不重复
            ON      COALESCE(A.YMD,'') = COALESCE(B.YMD,'')
            AND     COALESCE(A.DXDM,'') = COALESCE(B.DXDM,'')
            AND     COALESCE(A.AMOUNT,'') = COALESCE(B.AMOUNT,'')
            AND     COALESCE(A.ISPAYOUT,'') = COALESCE(B.ISPAYOUT,'')
            AND     COALESCE(A.VIRTBACODE,'') = COALESCE(B.VIRTBACODE,'')
            AND     COALESCE(A.OTHERBACODE,'') = COALESCE(B.OTHERBACODE,'')
            LEFT OUTER JOIN (
                                SELECT  COUNT(1) AS CNT
                                        ,YMD
                                        ,DXDM
                                        ,AMOUNT
                                        ,ISPAYOUT
                                        ,VIRTBACODE
                                FROM    (SELECT * FROM FIN WHERE ISPAYOUT= 0)
                                GROUP BY YMD
                                         ,DXDM
                                         ,AMOUNT
                                         ,ISPAYOUT
                                         ,VIRTBACODE
                            ) AS C    --日期+账号+金额+方向+子账号 不重复
            ON      COALESCE(A.YMD,'') = COALESCE(C.YMD,'')
            AND     COALESCE(A.DXDM,'') = COALESCE(C.DXDM,'')
            AND     COALESCE(A.AMOUNT,'') = COALESCE(C.AMOUNT,'')
            AND     COALESCE(A.ISPAYOUT,'') = COALESCE(C.ISPAYOUT,'')
            AND     COALESCE(A.VIRTBACODE,'') = COALESCE(C.VIRTBACODE,'')
            LEFT OUTER JOIN (
                                SELECT  COUNT(1) AS CNT
                                        ,YMD
                                        ,DXDM
                                        ,AMOUNT
                                        ,ISPAYOUT
                                FROM    (SELECT * FROM FIN WHERE ISPAYOUT= 0)
                                GROUP BY YMD
                                         ,DXDM
                                         ,AMOUNT
                                         ,ISPAYOUT
                            ) AS D    --日期+账号+金额+方向 不重复
            ON      COALESCE(A.YMD,'') = COALESCE(D.YMD,'')
            AND     COALESCE(A.DXDM,'') = COALESCE(D.DXDM,'')
            AND     COALESCE(A.AMOUNT,'') = COALESCE(D.AMOUNT,'')
            AND     COALESCE(A.ISPAYOUT,'') = COALESCE(D.ISPAYOUT,'')
        ) AS A    --管控 
FULL OUTER JOIN (
                    SELECT  BANK_CAP_RUN_SNPST_ID    --'银行资金流水快照标识是指银行资金流水快照的唯一标识',
                            ,BANK_CAP_RUN_ACCT_CODE    --'银行资金流水编码是指银行资金流水的唯一编码',
                            ,OCCUR_FLAG    --'产生标志是指银行资金流水的发生标志，引用国家电网公司营销管理代码类集：发生标志，枚举值，如历史、当日',
                            ,OCCUR_FLAG_DESC    --'产生标志描述',
                            ,A.OCCUR_AMT    --'发生额是指银行资金流水发生的金额',
                            ,AVAIL_AMT    --'可用金额是指银行资金流水的可用剩余金额',
                            ,A.TRANS_DATE    --'交易日期是指银行资金流水的交易日期，格式为yyyyMMDD',
                            ,A.OPPO_BANK_ACCT    --'对方账号是指银行资金流水流出的银行账号',
                            ,OPPO_BANK_NAME    --'对方银行名称是指银行资金流水流出的银行名称',
                            ,OPPO_ORG_NAME    --'对方单位名称是指银行资金流水流出的单位名称',
                            ,COLL_SISTER_BANK_NO    --'收款方联行号是指收款方所在地区银行的唯一识别标志，由12位组成：3位银行代码+4位城市代码+4位银行编号+1位校验位',
                            ,A.COLL_BANK_ACCT_NO    --'收款方银行账号是指银行资金流水流入的银行账号',
                            ,COLL_BANK_ACCT_NAME    --'收款方银行名称是指银行资金流水流入的银行名称',
                            ,A.SUB_ACCT    --'子账号是指通过能源卡交费产生的银行资金流水，用于记录能源卡卡号',
                            ,SUB_ACCT_NAME    --'子账号名称是指通过能源卡交费产生的银行资金流水，用于记录能源卡归属的客户名称',
                            ,TP_SISTER_BANK_NO    --'勾对联行号'
                            ,PRFT_CENTER    --'利润中心'
                            ,GEN_TIME    --'生成时间是指催费计划生成的时间',
                            ,ATTACH_ID    --'附件标识是指附件的唯一标识',
                            ,DATA_SRC  ----'采集方式 01-手工录入，02-银行电子回单，03-财务管控，04-网银文本导入，DIGIT-数字货币'
                            ,INPUT_STF  --'录入人员'
                            ,FILE_NAME    --'存储文件名称数据',
                            ,CAP_CATEGE    --'资金类别'
                            ,CAP_CATEGE_DESC    --'资金类别描述'
                            ,BANK_DESC    --'备注',
                            ,UDS_ATTACH_ID    --'存储UDS附件标识数据',
                            ,WRITE_TIME    --'数据插入MAXCOMPUTE的系统时间'
                            ,USE_ORG_CODE    --'使用单位编码'
                            ,OPEN_ORG_CODE    --'开户单位编码'
                            ,FIN_INST    --'银行金融机构代码'
                            ,ACC_TYPE_CODE    --'账号分类'
                            ,ACC_TYPE_NAME    --'账号分类名称'
                            ,BATITLE    --'账户名称',
                            ,MKT_FUSION_FLAG  --'融合标志'
                            ,CASE    WHEN D.CNT = 1 THEN 1    --日期+账号+金额 不重复
                                     WHEN C.CNT = 1 THEN 2    --日期+账号+金额+子账号 不重复
                                     WHEN B.CNT = 1 THEN 3    --日期+账号+金额+子账号+对方账号 不重复
                                     ELSE 4     --日期+账号+金额+子账号+对方账号 重复
                             END AS MKT_FLAG
                    FROM    MKT AS A
                    LEFT OUTER JOIN (
                                        SELECT  COUNT(1) AS CNT
                                                ,TRANS_DATE
                                                ,COLL_BANK_ACCT_NO
                                                ,OCCUR_AMT
                                                ,SUB_ACCT
                                                ,OPPO_BANK_ACCT
                                        FROM    MKT
                                        GROUP BY TRANS_DATE
                                                 ,COLL_BANK_ACCT_NO
                                                 ,OCCUR_AMT
                                                 ,SUB_ACCT
                                                 ,OPPO_BANK_ACCT
                                    ) AS B    --日期+账号+金额+子账号+对方账号 不重复
                    ON      COALESCE(A.TRANS_DATE,'') = COALESCE(B.TRANS_DATE,'')
                    AND     COALESCE(A.COLL_BANK_ACCT_NO,'') = COALESCE(B.COLL_BANK_ACCT_NO,'')
                    AND     COALESCE(A.OCCUR_AMT,'') = COALESCE(B.OCCUR_AMT,'')
                    AND     COALESCE(A.SUB_ACCT,'') = COALESCE(B.SUB_ACCT,'')
                    AND     COALESCE(A.OPPO_BANK_ACCT,'') = COALESCE(B.OPPO_BANK_ACCT,'')
                    LEFT OUTER JOIN (
                                        SELECT  COUNT(1) AS CNT
                                                ,TRANS_DATE
                                                ,COLL_BANK_ACCT_NO
                                                ,OCCUR_AMT
                                                ,SUB_ACCT
                                        FROM    MKT
                                        GROUP BY TRANS_DATE
                                                 ,COLL_BANK_ACCT_NO
                                                 ,OCCUR_AMT
                                                 ,SUB_ACCT
                                    ) AS C    --日期+账号+金额+子账号 不重复
                    ON      COALESCE(A.TRANS_DATE,'') = COALESCE(C.TRANS_DATE,'')
                    AND     COALESCE(A.COLL_BANK_ACCT_NO,'') = COALESCE(C.COLL_BANK_ACCT_NO,'')
                    AND     COALESCE(A.OCCUR_AMT,'') = COALESCE(C.OCCUR_AMT,'')
                    AND     COALESCE(A.SUB_ACCT,'') = COALESCE(C.SUB_ACCT,'')
                    LEFT OUTER JOIN (
                                        SELECT  COUNT(1) AS CNT
                                                ,TRANS_DATE
                                                ,COLL_BANK_ACCT_NO
                                                ,OCCUR_AMT
                                        FROM    MKT
                                        GROUP BY TRANS_DATE
                                                 ,COLL_BANK_ACCT_NO
                                                 ,OCCUR_AMT
                                    ) AS D    --日期+账号+金额 不重复
                    ON      COALESCE(A.TRANS_DATE,'') = COALESCE(D.TRANS_DATE,'')
                    AND     COALESCE(A.COLL_BANK_ACCT_NO,'') = COALESCE(D.COLL_BANK_ACCT_NO,'')
                    AND     COALESCE(A.OCCUR_AMT,'') = COALESCE(D.OCCUR_AMT,'')
                ) AS B    --营销
ON      A.DXDM = B.COLL_BANK_ACCT_NO
AND     A.YMD = B.TRANS_DATE
AND     ROUND(A.AMOUNT,2) = ROUND(B.OCCUR_AMT,2)
AND     CASE WHEN A.FIN_INST = '104' THEN CASE WHEN SUBSTR(A.LSH,1,if(LENGTH(B.bank_cap_run_acct_code)<>16,INSTR(B.BANK_CAP_RUN_ACCT_CODE,'-')-1,LENGTH(A.lsh))) = SUBSTR(B.BANK_CAP_RUN_ACCT_CODE,1,if(LENGTH(B.bank_cap_run_acct_code)<>16,INSTR(B.BANK_CAP_RUN_ACCT_CODE,'-')-1,LENGTH(B.BANK_CAP_RUN_ACCT_CODE)))  THEN TRUE
											   WHEN MKT_FLAG = 3 AND FIN_FLAG = 3 THEN  A.OTHERBACODE = B.OPPO_BANK_ACCT AND A.VIRTBACODE = B.SUB_ACCT
											   WHEN MKT_FLAG = 2 AND FIN_FLAG = 2 THEN  A.VIRTBACODE = B.SUB_ACCT
											   WHEN MKT_FLAG = 1 AND FIN_FLAG = 1 THEN  TRUE
                                          ELSE FALSE 
										  END											  
             WHEN A.FIN_INST = '103' THEN CASE WHEN SUBSTR(A.LSH,-9,9) = SUBSTR(B.BANK_CAP_RUN_ACCT_CODE,5,9) THEN TRUE 
											   WHEN MKT_FLAG = 3 AND FIN_FLAG = 3 THEN  A.OTHERBACODE = B.OPPO_BANK_ACCT AND A.VIRTBACODE = SUBSTR(B.SUB_ACCT,-10,10)
											   WHEN MKT_FLAG = 2 AND FIN_FLAG = 2 THEN  A.VIRTBACODE = SUBSTR(B.SUB_ACCT,-10,10)
											   WHEN MKT_FLAG = 1 AND FIN_FLAG = 1 THEN  TRUE
                                          ELSE FALSE 
										  END	
             WHEN A.FIN_INST = '102' THEN CASE WHEN A.LSH = B.BANK_CAP_RUN_ACCT_CODE THEN TRUE
											   WHEN MKT_FLAG = 3 AND FIN_FLAG = 3 THEN  A.OTHERBACODE = B.OPPO_BANK_ACCT AND A.VIRTBACODE = B.SUB_ACCT
											   WHEN MKT_FLAG = 2 AND FIN_FLAG = 2 THEN  A.VIRTBACODE = B.SUB_ACCT
											   WHEN MKT_FLAG = 1 AND FIN_FLAG = 1 THEN  TRUE
                                          ELSE FALSE 
										  END
             WHEN A.FIN_INST = '105' THEN CASE WHEN SUBSTR(A.LSH,1,19) = SUBSTR(B.BANK_CAP_RUN_ACCT_CODE,1,19) THEN TRUE
											   WHEN MKT_FLAG = 3 AND FIN_FLAG = 3 THEN  A.OTHERBACODE = B.OPPO_BANK_ACCT AND SUBSTR(A.VIRTBACODE,-4,4) = SUBSTR(B.SUB_ACCT,-4,4)
											   WHEN MKT_FLAG = 2 AND FIN_FLAG = 2 THEN  SUBSTR(A.VIRTBACODE,-4,4) = SUBSTR(B.SUB_ACCT,-4,4)
											   WHEN MKT_FLAG = 1 AND FIN_FLAG = 1 THEN  TRUE
										  END	  
             WHEN A.FIN_INST = '301' THEN CASE WHEN LENGTH(B.BANK_CAP_RUN_ACCT_CODE) IN ('24') AND  SUBSTR(A.LSH,1,16) = SUBSTR(B.BANK_CAP_RUN_ACCT_CODE,9,16) THEN TRUE
											   WHEN A.LSH = SUBSTR(B.BANK_CAP_RUN_ACCT_CODE,9,17) THEN TRUE
											   WHEN MKT_FLAG = 3 AND FIN_FLAG = 3 THEN  A.OTHERBACODE = B.OPPO_BANK_ACCT AND A.VIRTBACODE = B.SUB_ACCT
											   WHEN MKT_FLAG = 2 AND FIN_FLAG = 2 THEN  A.VIRTBACODE = B.SUB_ACCT
											   WHEN MKT_FLAG = 1 AND FIN_FLAG = 1 THEN  TRUE
                                          ELSE FALSE 
										  END											  
             WHEN A.FIN_INST = '403' THEN CASE WHEN A.LSH = B.BANK_CAP_RUN_ACCT_CODE THEN  TRUE
											  WHEN MKT_FLAG = 3 AND FIN_FLAG = 3  THEN  A.OTHERBACODE = B.OPPO_BANK_ACCT AND A.VIRTBACODE = B.SUB_ACCT
											  WHEN MKT_FLAG = 2 AND FIN_FLAG = 2  THEN  A.VIRTBACODE = B.SUB_ACCT
											  WHEN MKT_FLAG = 1 AND FIN_FLAG = 1  THEN  TRUE
                                          ELSE FALSE 
										  END		
         ELSE FALSE 
         END 
UNION ALL 
SELECT  UUID() AS GID    -- '主键',
    ,2 AS DATA_SRC    -- '数据来源'3-营销 2-管控 1-融合
        ,A.DXDM AS BANK_ACC    -- '账号',
        ,A.BATITLE AS BANK_ACC_TITLE    -- '账户名称',
        ,A.ACC_TYPE_CODE AS ACC_TYPE_CODE    --'账号分类',
        ,A.ACC_TYPE_NAME AS ACC_TYPE_NAME    --'账号分类名称',
        ,A.COMPID AS OPEN_ORG_CODE    --'开户单位编码'
        ,A.USE_ORG_CODE AS USE_ORG_CODE    --'使用单位编码'
        ,A.FIN_INST AS FIN_INST    -- '开户金融机构代码',
        ,A.YMD AS TRANS_DATE    -- '交易日期',
        ,A.ISPAYOUT AS ISPAYOUT    -- '收支方向',
        ,A.AMOUNT AS OCCUR_AMT    -- '发生金额',
        ,A.TRANSFLAG AS OCCUR_FLAG    -- '交易标志',
        ,A.ENDBALANCE AS SURPLUS    -- '交易后余额',
        ,A.OTHERBACODE AS OPPO_BANK_ACCT    -- '对方账号',
        ,A.OTHERBTITLE AS OPPO_BANK_NAME    -- '对方银行',
        ,A.OTHERCTITLE AS OPPO_UNIT    -- '对方单位',
        ,A.OCCTIME AS OCCUR_TIME    -- '交易发生时间',
        ,A.LDESC    -- '摘要',
        ,A.LSH AS BANK_SERIAL_NUM    -- '银行流水号',
        ,A.FINANCEUSE AS FIN_USE    -- '资金用途',
        ,A.DZNO AS RECO_FLAG    -- '对账标志',
        ,A.TRANSID    -- '交易流水号',
        ,A.VIRTBACODE AS SUB_ACCT    -- '虚拟子账号',
        ,A.VIRTBANAME AS SUB_ACCT_NAME    -- '虚拟子账号名称',
        ,A.ERECEIPTBZ AS RCPT_IDENT    -- '电子回单标识',
        ,A.BZ AS CURRENCY    -- '币种简称',
        ,A.FY AS POSTSCRIPT    -- '附言',
        ,A.S_CREATE_TIME AS CREATE_TIME    -- '创建时间',
        ,A.HDNO AS RCPT_NO    -- '回单编号',
        ,A.HDPATH AS RCPT_PATH    -- '回单存放路径',
        ,A.HDNAME AS RCPT_NAME    -- '回单文件名',
        ,A.COLMODE    -- '回单采集方式',
        ,A.PZNO AS DOC_NO    -- '凭证编号',
        ,A.SERIALNUM    -- '序列号',
        ,A.SLIPTYPE AS RCPT_TYPE    -- '回单类型',
        ,A.SIGNATUREVALID AS VERIF_RESULT    -- '验签结果',
        ,NULL AS BANK_CAP_FLOW_IDENT    -- '营销银行资金流水快照标识',
        ,NULL AS BANK_CAP_FLOW_CODE    -- '营销银行资金流水编码',
        ,NULL AS OCCUR_FLAG_DESC    -- '产生标志描述',
        ,NULL AS COLL_SISTER_BANK_NO    -- '收款方联行号',
        ,NULL AS COLL_BANK_ACCT_NAME    -- '收款方银行名称',
        ,NULL AS GEN_TIME    -- '接收时间',
        ,NULL AS ATTACH_ID    -- '附件标识',
        ,NULL AS ACP_METH  ----'采集方式 01-手工录入，02-银行电子回单，03-财务管控，04-网银文本导入，DIGIT-数字货币'
        ,NULL AS INPUT_STF --'录入人员'
        ,NULL AS FILE_NAME    -- '文件名',
        ,NULL AS BANK_DESC    -- '备注',
        ,NULL AS UDS_ATTACH_ID    -- 'UDS附件标识',
        ,NULL AS CAP_CATEGE    -- '资金类别',
        ,NULL AS CAP_CATEGE_DESC    -- '资金类别描述'
        ,A.FIN_FUSION_FLAG AS FUSION_FLAG --'融合标志'
        ,CASE    WHEN A.HDPATH IS NOT NULL THEN 1
                 ELSE 0 
         END AS RCPT_STATUS    -- '回单挂接状态 1-已挂接 0-未挂接'
        ,GETDATE() AS UPDATE_TIME    -- '更新时间',
FROM (SELECT * FROM FIN WHERE ISPAYOUT = 1) AS A