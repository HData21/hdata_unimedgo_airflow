query_estabelecimento = 'SELECT CD_EMPRESA, CD_ESTABELECIMENTO, NM_FANTASIA_ESTAB FROM TASY.VW_HDATA_ESTABELECIMENTO'

query_empresa = 'SELECT CD_EMPRESA, NM_RAZAO_SOCIAL, DS_NOME_CURTO FROM TASY.VW_HDATA_EMPRESA'

query_ped_ex_ext_item = 'SELECT NR_PROC_INTERNO, NR_SEQ_PEDIDO FROM TASY.VW_HDATA_PED_EX_EXT_IT'

query_ped_ex_ext = "SELECT NR_SEQUENCIA, PEE.NR_ATENDIMENTO, DT_INATIVACAO FROM TASY.VW_HDATA_PED_EX_EXT PEE INNER JOIN TASY.VW_HDATA_ATENDIMENTO_PACIENTE AP ON AP.NR_ATENDIMENTO = PEE.NR_ATENDIMENTO WHERE AP.DT_ENTRADA >= TO_DATE('{data_ini}', 'DD/MM/YYYY') AND AP.DT_ENTRADA < TO_DATE('{data_fim}', 'DD/MM/YYYY') + INTERVAL '1' DAY"

query_exame_lab = 'SELECT NR_SEQ_EXAME, NR_SEQ_GRUPO, NM_EXAME, DS_UNIDADE_MEDIDA FROM TASY.VW_HDATA_EXAME_LABORATORIO'

query_prescr_procedimento = "SELECT PP.IE_STATUS_ATEND, PP.NR_SEQ_EXAME, PP.NR_PRESCRICAO, PP.CD_PROCEDIMENTO, P.DS_PROCEDIMENTO, P.CD_TIPO_PROCEDIMENTO, PP.IE_ORIGEM_PROCED, PP.NR_SEQUENCIA, PP.IE_VIA_APLICACAO, PP.DS_HORARIOS, PP.DS_JUSTIFICATIVA, AP.CD_ESTABELECIMENTO FROM TASY.PRESCR_PROCEDIMENTO PP INNER JOIN TASY.PRESCR_MEDICA PM ON PM.NR_PRESCRICAO = PP.NR_PRESCRICAO INNER JOIN TASY.PROCEDIMENTO P ON PP.CD_PROCEDIMENTO = P.CD_PROCEDIMENTO AND PP.IE_ORIGEM_PROCED = P.IE_ORIGEM_PROCED INNER JOIN TASY.VW_HDATA_ATENDIMENTO_PACIENTE AP ON PM.NR_ATENDIMENTO = AP.NR_ATENDIMENTO WHERE AP.DT_ENTRADA >= TO_DATE('{data_ini}', 'DD/MM/YYYY') AND AP.DT_ENTRADA < TO_DATE('{data_fim}', 'DD/MM/YYYY') + INTERVAL '1' DAY"

query_prescr_medica_v = "SELECT DISTINCT PM.NR_ATENDIMENTO, PM.NR_PRESCRICAO, PM.DT_PRESCRICAO, PM.CD_MEDICO FROM TASY.VW_HDATA_PRESCR_MEDICA_V PM INNER JOIN TASY.VW_HDATA_ATENDIMENTO_PACIENTE AP ON PM.NR_ATENDIMENTO = AP.NR_ATENDIMENTO WHERE AP.DT_ENTRADA >= TO_DATE('{data_ini}', 'DD/MM/YYYY') AND AP.DT_ENTRADA < TO_DATE('{data_fim}', 'DD/MM/YYYY') + INTERVAL '1' DAY"

query_diagnostico_doenca = "SELECT CD_DOENCA, DC.NR_ATENDIMENTO, NR_SEQ_INTERNO FROM TASY.VW_HDATA_DIAGNOSTICO_DOENCA DC INNER JOIN TASY.VW_HDATA_ATENDIMENTO_PACIENTE AP ON AP.NR_ATENDIMENTO = DC.NR_ATENDIMENTO WHERE AP.DT_ENTRADA >= TO_DATE('{data_ini}', 'DD/MM/YYYY') AND AP.DT_ENTRADA < TO_DATE('{data_fim}', 'DD/MM/YYYY') + INTERVAL '1' DAY"

query_atendimento_paciente = "SELECT VAP.NR_ATENDIMENTO, VAP.NR_ATENDIMENTO_MAE, VAP.DT_ENTRADA, VAP.DT_INICIO_ATENDIMENTO, VAP.DT_ATEND_MEDICO, VAP.DT_ALTA, VAP.DT_ALTA_MEDICO, VAP.DT_FIM_TRIAGEM, VAP.NR_SEQ_TRIAGEM, VAP.DT_MEDICACAO, VAP.CD_MOTIVO_ALTA, VAP.CD_MOTIVO_ALTA_MEDICA, VAP.IE_TIPO_ATENDIMENTO, VAP.CD_PESSOA_FISICA, VAP.CD_MEDICO_RESP, VAP.NR_SEQ_PAC_SENHA_FILA, VAP.IE_CLINICA, VAP.CD_ESTABELECIMENTO, VAP.DS_SENHA_QMATIC, AP.NR_SEQ_CLASSIFICACAO FROM TASY.VW_HDATA_ATENDIMENTO_PACIENTE VAP INNER JOIN ATENDIMENTO_PACIENTE AP ON VAP.NR_ATENDIMENTO = AP.NR_ATENDIMENTO WHERE DT_ENTRADA >= TO_DATE('{data_ini}', 'DD/MM/YYYY') AND DT_ENTRADA < TO_DATE('{data_fim}', 'DD/MM/YYYY') + INTERVAL '1' DAY"

query_atend_paciente_unidade = "SELECT NR_ATENDIMENTO, CD_SETOR_ATENDIMENTO, NR_SEQ_INTERNO, DT_ENTRADA_UNIDADE, DT_SAIDA_UNIDADE FROM TASY.VW_HDATA_ATEND_PAC_UNID WHERE DT_ENTRADA_UNIDADE >= TO_DATE('{data_ini}', 'DD/MM/YYYY') AND DT_ENTRADA_UNIDADE < TO_DATE('{data_fim}', 'DD/MM/YYYY') + INTERVAL '1' DAY"

query_setor_atendimento = 'SELECT CD_SETOR_ATENDIMENTO, DS_SETOR_ATENDIMENTO, CD_CLASSIF_SETOR FROM TASY.VW_HDATA_SETOR_ATENDIMENTO'

query_atend_categoria_convenio = "SELECT ACC.NR_ATENDIMENTO, ACC.CD_CONVENIO, NR_SEQ_INTERNO, CD_CATEGORIA, DT_INICIO_VIGENCIA FROM TASY.VW_HDATA_ATEND_CATEG_CONVENIO ACC INNER JOIN TASY.VW_HDATA_ATENDIMENTO_PACIENTE AP ON AP.NR_ATENDIMENTO = ACC.NR_ATENDIMENTO WHERE AP.DT_ENTRADA >= TO_DATE('{data_ini}', 'DD/MM/YYYY') AND AP.DT_ENTRADA < TO_DATE('{data_fim}', 'DD/MM/YYYY') + INTERVAL '1' DAY"

query_convenio = 'SELECT DISTINCT CD_CONVENIO, DS_CONVENIO FROM TASY.VW_HDATA_CONVENIO'

query_categoria_convenio = 'SELECT DISTINCT CD_CONVENIO, CD_CATEGORIA FROM TASY.VW_HDATA_CATEGORIA_CONVENIO'

query_pessoa_fisica_medico = "SELECT DISTINCT CD_PESSOA_FISICA, IE_SEXO, DT_CADASTRO_ORIGINAL, NM_PESSOA_PESQUISA FROM TASY.VW_HDATA_PESSOA_FISICA_MEDICO"

query_pessoa_fisica_pac = "SELECT DISTINCT CD_PESSOA_FISICA, DT_NASCIMENTO, IE_SEXO, DT_CADASTRO_ORIGINAL FROM TASY.VW_HDATA_PESSOA_FISICA_PAC"

query_pac_senha_fila = "SELECT DT_INICIO_ATENDIMENTO, DT_GERACAO_SENHA, DT_FIM_ATENDIMENTO, NR_SEQ_FILA_SENHA, NR_SEQUENCIA FROM TASY.VW_HDATA_PAC_SENHA_FILA WHERE DT_GERACAO_SENHA >= TO_DATE('{data_ini}', 'DD/MM/YYYY') AND DT_GERACAO_SENHA < TO_DATE('{data_fim}', 'DD/MM/YYYY') + INTERVAL '1' DAY"

query_motivo_alta = 'SELECT CD_MOTIVO_ALTA, DS_MOTIVO_ALTA FROM TASY.VW_HDATA_MOTIVO_ALTA'

query_valor_dominio = 'SELECT CD_DOMINIO, VL_DOMINIO, DS_VALOR_DOMINIO FROM TASY.VW_HDATA_VALOR_DOMINIO'

query_cid_doenca = 'SELECT CD_DOENCA_CID, DS_DOENCA_CID FROM TASY.VW_HDATA_CID_DOENCA'

query_triagem_classif_risco = 'SELECT NR_SEQUENCIA, DS_CLASSIFICACAO FROM TASY.VW_HDATA_TRIAG_CLASSIF_RISCO'

query_medico_especialidade = 'SELECT CD_PESSOA_FISICA, CD_ESPECIALIDADE FROM TASY.VW_HDATA_MEDICO_ESPECIALIDADE'

query_especialidade_medica = 'SELECT CD_ESPECIALIDADE, DS_ESPECIALIDADE FROM TASY.VW_HDATA_ESPECIALIDADE_MEDICA'

query_prescr_material = "SELECT DISTINCT PMAT.NR_PRESCRICAO, PMAT.NR_SEQUENCIA, PMAT.IE_VIA_APLICACAO, PMAT.DS_HORARIOS, PMAT.DS_JUSTIFICATIVA, PMAT.CD_MATERIAL, PMAT.IE_ORIGEM_INF FROM TASY.PRESCR_MATERIAL PMAT INNER JOIN TASY.PRESCR_MEDICA_V PM ON PM.NR_PRESCRICAO = PMAT.NR_PRESCRICAO INNER JOIN TASY.ATENDIMENTO_PACIENTE AP ON PM.NR_ATENDIMENTO = AP.NR_ATENDIMENTO INNER JOIN TASY.ESTABELECIMENTO ON ESTABELECIMENTO.CD_ESTABELECIMENTO = AP.CD_ESTABELECIMENTO INNER JOIN TASY.EMPRESA ON EMPRESA.CD_EMPRESA = ESTABELECIMENTO.CD_EMPRESA WHERE EMPRESA.CD_EMPRESA = 1 AND ESTABELECIMENTO.CD_ESTABELECIMENTO IN (1, 34) AND AP.IE_TIPO_ATENDIMENTO = 3 AND AP.DT_ENTRADA >= TO_DATE('{data_ini}', 'DD/MM/YYYY') AND AP.DT_ENTRADA < TO_DATE('{data_fim}', 'DD/MM/YYYY') + INTERVAL '1' DAY"

query_material = "SELECT M.CD_MATERIAL, M.DS_MATERIAL, GM.CD_GRUPO_MATERIAL, VA.DS_VIA_APLICACAO FROM TASY.MATERIAL M INNER JOIN TASY.CLASSE_MATERIAL CM ON M.CD_CLASSE_MATERIAL = CM.CD_CLASSE_MATERIAL INNER JOIN TASY.SUBGRUPO_MATERIAL SM ON CM.CD_SUBGRUPO_MATERIAL = SM.CD_SUBGRUPO_MATERIAL INNER JOIN TASY.GRUPO_MATERIAL GM ON SM.CD_GRUPO_MATERIAL = GM.CD_GRUPO_MATERIAL LEFT JOIN TASY.VIA_APLICACAO VA ON M.IE_VIA_APLICACAO = VA.IE_VIA_APLICACAO"

query_prescr_recomendacao = "SELECT DISTINCT PR.NR_PRESCRICAO, PR.NR_SEQUENCIA, PR.CD_RECOMENDACAO, TR.DS_TIPO_RECOMENDACAO, PR.DS_HORARIOS FROM TASY.PRESCR_RECOMENDACAO PR INNER JOIN TASY.PRESCR_MEDICA_V PM ON PM.NR_PRESCRICAO = PR.NR_PRESCRICAO INNER JOIN TASY.VW_HDATA_ATENDIMENTO_PACIENTE AP ON PM.NR_ATENDIMENTO = AP.NR_ATENDIMENTO LEFT JOIN TASY.TIPO_RECOMENDACAO TR ON PR.CD_RECOMENDACAO = TR.CD_TIPO_RECOMENDACAO WHERE AP.DT_ENTRADA >= TO_DATE('{data_ini}', 'DD/MM/YYYY') AND AP.DT_ENTRADA < TO_DATE('{data_fim}', 'DD/MM/YYYY') + INTERVAL '1' DAY"