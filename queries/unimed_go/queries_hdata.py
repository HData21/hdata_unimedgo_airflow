query_estabelecimento_hdata = 'SELECT CD_EMPRESA, CD_ESTABELECIMENTO, NM_FANTASIA_ESTAB FROM UNIMED_GYN.ESTABELECIMENTO'

query_empresa_hdata = 'SELECT CD_EMPRESA, NM_RAZAO_SOCIAL, DS_NOME_CURTO FROM UNIMED_GYN.EMPRESA'

query_ped_ex_ext_item_hdata = 'SELECT NR_PROC_INTERNO, NR_SEQ_PEDIDO FROM UNIMED_GYN.PED_EX_EXT_IT'

query_ped_ex_ext_hdata = "SELECT NR_SEQUENCIA, PEE.NR_ATENDIMENTO, DT_INATIVACAO FROM UNIMED_GYN.PED_EX_EXT PEE INNER JOIN UNIMED_GYN.ATENDIMENTO_PACIENTE AP ON AP.NR_ATENDIMENTO = DC.NR_ATENDIMENTO WHERE AP.DT_ENTRADA >= TO_DATE('{data_ini}', 'DD/MM/YYYY') AND AP.DT_ENTRADA < TO_DATE('{data_fim}', 'DD/MM/YYYY') + INTERVAL '1' DAY"

query_exame_lab_hdata = 'SELECT NR_SEQ_EXAME, NR_SEQ_GRUPO, NM_EXAME, DS_UNIDADE_MEDIDA FROM UNIMED_GYN.EXAME_LABORATORIO'

query_prescr_procedimento_hdata = "SELECT PP.IE_STATUS_ATEND, PP.NR_SEQ_EXAME, PP.NR_PRESCRICAO, PP.CD_PROCEDIMENTO, PP.IE_ORIGEM_PROCED, PP.NR_SEQUENCIA, PP.IE_VIA_APLICACAO, PP.DS_HORARIOS, PP.DS_JUSTIFICATIVA, AP.CD_ESTABELECIMENTO FROM UNIMED_GYN.PRESCR_PROCEDIMENTO PP INNER JOIN UNIMED_GYN.PRESCR_MEDICA_V PM ON PM.NR_PRESCRICAO = PP.NR_PRESCRICAO INNER JOIN UNIMED_GYN.ATENDIMENTO_PACIENTE AP ON PM.NR_ATENDIMENTO = AP.NR_ATENDIMENTO WHERE AP.DT_ENTRADA >= TO_DATE('{data_ini}', 'DD/MM/YYYY') AND AP.DT_ENTRADA < TO_DATE('{data_fim}', 'DD/MM/YYYY') + INTERVAL '1' DAY"

query_prescr_medica_v_hdata = "SELECT DISTINCT PM.NR_ATENDIMENTO, PM.NR_PRESCRICAO, PM.DT_PRESCRICAO, PM.CD_MEDICO FROM UNIMED_GYN.PRESCR_MEDICA_V PM INNER JOIN UNIMED_GYN.ATENDIMENTO_PACIENTE AP ON PM.NR_ATENDIMENTO = AP.NR_ATENDIMENTO WHERE AP.DT_ENTRADA >= TO_DATE('{data_ini}', 'DD/MM/YYYY') AND AP.DT_ENTRADA < TO_DATE('{data_fim}', 'DD/MM/YYYY') + INTERVAL '1' DAY"

query_diagnostico_doenca_hdata = "SELECT CD_DOENCA, DC.NR_ATENDIMENTO, NR_SEQ_INTERNO FROM UNIMED_GYN.DIAGNOSTICO_DOENCA DC INNER JOIN UNIMED_GYN.ATENDIMENTO_PACIENTE AP ON AP.NR_ATENDIMENTO = DC.NR_ATENDIMENTO WHERE AP.DT_ENTRADA >= TO_DATE('{data_ini}', 'DD/MM/YYYY') AND AP.DT_ENTRADA < TO_DATE('{data_fim}', 'DD/MM/YYYY') + INTERVAL '1' DAY"

query_atendimento_paciente_hdata = "SELECT NR_ATENDIMENTO, NR_ATENDIMENTO_MAE, DT_ENTRADA, DT_INICIO_ATENDIMENTO, DT_ATEND_MEDICO, DT_ALTA, DT_ALTA_MEDICO, DT_FIM_TRIAGEM, NR_SEQ_TRIAGEM, DT_MEDICACAO, CD_MOTIVO_ALTA, CD_MOTIVO_ALTA_MEDICA, IE_TIPO_ATENDIMENTO, CD_PESSOA_FISICA, CD_MEDICO_RESP, NR_SEQ_PAC_SENHA_FILA, IE_CLINICA FROM UNIMED_GYN.ATENDIMENTO_PACIENTE WHERE DT_ENTRADA >= TO_DATE('{data_ini}', 'DD/MM/YYYY') AND DT_ENTRADA < TO_DATE('{data_fim}', 'DD/MM/YYYY') + INTERVAL '1' DAY"

query_atend_paciente_unidade_hdata = "SELECT NR_ATENDIMENTO, CD_SETOR_ATENDIMENTO, NR_SEQ_INTERNO, DT_ENTRADA_UNIDADE, DT_SAIDA_UNIDADE FROM UNIMED_GYN.ATEND_PAC_UNID WHERE DT_ENTRADA_UNIDADE >= TO_DATE('{data_ini}', 'DD/MM/YYYY') AND DT_ENTRADA_UNIDADE < TO_DATE('{data_fim}', 'DD/MM/YYYY') + INTERVAL '1' DAY"

query_setor_atendimento_hdata = 'SELECT CD_SETOR_ATENDIMENTO, DS_SETOR_ATENDIMENTO, CD_CLASSIF_SETOR FROM UNIMED_GYN.SETOR_ATENDIMENTO'

query_atend_categoria_convenio_hdata = "SELECT ACC.NR_ATENDIMENTO, ACC.CD_CONVENIO, NR_SEQ_INTERNO, CD_CATEGORIA, DT_INICIO_VIGENCIA FROM UNIMED_GYN.ATEND_CATEG_CONVENIO ACC INNER JOIN UNIMED_GYN.ATENDIMENTO_PACIENTE AP ON AP.NR_ATENDIMENTO = ACC.NR_ATENDIMENTO WHERE AP.DT_ENTRADA >= TO_DATE('{data_ini}', 'DD/MM/YYYY') AND AP.DT_ENTRADA < TO_DATE('{data_fim}', 'DD/MM/YYYY') + INTERVAL '1' DAY"

query_convenio_hdata = 'SELECT DISTINCT CD_CONVENIO, DS_CONVENIO FROM UNIMED_GYN.CONVENIO'

query_categoria_convenio_hdata = 'SELECT DISTINCT CD_CONVENIO, CD_CATEGORIA FROM UNIMED_GYN.CATEGORIA_CONVENIO'

query_pessoa_fisica_medico_hdata = "SELECT DISTINCT CD_PESSOA_FISICA, IE_SEXO, DT_CADASTRO_ORIGINAL, NM_PESSOA_PESQUISA FROM UNIMED_GYN.PESSOA_FISICA_MEDICO"

query_pessoa_fisica_pac_hdata = "SELECT DISTINCT CD_PESSOA_FISICA, DT_NASCIMENTO, IE_SEXO, DT_CADASTRO_ORIGINAL FROM UNIMED_GYN.PESSOA_FISICA_PAC"

query_pac_senha_fila_hdata = "SELECT DT_INICIO_ATENDIMENTO, DT_GERACAO_SENHA, DT_FIM_ATENDIMENTO, NR_SEQ_FILA_SENHA, NR_SEQUENCIA FROM UNIMED_GYN.PAC_SENHA_FILA WHERE DT_GERACAO_SENHA >= TO_DATE('{data_ini}', 'DD/MM/YYYY') AND DT_GERACAO_SENHA < TO_DATE('{data_fim}', 'DD/MM/YYYY') + INTERVAL '1' DAY"

query_motivo_alta_hdata = 'SELECT CD_MOTIVO_ALTA, DS_MOTIVO_ALTA FROM UNIMED_GYN.MOTIVO_ALTA'

query_valor_dominio_hdata = 'SELECT CD_DOMINIO, VL_DOMINIO, DS_VALOR_DOMINIO FROM UNIMED_GYN.VALOR_DOMINIO'

query_cid_doenca_hdata = 'SELECT CD_DOENCA_CID, DS_DOENCA_CID FROM UNIMED_GYN.CID_DOENCA'

query_triagem_classif_risco_hdata = 'SELECT NR_SEQUENCIA, DS_CLASSIFICACAO FROM UNIMED_GYN.TRIAG_CLASSIF_RISCO'

query_medico_especialidade_hdata = 'SELECT CD_PESSOA_FISICA, CD_ESPECIALIDADE FROM UNIMED_GYN.MEDICO_ESPECIALIDADE'

query_especialidade_medica_hdata = 'SELECT CD_ESPECIALIDADE, DS_ESPECIALIDADE FROM UNIMED_GYN.ESPECIALIDADE_MEDICA'

query_prescr_material_hdata = "SELECT DISTINCT PMAT.NR_PRESCRICAO, PMAT.NR_SEQUENCIA, PMAT.IE_VIA_APLICACAO, PMAT.DS_HORARIOS, PMAT.DS_JUSTIFICATIVA, PMAT.CD_MATERIAL, PMAT.IE_ORIGEM_INF FROM UNIMED_GYN.PRESCR_MATERIAL PMAT INNER JOIN UNIMED_GYN.PRESCR_MEDICA_V PM ON PM.NR_PRESCRICAO = PMAT.NR_PRESCRICAO INNER JOIN UNIMED_GYN.ATENDIMENTO_PACIENTE AP ON PM.NR_ATENDIMENTO = AP.NR_ATENDIMENTO INNER JOIN UNIMED_GYN.ESTABELECIMENTO ON ESTABELECIMENTO.CD_ESTABELECIMENTO = AP.CD_ESTABELECIMENTO INNER JOIN UNIMED_GYN.EMPRESA ON EMPRESA.CD_EMPRESA = ESTABELECIMENTO.CD_EMPRESA WHERE EMPRESA.CD_EMPRESA = 1 AND ESTABELECIMENTO.CD_ESTABELECIMENTO IN (1, 34) AND AP.IE_TIPO_ATENDIMENTO = 3 AND AP.DT_ENTRADA >= TO_DATE('{data_ini}', 'DD/MM/YYYY') AND AP.DT_ENTRADA < TO_DATE('{data_fim}', 'DD/MM/YYYY') + INTERVAL '1' DAY"

query_material_hdata = "SELECT M.CD_MATERIAL, M.DS_MATERIAL, M.CD_GRUPO_MATERIAL, M.DS_VIA_APLICACAO FROM UNIMED_GYN.MATERIAL M"

query_prescr_recomendacao_hdata = "SELECT DISTINCT PR.NR_PRESCRICAO, PR.NR_SEQUENCIA, PR.CD_RECOMENDACAO, PR.DS_TIPO_RECOMENDACAO, PR.DS_HORARIOS FROM UNIMED_GYN.PRESCR_RECOMENDACAO PR INNER JOIN UNIMED_GYN.PRESCR_MEDICA_V PM ON PM.NR_PRESCRICAO = PR.NR_PRESCRICAO INNER JOIN UNIMED_GYN.ATENDIMENTO_PACIENTE AP ON PM.NR_ATENDIMENTO = AP.NR_ATENDIMENTO WHERE AP.DT_ENTRADA >= TO_DATE('{data_ini}', 'DD/MM/YYYY') AND AP.DT_ENTRADA < TO_DATE('{data_fim}', 'DD/MM/YYYY') + INTERVAL '1' DAY"