-- SELECT
--     --informações pessoais chaves
--     nt.id,
--     nt.paciente,
--     to_char(nt.data_nascimento,'DD/MM/YYYY') AS data_nascimento,
--     nt.nome_mae,
--     nt.cpf,

--     --outras informações pessoais
--     nt.sexo,

--     --informacoes de localização
--     nt.uf_residencia,
--     nt.ibge_residencia,

--     --informações epidemiológicas
--     nt.classificacao_final,
--     nt.criterio_classificacao,
--     nt.evolucao,
--     to_char(nt.data_1o_sintomas,'DD/MM/YYYY') AS data_1o_sintomas,
--     to_char(nt.data_cura_obito,'DD/MM/YYYY') AS data_cura_obito,

--     --informações laboratoriais
--     -- nt.co_seq_exame,
--     nt.metodo,
--     nt.exame,
--     nt.resultado,
--     to_char(nt.data_coleta,'DD/MM/YYYY') AS data_coleta,
--     to_char(nt.data_recebimento,'DD/MM/YYYY') AS data_recebimento,
--     to_char(nt.data_liberacao,'DD/MM/YYYY') AS data_liberacao,

--     --informações sobre notificação
--     nt.status_notificacao,
--     nt.excluir_ficha,
--     nt.origem,
--     nt.uf_unidade_notifica,
--     nt.ibge_unidade_notifica,
--     to_char(nt.data_notificacao,'DD/MM/YYYY') AS data_notificacao,
--     to_char(nt.updated_at,'DD/MM/YYYY') AS updated_at

-- FROM public.notificacao nt

-- WHERE nt.classificacao_final = 2
--     AND nt.excluir_ficha = 2
--     AND nt.status_notificacao IN (1, 2)
-- ORDER BY id ASC

-- LIMIT ALL
-- OFFSET 0

select 
    CASE
        WHEN nt.evolucao = 1 THEN 'SIM'
        ELSE 'NAO'
    END AS Recuperados,
    ori.valor AS Fonte,
    munn.no_municipio AS mun_resid,
    nt.ibge_residencia AS IBGE,
    nt.paciente AS NOME,
    CASE
        WHEN nt.sexo = 1 THEN 'M'
        WHEN nt.sexo = 2 THEN 'F'
        ELSE NULL
    END AS sexo,
    nt.idade AS Idade,
    to_char(nt.data_cura_obito, 'dd/mm/YYYY') AS Data_atualizacao_notifica,
    nt.id

FROM public.notificacao nt 
LEFT JOIN public.municipio munn ON (munn.co_municipio::character varying = nt.ibge_residencia::character varying)
LEFT JOIN public.termo ori ON (ori.codigo::character varying = nt.origem::character varying and ori.tipo = 'origem')
