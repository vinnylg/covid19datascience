SELECT
    nt.id,
    nt.origem,
    CASE
        WHEN nt.data_notificacao IS NOT NULL THEN to_char(nt.data_notificacao,'DD/MM/YYYY') 
        ELSE ''
    END AS data_notificacao,
    CASE
        WHEN nt.updated_at IS NOT NULL THEN to_char(nt.updated_at,'DD/MM/YYYY') 
        ELSE ''
    END AS updated_at,
    nt.paciente,
    CASE
        WHEN nt.sexo = '1' THEN 'M'
        WHEN nt.sexo = '2' THEN 'F'
        ELSE ''
    END AS "sexo",
    nt.nome_mae,
    nt.idade,
    CASE
        WHEN nt.data_nascimento IS NOT NULL THEN to_char(nt.data_nascimento,'DD/MM/YYYY') 
        ELSE ''
    END AS data_nascimento,
    nt.pais_residencia,
    nt.uf_residencia,
    nt.ibge_residencia,
    nt.uf_unidade_notifica,
    nt.ibge_unidade_notifica,
    exa.exame,
    nt.criterio_classificacao,
    CASE
        WHEN nt.data_liberacao IS NOT NULL THEN to_char(nt.data_liberacao,'DD/MM/YYYY') 
        ELSE ''
    END AS data_liberacao,
    CASE
        WHEN nt.data_coleta IS NOT NULL THEN to_char(nt.data_coleta,'DD/MM/YYYY') 
        ELSE ''
    END AS data_coleta,
    CASE
        WHEN nt.data_recebimento IS NOT NULL THEN to_char(nt.data_recebimento,'DD/MM/YYYY') 
        ELSE ''
    END AS data_recebimento,
    CASE
        WHEN nt.data_1o_sintomas IS NOT NULL THEN to_char(nt.data_1o_sintomas,'DD/MM/YYYY') 
        ELSE ''
    END AS data_1o_sintomas,
    nt.evolucao,
    CASE
        WHEN nt.data_cura_obito IS NOT NULL THEN to_char(nt.data_cura_obito,'DD/MM/YYYY') 
        ELSE ''
    END AS data_cura_obito

FROM public.notificacao nt
LEFT JOIN public.exame exa 
    ON nt.exame = exa.id

WHERE nt.classificacao_final = 2
    AND nt.excluir_ficha = 2
    AND nt.status_notificacao IN (1, 2)
    AND ((nt.data_notificacao >= NOW() - INTERVAL '7 DAY') or (nt.data_liberacao >= NOW() - INTERVAL '7 DAY') or (nt.updated_at >= NOW() - INTERVAL '7 DAY')) 
ORDER BY id ASC
LIMIT all
OFFSET 0