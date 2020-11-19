SELECT 
    nt.id,
    to_char(nt.data_notificacao,'DD/MM/YYYY') AS data_notificacao,
    to_char(nt.updated_at,'DD/MM/YYYY') AS updated_at,
    nt.paciente,
    CASE
        WHEN nt.sexo = '1' THEN 'M'
        WHEN nt.sexo = '2' THEN 'F'
        ELSE ''
    END AS "sexo",
    nt.idade,
    nt.ibge_residencia,
    nt.ibge_unidade_notifica,
    nt.exame,
    to_char(nt.data_liberacao,'DD/MM/YYYY') AS data_liberacao,
    to_char(nt.data_1o_sintomas,'DD/MM/YYYY') AS data_1o_sintomas,
    nt.evolucao,
    to_char(nt.data_cura_obito,'DD/MM/YYYY') AS data_cura_obito

FROM public.notificacao nt

WHERE nt.classificacao_final = 2
    AND nt.excluir_ficha = 2
    AND nt.status_notificacao IN (1, 2)
ORDER BY id ASC;