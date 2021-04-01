SELECT
    --informações pessoais chaves
    nt.id,
    nt.paciente,
    tpp.valor AS tipo_paciente,
    to_char(nt.data_nascimento,'DD/MM/YYYY') AS data_nascimento,
    nt.nome_mae,
    nt.cpf,

    --outras informações pessoais
    nt.idade,
    CASE
        WHEN nt.sexo = '1' THEN 'M'
        WHEN nt.sexo = '2' THEN 'F'
        ELSE ''
    END AS "sexo",
    CASE
        WHEN nt.raca_cor = '99' THEN NULL
        ELSE rc.valor
    END AS raca_cor,
    etn.etnia,

    --informacoes de localização
    nt.uf_residencia,
    nt.ibge_residencia,

    --informações epidemiológicas
    nt.semana_epidemiologica,
    clf.valor AS classificacao_final,
    ccl.valor AS criterio_classificacao,
    CASE
        WHEN nt.evolucao = '3' THEN NULL
        ELSE evo.valor
    END AS evolucao,
    nt.numero_do,
    to_char(nt.data_1o_sintomas,'DD/MM/YYYY') AS data_1o_sintomas,
    to_char(nt.data_cura_obito,'DD/MM/YYYY') AS data_cura_obito,

    --informações laboratoriais
    nt.lab_executor,
    nt.requisicao,
    nt.co_seq_exame,
    CASE
        WHEN nt.metodo = '3' THEN NULL 
        ELSE met.valor
    END AS metodo,
    exa.valor AS exame,
    res.valor AS resultado,
    to_char(nt.data_coleta,'DD/MM/YYYY') AS data_coleta,
    to_char(nt.data_recebimento,'DD/MM/YYYY') AS data_recebimento,
    to_char(nt.data_liberacao,'DD/MM/YYYY') AS data_liberacao,

    --informações sobre notificação
    ori.valor AS origem,
    nt.uf_unidade_notifica,
    nt.ibge_unidade_notifica,
    nt.nome_unidade_notifica,
    nt.nome_notificador,
    nt.email_notificador,
    nt.telefone_notificador,
    to_char(nt.data_notificacao,'DD/MM/YYYY') AS data_notificacao,
    to_char(nt.updated_at,'DD/MM/YYYY') AS updated_at

FROM public.notificacao nt
LEFT JOIN public.termo exa ON (exa.codigo::character varying = nt.exame::character varying AND exa.tipo = 'exame')
LEFT JOIN public.termo ori ON (ori.codigo::character varying = nt.origem::character varying AND ori.tipo = 'origem')
LEFT JOIN public.termo tpp ON (tpp.codigo::character varying = nt.tipo_paciente::character varying AND tpp.tipo = 'tipo_paciente')
LEFT JOIN public.termo rc ON (rc.codigo::character varying = nt.raca_cor::character varying AND rc.tipo = 'raca_cor')
LEFT JOIN public.termo res ON (res.codigo::character varying = nt.resultado::character varying AND res.tipo = 'resultado')
LEFT JOIN public.termo met ON (met.codigo::character varying = nt.metodo::character varying AND met.tipo = 'metodo')
LEFT JOIN public.termo ccl ON (ccl.codigo::character varying = nt.criterio_classificacao::character varying AND ccl.tipo = 'criterio_classificacao')
LEFT JOIN public.termo clf ON (clf.codigo::character varying = nt.classificacao_final::character varying AND clf.tipo = 'classificacao_final')
LEFT JOIN public.termo evo ON (evo.codigo::character varying = nt.evolucao::character varying AND evo.tipo = 'evolucao')
LEFT JOIN public.etnia etn ON (etn.co_etnia::character varying = nt.etnia::character varying)

WHERE nt.classificacao_final = 2
    AND nt.excluir_ficha = 2
    AND nt.status_notificacao IN (1, 2)
ORDER BY id ASC

LIMIT ALL
OFFSET 0