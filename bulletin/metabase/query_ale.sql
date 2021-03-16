SELECT
    --informações pessoais chaves
    nt.id,
    nt.cpf,
    nt.passaporte,
    nt.paciente,
    sx.sexo,
    to_char(nt.data_nascimento,'DD/MM/YYYY') AS nt.data_nascimento,
    
    --outras informações pessoais
    nt.cod_cbo,
    itt.instituicao,
    occ.ocupacao,
    tpi.tipo_paciente_institucionalizado,

    etn.etnia,
    rc.raca_cor,

    --informacoes de localização
    pr.pais,
    nt.uf_residencia,
    munr.ibge_residencia,

    --informações epidemiologicas e laboratoriais
    to_char(nt.data_1o_sintomas,'DD/MM/YYYY') AS nt.data_1o_sintomas,
    clf.classificacao_final,
    crc.criterio_classificacao,
    to_char(nt.data_coleta,'DD/MM/YYYY') AS nt.data_coleta,
    met.metodo,
    exa.exame,
    res.resultado,

    evo.evolucao,
    to_char(nt.data_cura_obito,'DD/MM/YYYY') AS nt.data_cura_obito,

    --informações sobre a notificação
    nt.uf_unidade_notifica,
    muna.ibge_unidade_notifica,
    stn.status_notificacao,
    to_char(nt.data_notificacao,'DD/MM/YYYY') AS nt.data_notificacao,
    to_char(nt.updated_at,'DD/MM/YYYY') AS nt.updated_at,
    to_char(nt.data_encerramento,'DD/MM/YYYY') AS nt.data_encerramento,

    --informações sobre a sintomas
    nt.assintomatico,
    nt.adinamia,
    nt.artralgia,
    nt.asas_nasais,
    nt.calafrios,
    nt.cefaleia,
    nt.cianose,
    nt.congestao_conjuntiva,
    nt.congestao_nasal,
    nt.coriza,
    nt.diarreia,
    nt.dificuldade_deglutir,
    nt.dispneia,
    nt.dor_garganta,
    nt.escarro,
    nt.febre,
    nt.ganglios_linfaticos,
    nt.irritabilidade_confusao,
    nt.manchas_vermelhas,
    nt.perda_olfato_paladar,
    nt.puerperio,
    nt.saturacao_o2,
    nt.tiragem_intercostal,
    nt.tosse,
    ant.uso_antiviral,

    --informações sobre a comobirdades
    nt.diabetes,
    nt.doenca_cardiovascular,
    nt.doenca_hepatica,
    nt.doenca_neurologica,
    nt.doenca_pulmonar,
    nt.doenca_renal,
    nt.gestante,
    nt.gestante_alto_risco,
    nt.hipertensao,
    nt.obesidade,
    nt.sindrome_down,
    nt.tabagismo

FROM public.notificacao nt

left join public.sexo sx on sx.id = nt.sexo
left join public.instituicao itt on itt.id = nt.instituicao
left join public.ocupacao occ on occ.id = nt.ocupacao
left join public.tipo_paciente_institucionalizado tpi on tpi.id = nt.tipo_paciente_institucionalizado
left join public.etnia etn on etn.id = nt.etnia
left join public.raca_cor rc on rc.id = nt.etnia
left join public.pais_residencia pr on pr.co_pais = nt.pais_residencia

left join public.estado ufr on ufr.co_uf = nt.uf_residencia
left join public.municipio munr on munr.co_municipio = nt.ibge_residencia
left join public.classificacao_final clf on  clf.id = nt.classificacao_final
left join public.criterio_classificacao crc on crc.id = nt.criterio_classificacao
left join public.metodo met on met.id = nt.metodo
left join public.exame exa on exa.id = nt.exame
left join public.resultado res on res.id = nt.resultado
left join public.evolucao evo on evo.id = nt.evolucao

left join public.municipio muna on munn.co_municipio = nt.ibge_unidade_notifica
left join public.status stn on stn.id = nt.status_notificacao
left join public.uso_antiviral ant on ant.id = nt.uso_antiviral

WHERE to_char(data_notificacao,'DD/MM/YYYY') = '09/03/2021'
