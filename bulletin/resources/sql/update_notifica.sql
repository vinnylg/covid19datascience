SELECT id, to_char(data_notificacao,'DD/MM/YYYY') AS data_notificacao, possui_cpf, tipo_paciente, pais_residencia, pais_municipio_residencia, cns, cpf, passaporte, etnia, paciente, sexo, to_char(data_nascimento,'DD/MM/YYYY') AS data_nascimento, idade, nome_mae, uf_residencia, ibge_residencia, cep_residencia, logradouro_residencia, numero_residencia, bairro_residencia, telefone_paciente, ocupacao, ocupacao_descricao, gestante, periodo_gestacao, to_char(data_1o_sintomas,'DD/MM/YYYY') AS data_1o_sintomas, febre, tosse, dor_garganta, mialgia, artralgia, diarreia, nausea_vomitos, cefaleia, coriza, irritabilidade_confusao, adinamia, escarro, calafrios, congestao_nasal, congestao_conjuntiva, dificuldade_deglutir, manchas_vermelhas, ganglios_linfaticos, asas_nasais, saturacao_o2, cianose, tiragem_intercostal, dispneia, raiox_torax, raiox_torax_descricao, tomografia, tomografia_descricao, doenca_cardiovascular, hipertensao, diabetes, doenca_hepatica, doenca_neurologica, sindrome_down, imunodeficiencia, infeccao_hiv, doenca_renal, doenca_pulmonar, neoplasia, puerperio, obesidade, tabagismo, outras_morbidades, hospitalizado, cnes_hospital, nome_hospital, uf_hospital, ibge_hospital, internacao_sus, tipo_internacao, to_char(data_entrada,'DD/MM/YYYY') AS data_entrada, to_char(data_isolamento,'DD/MM/YYYY') AS data_isolamento, to_char(data_alta,'DD/MM/YYYY') AS data_alta, uso_antiviral, uso_antiviral_descricao, coleta_amostra, to_char(data_cadastro,'DD/MM/YYYY') AS data_cadastro, to_char(data_coleta,'DD/MM/YYYY') AS data_coleta, to_char(data_recebimento,'DD/MM/YYYY') AS data_recebimento, to_char(data_liberacao,'DD/MM/YYYY') AS data_liberacao, requisicao, co_seq_exame, resultado, exame, metodo, lab_executor, historico_viagem, local_viagem, to_char(data_ida_local,'DD/MM/YYYY') AS data_ida_local, to_char(data_retorno_local,'DD/MM/YYYY') AS data_retorno_local, descritivo_viagem, to_char(data_chegada_brasil,'DD/MM/YYYY') AS data_chegada_brasil, to_char(data_chegada_estado,'DD/MM/YYYY') AS data_chegada_estado, contato_suspeito, local_contato_suspeito, local_contato_suspeito_descricao, contato_confirmado, local_contato_confirmado, local_contato_confirmado_descricao, nome_caso_fonte, frequentou_unidade, frequentou_unidade_cnes, frequentou_unidade_descricao, cnes_unidade_notifica, nome_unidade_notifica, uf_unidade_notifica, ibge_unidade_notifica, nome_notificador, email_notificador, ocupacao_notificador, telefone_notificador, classificacao_final, criterio_classificacao, evolucao, to_char(data_cura_obito,'DD/MM/YYYY') AS data_cura_obito, excluir_ficha, motivo_exclusao, to_char(data_encerramento,'DD/MM/YYYY') AS data_encerramento, status_notificacao, origem, outros_sintomas, gestante_alto_risco, raca_cor, numero_do, id_externa, assintomatico, rash_paciente, opcoes_de_surto, paciente_institucionalizado, to_char(updated_at,'DD/MM/YYYY') AS updated_at, rash_paciente_1, rash_paciente_2, perda_olfato_paladar, unidade_solicitante_gal, api_gal, api_cad_sus, instituicao, pesquisa_gal, cnpj_unidade_notifica, tipo_paciente_institucionalizado, telefone_residencial_paciente, email_paciente, alerta, pressao_torax, cnpj_empresa, nome_empresa, cep_empresa, uf_empresa, ibge_empresa, logradouro_empresa, numero_empresa, bairro_empresa, monitoramento_id, semana_epidemiologica, cod_cbo, situacao_trabalho, doenca_trabalho, amostra_material, latitude_residencia, longitude_residencia, to_char(data_confirmacao,'DD/MM/YYYY') AS data_confirmacao, resultado_enviado, observacao, instituicao_ensino, escolaridade, vacina_covid19, lab_produtor_covid19, to_char(data_covid19_1a,'DD/MM/YYYY') AS data_covid19_1a, lote_covid19_1a, to_char(data_covid19_2a,'DD/MM/YYYY') AS data_covid19_2a, lote_covid19_2a, vacina_influenza, to_char(data_vac_influenza,'DD/MM/YYYY') AS data_vac_influenza, nome_social, envio_ms, jogos_escolares, estrategia_laboratorial FROM notificacao WHERE (data_notificacao >= NOW() - INTERVAL '1 DAY') or (data_liberacao >= NOW() - INTERVAL '1 DAY') or (updated_at >= NOW() - INTERVAL '1 DAY') or (data_coleta >= NOW() - INTERVAL '1 DAY') or (data_encerramento >= NOW() - INTERVAL '1 DAY') or (data_cura_obito >= NOW() - INTERVAL '1 DAY') ORDER BY 1 LIMIT ALL OFFSET 0