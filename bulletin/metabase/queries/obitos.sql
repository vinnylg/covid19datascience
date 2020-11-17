SELECT  nt.id,
        nt.paciente,
	    CASE  
    		WHEN nt.sexo = 1 THEN 'M'
    		WHEN nt.sexo = 2 THEN 'F'
    		WHEN nt.sexo = 3 THEN ''
    		WHEN nt.sexo IS NULL THEN ''
    	END,
        to_char(nt.data_nascimento,'DD/MM/YYYY') AS data_nascimento,
		nt.idade,
        nt.ibge_residencia,
        nt.ibge_unidade_notifica,
        CASE
           WHEN nt.exame = '' OR nt.exame IS NULL THEN 'Não informado'
           ELSE exa.valor
        END
        to_char(nt.data_notificacao,'DD/MM/YYYY') AS data_notificacao,
        to_char(nt.data_liberacao,'DD/MM/YYYY') AS 'Dt diag',
        to_char(nt.data_1o_sintomas,'DD/MM/YYYY') AS 'IS',
        to_char(nt.data_cura_obito,'DD/MM/YYYY') AS data_cura_obito
  FROM  public.notificacao nt
  LEFT JOIN public.termo exa ON (exa.codigo::character varying = nt.exame::character varying AND exa.tipo = 'exame')
  WHERE nt.classificacao_final = '2' AND nt.evolucao = '2' AND nt.excluir_ficha = '2' AND (nt.status_notificacao = '1' OR nt.status_notificacao = '2')
ORDER BY 1;