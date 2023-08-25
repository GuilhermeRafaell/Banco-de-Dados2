SELECT concatena_cliente('Clayton Pereira Bonfim');

CREATE OR REPLACE FUNCTION concatena_cliente(p_nome_cliente varchar(100))
    RETURNS varchar(100) AS
    $BODY$
DECLARE
    l_nome_agencia varchar(100);
    l_numero_conta integer;
    l_concatenado varchar(5000) := '';
        cursor_relatorio CURSOR FOR SELECT distinct e.nome_agencia, e.numero_conta
            FROM EMPRESTIMO AS e
        WHERE e.nome_cliente = d.numero_conta
        AND e.nome_cliente = p_nome_cliente;
BEGIN
    OPEN cursor_relatorio;
        LOOP
            FETCH cursor_relatorio INTO l_nome_agencia, l_numero_conta;
            IF FOUND THEN
                l_concatenado := l_concatenado || l_nome_agencia || ' - ' || l_numero_conta;
                END IF
                IF NOT FOUND THEN EXIT;
                END IF;
        END LOOP;
    CLOSE cursor_relatorio;
    RETURN l_concatenado;
END
$BODY$
    LANGUAGE plpgsql VOLATILE COST 100;