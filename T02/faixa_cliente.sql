SELECT  faixa_cliente('Clayton Pereira Bonfim');
SELECT distinct faixa_cliente(nome_cliente) from cliente;

drop function faixa_cliente(p_nome_cliente varchar(100));

CREATE OR REPLACE FUNCTION faixa_cliente(p_nome_cliente varchar(100))
    RETURNS varchar(100) AS
    $BODY$
DECLARE
    l_soma_deposito float;
    l_faixa varchar(100);
    l_nome_cliente varchar(100);
    l_info varchar(1000);
        cursor_relatorio CURSOR FOR SELECT nome_cliente, sum(D.saldo_deposito) as TOTAL_DEPOSITO
            FROM DEPOSITO AS D
        WHERE D.nome_cliente = p_nome_cliente
        GROUP BY D.nome_cliente;
BEGIN
    OPEN cursor_relatorio;
        FETCH cursor_relatorio INTO l_nome_cliente, l_soma_deposito;
        IF FOUND THEN
    IF l_soma_deposito IS NULL then l_soma_deposito=0; end if;
    IF l_soma_deposito > 6000.0 then l_faixa := 'A';
    ELSIF l_soma_deposito < 4000.0 then l_faixa := 'B';
    ELSE l_faixa := 'C';
    END IF;
    l_info = l_nome_cliente || ' - ' || l_faixa;
    CLOSE  cursor_relatorio;
    RETURN l_info;
END
$BODY$
    LANGUAGE plpgsql VOLATILE COST 100;

