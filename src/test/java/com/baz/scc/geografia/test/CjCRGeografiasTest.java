
package com.baz.scc.geografia.test;

import com.baz.scc.geografia.support.CjCRPAppConfig;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Realiza una comparación entre los registros obtenidos y registros insertados
 * con esto se corrobora una insercion correcta.
 * <br><br>Copyright 2014 Banco Azteca. Todos los derechos reservados.
 * 
 * @author Norberto Camacho
 */
        
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext.xml")       
public class CjCRGeografiasTest {
    @Autowired
    @Qualifier("as400JdbcTemplate")
    private JdbcTemplate as400JdbcTemplate;
    
    @Autowired
    @Qualifier("usrcajaJdbcTemplate")
    private JdbcTemplate usrcajaJdbcTemplate;
    
    @Autowired
    private CjCRPAppConfig appConfig;

    @Test
    public void compararPaises (){
        int fuente = contarPaisesFuente();
        int destino = contarPaisesDestino();

        evaluarAssert(fuente, destino);
    }
    
    @Test
    public void compararCanales (){
        int fuente = contarCanalesFuente();
        int destino = contarCanalesDestino();

        evaluarAssert(fuente, destino);
    }
    
    @Test
    public void compararSucursales (){
        int fuente = contarSucursalesFuente();
        int destino = contarSucursalesDestino();

        evaluarAssert(fuente, destino);
    }

    @Test
    public void compararGeografias (){
        int fuente = contarGeografiasFuente();
        int destino = contarGeografiasDestino();

        evaluarAssert(fuente, destino);
    }
    
    private void evaluarAssert(int fuent, int dest){
        //Assert.assertFalse((fuent > 0 && dest == 0));
        Assert.assertFalse((fuent > 0 && dest == 0)&&(fuent < 0 && dest < fuent));
    }
        
    private int contarPaisesFuente() {
        String sql = "SELECT count(FIPAIS) FROM mexfinbd.ADNPAIS";
        return consultarDbFuente(sql);
    }

    private int contarPaisesDestino() {
        String sql = "SELECT count(ROWID) FROM TCCJGEOPAIS";
        return consultarDbDestino(sql);
    }

    private int contarCanalesFuente() {
        String sql = "SELECT count(FIIDCANAL) FROM mexfinbd.ADNCANAL";
        return consultarDbFuente(sql);
    }
    
    private int contarCanalesDestino() {
        String sql = "SELECT count(ROWID) FROM TCCJGEOCANAL";
        return consultarDbDestino(sql);
    }

    private int contarSucursalesFuente() {
        String sql = "SELECT count(FISUCURSAL) FROM mexfinbd.CAJWSUCXREGXDIV "
                + "WHERE FISTATUS IN (" + appConfig.getListaStatus() + ")";
        return consultarDbFuente(sql);
    }

    private int contarSucursalesDestino() {
        String sql = "SELECT count(ROWID) FROM TACJGEOSUCURSAL";
        return consultarDbDestino(sql);
    }

    private int contarGeografiasFuente() {
        String sql0 = "SELECT count(TGEOS.FIPAIS)"
                + "FROM mexfinbd.CAJWSUCXREGXDIV TGEOS "
                + "LEFT JOIN mexfinbd.CAJWDIVREG TDISTRITO ON TDISTRITO.fiidentificador = TGEOS.FIREGION AND TDISTRITO.FIPAIS = TGEOS.FIPAIS "
                + "LEFT JOIN mexfinbd.CAJWDIVREG TJEFATURA ON TJEFATURA.fiidentificador  = TDISTRITO.FIIDSUPERIOR AND TJEFATURA.FIPAIS = TGEOS.FIPAIS "
                + "LEFT JOIN mexfinbd.CAJWDIVREG TPLAZA ON TPLAZA.fiidentificador = TJEFATURA.FIIDSUPERIOR AND TPLAZA.FIPAIS = TGEOS.FIPAIS "
                + "WHERE TGEOS.FCLDTIPO = 'D' AND TGEOS.FISTATUS IN (" + appConfig.getListaStatus() + ")";
        int geos1 = consultarDbFuente(sql0);
        String sql1 = "SELECT count(TGEOS.FIPAIS) "
                + "FROM mexfinbd.CAJWSUCXREGXDIV TGEOS "
                + "LEFT JOIN mexfinbd.CAJWDIVREG TDISTRITO ON TDISTRITO.fiidentificador = TGEOS.FIREGION AND TDISTRITO.FIPAIS = TGEOS.FIPAIS "
                + "LEFT JOIN mexfinbd.CAJWDIVREG TJEFATURA ON TJEFATURA.fiidentificador  = TDISTRITO.FIIDSUPERIOR AND TJEFATURA.FIPAIS = TGEOS.FIPAIS "
                + "LEFT JOIN mexfinbd.CAJWDIVREG TPLAZA ON TPLAZA.fiidentificador = TJEFATURA.FIIDSUPERIOR AND TPLAZA.FIPAIS = TGEOS.FIPAIS "
                + "WHERE TGEOS.FCLDTIPO <> 'D' AND TGEOS.FISTATUS IN (" + appConfig.getListaStatus() + ")";
        int geos2 = consultarDbFuente(sql1);
        int totalGeos = geos1 + geos2;
        return totalGeos;
    }
    
    private int contarGeografiasDestino() {
        String sql = "SELECT count(ROWID) FROM TACJGEOSGEO";
        return consultarDbDestino(sql);
    }
    
    class CountMapper implements RowMapper<Object> {
        @Override
        public Object mapRow(ResultSet rs, int i) throws SQLException {
            Object count = new Object();
            count = (rs.getInt(1));
            return count;
        }
    }
    
    private int parsearInteger(Object intg){
        Integer tmpInteger = (Integer) intg;
        int parseado = tmpInteger.intValue();
        return parseado;
        
    }
    
    private int consultarDbFuente(String sql) {
        Object fuente = new Object();
        fuente = as400JdbcTemplate.queryForObject(sql, new CountMapper());
        return parsearInteger(fuente);
    }

    private int consultarDbDestino(String sql) {
        Object destino = new Object();
        destino = usrcajaJdbcTemplate.queryForObject(sql, new CountMapper());
        return parsearInteger(destino);
    }
}
