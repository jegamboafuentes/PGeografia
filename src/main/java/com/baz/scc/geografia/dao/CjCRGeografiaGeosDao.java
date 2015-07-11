package com.baz.scc.geografia.dao;

import com.baz.scc.commons.model.CjCROracleResponse;
import com.baz.scc.commons.support.CjCRDaoConfig;
import com.baz.scc.commons.util.CjCRDaoUtils;
import com.baz.scc.commons.util.CjCRDaoUtils.ListStructureArray;
import com.baz.scc.geografia.model.CjCRGeografiaSucursalGeo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import com.baz.scc.geografia.logic.CjCRGeografiaGeosLogic;
import com.baz.scc.geografia.support.CjCRPAppConfig;
import java.sql.CallableStatement;
import oracle.jdbc.OracleTypes;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.CallableStatementCallback;

/**
 * DAO .
 * <br><br>Copyright 2013 Banco Azteca. Todos los derechos reservados.
 *
 * @author Norberto Camacho
 */
@Repository
public class CjCRGeografiaGeosDao {

    private static final Logger log = Logger.getLogger(CjCRGeografiaSucursalGeo.class);

    @Autowired
    @Qualifier("usrcajaJdbcTemplate")
    private JdbcTemplate usrcajaJdbcTemplate;
    @Autowired
    @Qualifier("as400JdbcTemplate")
    private JdbcTemplate as400JdbcTemplate;
    @Autowired
    private CjCRPAppConfig appConfig;
    
    @Autowired
    private CjCRDaoConfig daoConfig;
    
    private static final ListStructureArray<CjCRGeografiaGeosLogic.GeosGeo> listStructureArray;
    private static final String TYPCJGEO0008_DESCRIPTOR = "%s.TYPCJGEO0008";
    private List<CjCRGeografiaSucursalGeo> listaGeoEvaluar;
    private List<CjCRGeografiaSucursalGeo> listaGeoNivelUnico;
    
    private void getSucGeoEvaluar() {
        List<CjCRGeografiaSucursalGeo> listaGeos = new ArrayList<CjCRGeografiaSucursalGeo>();
        String sql = "SELECT TGEOS.FIPAIS,TGEOS.FISUCURSAL,TGEOS.FIREGION, TGEOS.FICANAL, "
                + "TDISTRITO.FIIDENTIFICADOR,TDISTRITO.FCDESCRIPCION, TDISTRITO.FIIDSUPERIOR, "
                + "TJEFATURA.FIIDENTIFICADOR,TJEFATURA.FCDESCRIPCION, TJEFATURA.FIIDSUPERIOR, "
                + "TPLAZA.FIIDENTIFICADOR,TPLAZA.FCDESCRIPCION, TPLAZA.FIIDSUPERIOR "
                + "FROM mexfinbd.CAJWSUCXREGXDIV TGEOS "
                + "LEFT JOIN mexfinbd.CAJWDIVREG TDISTRITO ON TDISTRITO.fiidentificador = TGEOS.FIREGION AND TDISTRITO.FIPAIS = TGEOS.FIPAIS "
                + "LEFT JOIN mexfinbd.CAJWDIVREG TJEFATURA ON TJEFATURA.fiidentificador  = TDISTRITO.FIIDSUPERIOR AND TJEFATURA.FIPAIS = TGEOS.FIPAIS "
                + "LEFT JOIN mexfinbd.CAJWDIVREG TPLAZA ON TPLAZA.fiidentificador = TJEFATURA.FIIDSUPERIOR AND TPLAZA.FIPAIS = TGEOS.FIPAIS "
                + "WHERE TGEOS.FCLDTIPO = 'D' AND TGEOS.FISTATUS IN (" + appConfig.getListaStatus() + ") ORDER BY TGEOS.FIPAIS,TGEOS.FICANAL, TGEOS.FISUCURSAL";
        listaGeos = as400JdbcTemplate.query(sql, new SucursalGeoMapper(), (Object[]) null);
        setListaGeoEvaluar(listaGeos);
    }
    
    private void getSucGeoSinNiveles(){
        List<CjCRGeografiaSucursalGeo> listaGeoSinNivel= new ArrayList<CjCRGeografiaSucursalGeo>(); 
        String sql = "SELECT TGEOS.FIPAIS,TGEOS.FISUCURSAL,TGEOS.FIREGION, TGEOS.FICANAL, "
                + "TDISTRITO.FIIDENTIFICADOR,TDISTRITO.FCDESCRIPCION, TDISTRITO.FIIDSUPERIOR, "
                + "TJEFATURA.FIIDENTIFICADOR,TJEFATURA.FCDESCRIPCION, TJEFATURA.FIIDSUPERIOR, "
                + "TPLAZA.FIIDENTIFICADOR,TPLAZA.FCDESCRIPCION, TPLAZA.FIIDSUPERIOR "
                + "FROM mexfinbd.CAJWSUCXREGXDIV TGEOS "
                + "LEFT JOIN mexfinbd.CAJWDIVREG TDISTRITO ON TDISTRITO.fiidentificador = TGEOS.FIREGION AND TDISTRITO.FIPAIS = TGEOS.FIPAIS "
                + "LEFT JOIN mexfinbd.CAJWDIVREG TJEFATURA ON TJEFATURA.fiidentificador  = TDISTRITO.FIIDSUPERIOR AND TJEFATURA.FIPAIS = TGEOS.FIPAIS "
                + "LEFT JOIN mexfinbd.CAJWDIVREG TPLAZA ON TPLAZA.fiidentificador = TJEFATURA.FIIDSUPERIOR AND TPLAZA.FIPAIS = TGEOS.FIPAIS "
                + "WHERE TGEOS.FCLDTIPO <> 'D' AND TGEOS.FISTATUS IN (" + appConfig.getListaStatus() + ") ORDER BY TGEOS.FIPAIS,TGEOS.FICANAL, TGEOS.FISUCURSAL";
        listaGeoSinNivel = as400JdbcTemplate.query(sql, new SucursalGeoMapper(), (Object[]) null);
        setListaGeoNivelUnico(listaGeoSinNivel);
    }
    
    class SucursalGeoMapper implements RowMapper<CjCRGeografiaSucursalGeo> {

        @Override
        public CjCRGeografiaSucursalGeo mapRow(ResultSet rs, int i) throws SQLException {
            CjCRGeografiaSucursalGeo SucursalGeoAct = new CjCRGeografiaSucursalGeo();
            //GEOGRAFIA
            SucursalGeoAct.setGeoPais(rs.getInt(1));
            SucursalGeoAct.setGeoSucursal(rs.getInt(2));
            SucursalGeoAct.setGeoRegion(rs.getInt(3));
            SucursalGeoAct.setGeoCanal(rs.getInt(4));
            //DISTRITO
            SucursalGeoAct.setDistritIdentificador(rs.getInt(5));
            SucursalGeoAct.setDistritDescripion(rs.getString(6));
            SucursalGeoAct.setDistritIdsuperior(rs.getInt(7));
            //JEFATURA
            SucursalGeoAct.setJefIdentificador(rs.getInt(8));
            SucursalGeoAct.setJefDescripion(rs.getString(9));
            SucursalGeoAct.setJefIdsuperior(rs.getInt(10));
            //PLAZA
            SucursalGeoAct.setPlazaIdentificador(rs.getInt(11));
            SucursalGeoAct.setPlazaDescripion(rs.getString(12));
            SucursalGeoAct.setPlazaIdsuperior(rs.getInt(13));            
            
            return SucursalGeoAct;
        }
    }

    public void buscarGeografias(){
        getSucGeoEvaluar();
        getSucGeoSinNiveles();
    }
    
    //Insercion de canales Oracle
    static {
        listStructureArray = new ListStructureArray<CjCRGeografiaGeosLogic.GeosGeo>() {

            @Override
            public Object getObject(CjCRGeografiaGeosLogic.GeosGeo geo) {
                Object[] row = new Object[8];

                
                row[0] = geo.getGeoSucursal().getPais().getIdPais();
                row[1] = geo.getGeoSucursal().getCanal().getIdCanal();
                row[2] = geo.getGeoSucursal().getIdSucursal();
                row[3] = geo.getGeoElement().getIdNivel();
                row[4] = geo.getGeo().getIdGeografia();
                row[5] = geo.getGeoElement().getNombre();
                row[6] = geo.getGeoElement().getValor();
                row[7] = geo.getGeoElement().getStatus();
                
                return row;
            }
        };
    }

    ////    Insercion de Canales en OracleBD
    public CjCROracleResponse InsertarGeografiasBD(final List<CjCRGeografiaGeosLogic.GeosGeo> listaGeos,
            final String usuario) {

        return usrcajaJdbcTemplate.execute(getInsertarGeosStatement(),
                new CallableStatementCallback<CjCROracleResponse>() {
            @Override
            public CjCROracleResponse doInCallableStatement(CallableStatement cs)
                    throws SQLException, DataAccessException {
                CjCROracleResponse or = new CjCROracleResponse();

                try {

                    CjCRDaoUtils.addArray(cs, 1, daoConfig.getSentence(TYPCJGEO0008_DESCRIPTOR), 
                            listStructureArray.getArray(listaGeos));
                    CjCRDaoUtils.addString(cs, 2, usuario);

                    cs.registerOutParameter(3, OracleTypes.NUMBER);
                    cs.registerOutParameter(4, OracleTypes.VARCHAR);

                    cs.execute();

                    or.setStatus(cs.getInt(3));
                    or.setMsg(cs.getString(4));
                    if (or.getStatus() == 0) {
                        log.info("Insercion de Geografias OK");
                    } else {
                        log.warn("Problemas en insercion Geografias: " + or.getMsg());
                    }

                    return or;

                } catch (Exception ex) {
                    log.error("Insercion de Geografias: Exception Valor de Return statement =" + or , ex);
                    return or;
                }
            }
        });
    }

    public String getInsertarGeosStatement() {
        return daoConfig.getSentence("call %s.PQCJGEO0001.PACJGEOLI0004(?,?,?,?)");
    }

    public List<CjCRGeografiaSucursalGeo> getListaGeoEvaluar() {
        return listaGeoEvaluar;
    }

    private void setListaGeoEvaluar(List<CjCRGeografiaSucursalGeo> listaGeoEvaluar) {
        this.listaGeoEvaluar = listaGeoEvaluar;
    }

    public List<CjCRGeografiaSucursalGeo> getListaGeoNivelUnico() {
        return listaGeoNivelUnico;
    }

    private void setListaGeoNivelUnico(List<CjCRGeografiaSucursalGeo> listaGeoNivelUnico) {
        this.listaGeoNivelUnico = listaGeoNivelUnico;
    }
}
