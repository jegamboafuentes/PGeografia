/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.baz.scc.geografia.dao;

import com.baz.scc.commons.model.CjCROracleResponse;
import com.baz.scc.commons.model.CjCRGeoCanal;
import com.baz.scc.commons.support.CjCRDaoConfig;
import com.baz.scc.commons.util.CjCRDaoUtils;
import com.baz.scc.commons.util.CjCRDaoUtils.ListStructureArray;
import java.sql.CallableStatement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import oracle.jdbc.OracleTypes;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.CallableStatementCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

/**
 * <br><br>Copyright 2013 Banco Azteca. Todos los derechos reservados.
 *
 * @author B938469 Israel G.M.
 */
@Repository
public class CjCRGeografiaCanalDao {

    private static final Logger log = Logger.getLogger(CjCRGeografiaCanalDao.class);
    
    //AS400
    @Autowired
    @Qualifier("as400JdbcTemplate")
    private JdbcTemplate as400JdbcTemplate;
    //Oracle Definicion de tipo Canal USRCAJADES.TYPCJGEO0003
    private static final String TYPCJGEO0004_DESCRIPTOR = "%s.TYPCJGEO0004";
    private static final ListStructureArray<CjCRGeoCanal> listStructureArray;
    @Autowired
    @Qualifier("usrcajaJdbcTemplate")
    private JdbcTemplate usrcajaJdbcTemplate;

    @Autowired
    private CjCRDaoConfig daoConfig;
    
    //Obtenciòn de canales consultas AS400
    public List<CjCRGeoCanal> getCanales() {
        //Canales Con cualquier tipo de estatus debido a que Sucursales con estatus 1 pueden taner canales
        //con estatus =0
        String sql = "SELECT FIIDCANAL, FCCANALDESC, FCLOGO , FISTS FROM mexfinbd.ADNCANAL ";
        return  as400JdbcTemplate.query(sql, new CanalMapper(), (Object[])  null);
        
    }

    class CanalMapper implements RowMapper<CjCRGeoCanal> {

        @Override
        public CjCRGeoCanal mapRow(ResultSet rs, int i) throws SQLException {
            CjCRGeoCanal canal = new CjCRGeoCanal();
            canal.setIdCanal(rs.getInt(1));
            canal.setNombre(rs.getString(2));
            canal.setAlias(rs.getString(3));
            canal.setStatus(rs.getInt(4));
            return canal;
        }
    }

    //Insercion de canales Oracle
    static {
        listStructureArray = new ListStructureArray<CjCRGeoCanal>() {
            @Override
            public Object getObject(CjCRGeoCanal canal) {
                Object[] row = new Object[4];

                row[0] = canal.getIdCanal();
                row[1] = canal.getNombre();
                row[2] = canal.getAlias();
                row[3] = canal.getStatus();
                return row;
            }
        };
    }

    ////    Insercion de Canales en OracleBD
    public CjCROracleResponse InsertarCanalesBD(final List<CjCRGeoCanal> listaCanales,
            final String usuario) {
        
        //Mientras Tanto....Para que funcione la Insercion de Mariana
        CjCRGeoCanal CanalTemp = new CjCRGeoCanal();
        CanalTemp.setIdCanal(0);
        CanalTemp.setNombre("Vitual");
        CanalTemp.setAlias("Vitual");
        CanalTemp.setStatus(1);
        listaCanales.add(CanalTemp);
        
        return usrcajaJdbcTemplate.execute(getInsertarCanalesStatement(),
                new CallableStatementCallback<CjCROracleResponse>() {
            @Override
            public CjCROracleResponse doInCallableStatement(CallableStatement cs)
                    throws SQLException, DataAccessException {
                CjCROracleResponse or = new CjCROracleResponse();

                try {

                    CjCRDaoUtils.addArray(cs, 1, daoConfig.getSentence(TYPCJGEO0004_DESCRIPTOR), 
                            listStructureArray.getArray(listaCanales));
                    CjCRDaoUtils.addString(cs, 2, "PRGEO");

                    cs.registerOutParameter(3, OracleTypes.NUMBER);
                    cs.registerOutParameter(4, OracleTypes.VARCHAR);

                    cs.execute();

                    or.setStatus(cs.getInt(3));
                    or.setMsg(cs.getString(4));

                    if (or.getStatus() == 0) {
                        log.info("Insercion de Canales OK");
                    } else {
                        log.warn("Problemas en insercion Canales: " + or.getMsg());
                    }
                    
                    return or;

                } catch (Exception ex) {
                    log.error("Error insercion de Canales: Exception Valor de Return statement =" + or, ex);
                    return or;
                }

            }
        });
    }

    public String getInsertarCanalesStatement() {
        return daoConfig.getSentence("call %s.PQCJGEO0001.PACJGEOLI0002(?,?,?,?)");
    }
}
