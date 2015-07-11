package com.baz.scc.geografia.dao;

import com.baz.scc.commons.model.CjCRGeoCanal;
import com.baz.scc.commons.model.CjCROracleResponse;
import com.baz.scc.commons.model.CjCRGeoSucursal;
import com.baz.scc.commons.model.CjCRGeoPais;
import com.baz.scc.commons.support.CjCRDaoConfig;
import com.baz.scc.commons.util.CjCRDaoUtils;
import com.baz.scc.commons.util.CjCRDaoUtils.ListStructureArray;
import com.baz.scc.geografia.main.CjCRBootstrap;
import com.baz.scc.geografia.support.CjCRPAppConfig;
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

@Repository
public class CjCRGeografiaSucursalDao {

    private static final String TYPCJGEO0006_DESCRIPTOR = "%s.TYPCJGEO0006";
    private static final ListStructureArray<CjCRGeoSucursal> listStructureArray;
    private static final Logger log = Logger.getLogger(CjCRBootstrap.class);

    @Autowired
    @Qualifier("as400JdbcTemplate")
    private JdbcTemplate as400JdbcTemplate;

    @Autowired
    @Qualifier("usrcajaJdbcTemplate")
    private JdbcTemplate usrcajaJdbcTemplate;
    
    @Autowired
    private CjCRPAppConfig appConfig;

    @Autowired
    private CjCRDaoConfig daoConfig;
    
    public List<CjCRGeoSucursal> getSucursal() {

        String sql = "SELECT FIPAIS,FICANAL,FISUCURSAL, FCDESCRIPCION, FISTATUS FROM mexfinbd.CAJWSUCXREGXDIV "
                + "WHERE FISTATUS IN (" + appConfig.getListaStatus() + ") ORDER BY FIPAIS,FICANAL,FISUCURSAL";
        return as400JdbcTemplate.query(sql, new SucursalMapper(), (Object[]) null);

    }

    class SucursalMapper implements RowMapper<CjCRGeoSucursal> {

        @Override
        public CjCRGeoSucursal mapRow(ResultSet rs, int i) throws SQLException {

            CjCRGeoSucursal sucursal = new CjCRGeoSucursal();
            CjCRGeoPais pais = new CjCRGeoPais();
            pais.setIdPais(rs.getInt(1));
            sucursal.setPais(pais);

            CjCRGeoCanal canal = new CjCRGeoCanal();
            canal.setIdCanal(rs.getInt(2));
            sucursal.setCanal(canal);

            sucursal.setIdSucursal(rs.getInt(3));
            sucursal.setNombre(rs.getString(4));
            sucursal.setStatus(rs.getInt(5));
            return sucursal;

        }
    }

    static {
        listStructureArray = new ListStructureArray<CjCRGeoSucursal>() {

            @Override
            public Object getObject(CjCRGeoSucursal sucursales) {
                Object[] row = new Object[5];

                row[0] = sucursales.getPais().getIdPais();
                row[1] = sucursales.getCanal().getIdCanal();
                row[2] = sucursales.getIdSucursal();
                row[3] = sucursales.getNombre();
                row[4] = sucursales.getStatus();
                return row;
            }
        };

    }

    public CjCROracleResponse registrarSucursales(final List<CjCRGeoSucursal> listasucursales,
            final String usuario) {

        return usrcajaJdbcTemplate.execute(getObtenerSucursalesStatement(),
                new CallableStatementCallback<CjCROracleResponse>() {

                    @Override
                    public CjCROracleResponse doInCallableStatement(CallableStatement cs)
                    throws SQLException, DataAccessException {

                        CjCROracleResponse or = new CjCROracleResponse();

                        try {
                            CjCRDaoUtils.addArrayNotNull(cs, 1, daoConfig.getSentence(TYPCJGEO0006_DESCRIPTOR), 
                                    listStructureArray.getArray(listasucursales));
                            CjCRDaoUtils.addString(cs, 2, usuario);
                            cs.registerOutParameter(3, OracleTypes.NUMBER);
                            cs.registerOutParameter(4, OracleTypes.VARCHAR);

                            cs.execute();

                            or.setStatus(cs.getInt(3));
                            or.setMsg(cs.getString(4));
                            if (or.getStatus() == 0) {
                                log.info("Insercion de Sucursales OK");
                            } else {
                                log.warn("Problemas insercion sucursales: " + or.getMsg());
                            }
                            return or;
                        } catch (Exception ex) {
                            log.error("Exception: Oracle Response = " + or, ex);
                            return or;
                        }
                    }
                });
    }

    public String getObtenerSucursalesStatement() {
        return daoConfig.getSentence("call %s.PQCJGEO0001.PACJGEOLI0003(?,?,?,?)");
    }

}
