package com.baz.scc.geografia.dao;

/**
 * DAO .
 * <br><br>Copyright 2013 Banco Azteca. Todos los derechos reservados.
 * 
 * @author B938201 Norberto C.F. 
 */
import com.baz.scc.commons.model.CjCRGeoPais;
import com.baz.scc.commons.model.CjCROracleResponse;
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

@Repository
public class CjCRGeografiaPaisDao {

    private static final Logger log = Logger.getLogger(CjCRGeografiaPaisDao.class);
    //AS400
    @Autowired
    @Qualifier("as400JdbcTemplate")
    private JdbcTemplate as400JdbcTemplate;
    //Oracle
    @Autowired
    @Qualifier("usrcajaJdbcTemplate")
    private JdbcTemplate usrcajaJdbcTemplate;
    
    @Autowired
    private CjCRDaoConfig daoConfig;
    
    //Oracle Definicion de tipo Canal USRCAJADES.TYPCJGEO0003
    private static final String TYPCJGEO0002_DESCRIPTOR = "%s.TYPCJGEO0002";
    private static final ListStructureArray<CjCRGeoPais> listStructureArray;

    //Obtenciòn de paises consultas AS400
    public List<CjCRGeoPais> getPaises() {
        String sql = "SELECT FIPAIS, FCPAISDESC, FCPAISCORTO FROM mexfinbd.ADNPAIS";
        return as400JdbcTemplate.query(sql, new PaisMapper(), (Object[]) null);
    }
    
    class PaisMapper implements RowMapper<CjCRGeoPais> {

        @Override
        public CjCRGeoPais mapRow(ResultSet rs, int i) throws SQLException {
            CjCRGeoPais pais = new CjCRGeoPais();
            pais.setIdPais(rs.getInt(1));
            pais.setNombre(rs.getString(2));
            pais.setAlias(rs.getString(3));
            pais.setStatus(1);
            return pais;

        }
    }

    static {
        listStructureArray = new ListStructureArray<CjCRGeoPais>() {
            @Override
            public Object getObject(CjCRGeoPais pais) {
                Object[] row = new Object[4];

                row[0] = pais.getIdPais();
                row[1] = pais.getNombre();
                row[2] = pais.getAlias();
                row[3] = pais.getStatus();
                return row;
            }
        };
    }

    //Insercion de paises Oracle
    public CjCROracleResponse InsertarPaisesBD(final List<CjCRGeoPais> listaPaises,
            final String usuario) {
        return usrcajaJdbcTemplate.execute(getInsertarPaisesStatement(),
                new CallableStatementCallback<CjCROracleResponse>() {
            @Override
            public CjCROracleResponse doInCallableStatement(CallableStatement cs)
                    throws SQLException, DataAccessException {
                CjCROracleResponse or = new CjCROracleResponse();
                try {
                    CjCRDaoUtils.addArray(cs, 1, daoConfig.getSentence(TYPCJGEO0002_DESCRIPTOR), 
                            listStructureArray.getArray(listaPaises));
                    CjCRDaoUtils.addString(cs, 2, usuario);
                    cs.registerOutParameter(3, OracleTypes.NUMBER);
                    cs.registerOutParameter(4, OracleTypes.VARCHAR);

                    cs.execute();

                    if (or.getStatus() == 0) {
                        log.info("Insercion de Paises OK");
                    } else {
                        log.warn("Problemas en insercion Pais: " + or.getMsg());
                    }

                    return or;
                } catch (Exception ex) {
                    log.error("Insercion de Paises: Exception Valor de Return statement = " + or, ex);
                    return or;
                }
            }
        });

    }

    public String getInsertarPaisesStatement() {
        return daoConfig.getSentence("call %s.PQCJGEO0001.PACJGEOLI0001(?,?,?,?)");
    }
}
