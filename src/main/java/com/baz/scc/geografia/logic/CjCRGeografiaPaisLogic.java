package com.baz.scc.geografia.logic;

import com.baz.scc.geografia.dao.CjCRGeografiaPaisDao;
import com.baz.scc.commons.model.CjCRGeoPais;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.baz.scc.commons.util.CjCRUtils;
import java.util.List;
/**
 * DAO .
 * <br><br>Copyright 2013 Banco Azteca. Todos los derechos reservados.
 * 
 * @author B938201 Norberto C.F. 
 */

@Component("application")
public class CjCRGeografiaPaisLogic {
    private static final Logger log = Logger.getLogger(CjCRGeografiaPaisLogic.class);
    private static final String USUARIO = "PRGEO";

    @Autowired
    private CjCRGeografiaPaisDao pais;
    
    public void InsertarPaises() {
        try {
            long begin = System.currentTimeMillis();
            //Obtener los Paises AS400
            List paises = pais.getPaises();
            //Insercion de paises en BD Oracle
            log.info("Paises obtenidos:" + paises.size());
            pais.InsertarPaisesBD(paises, USUARIO);
            long end = System.currentTimeMillis();
            log.info(CjCRUtils.concat("----- Paises completos [",CjCRUtils.formatElapsedTime(begin, end), "]"));
        } catch (Exception ex) {
            log.error("Error DB (Paises)" + ex);
        }

    }
}
