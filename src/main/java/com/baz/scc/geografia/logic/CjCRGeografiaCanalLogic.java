/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.baz.scc.geografia.logic;

import com.baz.scc.commons.model.CjCRGeoCanal;
import com.baz.scc.commons.util.CjCRUtils;
import com.baz.scc.geografia.dao.CjCRGeografiaCanalDao;
import java.util.Iterator;
import java.util.List;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * DAO .
 * <br><br>Copyright 2013 Banco Azteca. Todos los derechos reservados.
 * 
 * @author B938469 Isarel G.M. 
 */

@Component("applicationCanal")
public class CjCRGeografiaCanalLogic {
    private static final Logger log = Logger.getLogger(CjCRGeografiaCanalLogic.class);
    
    private static final String USUARIO = "PRGEO";
    @Autowired
    private CjCRGeografiaCanalDao canal;
    
    public void InsertarCanales() {
        //Obtener los canales de AS400
        try{
            long begin = System.currentTimeMillis();
            List canales = canal.getCanales();
            log.info("Canales obtenidos: " + canales.size());
            //Insercion de canales en BD Oracle
            canal.InsertarCanalesBD(canales, USUARIO);
            long end = System.currentTimeMillis();
            log.info(CjCRUtils.concat("----- Canales completos [",CjCRUtils.formatElapsedTime(begin, end), "]"));
        }catch(Exception ex){
            log.error("Error DB(Canales)" + ex);
        }
    }
}
