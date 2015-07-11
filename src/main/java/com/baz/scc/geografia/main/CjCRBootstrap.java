package com.baz.scc.geografia.main;

import com.baz.scc.commons.util.CjCRSpringContext;
import com.baz.scc.commons.util.CjCRUtils;
import com.baz.scc.geografia.logic.CjCRGeografiaCanalLogic;
import com.baz.scc.geografia.logic.CjCRGeografiaGeosLogic;
import com.baz.scc.geografia.logic.CjCRGeografiaPaisLogic;
import com.baz.scc.geografia.logic.CjCRGeografiaSucursalLogic;
import org.apache.log4j.Logger;


/**
 * <br><br>Copyright 2013 Banco Azteca. Todos los derechos reservados.
 *
 * @author B938469 Israel G.M.
 */

public class CjCRBootstrap {
    
    private static final Logger log = Logger.getLogger(CjCRBootstrap.class);
    
    public static void main(String[] args) {
        try{
            long begin = System.currentTimeMillis();
            CjCRSpringContext.init();
            log.info("Inicio Proceso - Geografias");
            //Muestra Paises 
            CjCRGeografiaPaisLogic appConfig = CjCRSpringContext.getBean(CjCRGeografiaPaisLogic.class);
            appConfig.InsertarPaises();

            //Muestra Canales
            CjCRGeografiaCanalLogic appConfigCanal = CjCRSpringContext.getBean(CjCRGeografiaCanalLogic.class);
            appConfigCanal.InsertarCanales();
            
            //Muestra Sucursales
            CjCRGeografiaSucursalLogic appsucursal = CjCRSpringContext.getBean(CjCRGeografiaSucursalLogic.class);
            appsucursal.InsertarSucursal();

            //Muestra Geografias
            CjCRGeografiaGeosLogic appConfigSucursales = CjCRSpringContext.getBean(CjCRGeografiaGeosLogic.class);
            appConfigSucursales.insertarGeografias();
            
            long end = System.currentTimeMillis();
            log.info(CjCRUtils.concat("----- Proceso completo en [",CjCRUtils.formatElapsedTime(begin, end), "]"));
            log.info("Termino Proceso - Geografias");
        } catch (Exception ex) {
            log.error(String.format("Error en aplicaci\u00F3n - ", ex.getMessage()), ex);
            
        }
    }
}
