package com.baz.scc.geografia.logic;

import com.baz.scc.geografia.dao.CjCRGeografiaSucursalDao;
import com.baz.scc.commons.model.CjCRGeoSucursal;
import com.baz.scc.commons.model.CjCROracleResponse;
import com.baz.scc.commons.util.CjCRUtils;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * DAO .
 * <br><br>Copyright 2013 Banco Azteca. Todos los derechos reservados.
 * 
 * @author Mariana
 */
@Component("applicationSucursal")
public class CjCRGeografiaSucursalLogic {

    private static final Logger log = Logger.getLogger(CjCRGeografiaSucursalLogic.class);
    private static final String USUARIO = "PRGEO";
    @Autowired
    private CjCRGeografiaSucursalDao sucursal;
    
    private List<CjCRGeoSucursal> sucRepetidas;

    public CjCRGeografiaSucursalDao getSucursal() {
        return sucursal;
    }

    public void setSucursal(CjCRGeografiaSucursalDao sucursal) {
        this.sucursal = sucursal;
    }

    public List<CjCRGeoSucursal> getSucRepetidas() {
        return sucRepetidas;
    }

    public void setSucRepetidas(List<CjCRGeoSucursal> sucRepetidas) {
        this.sucRepetidas = sucRepetidas;
    }

    public void InsertarSucursal() {
        try {
            long begin = System.currentTimeMillis();
            List<CjCRGeoSucursal> sucursales = eliminarSucRep(sucursal.getSucursal());

            log.info("Sucursales obtenidas:" + sucursales.size());
            CjCROracleResponse or = new CjCROracleResponse();
            or = sucursal.registrarSucursales(sucursales, USUARIO);
            long end = System.currentTimeMillis();
            log.info(CjCRUtils.concat("----- Canales completos [",CjCRUtils.formatElapsedTime(begin, end), "]"));
        } catch (Exception ex){
            log.error("Error DB (Sucursales)" + ex);
        }
    }
    
    private List<CjCRGeoSucursal> eliminarSucRep(List<CjCRGeoSucursal> suc) {
        List<CjCRGeoSucursal> sucSinRepetir = new ArrayList<CjCRGeoSucursal>();
        List<CjCRGeoSucursal> sucRepetidas = new ArrayList<CjCRGeoSucursal>();

        for (CjCRGeoSucursal sucAct : suc) {
            boolean repetido = false;

            if (sucSinRepetir.isEmpty()) {
                sucSinRepetir.add(sucAct);
            } else {
                for (CjCRGeoSucursal ls : sucSinRepetir) {
                    if (sucAct.getIdSucursal() == ls.getIdSucursal()) {
                        repetido = true;
                        break;
                    }
                }
                if (repetido) {
                    sucRepetidas.add(sucAct); //Lista de sucursales duplicadas
                    log.warn("La sucursal:" + sucAct.getIdSucursal() + " pais:" + sucAct.getPais().getIdPais()
                            + " canal:" + sucAct.getCanal().getIdCanal() + " esta duplicada");
                } else {
                    sucSinRepetir.add(sucAct); //Agregar Sucusal al mapa
                }
            }
        }
        log.warn("Total sucursales duplicadas: " + sucRepetidas.size());
        this.sucRepetidas = sucRepetidas;
        return sucSinRepetir;
    }
}
