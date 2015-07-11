package com.baz.scc.geografia.logic;

import com.baz.scc.commons.model.CjCRGeoCanal;
import com.baz.scc.commons.model.CjCRGeoElement;
import com.baz.scc.commons.model.CjCRGeoPais;
import com.baz.scc.commons.model.CjCRGeoSucursal;
import com.baz.scc.commons.model.CjCRGeografia;
import com.baz.scc.commons.util.CjCRUtils;
import com.baz.scc.geografia.dao.CjCRGeografiaGeosDao;
import com.baz.scc.geografia.model.CjCRGeografiaNivel;
import com.baz.scc.geografia.model.CjCRGeografiaSucursalGeo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
/**
 * DAO .
 * <br><br>Copyright 2013 Banco Azteca. Todos los derechos reservados.
 * 
 * @author B938201 Norberto C.F. 
 */

@Repository
public class CjCRGeografiaGeosLogic {

    @Autowired
    private CjCRGeografiaGeosDao geo;
    private static final String USUARIO = "PRGEO";
    private static final Logger log = Logger.getLogger(CjCRGeografiaGeosLogic.class);
    @Autowired
    private CjCRGeografiaSucursalLogic sucLogic;

    private List<GeosGeo> obtenerGeografias() {
        log.info("Comienzo de Geografias");
        geo.buscarGeografias();
        List<CjCRGeografiaSucursalGeo> listaGeoEvaluar = geo.getListaGeoEvaluar();
        List<CjCRGeografiaSucursalGeo> listaGeoUnNivel = geo.getListaGeoNivelUnico();
        List<CjCRGeografiaSucursalGeo> listaGeoCompleta = new ArrayList<CjCRGeografiaSucursalGeo>();
        List<CjCRGeografiaSucursalGeo> listaGeoIncompletaTmp = new ArrayList<CjCRGeografiaSucursalGeo>();
        List<CjCRGeografiaSucursalGeo> listaGeoIncompleta = new ArrayList<CjCRGeografiaSucursalGeo>();
        List<GeosGeo> listaTerminadaGeos = new ArrayList<GeosGeo>();
        
        //SEPARAR GEOGRAFIAS COMPLETAS E INCOMPLETAS
        for (CjCRGeografiaSucursalGeo geoAct : listaGeoEvaluar) {
            //Verificar las Geografías (incompletas-completas)
            if (geoAct.getDistritDescripion() == null
                    || geoAct.getJefDescripion() == null
                    || geoAct.getPlazaDescripion() == null) {
                listaGeoIncompletaTmp.add(geoAct);
            } else {
                listaGeoCompleta.add(geoAct);
            }
        }

        //UNIR GEOGRAFIAS INCOMPLETAS CON GEOGRAFIAS DE UN NIVEL
        listaGeoIncompleta.addAll(listaGeoIncompletaTmp);
        listaGeoIncompleta.addAll(listaGeoUnNivel);

        //ELIMINAR SUCURSALES REPETIDAS
        Map<String, List<CjCRGeografiaSucursalGeo>> mapGeosSinRep = new HashMap<String, List<CjCRGeografiaSucursalGeo>>();
        
        if (sucLogic.getSucRepetidas()!=null){
            mapGeosSinRep.putAll(eliminarSucRep(listaGeoIncompleta, listaGeoCompleta));
        }

        //ARMAR GEOGRAFIAS 
        for (Entry<String, List<CjCRGeografiaSucursalGeo>> e : mapGeosSinRep.entrySet()) {
            if(e.getKey().equals("completas")){
                listaTerminadaGeos.addAll(armarGeosGeoCompletas(e.getValue()));
            }else if (e.getKey().equals("incompletas")){
                listaTerminadaGeos.addAll(armarGeosGeoIncompletas(e.getValue()));
            }
        }
        return listaTerminadaGeos;
    }

    private Map<String, List<CjCRGeografiaSucursalGeo>> eliminarSucRep(
            List<CjCRGeografiaSucursalGeo> lsGeoIncompleta,
            List<CjCRGeografiaSucursalGeo> lsGeoCompleta){
        
        log.info("Eliminación de sucursales duplicadas");
        
        Map<String, List<CjCRGeografiaSucursalGeo>> mapSuc = new HashMap<String, List<CjCRGeografiaSucursalGeo>>();
        
        mapSuc.put("incompletas", lsGeoIncompleta);
        mapSuc.put("completas", lsGeoCompleta);
        
        return eliminarSucRepAux(mapSuc);
    }

    private Map<String, List<CjCRGeografiaSucursalGeo>> eliminarSucRepAux(
            Map<String, List<CjCRGeografiaSucursalGeo>> mapSuc) {

        //Map<Integer, CjCRGeografiaSucursalGeo> mapAuxGeos = new HashMap<Integer, CjCRGeografiaSucursalGeo>();
        Map<String, List<CjCRGeografiaSucursalGeo>> mapGeosSinDupl = new HashMap<String, List<CjCRGeografiaSucursalGeo>>();
        List<CjCRGeografiaSucursalGeo> sucRepetidas = new ArrayList<CjCRGeografiaSucursalGeo>();

        for (Entry<String, List<CjCRGeografiaSucursalGeo>> en : mapSuc.entrySet()) {

            List<CjCRGeografiaSucursalGeo> lsGeos = new ArrayList<CjCRGeografiaSucursalGeo>();
            lsGeos.addAll(en.getValue());

            for (CjCRGeografiaSucursalGeo geoAux : en.getValue()) {
                Integer idSucursal = geoAux.getGeoSucursal();
                
                for (CjCRGeoSucursal sucAct : sucLogic.getSucRepetidas()) {
                    if (idSucursal.intValue() == sucAct.getIdSucursal() && geoAux.getGeoCanal() == sucAct.getCanal().getIdCanal()
                            && geoAux.getGeoPais() == sucAct.getPais().getIdPais()) {
                        lsGeos.remove(geoAux);//eliminar el sucursal de la lista
                        sucRepetidas.add(geoAux); //Lista de sucursales duplicadas
                        log.warn("La sucursal:" + idSucursal + " pais:" + geoAux.getGeoPais()
                                + " canal:" + geoAux.getGeoCanal() + " esta duplicada");
                        break;
                    }
                }
                mapGeosSinDupl.put(en.getKey(), lsGeos);
            }
        }
        log.warn("Total sucursales duplicadas: " + sucRepetidas.size());
        return mapGeosSinDupl;
    }
        

    public List<GeosGeo> armarGeosGeoIncompletas(List<CjCRGeografiaSucursalGeo> listaGeoIncompleta) {
        List<GeosGeo> listaGeosGeoIncompleta = new ArrayList<GeosGeo>();
        log.info("Comienzo de Armado de Geografias Incompletas:");

        for (CjCRGeografiaSucursalGeo geoIncompleta : listaGeoIncompleta) {
            //ARMAR GEO
            geoIncompleta.getGeoPais();
            geoIncompleta.getGeoSucursal();
            geoIncompleta.getGeoRegion();
            geoIncompleta.getGeoCanal();
            //ARMAR SU UNICO NIVEL - PLAZA
            CjCRGeografiaNivel Plaza = new CjCRGeografiaNivel();
            Plaza.setDescripion("Sin plaza");

            listaGeosGeoIncompleta.add(procesarNivelIncompleto(1, Plaza, geoIncompleta));
        }
        log.info("Geografias Incompletas: " + listaGeosGeoIncompleta.size());
        return listaGeosGeoIncompleta;
    }

    public List<GeosGeo> armarGeosGeoCompletas (List<CjCRGeografiaSucursalGeo> listaGeoCompleta){
        List<GeosGeo> listaGeosGeoCompleta = new ArrayList<GeosGeo>();
        log.info("Comienzo de Armado de Geografias Completas");
        for (CjCRGeografiaSucursalGeo geoTemp : listaGeoCompleta) {
            //Remover espacios sobrantes en las cadenas
            CjCRGeografiaSucursalGeo geoCompleta = quitarEspacios(geoTemp);
            //ARMAR GEO
            geoCompleta.getGeoPais();
            geoCompleta.getGeoSucursal();
            geoCompleta.getGeoRegion();
            geoCompleta.getGeoCanal();
            //DISRITO
            CjCRGeografiaNivel Distrito = new CjCRGeografiaNivel();
            Distrito.setPais(geoCompleta.getGeoPais());
            Distrito.setIdentificador(geoCompleta.getDistritIdentificador());
            Distrito.setDescripion(geoCompleta.getDistritDescripion());
            Distrito.setIdsuperior(geoCompleta.getDistritIdsuperior());
            //JEFATURA
            CjCRGeografiaNivel Jefatura = new CjCRGeografiaNivel();
            Jefatura.setPais(geoCompleta.getGeoPais());
            Jefatura.setIdentificador(geoCompleta.getJefIdentificador());
            Jefatura.setDescripion(geoCompleta.getJefDescripion());
            Jefatura.setIdsuperior(geoCompleta.getJefIdsuperior());
            //PLAZA
            CjCRGeografiaNivel Plaza = new CjCRGeografiaNivel();
            Plaza.setPais(geoCompleta.getGeoPais());
            Plaza.setIdentificador(geoCompleta.getPlazaIdentificador());
            Plaza.setDescripion(geoCompleta.getPlazaDescripion());
            Plaza.setIdsuperior(geoCompleta.getPlazaIdsuperior());

            listaGeosGeoCompleta.add(procesarNivelCompleto(1, Plaza, geoCompleta));
            listaGeosGeoCompleta.add(procesarNivelCompleto(2, Jefatura, geoCompleta));
            listaGeosGeoCompleta.add(procesarNivelCompleto(3, Distrito, geoCompleta));
        }
        log.info("Geografias Completas: " + listaGeosGeoCompleta.size());
        return listaGeosGeoCompleta;
    }
    
    private GeosGeo procesarNivelCompleto(int nivel, CjCRGeografiaNivel nivelActual, CjCRGeografiaSucursalGeo geoAct) {
        CjCRGeoElement registroGeo = new CjCRGeoElement();
        CjCRGeografia geografia = new CjCRGeografia();
        CjCRGeoSucursal sucursal = new CjCRGeoSucursal();

        //IDGEOGRAFIA
        geografia.setIdGeografia(1);
        //PAIS            
        CjCRGeoPais Pais = new CjCRGeoPais();
        Pais.setIdPais(geoAct.getGeoPais());
        sucursal.setPais(Pais);
        //CANAL
        CjCRGeoCanal Canal = new CjCRGeoCanal();
        Canal.setIdCanal(geoAct.getGeoCanal());
        sucursal.setCanal(Canal);
        //IDSUCURSAL
        sucursal.setIdSucursal(geoAct.getGeoSucursal());
        //NIVEL
        registroGeo.setIdNivel(nivel);
        registroGeo.setNombre(nivelActual.getDescripion());
        registroGeo.setStatus(1);
        registroGeo.setValor(nivelActual.getIdentificador());
        GeosGeo geos = new GeosGeo(geografia, registroGeo, sucursal);

        return geos;
    }
    
    private GeosGeo procesarNivelIncompleto(int nivel, CjCRGeografiaNivel nivelActual, CjCRGeografiaSucursalGeo geoAct) {
        CjCRGeoElement registroGeo = new CjCRGeoElement();
        CjCRGeografia geografia = new CjCRGeografia();
        CjCRGeoSucursal sucursal = new CjCRGeoSucursal();

        //IDGEOGRAFIA
        geografia.setIdGeografia(1);
        //PAIS            
        CjCRGeoPais pais = new CjCRGeoPais();
        pais.setIdPais(geoAct.getGeoPais());
        sucursal.setPais(pais);
        //CANAL
        CjCRGeoCanal canal = new CjCRGeoCanal();
        canal.setIdCanal(geoAct.getGeoCanal());
        sucursal.setCanal(canal);
        //IDSUCURSAL
        sucursal.setIdSucursal(geoAct.getGeoSucursal());
        //NIVEL
        registroGeo.setIdNivel(nivel);
        registroGeo.setNombre(nivelActual.getDescripion());
        registroGeo.setStatus(1);
        registroGeo.setValor(0);
        GeosGeo geos = new GeosGeo(geografia, registroGeo, sucursal);
        
        return geos;
    }
    
    public CjCRGeografiaSucursalGeo quitarEspacios(CjCRGeografiaSucursalGeo geoT) {
        //Eliminar los espacios que tiene el campo descripcion
        String[] temp = new String[3];
        temp[0] = geoT.getDistritDescripion().trim();
        geoT.setDistritDescripion(temp[0]);
        temp[1] = geoT.getJefDescripion().trim();
        geoT.setJefDescripion(temp[1]);
        temp[2] = geoT.getPlazaDescripion().trim();
        geoT.setPlazaDescripion(temp[2]);
        
        return geoT;
    }
    
    public class GeosGeo {
        //CLASE INTERMEDIA PARA CONTENER CjCRGeografia - CjCRGeoElement -CjCRGeoSucursal
        CjCRGeografia geo = new CjCRGeografia();
        CjCRGeoElement geoElement = new CjCRGeoElement();
        CjCRGeoSucursal geoSucursal = new CjCRGeoSucursal();

        public GeosGeo(CjCRGeografia geoR, CjCRGeoElement geoEelementR, CjCRGeoSucursal geoSucursalR) {
            this.geo = geoR;
            this.geoElement = geoEelementR;
            this.geoSucursal = geoSucursalR;
        }
        
        public CjCRGeografia getGeo() {
            return geo;
        }

        public void setGeo(CjCRGeografia geo) {
            this.geo = geo;
        }

        public CjCRGeoElement getGeoElement() {
            return geoElement;
        }

        public void setGeoElement(CjCRGeoElement geoElement) {
            this.geoElement = geoElement;
        }

        public CjCRGeoSucursal getGeoSucursal() {
            return geoSucursal;
        }

        public void setGeoSucursal(CjCRGeoSucursal geoSucursal) {
            this.geoSucursal = geoSucursal;
        }
    }
    
    public void insertarGeografias() {
        try {
            long begin = System.currentTimeMillis();
            List<GeosGeo> geografias = obtenerGeografias();
            log.info("Geografias totales(Completas-Incompletas):" + geografias.size());
            //Inserción de Geografias en DB Oracle
            geo.InsertarGeografiasBD(geografias, USUARIO);
            long end = System.currentTimeMillis();
            log.info(CjCRUtils.concat("----- Geografias completas [",CjCRUtils.formatElapsedTime(begin, end), "]"));
        } catch (Exception ex) {
            log.error("Error DB (Geografias)" + ex);
        }
    }
    
}
