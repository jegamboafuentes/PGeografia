package com.baz.scc.geografia.model;


public class CjCRGeografiaNivel {
    private int pais;
    private int identificador;
    private String descripion;
    private int idsuperior;

    public int getIdsuperior() {
        return idsuperior;
    }

    public void setIdsuperior(int idsuperior) {
        this.idsuperior = idsuperior;
    }

    public int getPais() {
        return pais;
    }

    public void setPais(int pais) {
        this.pais = pais;
    }

    public int getIdentificador() {
        return identificador;
    }

    public void setIdentificador(int identificador) {
        this.identificador = identificador;
    }

    public String getDescripion() {
        return descripion;
    }

    public void setDescripion(String descripion) {
        this.descripion = descripion;
    }
    
}
