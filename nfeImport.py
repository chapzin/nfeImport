#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
nfeImport
'''

import xml.dom.minidom as minidom
import sqlite3
import os
import shutil
import sys

from mysql.connector import MySQLConnection, Error
from python_mysql_dbconfig import read_db_config

def insertNFe(extracNFeDataResult, extracNFeDetailResult):
  
    query = """INSERT INTO nfe (ide_cUF, ide_cNF, ide_natOp, ide_indPag, ide_mod, ide_serie, ide_nNF, ide_dhEmi, 
    ide_tpNF, ide_idDest, ide_cMunFG, ide_tpImp, ide_tpEmis, ide_cDV, ide_tpAmb, ide_finNFe, ide_indFinal, 
    ide_indPres, ide_procEmi, ide_verProc, emit_CNPJ, emit_xNome, emit_xFant, emit_xLgr, emit_nro, emit_xBairro, 
    emit_cMun, emit_xMun, emit_UF, emit_CEP, emit_xPais, emit_fone, emit_IE, emit_IEST, emit_IM, emit_CNAE, 
    emit_CRT, dest_CNPJ, dest_xNome, dest_xLgr, dest_nro, dest_xBairro, dest_cMun, dest_xMun, dest_UF, 
    dest_CEP, dest_xPais, dest_fone, dest_indIEDest, dest_IE, total_vBC, total_vICMS, total_vICMSDeson, 
    total_vBCST, total_vST, total_vProd, total_vFrete, total_vSeg, total_vDesc, total_vII, total_vIPI, 
    total_vPIS, total_vCOFINS, total_vOutro, total_vNF, transp_modFrete, transp_CNPJ, transp_xNome, 
    transp_IE, transp_xEnder, transp_xMun, transp_UF, transp_qVol, transp_marca, transp_nVol, cobr_nFat, cobr_vOrig, cobr_vLiq) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s)"""

    try:
        db_config = read_db_config()
        conn = MySQLConnection(**db_config)

        cursor = conn.cursor()
        cursor.execute(query, extracNFeDataResult)
        conn.commit()

        if cursor.lastrowid:
            cursor = conn.cursor()
            
            for detail in extracNFeDetailResult:
                query = None
                query = """INSERT INTO nfedetalhe (nNF, det_cProd, det_cEAN, det_xProd, det_NCM, det_CEST, det_CFOP, det_uCom, det_qCom, det_vUnCom, 
                det_vProd, det_cEANTrib, det_uTrib, det_qTrib, det_vUnTrib, det_indTot, det_xPed, det_ICMS_orig, det_ICMS_CST, det_ICMS_modBC, 
                det_ICMS_vBC, det_ICMS_pICMS, det_ICMS_vICMS, det_IPI_cEnq, det_IPI_CST, det_IPI_vBC, det_IPI_pIPI, det_IPI_vIPI, det_PIS_CST, 
                det_PIS_vBC, det_PIS_pPIS, det_PIS_vPIS, det_COFINS_CST, det_COFINS_vBC, det_COFINS_pCOFINS, det_COFINS_vCOFINS, det_infAdProd) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s)"""
                
                cursor.execute(query, detail)
                conn.commit()
        else:
            print('last insert id not found')

    except Error as error:
        print(error)

    finally:
        cursor.close()
        conn.close()

"""
Dados da NFe
"""
def extracNFeData(xml):
    doc = minidom.parse(xml)
    node = doc.documentElement
    nfeData = []

    ides = doc.getElementsByTagName("ide")
    for ide in ides:
        nfeData.append(ide.getElementsByTagName("cUF")[0].childNodes[0].nodeValue)
        nfeData.append(ide.getElementsByTagName("cNF")[0].childNodes[0].nodeValue)
        nfeData.append(ide.getElementsByTagName("natOp")[0].childNodes[0].nodeValue)
        nfeData.append(ide.getElementsByTagName("indPag")[0].childNodes[0].nodeValue)
        nfeData.append(ide.getElementsByTagName("mod")[0].childNodes[0].nodeValue)
        nfeData.append(ide.getElementsByTagName("serie")[0].childNodes[0].nodeValue)
        nfeData.append(ide.getElementsByTagName("nNF")[0].childNodes[0].nodeValue)
        nfeData.append(ide.getElementsByTagName("dhEmi")[0].childNodes[0].nodeValue)
        nfeData.append(ide.getElementsByTagName("tpNF")[0].childNodes[0].nodeValue)
        nfeData.append(ide.getElementsByTagName("idDest")[0].childNodes[0].nodeValue)
        nfeData.append(ide.getElementsByTagName("cMunFG")[0].childNodes[0].nodeValue)
        nfeData.append(ide.getElementsByTagName("tpImp")[0].childNodes[0].nodeValue)
        nfeData.append(ide.getElementsByTagName("tpEmis")[0].childNodes[0].nodeValue)
        nfeData.append(ide.getElementsByTagName("cDV")[0].childNodes[0].nodeValue)
        nfeData.append(ide.getElementsByTagName("tpAmb")[0].childNodes[0].nodeValue)
        nfeData.append(ide.getElementsByTagName("finNFe")[0].childNodes[0].nodeValue)
        nfeData.append(ide.getElementsByTagName("indFinal")[0].childNodes[0].nodeValue)
        nfeData.append(ide.getElementsByTagName("indPres")[0].childNodes[0].nodeValue)
        nfeData.append(ide.getElementsByTagName("procEmi")[0].childNodes[0].nodeValue)
        nfeData.append(ide.getElementsByTagName("verProc")[0].childNodes[0].nodeValue)

    emits = doc.getElementsByTagName("emit")
    for emit in emits:
        nfeData.append(emit.getElementsByTagName("CNPJ")[0].childNodes[0].nodeValue)
        nfeData.append(emit.getElementsByTagName("xNome")[0].childNodes[0].nodeValue)
        nfeData.append(emit.getElementsByTagName("xFant")[0].childNodes[0].nodeValue)
        nfeData.append(emit.getElementsByTagName("xLgr")[0].childNodes[0].nodeValue)
        nfeData.append(emit.getElementsByTagName("nro")[0].childNodes[0].nodeValue)
        nfeData.append(emit.getElementsByTagName("xBairro")[0].childNodes[0].nodeValue)
        nfeData.append(emit.getElementsByTagName("cMun")[0].childNodes[0].nodeValue)
        nfeData.append(emit.getElementsByTagName("xMun")[0].childNodes[0].nodeValue)
        nfeData.append(emit.getElementsByTagName("UF")[0].childNodes[0].nodeValue)
        nfeData.append(emit.getElementsByTagName("CEP")[0].childNodes[0].nodeValue)
        nfeData.append(emit.getElementsByTagName("xPais")[0].childNodes[0].nodeValue)
        nfeData.append(emit.getElementsByTagName("fone")[0].childNodes[0].nodeValue)
        nfeData.append(emit.getElementsByTagName("IE")[0].childNodes[0].nodeValue)
        nfeData.append(emit.getElementsByTagName("IEST")[0].childNodes[0].nodeValue)
        nfeData.append(emit.getElementsByTagName("IM")[0].childNodes[0].nodeValue)
        nfeData.append(emit.getElementsByTagName("CNAE")[0].childNodes[0].nodeValue)
        nfeData.append(emit.getElementsByTagName("CRT")[0].childNodes[0].nodeValue)

    dests = doc.getElementsByTagName("dest")
    for dest in dests:
        nfeData.append(dest.getElementsByTagName("CNPJ")[0].childNodes[0].nodeValue)
        nfeData.append(dest.getElementsByTagName("xNome")[0].childNodes[0].nodeValue)
        nfeData.append(dest.getElementsByTagName("xLgr")[0].childNodes[0].nodeValue)
        nfeData.append(dest.getElementsByTagName("nro")[0].childNodes[0].nodeValue)
        nfeData.append(dest.getElementsByTagName("xBairro")[0].childNodes[0].nodeValue)
        nfeData.append(dest.getElementsByTagName("cMun")[0].childNodes[0].nodeValue)
        nfeData.append(dest.getElementsByTagName("xMun")[0].childNodes[0].nodeValue)
        nfeData.append(dest.getElementsByTagName("UF")[0].childNodes[0].nodeValue)
        nfeData.append(dest.getElementsByTagName("CEP")[0].childNodes[0].nodeValue)
        nfeData.append(dest.getElementsByTagName("xPais")[0].childNodes[0].nodeValue)
        nfeData.append(dest.getElementsByTagName("fone")[0].childNodes[0].nodeValue)
        nfeData.append(dest.getElementsByTagName("indIEDest")[0].childNodes[0].nodeValue)
        nfeData.append(dest.getElementsByTagName("IE")[0].childNodes[0].nodeValue)

    totals = doc.getElementsByTagName("total")
    for total in totals:
        nfeData.append(total.getElementsByTagName("vBC")[0].childNodes[0].nodeValue)
        nfeData.append(total.getElementsByTagName("vICMS")[0].childNodes[0].nodeValue)
        nfeData.append(total.getElementsByTagName("vICMSDeson")[0].childNodes[0].nodeValue)
        nfeData.append(total.getElementsByTagName("vBCST")[0].childNodes[0].nodeValue)
        nfeData.append(total.getElementsByTagName("vST")[0].childNodes[0].nodeValue)
        nfeData.append(total.getElementsByTagName("vProd")[0].childNodes[0].nodeValue)
        nfeData.append(total.getElementsByTagName("vFrete")[0].childNodes[0].nodeValue)
        nfeData.append(total.getElementsByTagName("vSeg")[0].childNodes[0].nodeValue)
        nfeData.append(total.getElementsByTagName("vDesc")[0].childNodes[0].nodeValue)
        nfeData.append(total.getElementsByTagName("vII")[0].childNodes[0].nodeValue)
        nfeData.append(total.getElementsByTagName("vIPI")[0].childNodes[0].nodeValue)
        nfeData.append(total.getElementsByTagName("vPIS")[0].childNodes[0].nodeValue)
        nfeData.append(total.getElementsByTagName("vCOFINS")[0].childNodes[0].nodeValue)
        nfeData.append(total.getElementsByTagName("vOutro")[0].childNodes[0].nodeValue)
        nfeData.append(total.getElementsByTagName("vNF")[0].childNodes[0].nodeValue)

    transps = doc.getElementsByTagName("transp")
    for transp in transps:
        nfeData.append(transp.getElementsByTagName("modFrete")[0].childNodes[0].nodeValue)
        nfeData.append(transp.getElementsByTagName("CNPJ")[0].childNodes[0].nodeValue)
        nfeData.append(transp.getElementsByTagName("xNome")[0].childNodes[0].nodeValue)
        nfeData.append(transp.getElementsByTagName("IE")[0].childNodes[0].nodeValue)
        nfeData.append(transp.getElementsByTagName("xEnder")[0].childNodes[0].nodeValue)
        nfeData.append(transp.getElementsByTagName("xMun")[0].childNodes[0].nodeValue)
        nfeData.append(transp.getElementsByTagName("UF")[0].childNodes[0].nodeValue)
        nfeData.append(transp.getElementsByTagName("qVol")[0].childNodes[0].nodeValue)
        nfeData.append(transp.getElementsByTagName("marca")[0].childNodes[0].nodeValue)
        nfeData.append(transp.getElementsByTagName("nVol")[0].childNodes[0].nodeValue)

    cobrs = doc.getElementsByTagName("cobr")
    for cobr in cobrs:
        nfeData.append(cobr.getElementsByTagName("nFat")[0].childNodes[0].nodeValue)
        nfeData.append(cobr.getElementsByTagName("vOrig")[0].childNodes[0].nodeValue)
        nfeData.append(cobr.getElementsByTagName("vLiq")[0].childNodes[0].nodeValue)

    return nfeData

"""
Produtos da NFe
"""
def extracNFeDetail(xml):
    doc = minidom.parse(xml)
    node = doc.documentElement
    nfeDetalhe = []
    nfeDetalhes = []
    nfeNumero = None

    ides = doc.getElementsByTagName("ide")
    for ide in ides:
        nfeNumero = ide.getElementsByTagName("nNF")[0].childNodes[0].nodeValue

    dets = doc.getElementsByTagName("det")
    for det in dets:

        prods = det.getElementsByTagName("prod")
        for prod in prods:
            nfeDetalhe.append(nfeNumero)
            nfeDetalhe.append(prod.getElementsByTagName("cProd")[0].childNodes[0].nodeValue)
            nfeDetalhe.append(prod.getElementsByTagName("cEAN")[0].childNodes[0].nodeValue)
            nfeDetalhe.append(prod.getElementsByTagName("xProd")[0].childNodes[0].nodeValue)
            nfeDetalhe.append(prod.getElementsByTagName("NCM")[0].childNodes[0].nodeValue)
            nfeDetalhe.append(prod.getElementsByTagName("CEST")[0].childNodes[0].nodeValue)
            nfeDetalhe.append(prod.getElementsByTagName("CFOP")[0].childNodes[0].nodeValue)
            nfeDetalhe.append(prod.getElementsByTagName("uCom")[0].childNodes[0].nodeValue)
            nfeDetalhe.append(prod.getElementsByTagName("qCom")[0].childNodes[0].nodeValue)
            nfeDetalhe.append(prod.getElementsByTagName("vUnCom")[0].childNodes[0].nodeValue)
            nfeDetalhe.append(prod.getElementsByTagName("vProd")[0].childNodes[0].nodeValue)
            nfeDetalhe.append(prod.getElementsByTagName("cEANTrib")[0].childNodes[0].nodeValue)
            nfeDetalhe.append(prod.getElementsByTagName("uTrib")[0].childNodes[0].nodeValue)
            nfeDetalhe.append(prod.getElementsByTagName("qTrib")[0].childNodes[0].nodeValue)
            nfeDetalhe.append(prod.getElementsByTagName("vUnTrib")[0].childNodes[0].nodeValue)
            nfeDetalhe.append(prod.getElementsByTagName("indTot")[0].childNodes[0].nodeValue)
            nfeDetalhe.append(prod.getElementsByTagName("xPed")[0].childNodes[0].nodeValue)

        impostos = det.getElementsByTagName("imposto")
        for imposto in impostos:
            icmss = det.getElementsByTagName("ICMS00")
            for icms in icmss:
                nfeDetalhe.append(icms.getElementsByTagName("orig")[0].childNodes[0].nodeValue)
                nfeDetalhe.append(icms.getElementsByTagName("CST")[0].childNodes[0].nodeValue)
                nfeDetalhe.append(icms.getElementsByTagName("modBC")[0].childNodes[0].nodeValue)
                nfeDetalhe.append(icms.getElementsByTagName("vBC")[0].childNodes[0].nodeValue)
                nfeDetalhe.append(icms.getElementsByTagName("pICMS")[0].childNodes[0].nodeValue)
                nfeDetalhe.append(icms.getElementsByTagName("vICMS")[0].childNodes[0].nodeValue)

        impostos = det.getElementsByTagName("imposto")
        for imposto in impostos:
            ipis = det.getElementsByTagName("IPI")
            for ipi in ipis:
                nfeDetalhe.append(ipi.getElementsByTagName("cEnq")[0].childNodes[0].nodeValue)
                nfeDetalhe.append(ipi.getElementsByTagName("CST")[0].childNodes[0].nodeValue)
                nfeDetalhe.append(ipi.getElementsByTagName("vBC")[0].childNodes[0].nodeValue)
                nfeDetalhe.append(ipi.getElementsByTagName("pIPI")[0].childNodes[0].nodeValue)
                nfeDetalhe.append(ipi.getElementsByTagName("vIPI")[0].childNodes[0].nodeValue)

        impostos = det.getElementsByTagName("imposto")
        for imposto in impostos:
            piss = det.getElementsByTagName("PISAliq")
            for pis in piss:
                nfeDetalhe.append(pis.getElementsByTagName("CST")[0].childNodes[0].nodeValue)
                nfeDetalhe.append(pis.getElementsByTagName("vBC")[0].childNodes[0].nodeValue)
                nfeDetalhe.append(pis.getElementsByTagName("pPIS")[0].childNodes[0].nodeValue)
                nfeDetalhe.append(pis.getElementsByTagName("vPIS")[0].childNodes[0].nodeValue)

        impostos = det.getElementsByTagName("imposto")
        for imposto in impostos:
            cofinss = det.getElementsByTagName("COFINSAliq")
            for cofins in cofinss:
                nfeDetalhe.append(cofins.getElementsByTagName("CST")[0].childNodes[0].nodeValue)
                nfeDetalhe.append(cofins.getElementsByTagName("vBC")[0].childNodes[0].nodeValue)
                nfeDetalhe.append(cofins.getElementsByTagName("pCOFINS")[0].childNodes[0].nodeValue)
                nfeDetalhe.append(cofins.getElementsByTagName("vCOFINS")[0].childNodes[0].nodeValue)

        nfeDetalhe.append(det.getElementsByTagName("infAdProd")[0].childNodes[0].nodeValue)

        nfeDetalhes.append(nfeDetalhe)
        nfeDetalhe = []

    return nfeDetalhes

if __name__ == "__main__":

    if len(sys.argv) - 1 == 0:
        print("Path nao informado.")
        sys.exit(0)

    path = sys.argv[1]
    
    xml_files = [x for x in os.listdir(path) if (x.startswith("SY3_XLOGD_")) and (x.endswith(".xml"))]
    
    for xml in xml_files:
        
        documentXML = path+xml
        
        print("Processando: " , documentXML)
        
        extracNFeDataResult = extracNFeData(documentXML)
        extracNFeDetailResult = extracNFeDetail(documentXML)

        insertNFe(extracNFeDataResult, extracNFeDetailResult)

        src = documentXML
        dest = path+'done/'+xml
        
        # move file after processed
        shutil.move(src, dest)
