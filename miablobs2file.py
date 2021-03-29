import base64

import urllib

import mysql.connector
import requests


global secret,service_url
secret = "hSTD479NbVNkn9A3TB9e4Yqo5B"
service_url = "http://10.20.91.3:9991/put.php"



global service_ctr
service_ctr = 0;

configdb= {
    'user': 'kth',
    'password': 'Mia*2013',
    'host': '10.20.91.3',
    'database': 'mia_f',
    'raise_on_warnings': False,
}

#für lokales testen
configdb___ = {
    'user': 'root',
    'password': 'MaeWest7#',
    'host': '127.0.0.1',
    'database': 'mia_f',
    'raise_on_warnings': False,
}

def check_file_via_service(context,filename):



    ddata = {"secret": secret,
             "context": context,
             "filename": filename,

             }

    data = urllib.parse.urlencode(ddata)
    dheader = {"Content-Type" : "application/x-www-form-urlencoded"}
    resp = requests.post(service_url,headers=dheader,data=data)

    if'"status":"success"' not in resp.text:
        print(resp.text)
        quit()

    return


def store_blob_via_service(context,filename,blob):

    blob64_tmp = base64.b64encode(blob)
    blob64 = blob64_tmp.decode("utf-8")

    ddata = {"secret": secret,
             "context": context,
             "filename": filename,
             "coding": "base64",
             "override": "0",
             "content": blob64}

    data = urllib.parse.urlencode(ddata)
    dheader = {"Content-Type" : "application/x-www-form-urlencoded"}
    resp = requests.post(service_url,headers=dheader,data=data)

    if'"status":"success"' not in resp.text:
        print(resp.text)
        quit()

    global service_ctr
    service_ctr += 1


    return


def check_new_columns(tablename):


    try:
        cmd = "select uebernommen from " + tablename +" limit 1;"
        cu.execute(cmd)
        for nop in cu:
            pass
    except mysql.connector.Error as err:
        if err.errno == 1054: # no such column
            cu.execute("alter table "+tablename+" add column uebernommen int(11);")

            gconn.commit()
        else:
            print(err)


def log_errx(errtext):
    print("**** "+errtext+" ***")
    quit()


def process_list(id_liste,process_doc):

    ctr = 0
    llen = len(id_liste)

    for id in id_liste:
        ctr +=1
        print(str(ctr)+"/"+str(llen))
        process_doc(id)
    pass

def setup():
    global gconn
    gconn = mysql.connector.connect(**configdb)
    global cu
    cu = gconn.cursor()
    #check_new_columns("temailnachrichten")
    #check_new_columns("temailanhaenge")


filectr = 0


def handle_single_doc(dokument_id):

    global filectr

    cu.execute("select uuid(),dokument,created_by, dokument_ext,dokument_name from tdokumente where dokumentid ="+str(dokument_id)+";")
    res = cu.fetchall()
    for row in res:

        uuid = row[0]
        blob = row[1]
        created_by = row[2]
        dokument_ext = row[3]
        dokument_name = row[4]
        if blob == None:
            print("meta ohne dok "+created_by)
            return
        if len(blob) == 0:
            print("leerer blob")
            return

        if dokument_ext == None:
            dokument_ext  = ""

        filectr += 1

        store_blob_via_service("other",uuid+"."+dokument_ext, blob)
        cmd = "update tdokumente_meta " \
                                       " set dateiid= '"+uuid+"', "\
                                       " dokument_name= '"+dokument_name+"', "\
                                       " kommentar= '"+dokument_ext+"', "\
                                       " dokument_ext= '"+dokument_ext+"',changed_by='KNV1',changed_on=now() " \
                                        " where dokumentid="+str(dokument_id)+";"
        cu.execute(cmd)

        print(str(dokument_id)+": "+dokument_name+"."+dokument_ext+"  "+uuid+" len: "+str(len(blob)))
        gconn.commit()


def get_list(cmd):
    cu.execute(cmd)
    id_liste = []
    res = cu.fetchall()
    for row in res:
        id_liste.append(row[0])

    return id_liste

def export_dokumente():

    cmd = "select dm.dokumentid from tdokumente_meta dm "\
               "  join tdokumente d on d.dokumentid = dm.dokumentid " \
                "where dm.created_by not in ('') and (isnull(dm.dateiid) or dm.dateiid = '') order by dm.mitgliedschaftid desc;"
    id_liste = get_list(cmd)

    process_list(id_liste,handle_single_doc)


def emailanhang_metadaten_nachtragen(anhangid, dokumentid):

    cmd = "select thema,pzuh_dokumentid_alt,geloescht_am,finalisiert,herkunftid,eingangsdatum," \
          " createuser,createtime,changeuser,changetime,kategorieid from temailanhaenge_meta " \
          " where anhangid ="+str(anhangid)+";"

    cu.execute(cmd)
    res = cu.fetchall()
    for row in res:
        thema = row[0]
        pzuh_dokumentid_alt = str(row[1])
        geloescht_am = str(row[2])
        finalisiert = str(row[3])

        herkunftid = str(row[4])

        eingangsdatum = str(row[5])
        createuser = row[6]
        createtime = str(row[7])
        changeuser = row[8]
        changetime = str(row[9])
        kategorieid = row[10]

        if kategorieid == None:
            kategorieid = "None"


        cmd = "update tdokumente_meta set " \
              "thema ='"+thema+"'," \
              " pzuh_dokumentid_alt ="+pzuh_dokumentid_alt+"," \
              " geloescht_am ="+geloescht_am+"," \
              " finalisiert = "+finalisiert+"," \
              " herkunftid = "+herkunftid+"," \
              " created_by ='"+createuser+"'," \
              " created_on = '"+createtime+"'," \
              " changed_by = 'KNV5'," \
              " eingangsdatum = '"+ str(eingangsdatum)  +"'," \
              " changed_on = now()," \
              " dokumentart ='"+kategorieid+"'" \
              " where dokumentid = " +str(dokumentid)

        cmd = cmd.replace("None","NULL")
        cmd = cmd.replace("'NULL'","NULL")

        cu.execute(cmd)

        return


def handle_emailanhang(anhangid):


    cmd = "select e.nachrichtid," \
               "e.formularid, " \
               "e.indokumenteverwalten," \
               "uuid(), " \
               "e.dokument, " \
               "e.dokument_name, " \
               "e.dokument_ext, " \
               "ltpn.mitgliedschaftid, " \
               "ltpn.personid,e.createuser,e.createtime,e.changeuser,e.changetime, en.server, date(en.datum) as emaildatum " \
               " " \
               " from temailanhaenge e " \
               " join temailnachrichten en on en.nachrichtid = e.nachrichtid " \
               " left join ltemailpersonnachricht ltpn on ltpn.nachrichtid = e.nachrichtid where anhangid =" + str(anhangid) + " limit 1;"
    print(cmd)
    cu.execute(cmd)
    result = cu.fetchall()
    for row in result:
        nachrichtid = str(row[0])
        formularid = str(row[1])
        indokumenteverwalten = str(row[2])
        uuid = row[3]
        blob = row[4]
        if blob == None:
            return
        if len(blob) == 0:
            return
        dokument_name = row[5]
        dokument_ext = row[6]
        if row[7] == None:
            mitgliedschaftid = "NULL"
        else:
            mitgliedschaftid = str(row[7])
        personid = str(row[8])
        createuser = row[9]
        createtime=str(row[10])
        changeuser=row[11]
        changetime=str(row[12])
        server = row[13]
        emaildatum = str(row[14])



        store_blob_via_service("anhaenge",uuid+"."+dokument_ext, blob)


        if server == 1:
            herkunftid = "'MM'"
        else:
            herkunftid = "NULL"

        cmd = "insert into tdokumente_meta " \
              " (eingangsdatum, herkunftid,nachrichtid," \
              "formularid," \
              "dateiid," \
              "mandantid," \
              "dokument_name,dokument_ext," \
              "indokumenteanzeigen, mitgliedschaftid,personid,thema,created_by,created_on,changed_by,changed_on) values " \
              "('"+str(emaildatum)+ "'," +herkunftid+","+nachrichtid+ ", " \
                ""+formularid+", "\
                "'"+uuid+"', "\
                "1,"\
                "'"+dokument_name+"'," \
                "'"+dokument_ext+"'," \
                +indokumenteverwalten+", "\
                +mitgliedschaftid+", " \
                ""+personid+"," \
                "'"+dokument_name[0:78]+"'," \
                                  "'"+createuser+"','"+createtime+"','KNV4',now() " \
                                  ");"
        cmd = cmd.replace("'None'","NULL")
        cmd = cmd.replace("None","NULL")
        cu.execute(cmd)
        gconn.commit()
        cu.execute("select last_insert_id();")
        dokumentid = cu.fetchone()
        emailanhang_metadaten_nachtragen(anhangid, dokumentid[0])


        break
    cmd = "update temailanhaenge set dokument_name=concat('*',dokument_name) where anhangid =" + str(anhangid) + ";"
    print(cmd)
    cu.execute(cmd)

    gconn.commit()


def handle_emailnachrichten(nachrichtid):

    cmd = "select text,html " \
               "" \
               " from temailnachrichten where nachrichtid ="+str(nachrichtid)+";"

    cu.execute(cmd)
    for row in cu.fetchall():
        text = row[0]
        html = row[1]
        if text != None and text != b"":
            if len(text) > 0:
                store_blob_via_service("mails",str(nachrichtid) + ".txt", text)
        if html!= None:
            if len(html) > 0:
                store_blob_via_service("mails",str(nachrichtid) + ".html", html)

    cu.execute("update temailnachrichten set uebernommen  = 1 where nachrichtid ="+str(nachrichtid)+";")

    gconn.commit()

def export_emailanhaenge():


    cmd = "select ea.anhangid from temailanhaenge ea " \
            " join ltemailpersonnachricht lep on lep.nachrichtid = ea.nachrichtid " \
            " where left(dokument_name,1) != '*' "\
            " order by ea.nachrichtid;"

    print(cmd)


    id_liste = get_list(cmd)

    process_list(id_liste,handle_emailanhang)


def export_emailnachrichten():

    cmd = "select en.nachrichtid from temailnachrichten en "\
            " where en.uebernommen = 0;"


    id_liste = get_list(cmd)

    process_list(id_liste,handle_emailnachrichten)


setup()
cu.execute("set @IGNORE_TRIGGERS := 1;")


#bl=b"dasisteinganzgrosserblob"
#store_blob_via_service("other","abc2.txt",bl)

export_dokumente()
export_emailnachrichten()
export_emailanhaenge()

print("service_ctr= "+str(service_ctr))
cu.close()

gconn.close()

