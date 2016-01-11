#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import urllib2
from bs4 import BeautifulSoup
import feedparser
import sys
import time
import MySQLdb
import re

from time import mktime
from datetime import datetime


#Inserta nombres de archivos con los feeds capturados - funcion initlista
import listafeeds
#considerar el nro de feed (feedkey) a partir del cual se va a insertar


########################################### def init_archivo
def init_archivo( tipo):
	archivo_salida= "ins//" + tipo +  time.strftime("%Y%m%d%H%M")  + ".sql"
	try:
	
		#modo  w de escritura b binario 
		output_file=open (archivo_salida, "wb")
		print ("Para tipo " + tipo + "Archivo abiertoo: %s" % (archivo_salida))
	except:
		print "Algo paso al abrir el archivo"
		output_file = -1
	return output_file
		
############################################ insert_feed
def insert_feed(db, cursor, d ,fno, output_file):

	ftitle = d.feed.title.encode('utf8') 
	flink = d.feed.link
	ptopic = flink.find('topic=')
	if ptopic == -1:
		ftopic ='all'
	else:
		ftopic  = flink[ptopic+len('topic=')]
	
	fpublished = d.feed.published
	fpublished_parsed = d.feed.published_parsed
	fpublished_date= datetime.fromtimestamp(mktime(fpublished_parsed))
	
	sql = """INSERT INTO bignews.feeds( title, link, published, published_date,topic)
			 VALUES ('%s', '%s', '%s', '%s', '%s' )"""
	values = (  ftitle, flink, fpublished, fpublished_date,ftopic)
	
	sql2 ="""INSERT INTO bignews.feeds( title, link, published, published_date,topic)
			 VALUES ('"""
			
	print sql2 +'\n'
	output_file.write(sql2)
	output_file.write(ftitle)
	output_file.write("' , '")
	output_file.write(flink)
	output_file.write("' , '")
	output_file.write(fpublished)
	output_file.write("' , '")
	#formato 2015-12-27 13:00:43
	output_file.write(fpublished_date.strftime('%Y-%m-%d %H:%M:%S'))
	output_file.write("' , '")
	output_file.write(ftopic)
	output_file.write("' ")
	output_file.write(""" );""")
	output_file.write("\n")

	#try:
	   # Execute the SQL command
	   #print "antes de execute feed"
	   #cursor.execute(sql % values)
	   #print "execute "
	   # Commit your changes in the database
	   #db.commit()
	   #print "commiteo"
	#except:
	   # Rollback in case there is any error
	   #db.rollback()
	   #print " exception feed !!!!"
	   
############################################ def insert_links	   
def insert_links(db, cursor,links,fno, Nentries):
	
	print " VAMOS POR LOS links \n"
	print links 
	for l in links:
		#print "\n links.rel"
		lrel= l.rel
		#print lrel
		#print '\n links.type'
		ltype =l.type
		#print ltype
		#print '\n links.href'
		lhref = l.href
		#print lhref
		#print '\n links.title'
		if 'title' in l:
			ltitle = l.title.encode('utf8')
		else:
			ltitle ='No title'
			
		#print ltitle
		#print '\n se viene el sql \n\n' 
		lfeedkey = fno
		lentry = Nentries
		
		sql = """INSERT INTO bignews.newslink( rel , type, href,title,feedkey, entry)
					VALUES ( '%s','%s','%s', '%s', %d, %d )"""
		values = (lrel, ltype, lhref, ltitle, lfeedkey, lentry  )
		#sql = """INSERT INTO bignews.newslink( feedkey, entry)
		#			VALUES (  %d, %d )"""
		#values = ( lfeedkey, lentry  )
		
		#print sql +'\n'
		#print values 
		#print sql % values
		
		try:
			# Execute the SQL command
			print "antes de execute de links"
			cursor.execute(sql % values)
			print "execute "
			# Commit your changes in the database
			db.commit()
			print " +++++++++ links commiteo +++++++++++++++++++++++++++"
		

		except:
		   # Rollback in case there is any error
		   db.rollback()
		   print " exception en newslinks "   
		   
############################################ def_news		   
def insert_news(db, cursor, d,fno, output_file ):
	nfeedkey = fno
	Nentries=0
	
	for post in d.entries: 
		Nentries =Nentries + 1
		print "en la entries "
		print Nentries
		print '\n'
		
		#print "title \n"
		#print post.title.encode('utf8')		
		#print post.title
		#print  '\n' 
		tntitle = post.title.encode('utf8')
		ntitle=tntitle.replace("'",'"'	)
		####print "resumen \n"
		####resumen =  re.sub('(()?)|(()?)|(()?)', '',post.summary_detail.value.encode('utf8'))	
		####print resumen
		#print "\n---------------------------------\n"
		#print "summary_detail type \n"
		#print post.summary_detail.type + '\n'
		#no lo tiene print "content \n"
		#print post.content + '\n'
		#da errror por el tipo de caracteres
		#print "summary detail  pero solo value encode \n"
		#print post.summary_detail
		tarticle = post.summary_detail.value.encode('utf8')
		article=tarticle.replace("'",'"'	)
		#print "id \n"
		#print post.id + '\n'
		nguid = post.id   	
		
		#print "link \n"
		#print post.link +'\n'
		tnlink = post.link
		nlink = tnlink.replace("'","\""	)	
			
		#print "published \n"
		#print post.published + '\n'
		npublished = post.published
		#print "published parsed \n"
		#print post.published_parsed 
		npublished_parsed = post.published_parsed
		npublished_date= datetime.fromtimestamp(mktime(npublished_parsed))
		#print npublished_date
		sql = """INSERT INTO bignews.news( title, link, published, published_date,guid,feedkey,entry) 
			VALUES ('%s', '%s', '%s', '%s', '%s', %d, %d )"""
		values = (  ntitle, nlink, npublished, npublished_date, nguid, nfeedkey,Nentries)
		#sql2 ="""INSERT INTO bignews.news( title, link, published, published_date,guid,feedkey,entry) 
		#	VALUES ('"""
		#VA con summary_detail	
		sql2 ="""INSERT INTO bignews.news( title, link, published, published_date,guid,summary, feedkey,entry) 
			VALUES ('"""
			
		print sql2 +'\n'
		output_file.write(sql2)
		output_file.write(ntitle)
		output_file.write("' , '")
		output_file.write(nlink)
		output_file.write("' , '")
		output_file.write(npublished)
		output_file.write("' , '")
		#2015-12-27 13:00:43
		output_file.write(npublished_date.strftime('%Y-%m-%d %H:%M:%S'))
		output_file.write("' , '")
		output_file.write(nguid)
		output_file.write("' , '")
		output_file.write(article)
		output_file.write("' , ")
		output_file.write(str(nfeedkey))
		output_file.write(" , ")
		output_file.write(str(Nentries))
		output_file.write(""" );""")
		output_file.write("\n")
		
		
		try:
			# Execute the SQL command
			#print "antes de execute"
			#cursor.execute(sql % values)
			#print "execute "
			# Commit your changes in the database
			#db.commit()
			#print " ******+ commiteo **************************"
			insert_links(db, cursor, post.links, fno,Nentries)
		except:
		   # Rollback in case there is any error
		   #db.rollback()
		   print " exception en news "
	
		
	   

#inicializa db
db_host = 'localhost'
usuario = 'root'
clave = '1234'
base_de_datos = 'bignews'
db = MySQLdb.connect(host=db_host, user=usuario, passwd=clave,
db=base_de_datos, charset='UTF8')
cursorfeed = db.cursor()
# CAMBIAR segun cantidad de feeds ya insertados
feedno = 0
lista_ar=init_lista()


print 	'cantidad de archivos ' + str(len(lista_ar ))
#archivo news
output_news = init_archivo("insertnews")
output_feed = init_archivo("insertfeed")
for archtxt in lista_ar:
	feedno += 1
	encabezado =r'C:\Users\Toshiba\Documents\csf\big data\tp final\news'
		varch = encabezado + '//' + archtxt
	d = feedparser.parse(varch)
	insert_feed(db,cursorfeed ,d, feedno, output_feed)
	insert_news(db,cursorfeed,d,feedno, output_news)
	
# disconnect from server
db.close()	
output_news.close()		
output_feed.close()	

exit()
