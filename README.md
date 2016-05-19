# sqlpusher

	Csv reader --> pusher to SQL server:
	
	Usage of ./sqlpusher:
  -I CVS
    	CVS file path/name (default "eventsLog_05_19_2016_09_52_19.csv")
  -P password
    	password (default "psw123psw.")
  -S server_name
    	server_name[\instance_name] (default "clickstream.c8rzulntog2k.us-west-2.rds.amazonaws.com")
  -U login
    	login_id (default "adsdbroot")
  -d db_name
    	db_name (default "Clickstream")
  -m How many
    	How many to insert at once (default 100)
