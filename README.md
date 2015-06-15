# ccdb-worker
Worker client for the Computational Chemistry Database.

# setup

create the config file „config.json“ in the same directory following this scheme:

{
	"database": {
		"host": "",
		"port": "",
		"database": "",
		"user": "",
		"password": ""
	},
	"jobs": {
		"directory": ""
	},
	"programs": {
		"orca": {
			"path": "",
			"version": ""
		}
	}
}
